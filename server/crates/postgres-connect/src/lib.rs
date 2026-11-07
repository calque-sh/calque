//! Opens a PostgreSQL connection, in plaintext or over TLS that checks the
//! server's certificate chain and name.
//!
//! The driver's own `sslmode=require` encrypts without checking who answered,
//! so this crate accepts libpq's `sslmode=verify-full` and `sslrootcert`
//! instead, and verifies the server against the certificates in that file.
//! The same connection string therefore means the same thing to libpq, which
//! carries replication, and to the Rust driver, which carries everything else.
//!
//! [`DropOffRuntime`] holds a blocking connection that may be dropped on a
//! thread driving a tokio runtime.

use std::{
    borrow::Cow,
    path::Path,
    sync::{Arc, OnceLock},
};

use percent_encoding::percent_decode_str;
use rustls::{
    pki_types::{pem::PemObject, CertificateDer},
    ClientConfig, RootCertStore,
};
use thiserror::Error;
use tokio_postgres::{config::SslMode, tls::MakeTlsConnect, Socket};
use tokio_postgres_rustls::MakeRustlsConnect;

mod close;

pub use close::DropOffRuntime;

/// The connection a [`Endpoint::connect_async`] caller drives to completion.
pub type Connection =
    tokio_postgres::Connection<Socket, <MakeRustlsConnect as MakeTlsConnect<Socket>>::Stream>;

/// A parsed connection string and the TLS connector it calls for.
#[derive(Clone)]
pub struct Endpoint {
    config: tokio_postgres::Config,
    tls: MakeRustlsConnect,
}

impl Endpoint {
    /// `sslmode=verify-full` with `sslrootcert` naming a PEM file opens TLS and
    /// verifies the server against the certificates in it. `sslmode=disable`,
    /// or no `sslmode`, opens plaintext. Every other mode is refused, because
    /// none of them checks the server's name. Only a URL's TLS parameters are
    /// read, so a libpq keyword string never opens TLS.
    pub fn parse(connection: &str) -> Result<Self, EndpointError> {
        let (connection, tls) = split_tls(connection)?;
        let mut config: tokio_postgres::Config =
            connection.parse().map_err(EndpointError::Config)?;
        let tls = match tls {
            Tls::Verified(roots) => {
                config.ssl_mode(SslMode::Require);
                MakeRustlsConnect::new(client_config(roots))
            }
            Tls::Absent => {
                match config.get_ssl_mode() {
                    SslMode::Disable => {}
                    // The driver's default. A connection without a TLS
                    // connector never attempted TLS, so this keeps plaintext.
                    SslMode::Prefer => {
                        config.ssl_mode(SslMode::Disable);
                    }
                    _ => return Err(EndpointError::UnverifiedTls),
                }
                plaintext().clone()
            }
        };
        Ok(Self { config, tls })
    }

    pub fn config(&self) -> &tokio_postgres::Config {
        &self.config
    }

    pub fn config_mut(&mut self) -> &mut tokio_postgres::Config {
        &mut self.config
    }

    pub fn uses_tls(&self) -> bool {
        self.config.get_ssl_mode() == SslMode::Require
    }

    /// A blocking client. Call it outside an async runtime, as
    /// `postgres::Client` requires.
    pub fn connect(&self) -> Result<postgres::Client, postgres::Error> {
        postgres::Config::from(self.config.clone()).connect(self.tls.clone())
    }

    /// An async client and the connection the caller must spawn.
    pub async fn connect_async(
        &self,
    ) -> Result<(tokio_postgres::Client, Connection), tokio_postgres::Error> {
        self.config.connect(self.tls.clone()).await
    }
}

impl std::str::FromStr for Endpoint {
    type Err = EndpointError;

    fn from_str(connection: &str) -> Result<Self, Self::Err> {
        Self::parse(connection)
    }
}

impl std::fmt::Debug for Endpoint {
    /// The configuration holds the password, so only the transport is shown.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Endpoint")
            .field("tls", &self.uses_tls())
            .finish_non_exhaustive()
    }
}

/// Every refusal names the rule, never the connection string, which may
/// hold a password.
#[derive(Debug, Error)]
pub enum EndpointError {
    #[error("PostgreSQL connection string does not parse")]
    Config(#[source] tokio_postgres::Error),
    #[error("PostgreSQL connection string query is not percent-encoded UTF-8")]
    Malformed,
    #[error("sslmode=verify-full needs sslrootcert, the absolute path of a PEM file of trusted certificates")]
    RootCertificateMissing,
    #[error("sslrootcert is read only with sslmode=verify-full")]
    RootCertificateWithoutVerification,
    #[error("sslrootcert must be an absolute path")]
    RootCertificateRelative,
    #[error("sslrootcert {0} could not be read as PEM")]
    RootCertificateUnreadable(String),
    #[error("sslrootcert {0} holds no certificate this TLS library can trust")]
    RootCertificateEmpty(String),
    #[error(
        "sslmode must be verify-full or disable; the other modes do not check the server's name"
    )]
    UnverifiedTls,
    #[error("sslmode and sslrootcert may each appear once")]
    RepeatedTlsParameter,
}

enum Tls {
    Absent,
    Verified(RootCertStore),
}

/// Rewrites libpq's TLS parameters into what the Rust driver parses. Every
/// other byte of the string is kept, so its percent-encoding reaches the
/// driver exactly as written.
fn split_tls(connection: &str) -> Result<(Cow<'_, str>, Tls), EndpointError> {
    let is_url = connection.starts_with("postgres://") || connection.starts_with("postgresql://");
    let Some((base, query)) = connection.split_once('?').filter(|_| is_url) else {
        return Ok((Cow::Borrowed(connection), Tls::Absent));
    };
    let mut kept = Vec::new();
    let mut verify_full = false;
    let mut sslmode_seen = false;
    let mut root_certificate = None;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match decode(key)?.as_ref() {
            "sslrootcert" => {
                if root_certificate.is_some() {
                    return Err(EndpointError::RepeatedTlsParameter);
                }
                root_certificate = Some(decode(value)?.into_owned());
            }
            "sslmode" => {
                if sslmode_seen {
                    return Err(EndpointError::RepeatedTlsParameter);
                }
                sslmode_seen = true;
                match decode(value)?.as_ref() {
                    "verify-full" => {
                        verify_full = true;
                        kept.push("sslmode=require");
                    }
                    "disable" => kept.push(pair),
                    // `prefer` would fall back to plaintext, and the rest
                    // do not check the server's name.
                    _ => return Err(EndpointError::UnverifiedTls),
                }
            }
            _ => kept.push(pair),
        }
    }
    let tls = match (verify_full, root_certificate) {
        (true, Some(path)) => Tls::Verified(trusted_roots(&path)?),
        (true, None) => return Err(EndpointError::RootCertificateMissing),
        (false, Some(_)) => return Err(EndpointError::RootCertificateWithoutVerification),
        (false, None) => Tls::Absent,
    };
    let connection = if kept.is_empty() {
        base.to_owned()
    } else {
        format!("{base}?{}", kept.join("&"))
    };
    Ok((Cow::Owned(connection), tls))
}

fn decode(value: &str) -> Result<Cow<'_, str>, EndpointError> {
    percent_decode_str(value)
        .decode_utf8()
        .map_err(|_| EndpointError::Malformed)
}

/// Reads the file on every call, so a rotated bundle takes effect on the next
/// connection.
fn trusted_roots(path: &str) -> Result<RootCertStore, EndpointError> {
    if !Path::new(path).is_absolute() {
        return Err(EndpointError::RootCertificateRelative);
    }
    let unreadable = || EndpointError::RootCertificateUnreadable(path.to_owned());
    let certificates = CertificateDer::pem_file_iter(path)
        .map_err(|_| unreadable())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| unreadable())?;
    let mut roots = RootCertStore::empty();
    // A system bundle can hold a legacy root this library cannot parse as a
    // trust anchor; skip it, as rustls-native-certs does.
    roots.add_parsable_certificates(certificates);
    if roots.is_empty() {
        return Err(EndpointError::RootCertificateEmpty(path.to_owned()));
    }
    Ok(roots)
}

fn client_config(roots: RootCertStore) -> ClientConfig {
    ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("aws-lc-rs supports the default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth()
}

/// Never asked to connect: every plaintext endpoint has `sslmode=disable`.
fn plaintext() -> &'static MakeRustlsConnect {
    static PLAINTEXT: OnceLock<MakeRustlsConnect> = OnceLock::new();
    PLAINTEXT.get_or_init(|| MakeRustlsConnect::new(client_config(RootCertStore::empty())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bundle() -> (tempfile::TempDir, String) {
        let directory = tempfile::tempdir().unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let certificate = params.self_signed(&key).unwrap();
        let path = directory.path().join("root ca.pem");
        std::fs::write(&path, certificate.pem()).unwrap();
        let path = path.to_str().unwrap().to_owned();
        (directory, path)
    }

    fn encoded(path: &str) -> String {
        percent_encoding::utf8_percent_encode(path, percent_encoding::NON_ALPHANUMERIC).to_string()
    }

    #[test]
    fn verify_full_becomes_a_verified_tls_connection() {
        let (_directory, path) = bundle();
        let endpoint = Endpoint::parse(&format!(
            "postgresql://user:p%40ss@db.example.com:6543/app?connect_timeout=5&sslmode=verify-full&sslrootcert={}",
            encoded(&path)
        ))
        .unwrap();
        assert!(endpoint.uses_tls());
        let config = endpoint.config();
        assert_eq!(config.get_ssl_mode(), SslMode::Require);
        assert_eq!(config.get_password(), Some(b"p@ss".as_slice()));
        assert_eq!(config.get_ports(), [6543]);
        assert_eq!(
            config.get_connect_timeout(),
            Some(&std::time::Duration::from_secs(5))
        );
        assert!(!format!("{endpoint:?}").contains("p@ss"));
    }

    #[test]
    fn a_plaintext_string_stays_plaintext() {
        for connection in [
            "postgresql://user@127.0.0.1:5432/app",
            "postgresql://user@127.0.0.1:5432/app?sslmode=disable",
            "postgresql:///app?host=%2Ftmp&sslmode=disable",
            "host=127.0.0.1 dbname=app",
        ] {
            let endpoint = Endpoint::parse(connection).unwrap();
            assert!(!endpoint.uses_tls(), "{connection}");
            assert_eq!(
                endpoint.config().get_ssl_mode(),
                SslMode::Disable,
                "{connection}"
            );
        }
    }

    #[test]
    fn tls_that_does_not_check_the_server_is_refused() {
        let (_directory, path) = bundle();
        let root = encoded(&path);
        for (connection, refusal) in [
            (
                "postgresql://db.example.com/app?sslmode=require".to_owned(),
                "UnverifiedTls",
            ),
            (
                "postgresql://db.example.com/app?sslmode=verify-ca".to_owned(),
                "UnverifiedTls",
            ),
            (
                "postgresql://db.example.com/app?sslmode=prefer".to_owned(),
                "UnverifiedTls",
            ),
            (
                "postgresql://db.example.com/app?sslmode=allow".to_owned(),
                "UnverifiedTls",
            ),
            (
                "postgresql://db.example.com/app?sslmode=verify-full".to_owned(),
                "RootCertificateMissing",
            ),
            (
                format!("postgresql://db.example.com/app?sslrootcert={root}"),
                "RootCertificateWithoutVerification",
            ),
            (
                format!("postgresql://db.example.com/app?sslmode=disable&sslrootcert={root}"),
                "RootCertificateWithoutVerification",
            ),
            (
                "postgresql://db.example.com/app?sslmode=verify-full&sslrootcert=ca.pem".to_owned(),
                "RootCertificateRelative",
            ),
            (
                "postgresql://db.example.com/app?sslmode=verify-full&sslrootcert=%2Fno%2Fsuch%2Ffile".to_owned(),
                "RootCertificateUnreadable",
            ),
            (
                format!(
                    "postgresql://db.example.com/app?sslmode=verify-full&sslmode=disable&sslrootcert={root}"
                ),
                "RepeatedTlsParameter",
            ),
            (
                format!(
                    "postgresql://db.example.com/app?sslmode=verify-full&sslrootcert={root}&sslrootcert={root}"
                ),
                "RepeatedTlsParameter",
            ),
            (
                "host=db.example.com dbname=app sslmode=verify-full".to_owned(),
                "Config",
            ),
        ] {
            let error = Endpoint::parse(&connection).unwrap_err();
            assert!(format!("{error:?}").starts_with(refusal), "{connection}: {error:?}");
        }
    }

    #[test]
    fn a_file_without_a_certificate_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("empty.pem");
        std::fs::write(&path, "not a certificate\n").unwrap();
        let error = Endpoint::parse(&format!(
            "postgresql://db.example.com/app?sslmode=verify-full&sslrootcert={}",
            encoded(path.to_str().unwrap())
        ))
        .unwrap_err();
        assert!(
            matches!(error, EndpointError::RootCertificateEmpty(_)),
            "{error:?}"
        );
    }
}

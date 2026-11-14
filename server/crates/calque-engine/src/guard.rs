use std::{
    ffi::OsString,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    str::FromStr,
};

use thiserror::Error;
use tokio_postgres::config::Host as DriverHost;
use url::{Host, Url};

const POSTGRES_PORT: u16 = 5432;
const POSTGRES_CONNECT_TIMEOUT_SECONDS: &str = "5";
const LIBPQ_ROUTING_ENVIRONMENT: &[&str] = &[
    "PGHOST",
    "PGHOSTADDR",
    "PGPORT",
    "PGDATABASE",
    "PGSERVICE",
    "PGSERVICEFILE",
    "PGSYSCONFDIR",
    "PGTARGETSESSIONATTRS",
    "PGLOADBALANCEHOSTS",
];

#[derive(Clone)]
pub struct DatabaseUrl(Url);

/// Every app-owned PostgreSQL client family. Each receives the same normalized
/// authority; the variants exist to keep composition audits complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PostgresConnectionFamily {
    Inspect,
    Enroll,
    Destroy,
    FenceControl,
    SlotProvisioning,
    SourceSessionControl,
    SlotLag,
    Replication,
    CanonicalWriter,
    CanonicalReader,
    MutationWriter,
    CanonicalBarrier,
    SnapshotRegistry,
    SnapshotControl,
    SnapshotDdlGuard,
    SnapshotPressure,
    SnapshotCopy,
    Recovery,
}

#[cfg(test)]
const ALL_POSTGRES_CONNECTION_FAMILIES: &[PostgresConnectionFamily] = &[
    PostgresConnectionFamily::Inspect,
    PostgresConnectionFamily::Enroll,
    PostgresConnectionFamily::Destroy,
    PostgresConnectionFamily::FenceControl,
    PostgresConnectionFamily::SlotProvisioning,
    PostgresConnectionFamily::SourceSessionControl,
    PostgresConnectionFamily::SlotLag,
    PostgresConnectionFamily::Replication,
    PostgresConnectionFamily::CanonicalWriter,
    PostgresConnectionFamily::CanonicalReader,
    PostgresConnectionFamily::MutationWriter,
    PostgresConnectionFamily::CanonicalBarrier,
    PostgresConnectionFamily::SnapshotRegistry,
    PostgresConnectionFamily::SnapshotControl,
    PostgresConnectionFamily::SnapshotDdlGuard,
    PostgresConnectionFamily::SnapshotPressure,
    PostgresConnectionFamily::SnapshotCopy,
    PostgresConnectionFamily::Recovery,
];

impl DatabaseUrl {
    /// Accepts three transports: a Unix socket directory and a numeric
    /// loopback address in plaintext, and any other host over TLS that
    /// verifies the server against `sslrootcert`. The result is one canonical
    /// URL every connection family and both drivers read the same way.
    pub fn parse(value: &str) -> Result<Self, GuardError> {
        Self::parse_with(value, PlaintextRemote::Refused)
    }

    /// What `calque dev` accepts: everything [`DatabaseUrl::parse`] does, and
    /// an explicit `sslmode=disable` on any other host. A container's
    /// `127.0.0.1` is the container, so a database on the developer's machine
    /// is reached by a host name or an address that is not loopback. A host
    /// without that parameter still needs verified TLS.
    pub fn parse_for_dev(value: &str) -> Result<Self, GuardError> {
        Self::parse_with(value, PlaintextRemote::Allowed)
    }

    fn parse_with(value: &str, plaintext_remote: PlaintextRemote) -> Result<Self, GuardError> {
        let mut url = Url::parse(value).map_err(|_| GuardError::DatabaseUrlMalformed)?;
        if !matches!(url.scheme(), "postgres" | "postgresql") {
            return Err(GuardError::DatabaseScheme);
        }
        // Before the rest: a `#` in a password cuts the URL short, and what
        // it cut off is what the later checks would report missing.
        if url.fragment().is_some() {
            return Err(GuardError::DatabaseUrlFragment);
        }
        if url.path().is_empty() || url.path() == "/" {
            return Err(GuardError::DatabaseNameMissing);
        }
        // tokio-postgres ends the credentials at the first `@` anywhere in the
        // URL, so a literal one in the database name would make it read the
        // rest as another host. The name is encoded as the query values are.
        let database = percent_encoding::percent_decode_str(&url.path()[1..])
            .decode_utf8()
            .map_err(|_| GuardError::DatabaseUrlMalformed)?
            .into_owned();
        url.set_path(&format!(
            "/{}",
            percent_encoding::utf8_percent_encode(&database, URL_VALUE)
        ));

        let routing = routing_query(&url)?;
        let transport = match url.host() {
            Some(Host::Ipv4(address)) if address.is_loopback() => Transport::Loopback,
            Some(Host::Ipv6(address)) if address.is_loopback() => Transport::Loopback,
            // `url` represents PostgreSQL IPv4 URL hosts as a domain. Only a
            // string that parses as a numeric loopback IP is local; a name
            // such as `localhost` is resolved, so it is remote.
            Some(Host::Domain(address))
                if IpAddr::from_str(address).is_ok_and(|address| address.is_loopback()) =>
            {
                Transport::Loopback
            }
            // One name or address. A comma would give both drivers a list
            // of hosts to try.
            Some(Host::Domain(address))
                if !address.is_empty()
                    && address.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-'
                    }) =>
            {
                Transport::Tls
            }
            Some(Host::Ipv6(_)) => Transport::Tls,
            Some(_) => return Err(GuardError::DatabaseHostMalformed),
            None if routing.unix_socket_host.is_some() => Transport::Unix,
            None => return Err(GuardError::DatabaseHostMissing),
        };
        match transport {
            Transport::Unix | Transport::Loopback => {
                if routing.sslrootcert.is_some()
                    || routing
                        .sslmode
                        .as_deref()
                        .is_some_and(|mode| mode != "disable")
                {
                    return Err(GuardError::DatabaseRoutingQuery);
                }
            }
            Transport::Tls => match routing.sslmode.as_deref() {
                Some("disable") if plaintext_remote == PlaintextRemote::Allowed => {
                    if routing.sslrootcert.is_some() {
                        return Err(GuardError::DatabaseRoutingQuery);
                    }
                }
                Some(mode @ ("require" | "verify-ca" | "prefer" | "allow")) => {
                    return Err(GuardError::DatabaseSslModeUnchecked(mode.to_owned()));
                }
                Some("verify-full") | None if routing.sslrootcert.is_some() => {}
                _ => {
                    return Err(match plaintext_remote {
                        PlaintextRemote::Refused => GuardError::DatabaseTlsRequired,
                        PlaintextRemote::Allowed => GuardError::DatabaseTlsRequiredForDev,
                    })
                }
            },
        }
        // Rebuild the only allowed query fields into one canonical endpoint.
        // Unix socket directories and the root certificate file are resolved
        // once, so later connection families cannot reinterpret `..`
        // components or a symlink.
        let unix_socket_host = routing
            .unix_socket_host
            .as_deref()
            .map(std::fs::canonicalize)
            .transpose()
            .map_err(|_| GuardError::DatabaseUnixSocketPath)?;
        if unix_socket_host.as_ref().is_some_and(|path| !path.is_dir()) {
            return Err(GuardError::DatabaseUnixSocketPath);
        }
        let unix_socket_host = unix_socket_host
            .as_deref()
            .map(|path| path.to_str().ok_or(GuardError::DatabaseUnixSocketPath))
            .transpose()?;
        let root_certificate = routing
            .sslrootcert
            .as_deref()
            .map(canonical_root_certificate)
            .transpose()?;
        if transport != Transport::Unix && url.port().is_none() {
            url.set_port(Some(POSTGRES_PORT))
                .map_err(|()| GuardError::DatabaseUrlMalformed)?;
        }
        let port = POSTGRES_PORT.to_string();
        let mut query = Vec::new();
        if let Some(host) = unix_socket_host {
            query.push(("host", host));
            query.push(("port", port.as_str()));
            if let Some(user) = routing.user.as_deref() {
                query.push(("user", user));
            }
            if let Some(password) = routing.password.as_deref() {
                query.push(("password", password));
            }
        }
        // Every app-owned PostgreSQL family receives the same finite
        // connection deadline and transport. These composition-owned values
        // cannot be replaced by caller query parameters or libpq environment
        // defaults.
        query.push(("connect_timeout", POSTGRES_CONNECT_TIMEOUT_SECONDS));
        match root_certificate.as_deref() {
            Some(root_certificate) => {
                query.push(("sslmode", "verify-full"));
                query.push(("sslrootcert", root_certificate));
            }
            None => query.push(("sslmode", "disable")),
        }
        url.set_query(Some(&encoded_query(&query)));
        // The driver must read the one host and port this guard approved.
        let approved = match (unix_socket_host, url.host()) {
            (Some(directory), _) => DriverHost::Unix(directory.into()),
            (None, Some(Host::Domain(name))) => DriverHost::Tcp(name.to_owned()),
            (None, Some(Host::Ipv4(address))) => DriverHost::Tcp(address.to_string()),
            (None, Some(Host::Ipv6(address))) => DriverHost::Tcp(address.to_string()),
            (None, None) => return Err(GuardError::DatabaseHostMissing),
        };
        let approved_port = url.port().unwrap_or(POSTGRES_PORT);
        let endpoint =
            postgres_connect::Endpoint::parse(url.as_str()).map_err(GuardError::DatabaseTls)?;
        if endpoint.config().get_hosts() != [approved]
            || endpoint.config().get_ports() != [approved_port]
            || endpoint.config().get_dbname() != Some(database.as_str())
        {
            return Err(GuardError::DatabaseUrlMalformed);
        }
        Ok(Self(url))
    }

    /// Secret-bearing PostgreSQL URL. Callers must never log this value.
    pub(crate) fn expose(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn connection_url(&self, _family: PostgresConnectionFamily) -> &str {
        self.expose()
    }

    /// The driver-side connection for one family, TLS included.
    pub(crate) fn endpoint(
        &self,
        family: PostgresConnectionFamily,
    ) -> Result<postgres_connect::Endpoint, GuardError> {
        postgres_connect::Endpoint::parse(self.connection_url(family))
            .map_err(GuardError::DatabaseTls)
    }

    pub(crate) fn uses_unix_socket(&self) -> bool {
        self.0.host().is_none()
    }

    pub(crate) fn uses_tls(&self) -> bool {
        self.0
            .query_pairs()
            .any(|(key, value)| key == "sslmode" && value == "verify-full")
    }

    /// The host a `calque dev` connection reaches in plaintext without it
    /// being a loopback address, for the warning dev prints.
    pub fn plaintext_remote_host(&self) -> Option<&str> {
        let loopback = match self.0.host() {
            None => true,
            Some(Host::Ipv4(address)) => address.is_loopback(),
            Some(Host::Ipv6(address)) => address.is_loopback(),
            Some(Host::Domain(host)) => IpAddr::from_str(host).is_ok_and(|ip| ip.is_loopback()),
        };
        (!self.uses_tls() && !loopback)
            .then(|| self.0.host_str())
            .flatten()
    }

    pub(crate) fn tcp_address(&self) -> Option<IpAddr> {
        self.0
            .host_str()
            .and_then(|host| IpAddr::from_str(host).ok())
    }

    /// Reject libpq process defaults that could introduce a second routing
    /// authority beside the normalized loopback/Unix-socket URL.
    pub(crate) fn validate_libpq_environment_with(
        &self,
        mut value: impl FnMut(&str) -> Option<OsString>,
    ) -> Result<(), GuardError> {
        for name in LIBPQ_ROUTING_ENVIRONMENT {
            if value(name).is_some() {
                return Err(GuardError::DatabaseRoutingEnvironment(name));
            }
        }
        Ok(())
    }
}

impl fmt::Debug for DatabaseUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatabaseUrl")
            .field("scheme", &self.0.scheme())
            .field(
                "transport",
                &if self.uses_unix_socket() {
                    "unix"
                } else if self.uses_tls() {
                    "tls"
                } else if self.plaintext_remote_host().is_some() {
                    "plaintext"
                } else {
                    "loopback"
                },
            )
            .field("redacted", &true)
            .finish()
    }
}

/// Whether a host that is not loopback may be reached without TLS: only
/// `calque dev` says so, and only when the URL writes `sslmode=disable`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PlaintextRemote {
    Refused,
    Allowed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Transport {
    Unix,
    Loopback,
    Tls,
}

/// No `Debug`: this holds the password, and only `DatabaseUrl`'s redacting
/// `Debug` may render a connection string.
#[derive(Default)]
struct RoutingQuery {
    unix_socket_host: Option<String>,
    user: Option<String>,
    password: Option<String>,
    sslmode: Option<String>,
    sslrootcert: Option<String>,
}

/// What the guard writes percent-encoded in a database name or a query value:
/// everything but letters, digits and `-_.~`, which libpq and tokio-postgres
/// both decode. Form encoding would write a space as `+`, which both read as a
/// plus sign.
const URL_VALUE: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

fn encoded_query(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(key, value)| {
            format!(
                "{key}={}",
                percent_encoding::utf8_percent_encode(value, URL_VALUE)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_root_certificate(path: &str) -> Result<String, GuardError> {
    let invalid = || {
        GuardError::DatabaseTls(postgres_connect::EndpointError::RootCertificateUnreadable(
            path.to_owned(),
        ))
    };
    if !std::path::Path::new(path).is_absolute() || path.contains('\0') {
        return Err(GuardError::DatabaseTls(
            postgres_connect::EndpointError::RootCertificateRelative,
        ));
    }
    let canonical = std::fs::canonicalize(path).map_err(|_| invalid())?;
    if !canonical.is_file() {
        return Err(invalid());
    }
    canonical.to_str().map(str::to_owned).ok_or_else(invalid)
}

fn routing_query(url: &Url) -> Result<RoutingQuery, GuardError> {
    let mut routing = RoutingQuery::default();
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "host" => {
                if routing.unix_socket_host.is_some()
                    || url.host().is_some()
                    || value.contains(',')
                    || value.contains('\0')
                    || !std::path::Path::new(value.as_ref()).is_absolute()
                {
                    return Err(GuardError::DatabaseRoutingQuery);
                }
                routing.unix_socket_host = Some(value.into_owned());
            }
            // The Unix-socket form's credentials. `rust-url` refuses userinfo
            // on an empty authority, so `postgresql://user:pass@/db?host=/run`
            // is not a URL at all, and a socket connection would otherwise
            // have to be trusted by address rather than authenticated. They
            // are refused for a TCP host, so each transport has one place to
            // write a credential.
            "user" if url.host().is_none() && routing.user.is_none() && !value.is_empty() => {
                if value.contains('\0') {
                    return Err(GuardError::DatabaseRoutingQuery);
                }
                routing.user = Some(value.into_owned());
            }
            "password"
                if url.host().is_none() && routing.password.is_none() && !value.is_empty() =>
            {
                if value.contains('\0') {
                    return Err(GuardError::DatabaseRoutingQuery);
                }
                routing.password = Some(value.into_owned());
            }
            "sslmode" if routing.sslmode.is_none() => {
                routing.sslmode = Some(value.into_owned());
            }
            "sslrootcert" if routing.sslrootcert.is_none() && url.host().is_some() => {
                routing.sslrootcert = Some(value.into_owned());
            }
            // Fail closed on every other libpq/tokio-postgres query key. In
            // particular, service/servicefile, hostaddr, target_session_attrs,
            // replication, and application_name must never redirect or widen
            // the endpoint selected by this composition.
            _ => return Err(GuardError::DatabaseRoutingQuery),
        }
    }
    Ok(routing)
}

/// Where the HTTP listener binds, and the one `Host` value it answers.
///
/// They are separate because a container binds `0.0.0.0` while browsers
/// address it by the name the composition publishes. The `Host` check stays
/// one byte-equal value, never a wildcard, a suffix, or a resolved name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpBinding {
    address: IpAddr,
    port: u16,
    expected_host: String,
}

impl HttpBinding {
    /// The development binding: listen on `127.0.0.1` and answer only
    /// `127.0.0.1:<port>`.
    pub fn loopback(port: u16) -> Result<Self, GuardError> {
        Self::configured(IpAddr::V4(Ipv4Addr::LOCALHOST), port, None)
    }

    /// A binding a composition configures.
    ///
    /// `expected_host` is the exact `Host` header this process answers. It is
    /// required whenever `address` is a wildcard, because `0.0.0.0:<port>` is
    /// not a value any browser sends. When it is omitted for a concrete
    /// address, the host is that address and port.
    ///
    /// The listener reaching more networks changes no check, and this process
    /// still speaks plaintext HTTP: TLS termination belongs to the composition
    /// in front of it.
    pub fn configured(
        address: IpAddr,
        port: u16,
        expected_host: Option<&str>,
    ) -> Result<Self, GuardError> {
        if port == 0 {
            return Err(GuardError::HttpPortZero);
        }
        let expected_host = match expected_host {
            Some(value) => validated_expected_host(value)?,
            None if address.is_unspecified() => return Err(GuardError::HttpExpectedHostRequired),
            None => SocketAddr::new(address, port).to_string(),
        };
        Ok(Self {
            address,
            port,
            expected_host,
        })
    }

    pub fn socket_address(&self) -> SocketAddr {
        SocketAddr::new(self.address, self.port)
    }

    /// Whether this listener is reachable only from the host it runs on.
    /// Callers use it to say so at startup; it changes no check.
    pub fn is_loopback_only(&self) -> bool {
        self.address.is_loopback()
    }

    pub fn expected_host(&self) -> &str {
        &self.expected_host
    }

    pub(crate) fn validate_host_header(&self, value: &str) -> Result<(), GuardError> {
        if value == self.expected_host {
            Ok(())
        } else {
            Err(GuardError::HttpHostMismatch)
        }
    }
}

/// The `Host` header grammar this process will compare against: a bounded
/// ASCII `host` or `host:port` with no scheme, userinfo, path, or whitespace.
///
/// A value carrying any of those could never equal a real `Host` header
/// anyway, but refusing it at configuration time keeps an operator from
/// believing a URL was accepted as a host.
fn validated_expected_host(value: &str) -> Result<String, GuardError> {
    if value.is_empty()
        || value.len() > 255
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-.:[]".contains(&byte))
    {
        return Err(GuardError::HttpExpectedHostMalformed);
    }
    // `[v6]`, `[v6]:port`, `name`, `name:port`. An IPv6 literal without
    // brackets is refused rather than guessed at, because its colons cannot be
    // told from a port separator.
    let (host, port) = if let Some(rest) = value.strip_prefix('[') {
        let (literal, tail) = rest
            .split_once(']')
            .ok_or(GuardError::HttpExpectedHostMalformed)?;
        let port = match tail {
            "" => None,
            tail => Some(
                tail.strip_prefix(':')
                    .ok_or(GuardError::HttpExpectedHostMalformed)?,
            ),
        };
        // Brackets mean an IPv6 literal, so it has to parse as one. Accepting
        // anything that merely carries a colon would let `[a:b]` or `[:::::]`
        // be reported to an operator as a validated host.
        if Ipv6Addr::from_str(literal).is_err() {
            return Err(GuardError::HttpExpectedHostMalformed);
        }
        (literal, port)
    } else {
        match value.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (value, None),
        }
    };
    if port.is_some_and(|port| !port.parse::<u16>().is_ok_and(|port| port != 0)) {
        return Err(GuardError::HttpExpectedHostMalformed);
    }
    if host.is_empty()
        || host.starts_with('.')
        || host.ends_with('.')
        || host.contains('[')
        || host.contains(']')
        || (!value.starts_with('[') && host.contains(':'))
        || IpAddr::from_str(host).is_ok_and(|address| address.is_unspecified())
    {
        return Err(GuardError::HttpExpectedHostMalformed);
    }
    Ok(value.to_owned())
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AllowedOrigin(String);

impl AllowedOrigin {
    /// A loopback origin, over HTTP or HTTPS: what `calque dev` admits.
    pub fn parse(value: &str) -> Result<Self, GuardError> {
        let url = origin_url(value)?;
        if !is_loopback_host(&url) {
            return Err(GuardError::OriginNotLoopback(
                url.origin().ascii_serialization(),
            ));
        }
        Ok(Self(url.origin().ascii_serialization()))
    }

    /// What `calque serve` admits: an HTTPS origin on any host, or a loopback
    /// origin as [`AllowedOrigin::parse`] takes it.
    ///
    /// Behind a TLS proxy the application's pages live on its own domain,
    /// which is not loopback. Plain HTTP stays loopback-only: a page fetched
    /// in cleartext from a network host could have been rewritten on the way,
    /// and that page holds the grant.
    pub fn parse_https_or_loopback(value: &str) -> Result<Self, GuardError> {
        let url = origin_url(value)?;
        if url.scheme() != "https" && !is_loopback_host(&url) {
            return Err(GuardError::OriginNotHttpsOrLoopback(
                url.origin().ascii_serialization(),
            ));
        }
        Ok(Self(url.origin().ascii_serialization()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn origin_url(value: &str) -> Result<Url, GuardError> {
    let url = Url::parse(value).map_err(|_| GuardError::OriginMalformed)?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.has_host()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(GuardError::OriginMalformed);
    }
    Ok(url)
}

fn is_loopback_host(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        Some(Host::Domain(domain)) => domain == "localhost",
        None => false,
    }
}

impl fmt::Debug for AllowedOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("AllowedOrigin")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for AllowedOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for AllowedOrigin {
    type Err = GuardError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[derive(Debug, Error)]
pub enum GuardError {
    #[error("database URL is not a URL; write postgresql://user:password@127.0.0.1:5432/dbname, percent-encoding reserved characters in the password. libpq keyword strings are not accepted")]
    DatabaseUrlMalformed,
    #[error("database URL must not contain a fragment; percent-encode a # in the password as %23")]
    DatabaseUrlFragment,
    #[error("database URL scheme must be postgres or postgresql")]
    DatabaseScheme,
    #[error("database URL must name a database")]
    DatabaseNameMissing,
    #[error(
        "a database host other than a numeric loopback address (127.0.0.1 or [::1]) or a Unix socket directory is reached only over verified TLS: add sslmode=verify-full&sslrootcert=<absolute path of a PEM file of trusted CA certificates>. Host names, localhost included, count as remote; for a database on this machine, write 127.0.0.1"
    )]
    DatabaseTlsRequired,
    #[error(
        "a database host other than a numeric loopback address (127.0.0.1 or [::1]) or a Unix socket directory is reached over verified TLS: add sslmode=verify-full&sslrootcert=<absolute path of a PEM file of trusted CA certificates>. For a disposable database, calque dev, inspect and destroy also accept sslmode=disable, which sends the password and every row in plaintext. Host names, localhost included, count as remote; for a database on this machine, write 127.0.0.1"
    )]
    DatabaseTlsRequiredForDev,
    #[error(
        "sslmode={0} does not check the server's name, so calque refuses it: use sslmode=verify-full with sslrootcert=<absolute path of a PEM file of trusted CA certificates>"
    )]
    DatabaseSslModeUnchecked(String),
    #[error("database URL host must be one host name or IP address")]
    DatabaseHostMalformed,
    #[error("database URL host is missing; a Unix socket host query parameter is required")]
    DatabaseHostMissing,
    #[error("database URL may carry only these query parameters, each once: sslmode=verify-full and sslrootcert for a remote host; sslmode=disable for a loopback address or a Unix socket; and host, an absolute Unix socket directory, with user and password, when the URL names no host")]
    DatabaseRoutingQuery,
    #[error("database TLS: {0}")]
    DatabaseTls(#[source] postgres_connect::EndpointError),
    #[error("libpq routing environment variable {0} is set; unset it, because calque connects only to the address in the database URL")]
    DatabaseRoutingEnvironment(&'static str),
    #[error("database Unix socket directory must exist and resolve to one exact directory")]
    DatabaseUnixSocketPath,
    #[error("HTTP port must be nonzero")]
    HttpPortZero,
    #[error("Host header does not exactly match the configured listener host")]
    HttpHostMismatch,
    #[error("a wildcard bind address requires an explicit expected Host value: pass --expected-host <host[:port]>, the exact Host header browsers send (--metrics-expected-host for the metrics listener)")]
    HttpExpectedHostRequired,
    #[error("expected Host must be a bare host or host:port, never a URL or a wildcard address")]
    HttpExpectedHostMalformed,
    #[error(
        "--allow-origin must be an HTTP(S) origin without credentials, path, query, or fragment"
    )]
    OriginMalformed,
    #[error("--allow-origin {0} is not loopback: calque dev admits only localhost or a loopback IP literal")]
    OriginNotLoopback(String),
    #[error("--allow-origin {0} must use HTTPS, or localhost or a loopback IP literal")]
    OriginNotHttpsOrLoopback(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A CA certificate in a directory whose name has a space, and the
    /// percent-encoded path a URL carries.
    fn root_certificate() -> (tempfile::TempDir, std::path::PathBuf, String) {
        let directory = tempfile::tempdir().unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let certificate = params.self_signed(&key).unwrap();
        std::fs::create_dir(directory.path().join("trusted roots")).unwrap();
        let path = directory.path().join("trusted roots/ca.pem");
        std::fs::write(&path, certificate.pem()).unwrap();
        let encoded = encoded_query(&[("", path.to_str().unwrap())])[1..].to_owned();
        (directory, path, encoded)
    }

    #[test]
    fn database_guard_accepts_local_plaintext_or_remote_tls_and_redacts_debug() {
        let tcp = DatabaseUrl::parse("postgresql://user:top-secret@127.0.0.1:5432/app").unwrap();
        let debug = format!("{tcp:?}");
        assert!(!debug.contains("top-secret"));
        assert!(!debug.contains("user"));
        let tcp_url = Url::parse(tcp.expose()).unwrap();
        let tcp_query = tcp_url
            .query_pairs()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(tcp_url.port(), Some(5432));
        assert_eq!(
            tcp_query.get("connect_timeout").map(|value| value.as_ref()),
            Some("5")
        );
        assert_eq!(
            tcp_query.get("sslmode").map(|value| value.as_ref()),
            Some("disable")
        );
        assert!(DatabaseUrl::parse("postgresql:///app?host=%2Ftmp")
            .unwrap()
            .uses_unix_socket());

        for invalid in [
            "host=127.0.0.1 dbname=app",
            "mysql://127.0.0.1/app",
            "postgresql://db.example.com/app",
            "postgresql://localhost/app",
            "postgresql://127.0.0.1/",
            "postgresql://127.0.0.1/app?host=example.com",
            "postgresql://127.0.0.1/app?hostaddr=10.0.0.1",
            "postgresql://127.0.0.1/app?port=6543",
            "postgresql://127.0.0.1/app?dbname=other",
            "postgresql://127.0.0.1/app?replication=database",
            "postgresql://127.0.0.1/app?application_name=caller",
            "postgresql://127.0.0.1/app?service=redirect",
            "postgresql://127.0.0.1/app?servicefile=%2Ftmp%2Fpg_service.conf",
            "postgresql://127.0.0.1/app?target_session_attrs=read-write",
            "postgresql://127.0.0.1/app?load_balance_hosts=random",
            "postgresql://127.0.0.1/app?sslmode=require",
            "postgresql://127.0.0.1/app?sslmode=disable&sslmode=disable",
            "postgresql://127.0.0.1/app?unknown=value",
            "postgresql:///app?host=%2Ftmp&host=example.com",
            "postgresql:///app?host=%2Ftmp,%2Fvar%2Frun",
            "postgresql:///app?hostaddr=127.0.0.1",
            "postgresql:///app?host=relative",
            "postgresql:///app?host=%2Fpath%2Fthat%2Fdoes%2Fnot%2Fexist",
        ] {
            assert!(DatabaseUrl::parse(invalid).is_err(), "{invalid}");
        }
        assert!(DatabaseUrl::parse("postgresql:///app?host=%2Ftmp&sslmode=disable").is_ok());
    }

    /// Any host but a loopback address or a socket is reached over TLS that
    /// verifies the server, and the URL says which certificates it trusts.
    #[test]
    fn a_remote_host_is_reached_only_over_verified_tls() {
        let (directory, path, root) = root_certificate();
        let canonical_root = std::fs::canonicalize(&path).unwrap();
        for (accepted, host, port) in [
            (
                format!("postgresql://user:s3cr3t@db.example.com/app?sslrootcert={root}"),
                "db.example.com",
                5432,
            ),
            (
                format!("postgresql://user:s3cr3t@db.example.com:6543/app?sslmode=verify-full&sslrootcert={root}"),
                "db.example.com",
                6543,
            ),
            (
                format!("postgresql://user@10.0.0.5/app?sslmode=verify-full&sslrootcert={root}"),
                "10.0.0.5",
                5432,
            ),
            (
                format!("postgresql://user@[fd00::5]/app?sslmode=verify-full&sslrootcert={root}"),
                "[fd00::5]",
                5432,
            ),
            (
                format!("postgresql://user@localhost/app?sslmode=verify-full&sslrootcert={root}"),
                "localhost",
                5432,
            ),
        ] {
            let database = DatabaseUrl::parse(&accepted).unwrap();
            assert!(database.uses_tls(), "{accepted}");
            let debug = format!("{database:?}");
            assert!(debug.contains("\"tls\""), "{debug}");
            assert!(!debug.contains("s3cr3t") && !debug.contains("user"), "{debug}");

            let url = Url::parse(database.expose()).unwrap();
            assert_eq!(url.host_str(), Some(host));
            assert_eq!(url.port(), Some(port));
            let query = url.query_pairs().collect::<Vec<_>>();
            assert_eq!(
                query,
                [
                    ("connect_timeout".into(), "5".into()),
                    ("sslmode".into(), "verify-full".into()),
                    ("sslrootcert".into(), canonical_root.to_str().unwrap().into()),
                ]
            );
            // Both drivers percent-decode and neither reads `+` as a space.
            assert!(url.query().unwrap().contains("%20"), "{url}");

            let endpoint = database.endpoint(PostgresConnectionFamily::Inspect).unwrap();
            assert!(endpoint.uses_tls());
            assert_eq!(
                endpoint.config().get_ssl_mode(),
                tokio_postgres::config::SslMode::Require
            );
        }

        let not_a_certificate = directory.path().join("key.txt");
        std::fs::write(&not_a_certificate, "not a certificate\n").unwrap();
        let not_a_certificate =
            encoded_query(&[("", not_a_certificate.to_str().unwrap())])[1..].to_owned();
        let trusted_directory =
            encoded_query(&[("", path.parent().unwrap().to_str().unwrap())])[1..].to_owned();
        for refused in [
            "postgresql://db.example.com/app".to_owned(),
            "postgresql://db.example.com/app?sslmode=disable".to_owned(),
            "postgresql://db.example.com/app?sslmode=verify-full".to_owned(),
            format!("postgresql://db.example.com/app?sslmode=require&sslrootcert={root}"),
            format!("postgresql://db.example.com/app?sslmode=verify-ca&sslrootcert={root}"),
            format!("postgresql://db.example.com/app?sslmode=prefer&sslrootcert={root}"),
            "postgresql://db.example.com/app?sslrootcert=ca.pem".to_owned(),
            format!("postgresql://db.example.com/app?sslrootcert={not_a_certificate}"),
            format!("postgresql://db.example.com/app?sslrootcert={trusted_directory}"),
            "postgresql://db.example.com/app?sslrootcert=%2Fno%2Fsuch%2Fca.pem".to_owned(),
            format!("postgresql://db.example.com/app?sslrootcert={root}&sslrootcert={root}"),
            format!("postgresql://db.example.com/app?sslrootcert={root}&sslmode=verify-full&sslmode=verify-full"),
            format!("postgresql://db.example.com/app?sslrootcert={root}&user=other"),
            format!("postgresql://db.example.com/app?sslrootcert={root}&host=%2Ftmp"),
            format!("postgresql://db.example.com/app?sslrootcert={root}&hostaddr=10.0.0.9"),
            format!("postgresql://a.example.com,b.example.com/app?sslrootcert={root}"),
            format!("postgresql://db.example.com%2Cother/app?sslrootcert={root}"),
            format!("postgresql://127.0.0.1/app?sslrootcert={root}"),
            format!("postgresql://127.0.0.1/app?sslmode=verify-full&sslrootcert={root}"),
            format!("postgresql:///app?host=%2Ftmp&sslrootcert={root}"),
        ] {
            assert!(DatabaseUrl::parse(&refused).is_err(), "{refused}");
        }
    }

    /// A refusal names the rule the URL broke, and never the URL itself,
    /// which may hold a password.
    #[test]
    fn a_refused_url_names_the_rule_it_broke() {
        let refusal = |url: &str| DatabaseUrl::parse(url).err().unwrap().to_string();

        let malformed = refusal("postgresql://user:s3cr3t@127.0.0.1:99999/app");
        assert!(
            malformed.starts_with("database URL is not a URL;"),
            "{malformed}"
        );
        let fragment = refusal("postgresql://user:12#s3cr3t@127.0.0.1/app");
        assert!(
            fragment.contains("percent-encode a # in the password as %23"),
            "{fragment}"
        );
        let query = refusal("postgresql://127.0.0.1/app?sslmode=require");
        assert!(query.contains("sslmode=disable"), "{query}");
        for message in [malformed, fragment, query] {
            assert!(
                !message.contains("s3cr3t") && !message.contains("require"),
                "{message}"
            );
        }
    }

    /// The refusal states the rule, because the usual first attempt is
    /// `localhost`, which reads as loopback and is a host name, or a managed
    /// database's own connection string, which names no CA file.
    #[test]
    fn a_host_name_without_tls_is_refused_with_the_rule_it_breaks() {
        for host_name in [
            "postgresql://localhost/app",
            "postgresql://db.example.com/app",
            "postgresql://db.example.com/app?sslmode=disable",
            "postgresql://db.example.com/app?sslmode=verify-full",
        ] {
            let Err(error) = DatabaseUrl::parse(host_name) else {
                panic!("{host_name} was accepted");
            };
            assert!(
                matches!(error, GuardError::DatabaseTlsRequired),
                "{host_name}: {error}"
            );
            let message = error.to_string();
            for named in [
                "numeric loopback address",
                "127.0.0.1",
                "[::1]",
                "Unix socket directory",
                "sslmode=verify-full&sslrootcert=",
                "localhost included",
            ] {
                assert!(message.contains(named), "{message}");
            }
        }
    }

    /// An unchecked mode is refused by name: `require` encrypts and still
    /// accepts any certificate, which is the usual first thing to try.
    #[test]
    fn an_sslmode_that_does_not_check_the_name_says_so() {
        let (_directory, _path, root) = root_certificate();
        for mode in ["require", "verify-ca", "prefer", "allow"] {
            for url in [
                format!("postgresql://db.example.com/app?sslmode={mode}"),
                format!("postgresql://db.example.com/app?sslmode={mode}&sslrootcert={root}"),
            ] {
                for parse in [DatabaseUrl::parse, DatabaseUrl::parse_for_dev] {
                    let error = parse(&url).err().unwrap();
                    assert!(
                        matches!(&error, GuardError::DatabaseSslModeUnchecked(found) if found == mode),
                        "{url}: {error}"
                    );
                    let message = error.to_string();
                    assert!(
                        message
                            .contains(&format!("sslmode={mode} does not check the server's name"))
                            && message.contains("sslmode=verify-full")
                            && message.contains("sslrootcert="),
                        "{message}"
                    );
                }
            }
        }
    }

    /// `calque dev` reaches a database on the developer's machine from a
    /// container, where `127.0.0.1` is the container itself. It takes an
    /// explicit `sslmode=disable` for that, and no other command does.
    #[test]
    fn dev_takes_an_explicit_plaintext_host_that_no_other_command_does() {
        for url in [
            "postgresql://u:s3cr3t@host.docker.internal:5432/app?sslmode=disable",
            "postgresql://u:s3cr3t@db.example.com/app?sslmode=disable",
            "postgresql://u:s3cr3t@10.0.0.5:6543/app?sslmode=disable",
            "postgresql://u:s3cr3t@localhost/app?sslmode=disable",
            "postgresql://u:s3cr3t@[fd00::5]/app?sslmode=disable",
        ] {
            let database = DatabaseUrl::parse_for_dev(url).unwrap();
            assert!(!database.uses_tls(), "{url}");
            let canonical = Url::parse(database.expose()).unwrap();
            let query = canonical
                .query_pairs()
                .collect::<std::collections::BTreeMap<_, _>>();
            assert_eq!(
                query.get("sslmode").map(|mode| mode.as_ref()),
                Some("disable")
            );
            assert!(!query.contains_key("sslrootcert"), "{url}");
            assert_eq!(
                database.plaintext_remote_host(),
                Url::parse(url).unwrap().host_str(),
                "{url}"
            );
            assert_eq!(
                format!("{database:?}"),
                "DatabaseUrl { scheme: \"postgresql\", transport: \"plaintext\", redacted: true }"
            );
            let refusal = DatabaseUrl::parse(url).err().unwrap();
            assert!(matches!(refusal, GuardError::DatabaseTlsRequired), "{url}");
            assert!(
                !refusal.to_string().contains("sslmode=disable"),
                "serve must not be told plaintext is an option: {refusal}"
            );
        }
    }

    #[test]
    fn dev_without_an_explicit_sslmode_disable_still_needs_verified_tls() {
        let (_directory, _path, root) = root_certificate();
        for url in [
            "postgresql://u@host.docker.internal/app".to_owned(),
            "postgresql://u@localhost/app".to_owned(),
            "postgresql://u@db.example.com/app?sslmode=verify-full".to_owned(),
        ] {
            let error = DatabaseUrl::parse_for_dev(&url).err().unwrap();
            assert!(
                matches!(error, GuardError::DatabaseTlsRequiredForDev),
                "{url}: {error}"
            );
            assert!(error.to_string().contains("sslmode=disable"), "{error}");
        }
        // A plaintext request that also names a CA contradicts itself.
        assert!(matches!(
            DatabaseUrl::parse_for_dev(&format!(
                "postgresql://u@db.example.com/app?sslmode=disable&sslrootcert={root}"
            )),
            Err(GuardError::DatabaseRoutingQuery)
        ));
        // Verified TLS and the local transports are unchanged.
        assert!(DatabaseUrl::parse_for_dev(&format!(
            "postgresql://u@db.example.com/app?sslrootcert={root}"
        ))
        .unwrap()
        .uses_tls());
        for local in [
            "postgresql://u@127.0.0.1/app",
            "postgresql://u@127.0.0.1/app?sslmode=disable",
            "postgresql:///app?host=%2Ftmp",
        ] {
            let database = DatabaseUrl::parse_for_dev(local).unwrap();
            assert_eq!(database.plaintext_remote_host(), None, "{local}");
        }
        assert!(
            DatabaseUrl::parse_for_dev("postgresql://u@127.0.0.1/app?sslmode=require").is_err()
        );
    }

    /// Each of these names the flag to change and, where it is safe to,
    /// the value that was refused.
    #[test]
    fn a_refused_bind_or_origin_names_its_flag_and_value() {
        let wildcard = HttpBinding::configured(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8787, None)
            .err()
            .unwrap()
            .to_string();
        assert!(
            wildcard.contains("--expected-host <host[:port]>"),
            "{wildcard}"
        );
        assert!(wildcard.contains("--metrics-expected-host"), "{wildcard}");

        let https = AllowedOrigin::parse_https_or_loopback("http://app.example.com:3000")
            .err()
            .unwrap()
            .to_string();
        assert_eq!(
            https,
            "--allow-origin http://app.example.com:3000 must use HTTPS, or localhost or a loopback IP literal"
        );
        let loopback = AllowedOrigin::parse("https://app.example.com")
            .err()
            .unwrap()
            .to_string();
        assert!(
            loopback.starts_with("--allow-origin https://app.example.com is not loopback"),
            "{loopback}"
        );
        // A malformed origin may carry a credential, so its value is not echoed.
        let malformed = AllowedOrigin::parse("https://user:s3cr3t@app.example.com")
            .err()
            .unwrap()
            .to_string();
        assert!(
            malformed.starts_with("--allow-origin must be"),
            "{malformed}"
        );
        assert!(!malformed.contains("s3cr3t"), "{malformed}");
    }

    /// A composition addresses a database container by service name, which
    /// this guard refuses without TLS. The self-hosted composition shares a
    /// Unix socket directory instead, so no cleartext crosses an interface.
    #[test]
    fn a_composition_may_not_address_its_database_container_by_name_or_address() {
        for refused in [
            "postgresql://calque_sync:s3cr3t@postgres:5432/calque",
            "postgresql://calque_sync:s3cr3t@postgres/calque",
            "postgresql://calque_sync:s3cr3t@10.97.3.5:5432/calque",
            "postgresql://calque_sync:s3cr3t@[fd00::5]:5432/calque",
            // rust-url refuses userinfo with an empty authority, so the
            // socket form takes credentials as `user`/`password` query
            // parameters instead, below.
            "postgresql://calque_sync:s3cr3t@/calque?host=%2Ftmp",
        ] {
            assert!(DatabaseUrl::parse(refused).is_err(), "{refused}");
        }
        // What a single-host deployment presents: loopback, credentials and
        // all, redacted in every diagnostic.
        let inside = DatabaseUrl::parse("postgresql://calque_sync:s3cr3t@127.0.0.1:5432/calque")
            .expect("a loopback connection string");
        assert_eq!(
            inside.tcp_address(),
            Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)))
        );
        assert!(!inside.uses_unix_socket());
        let debug = format!("{inside:?}");
        assert!(!debug.contains("s3cr3t"), "{debug}");
        assert!(!debug.contains("calque_sync"), "{debug}");
    }

    /// The composition's own connection string: a Unix socket on a directory
    /// both containers mount, authenticated by a password the socket form
    /// carries as query parameters. No host is resolved, and the endpoint is
    /// still one directory this process canonicalized once.
    #[test]
    fn the_unix_socket_form_carries_a_credential_and_still_reaches_no_network() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().to_str().unwrap();
        let encoded = socket.replace('/', "%2F");
        let database = DatabaseUrl::parse(&format!(
            "postgresql:///calque?host={encoded}&user=calque_sync&password=s3%20cr%2Bt"
        ))
        .expect("the composition's own connection string");
        assert!(database.uses_unix_socket());
        assert_eq!(database.tcp_address(), None);
        let debug = format!("{database:?}");
        assert!(
            !debug.contains("cr+t") && !debug.contains("cr%2Bt"),
            "{debug}"
        );
        assert!(!debug.contains("calque_sync"), "{debug}");

        // The credential reaches the PostgreSQL client as a credential, and the
        // endpoint is still the socket directory: this is the parse the driver
        // itself does, not a claim about the string.
        let configured: tokio_postgres::Config = database.expose().parse().unwrap();
        assert_eq!(configured.get_user(), Some("calque_sync"));
        // A space and a plus sign survive, because the guard writes `%20`
        // where form encoding would write `+`.
        assert_eq!(configured.get_password(), Some(b"s3 cr+t".as_slice()));
        assert_eq!(configured.get_dbname(), Some("calque"));
        assert_eq!(
            configured.get_hosts(),
            [tokio_postgres::config::Host::Unix(
                std::fs::canonicalize(directory.path()).unwrap()
            )]
        );
        assert!(matches!(
            configured.get_ssl_mode(),
            tokio_postgres::config::SslMode::Disable
        ));

        // And the two keys are the socket form's alone. A TCP host already has
        // userinfo, so a second way to write a credential is refused rather
        // than silently preferred one way or the other -- as is an empty one,
        // a repeated one, and every other libpq key.
        for refused in [
            "postgresql://127.0.0.1/app?user=calque_sync",
            "postgresql://127.0.0.1/app?password=s3cr3t",
            "postgresql://calque_sync@127.0.0.1/app?password=s3cr3t",
        ] {
            assert!(DatabaseUrl::parse(refused).is_err(), "{refused}");
        }
        for refused in [
            format!("postgresql:///app?host={encoded}&user="),
            format!("postgresql:///app?host={encoded}&password="),
            format!("postgresql:///app?host={encoded}&user=a&user=b"),
            format!("postgresql:///app?host={encoded}&password=a&password=b"),
            format!("postgresql:///app?host={encoded}&username=a"),
            "postgresql:///app?user=calque_sync&password=s3cr3t".to_owned(),
        ] {
            assert!(DatabaseUrl::parse(&refused).is_err(), "{refused}");
        }
    }

    /// tokio-postgres ends the credentials at the first `@` anywhere in the
    /// URL. A database name holding one still reaches the driver as that name,
    /// on the host the guard approved, for a TCP host and for a socket.
    #[test]
    fn an_at_sign_in_the_database_name_names_no_other_host() {
        let directory = tempfile::tempdir().unwrap();
        let encoded = directory.path().to_str().unwrap().replace('/', "%2F");
        for (url, host) in [
            (
                "postgresql://calque_sync:secret@127.0.0.1/app@evil.example".to_owned(),
                tokio_postgres::config::Host::Tcp("127.0.0.1".to_owned()),
            ),
            (
                format!(
                    "postgresql:///app@evil.example?host={encoded}&user=calque_sync&password=secret"
                ),
                tokio_postgres::config::Host::Unix(
                    std::fs::canonicalize(directory.path()).unwrap(),
                ),
            ),
        ] {
            let database = DatabaseUrl::parse(&url).expect("a database name with an @");
            let configured: tokio_postgres::Config = database.expose().parse().unwrap();
            assert_eq!(configured.get_hosts(), [host]);
            assert_eq!(configured.get_ports(), [POSTGRES_PORT]);
            assert_eq!(configured.get_dbname(), Some("app@evil.example"));
            assert_eq!(configured.get_user(), Some("calque_sync"));
        }
    }

    #[test]
    fn http_host_checks_fail_closed() {
        let binding = HttpBinding::loopback(8787).unwrap();
        assert_eq!(binding.socket_address().to_string(), "127.0.0.1:8787");
        assert!(binding.is_loopback_only());
        binding.validate_host_header("127.0.0.1:8787").unwrap();
        assert!(binding.validate_host_header("localhost:8787").is_err());
        assert!(binding.validate_host_header("127.0.0.1:9999").is_err());
    }

    /// A composition reaches the container on an address it does not bind, so
    /// the bind address is configuration. The `Host` check is not: it stays
    /// one byte-equal value, and binding every interface does not widen it.
    #[test]
    fn a_configured_bind_address_does_not_widen_the_host_check() {
        let binding = HttpBinding::configured(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            8787,
            Some("calque.internal:8787"),
        )
        .unwrap();
        assert_eq!(binding.socket_address().to_string(), "0.0.0.0:8787");
        assert!(!binding.is_loopback_only());
        binding
            .validate_host_header("calque.internal:8787")
            .unwrap();
        for refused in [
            "0.0.0.0:8787",
            "calque.internal",
            "CALQUE.INTERNAL:8787",
            "evil.calque.internal:8787",
            "calque.internal:8787.",
            "calque.internal:9999",
            "127.0.0.1:8787",
        ] {
            assert!(binding.validate_host_header(refused).is_err(), "{refused}");
        }
    }

    /// A wildcard bind with no expected host would have nothing meaningful to
    /// compare against, so it is refused rather than defaulted.
    #[test]
    fn a_wildcard_bind_without_an_expected_host_is_refused() {
        for address in [
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        ] {
            assert!(matches!(
                HttpBinding::configured(address, 8787, None),
                Err(GuardError::HttpExpectedHostRequired)
            ));
        }
        assert!(matches!(
            HttpBinding::configured(IpAddr::V4(Ipv4Addr::LOCALHOST), 0, Some("calque:1")),
            Err(GuardError::HttpPortZero)
        ));
        for malformed in [
            "http://calque.internal:8787",
            "calque.internal:8787/v1",
            "calque internal",
            "0.0.0.0:8787",
            "[::]:8787",
            "",
            ".calque.internal",
            "calque.internal:0",
            "calque.internal:70000",
            // Bracketed, so it claims to be an IPv6 literal, and is not one.
            // An operator must not be told any of these was validated.
            "[:::::]",
            "[::::]:8787",
            "[a:b]",
            "[]",
            "[]:8787",
        ] {
            assert!(
                HttpBinding::configured(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787, Some(malformed))
                    .is_err(),
                "{malformed}"
            );
        }
        // A concrete address with no expected host answers that address.
        assert_eq!(
            HttpBinding::configured(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4)), 8787, None)
                .unwrap()
                .expected_host(),
            "10.0.0.4:8787"
        );
        // A bracketed IPv6 literal is the only form whose colons are not a port
        // separator, so it is accepted only bracketed.
        assert_eq!(
            HttpBinding::configured(
                IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
                8787,
                Some("[fd00::1]:8787")
            )
            .unwrap()
            .expected_host(),
            "[fd00::1]:8787"
        );
        assert!(HttpBinding::configured(
            IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            8787,
            Some("fd00::1:8787")
        )
        .is_err());
    }

    #[test]
    fn every_libpq_routing_environment_override_is_rejected_without_reading_values() {
        let database = DatabaseUrl::parse("postgresql://127.0.0.1/app").unwrap();
        database.validate_libpq_environment_with(|_| None).unwrap();
        for expected in LIBPQ_ROUTING_ENVIRONMENT {
            let error = database
                .validate_libpq_environment_with(|name| {
                    (name == *expected).then(|| OsString::from("hostile-value"))
                })
                .unwrap_err();
            assert!(matches!(
                error,
                GuardError::DatabaseRoutingEnvironment(actual) if actual == *expected
            ));
            assert!(!error.to_string().contains("hostile-value"));
        }
    }

    #[test]
    fn every_app_owned_postgres_family_receives_the_same_normalized_authority() {
        let database = DatabaseUrl::parse("postgresql://127.0.0.1/app").unwrap();
        let expected = database.expose();
        assert_eq!(ALL_POSTGRES_CONNECTION_FAMILIES.len(), 18);
        for family in ALL_POSTGRES_CONNECTION_FAMILIES {
            assert_eq!(database.connection_url(*family), expected, "{family:?}");
        }
    }

    #[test]
    fn origins_are_exact_and_loopback_only() {
        assert_eq!(
            AllowedOrigin::parse("http://127.0.0.1:5173")
                .unwrap()
                .as_str(),
            "http://127.0.0.1:5173"
        );
        assert!(AllowedOrigin::parse("https://localhost:5173").is_ok());
        for invalid in [
            "http://example.com:5173",
            "https://app.example.com",
            "http://127.0.0.1:5173/path",
            "http://user@127.0.0.1:5173",
            "file:///tmp/app",
        ] {
            assert!(AllowedOrigin::parse(invalid).is_err(), "{invalid}");
        }
    }

    /// `serve` behind a TLS proxy admits the application's own HTTPS origin,
    /// and still refuses a cleartext page from anywhere but loopback.
    #[test]
    fn served_origins_are_https_or_loopback() {
        for (value, origin) in [
            ("https://app.example.com", "https://app.example.com"),
            ("https://app.example.com:443/", "https://app.example.com"),
            (
                "https://app.example.com:8443",
                "https://app.example.com:8443",
            ),
            ("https://203.0.113.9", "https://203.0.113.9"),
            ("http://127.0.0.1:5173", "http://127.0.0.1:5173"),
            ("http://localhost:5173", "http://localhost:5173"),
            ("http://[::1]:5173", "http://[::1]:5173"),
        ] {
            assert_eq!(
                AllowedOrigin::parse_https_or_loopback(value)
                    .unwrap()
                    .as_str(),
                origin,
                "{value}"
            );
        }
        for refused in [
            "http://app.example.com",
            "http://10.0.0.4:5173",
            "http://app.localhost:5173",
        ] {
            assert!(
                matches!(
                    AllowedOrigin::parse_https_or_loopback(refused),
                    Err(GuardError::OriginNotHttpsOrLoopback(_))
                ),
                "{refused}"
            );
        }
        for malformed in [
            "https://app.example.com/path",
            "https://app.example.com/?q",
            "https://app.example.com/#f",
            "https://user@app.example.com",
            "https://user:secret@app.example.com",
            "ws://app.example.com",
            "file:///tmp/app",
            "https:",
        ] {
            assert!(
                matches!(
                    AllowedOrigin::parse_https_or_loopback(malformed),
                    Err(GuardError::OriginMalformed)
                ),
                "{malformed}"
            );
        }
    }
}

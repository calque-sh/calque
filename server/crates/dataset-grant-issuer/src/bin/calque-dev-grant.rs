//! The development grant minter: a development application backend in one
//! command, since `calque serve` verifies grants and can mint none.
//!
//! ```text
//! calque-dev-grant --key-file dev/grant-key.pk8 --key-id dev-key \
//!   --audience incident.test --generation 1 --subject alice \
//!   --lifetime-seconds 900 --attribute organization_id=uuid:0a00…01 \
//!   --command tasks=update
//! ```
//!
//! It prints one JSON document: the token, when it expires, and the
//! verification-key environment the server must be started with. The key file
//! is created on first use and reused afterwards, so the same key survives a
//! restart of whatever is driving it.
//!
//! Development only. It writes a private key to a path a caller names and
//! takes every claim from the command line without checking any of them
//! against an application's own authorization; in production that check is
//! the application backend's job.

use std::{
    collections::BTreeMap,
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::ExitCode,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::Parser;
use dataset_grant::{GrantAttributeValue, GrantCommandCapability, GrantCommandOperation};
use dataset_grant_issuer::GrantIssuer;

/// Mint a development dataset grant and print it, with the verification-key
/// environment `calque serve` needs, as one JSON document.
#[derive(Debug, Parser)]
#[command(name = "calque-dev-grant")]
struct Cli {
    /// PKCS#8 signing key. Created owner-read-only on first use and reused
    /// afterwards, so the server keeps verifying what later runs mint.
    #[arg(long, value_name = "PATH")]
    key_file: PathBuf,
    /// Key id the grant and the printed verification key carry.
    #[arg(long, value_name = "ID")]
    key_id: String,
    /// Audience `calque serve` was started with (`--grant-audience`).
    #[arg(long, value_name = "AUDIENCE")]
    audience: String,
    /// Subject the grant is issued to.
    #[arg(long, value_name = "SUBJECT")]
    subject: String,
    /// Authorization generation the grant is valid for.
    #[arg(long, value_name = "N")]
    generation: u64,
    /// How long the grant is valid, at most 900 seconds.
    #[arg(long, value_name = "SECONDS")]
    lifetime_seconds: u64,
    /// Grant attribute, as `<name>=<type>:<value>`, where the type is uuid,
    /// bytes, int8, int4, int2 or bool. Repeat for more than one.
    #[arg(long = "attribute", value_name = "NAME=TYPE:VALUE", value_parser = parse_attribute)]
    attributes: Vec<(String, GrantAttributeValue)>,
    /// Write capability, as `<collection>=<operation>[+<operation>...]` with
    /// insert, update or delete. Repeat per collection; without one the grant
    /// is read-only.
    #[arg(long = "command", value_name = "COLLECTION=OPERATIONS", value_parser = parse_command)]
    commands: Vec<GrantCommandCapability>,
}

fn main() -> ExitCode {
    match Cli::parse().arguments().and_then(mint) {
        Ok(document) => {
            println!("{document}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("calque-dev-grant: {message}");
            ExitCode::FAILURE
        }
    }
}

struct Arguments {
    key_file: PathBuf,
    key_id: String,
    audience: String,
    subject: String,
    generation: u64,
    lifetime_seconds: u64,
    attributes: BTreeMap<String, GrantAttributeValue>,
    /// The command capability, empty unless `--command` was given. Empty is a
    /// read-only grant.
    commands: Vec<GrantCommandCapability>,
}

impl Cli {
    /// Refuses what clap cannot see one value at a time: an attribute or a
    /// collection named twice.
    fn arguments(self) -> Result<Arguments, String> {
        let mut attributes = BTreeMap::new();
        for (name, attribute) in self.attributes {
            if attributes.insert(name.clone(), attribute).is_some() {
                return Err(format!("--attribute {name} was given twice"));
            }
        }
        for (index, capability) in self.commands.iter().enumerate() {
            if self.commands[..index]
                .iter()
                .any(|existing| existing.collection == capability.collection)
            {
                return Err(format!("--command names {} twice", capability.collection));
            }
        }
        Ok(Arguments {
            key_file: self.key_file,
            key_id: self.key_id,
            audience: self.audience,
            subject: self.subject,
            generation: self.generation,
            lifetime_seconds: self.lifetime_seconds,
            attributes,
            commands: self.commands,
        })
    }
}

/// The mint path, apart from reading the process arguments, so a test can
/// reach it.
fn mint(arguments: Arguments) -> Result<String, String> {
    let issuer =
        load_or_create_issuer(&arguments.key_file, &arguments.key_id, &arguments.audience)?;
    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "the system clock is before the Unix epoch".to_owned())?
        .as_secs();
    let rendered_commands = arguments
        .commands
        .iter()
        .map(|capability| {
            format!(
                "{}:{}",
                capability.collection,
                capability
                    .operations
                    .iter()
                    .map(|operation| operation.as_str())
                    .collect::<Vec<_>>()
                    .join("+")
            )
        })
        .collect::<Vec<_>>();
    let token = issuer
        .issue_with_commands(
            arguments.subject.clone(),
            arguments.attributes,
            arguments.commands,
            arguments.generation,
            issued_at,
            arguments.lifetime_seconds,
        )
        .map_err(|error| error.to_string())?;
    let environment = issuer
        .verification_key_environment()
        .map_err(|error| error.to_string())?;
    let environment: serde_json::Value =
        serde_json::from_str(&environment).map_err(|error| error.to_string())?;
    serde_json::to_string_pretty(&serde_json::json!({
        "subject": arguments.subject,
        "token": token,
        "issuedAtUnixSeconds": issued_at,
        "expiresAtUnixSeconds": issued_at + arguments.lifetime_seconds,
        "authorizationGeneration": arguments.generation,
        "commands": rendered_commands,
        "verificationKeyEnvironment": environment,
    }))
    .map_err(|error| error.to_string())
}

/// Reads the PKCS#8 document at `path`, creating one there if it is absent.
///
/// Written owner-read-only, and never overwritten: a caller that reruns the
/// command keeps minting under the key the running server was configured with.
fn load_or_create_issuer(path: &Path, key_id: &str, audience: &str) -> Result<GrantIssuer, String> {
    match fs::read(path) {
        Ok(pkcs8) => {
            return GrantIssuer::from_pkcs8(key_id, audience, &pkcs8)
                .map_err(|error| format!("{}: {error}", path.display()));
        }
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            return Err(format!("{}: {error}", path.display()));
        }
        Err(_) => {}
    }
    let (issuer, pkcs8) =
        GrantIssuer::generate(key_id, audience).map_err(|error| error.to_string())?;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|error| format!("{}: {error}", parent.display()))?;
        }
    }
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    file.write_all(&pkcs8)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    file.sync_all()
        .map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(issuer)
}

/// `<collection>=<operation>[+<operation>…]`. There is no `*`: an operator
/// that means all three operations writes all three.
fn parse_command(argument: &str) -> Result<GrantCommandCapability, String> {
    let (collection, operations) = argument
        .split_once('=')
        .ok_or_else(|| format!("--command {argument} is not <collection>=<operation>[+…]"))?;
    if collection.is_empty() {
        return Err("--command needs a collection".to_owned());
    }
    let mut parsed = Vec::new();
    for operation in operations.split('+') {
        let operation = match operation {
            "insert" => GrantCommandOperation::Insert,
            "update" => GrantCommandOperation::Update,
            "delete" => GrantCommandOperation::Delete,
            other => {
                return Err(format!(
                    "--command {collection} has unknown operation {other}"
                ))
            }
        };
        if parsed.contains(&operation) {
            return Err(format!(
                "--command {collection} repeats {}",
                operation.as_str()
            ));
        }
        parsed.push(operation);
    }
    Ok(GrantCommandCapability {
        collection: collection.to_owned(),
        operations: parsed,
    })
}

/// `<name>=<type>:<value>`, in the same typed vocabulary a registration file
/// uses, so a grant and the registration it must match parse to one value.
fn parse_attribute(argument: &str) -> Result<(String, GrantAttributeValue), String> {
    let (name, typed) = argument
        .split_once('=')
        .ok_or_else(|| format!("--attribute {argument} is not <name>=<type>:<value>"))?;
    let (kind, value) = typed
        .split_once(':')
        .ok_or_else(|| format!("--attribute {argument} is not <name>=<type>:<value>"))?;
    let attribute = match kind {
        "uuid" => GrantAttributeValue::Uuid(value.to_owned()),
        "bytes" => GrantAttributeValue::Bytes(value.to_owned()),
        "int8" => GrantAttributeValue::Int8(value.to_owned()),
        "int4" => GrantAttributeValue::Int4(
            value
                .parse()
                .map_err(|_| format!("--attribute {name} is not an int4"))?,
        ),
        "int2" => GrantAttributeValue::Int2(
            value
                .parse()
                .map_err(|_| format!("--attribute {name} is not an int2"))?,
        ),
        "bool" => GrantAttributeValue::Bool(
            value
                .parse()
                .map_err(|_| format!("--attribute {name} is not a bool"))?,
        ),
        other => return Err(format!("--attribute {name} has unknown type {other}")),
    };
    if name.is_empty() {
        return Err("--attribute needs a name".to_owned());
    }
    Ok((name.to_owned(), attribute))
}

#[cfg(test)]
mod tests {
    use clap::{error::ErrorKind, CommandFactory as _};
    use dataset_grant::MAX_GRANT_LIFETIME_SECONDS;

    use super::*;

    /// `main` without the process: clap's parse, then the duplicate checks.
    fn parse(arguments: impl Iterator<Item = String>) -> Result<Arguments, String> {
        Cli::try_parse_from(std::iter::once("calque-dev-grant".to_owned()).chain(arguments))
            .map_err(|error| error.to_string())?
            .arguments()
    }

    fn run_with(arguments: impl Iterator<Item = String>) -> Result<String, String> {
        parse(arguments).and_then(mint)
    }

    #[test]
    fn command_definition_is_valid() {
        Cli::command().debug_assert();
    }

    /// `--help` is a usage text on stdout, not a flag waiting for its value,
    /// and it states the lifetime cap the grant format enforces.
    #[test]
    fn help_describes_every_flag() {
        let error = Cli::try_parse_from(["calque-dev-grant", "--help"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DisplayHelp);
        let help = error.to_string();
        assert!(help.contains("Usage: calque-dev-grant"), "{help}");
        for flag in [
            "--key-file",
            "--key-id",
            "--audience",
            "--subject",
            "--generation",
            "--lifetime-seconds",
            "--attribute",
            "--command",
        ] {
            assert!(help.contains(flag), "{flag}: {help}");
        }
        assert!(
            help.contains(&format!("at most {MAX_GRANT_LIFETIME_SECONDS} seconds")),
            "{help}"
        );
    }

    fn arguments(extra: &[&str]) -> Vec<String> {
        let mut all = vec![
            "--key-file",
            "key.pk8",
            "--key-id",
            "dev-key",
            "--audience",
            "incident.test",
            "--subject",
            "alice",
            "--generation",
            "1",
            "--lifetime-seconds",
            "900",
        ];
        all.extend_from_slice(extra);
        all.into_iter().map(str::to_owned).collect()
    }

    /// The key file is created, the capability reaches the signed payload, and
    /// the document an operator reads names the same capability the token
    /// carries, checked through the real `GrantVerifier`.
    #[test]
    fn the_mint_path_signs_the_capability_it_was_asked_for() {
        let directory = tempfile::tempdir().unwrap();
        let key_file = directory.path().join("nested").join("grant-key.pk8");
        let mint = |extra: &[&str]| {
            let mut all = vec![
                "--key-file".to_owned(),
                key_file.to_str().unwrap().to_owned(),
                "--key-id".to_owned(),
                "dev-key".to_owned(),
                "--audience".to_owned(),
                "incident.test".to_owned(),
                "--subject".to_owned(),
                "alice".to_owned(),
                "--generation".to_owned(),
                "4".to_owned(),
                "--lifetime-seconds".to_owned(),
                "900".to_owned(),
            ];
            all.extend(extra.iter().map(|value| (*value).to_owned()));
            let document = run_with(all.into_iter()).unwrap();
            serde_json::from_str::<serde_json::Value>(&document).unwrap()
        };

        let capable = mint(&[
            "--command",
            "tasks=update+delete",
            "--command",
            "notes=insert",
        ]);
        assert!(key_file.exists(), "the mint path creates its own key file");
        assert_eq!(
            capable["commands"],
            serde_json::json!(["tasks:update+delete", "notes:insert"])
        );

        // The verification key the document prints, read back from the file the
        // run wrote, verifies the token printed beside it.
        let persisted =
            GrantIssuer::from_pkcs8("dev-key", "incident.test", &fs::read(&key_file).unwrap())
                .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                &persisted.verification_key_environment().unwrap()
            )
            .unwrap(),
            capable["verificationKeyEnvironment"],
            "the document does not print the key the run signed with"
        );
        let verifier = dataset_grant::GrantVerifier::new(
            "incident.test",
            [persisted.verification_key().unwrap()],
        )
        .unwrap();
        let token = capable["token"].as_str().unwrap();
        let verified = verifier
            .verify(&dataset_grant::GrantPresentation {
                token,
                now_unix_seconds: capable["issuedAtUnixSeconds"].as_u64().unwrap(),
                authorization_generation: 4,
            })
            .unwrap();
        let commands = verified.commands();
        assert!(commands.permits("tasks", GrantCommandOperation::Update));
        assert!(commands.permits("tasks", GrantCommandOperation::Delete));
        assert!(!commands.permits("tasks", GrantCommandOperation::Insert));
        assert!(commands.permits("notes", GrantCommandOperation::Insert));
        assert!(!commands.permits("notes", GrantCommandOperation::Update));

        // Rerunning keeps the same key, which the server was started with, and
        // without `--command` the same command mints a read-only grant.
        let read_only = mint(&[]);
        assert_eq!(read_only["commands"], serde_json::json!([]));
        assert_eq!(
            read_only["verificationKeyEnvironment"], capable["verificationKeyEnvironment"],
            "a rerun rotated the signing key"
        );
        assert!(verifier
            .verify(&dataset_grant::GrantPresentation {
                token: read_only["token"].as_str().unwrap(),
                now_unix_seconds: read_only["issuedAtUnixSeconds"].as_u64().unwrap(),
                authorization_generation: 4,
            })
            .unwrap()
            .commands()
            .is_empty());
    }

    #[test]
    fn attributes_parse_into_the_registration_vocabulary() {
        let parsed = parse(
            arguments(&[
                "--attribute",
                "organization_id=uuid:0a000000000000000000000000000001",
                "--attribute",
                "tenant=int8:42",
                "--attribute",
                "active=bool:true",
            ])
            .into_iter(),
        )
        .unwrap();
        assert!(matches!(
            parsed.attributes.get("organization_id"),
            Some(GrantAttributeValue::Uuid(value))
                if value == "0a000000000000000000000000000001"
        ));
        assert!(matches!(
            parsed.attributes.get("tenant"),
            Some(GrantAttributeValue::Int8(value)) if value == "42"
        ));
        assert!(matches!(
            parsed.attributes.get("active"),
            Some(GrantAttributeValue::Bool(true))
        ));
        assert_eq!(parsed.generation, 1);
        assert_eq!(parsed.lifetime_seconds, 900);
    }

    /// A misspelled type or a repeated attribute is refused rather than minted
    /// into a token that authorizes something the caller did not ask for.
    #[test]
    fn a_malformed_or_repeated_attribute_is_refused() {
        for bad in [
            "organization_id",
            "organization_id=0a00",
            "organization_id=guid:0a00",
            "=uuid:0a00",
        ] {
            assert!(
                parse(arguments(&["--attribute", bad]).into_iter()).is_err(),
                "{bad}"
            );
        }
        assert!(parse(
            arguments(&[
                "--attribute",
                "organization_id=uuid:0a00",
                "--attribute",
                "organization_id=uuid:0b00",
            ])
            .into_iter()
        )
        .is_err());
    }

    /// A capability is enumerated and closed: no wildcard, no unknown
    /// operation, no collection twice.
    #[test]
    fn command_capabilities_parse_into_the_grant_vocabulary() {
        let parsed = parse(
            arguments(&[
                "--command",
                "tasks=update",
                "--command",
                "notes=insert+delete",
            ])
            .into_iter(),
        )
        .unwrap();
        assert_eq!(parsed.commands.len(), 2);
        assert_eq!(parsed.commands[0].collection, "tasks");
        assert_eq!(
            parsed.commands[0].operations,
            vec![GrantCommandOperation::Update]
        );
        assert_eq!(
            parsed.commands[1].operations,
            vec![GrantCommandOperation::Insert, GrantCommandOperation::Delete]
        );

        // The default is a read-only grant.
        assert!(parse(arguments(&[]).into_iter())
            .unwrap()
            .commands
            .is_empty());

        for bad in [
            "tasks",
            "tasks=",
            "tasks=*",
            "tasks=truncate",
            "tasks=update+update",
            "=update",
        ] {
            assert!(
                parse(arguments(&["--command", bad]).into_iter()).is_err(),
                "{bad}"
            );
        }
        assert!(parse(
            arguments(&["--command", "tasks=update", "--command", "tasks=delete"]).into_iter()
        )
        .is_err());
    }

    #[test]
    fn every_required_argument_is_required() {
        assert!(parse(arguments(&[]).into_iter()).is_ok());
        for flag in [
            "--key-file",
            "--key-id",
            "--audience",
            "--subject",
            "--generation",
            "--lifetime-seconds",
        ] {
            let all = arguments(&[]);
            let index = all.iter().position(|value| value == flag).unwrap();
            let mut without = all.clone();
            without.drain(index..index + 2);
            assert!(parse(without.into_iter()).is_err(), "{flag}");
        }
        assert!(parse(arguments(&["--unknown", "x"]).into_iter()).is_err());
    }

    /// The key file is the private key, and a second run must not replace the
    /// one a running server was configured to verify against.
    #[test]
    fn the_key_file_is_created_once_and_reused() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/grant-key.pk8");
        let first = load_or_create_issuer(&path, "dev-key", "incident.test").unwrap();
        let bytes = fs::read(&path).unwrap();
        let second = load_or_create_issuer(&path, "dev-key", "incident.test").unwrap();
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(
            first.verification_key_environment().unwrap(),
            second.verification_key_environment().unwrap()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    /// A key path that cannot be read for a reason other than absence is
    /// reported with that reason, and no new key is written in its place.
    #[test]
    fn an_unreadable_key_path_is_reported_rather_than_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let Err(error) = load_or_create_issuer(directory.path(), "dev-key", "incident.test") else {
            panic!("a directory was read as a key file");
        };
        let cause = fs::read(directory.path()).unwrap_err();
        assert_eq!(error, format!("{}: {cause}", directory.path().display()));
    }
}

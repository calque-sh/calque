use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use source_control::{QualifiedTable, SchemaRegistry, SourceIdentity};
use sync_types::{SourceEpoch, SourceId};
use thiserror::Error;

use crate::HttpBinding;

#[cfg(unix)]
use std::os::unix::{
    ffi::OsStrExt,
    fs::{OpenOptionsExt, PermissionsExt},
};

pub(crate) const STATE_FILE: &str = "state.json";
pub(crate) const REGISTRY_FILE: &str = "registry.json";
pub(crate) const SECRETS_FILE: &str = "secrets.json";
pub(crate) const CLIENT_ENV_FILE: &str = "client.env";
pub(crate) const SNAPSHOT_BUILD_DIRECTORY: &str = "builds";
pub(crate) const SNAPSHOT_JOURNAL_DIRECTORY: &str = "journals";
pub(crate) const SNAPSHOT_WORKSPACE_DIRECTORY: &str = "workspaces";
pub(crate) const RECUT_JOURNAL_DIRECTORY: &str = "recut-journals";
const STATE_FORMAT_VERSION: u32 = 1;
const SECRETS_FORMAT_VERSION: u32 = 3;
const MAX_STATE_FILE_BYTES: usize = 1024 * 1024;
const MAX_REGISTRY_FILE_BYTES: usize = 16 * 1024 * 1024;
const SECRET_BYTES: usize = 32;
const MAX_STATE_TABLES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocalNames {
    pub source_id: SourceId,
    pub publication_name: String,
    pub slot_name: String,
}

impl LocalNames {
    pub fn derive(state_root: &Path, identity: &SourceIdentity) -> Result<Self, StateError> {
        let canonical = state_root.canonicalize().map_err(io_at(state_root))?;
        let mut digest = Sha256::new();
        digest.update(b"calque.local-identifiers.v1\0");
        update_path_identity(&mut digest, &canonical)?;
        digest.update([0]);
        digest.update(identity.system_identifier.as_bytes());
        digest.update([0]);
        digest.update(identity.database_name.as_bytes());
        digest.update(identity.timeline_id.to_be_bytes());
        digest.update(identity.previous_timeline_id.to_be_bytes());
        let digest = hex::encode(digest.finalize());
        let suffix = &digest[..24];
        Ok(Self {
            source_id: SourceId::new(format!("local:{suffix}"))?,
            publication_name: format!("calque_{suffix}"),
            slot_name: format!("calque_{suffix}"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocalState {
    format_version: u32,
    pub source_identity: SourceIdentity,
    pub source_epoch: SourceEpoch,
    pub names: LocalNames,
    pub tables: Vec<QualifiedTable>,
}

impl LocalState {
    pub fn new(
        source_identity: SourceIdentity,
        source_epoch: SourceEpoch,
        names: LocalNames,
        mut tables: Vec<QualifiedTable>,
    ) -> Result<Self, StateError> {
        tables.sort();
        if tables.is_empty()
            || tables.len() > MAX_STATE_TABLES
            || tables.windows(2).any(|window| window[0] == window[1])
        {
            return Err(StateError::InvalidState("table set is empty or duplicate"));
        }
        Ok(Self {
            format_version: STATE_FORMAT_VERSION,
            source_identity,
            source_epoch,
            names,
            tables,
        })
    }

    fn validate(&self) -> Result<(), StateError> {
        if self.format_version != STATE_FORMAT_VERSION
            || self.tables.is_empty()
            || self.tables.len() > MAX_STATE_TABLES
            || self.tables.windows(2).any(|window| window[0] >= window[1])
        {
            return Err(StateError::InvalidState(
                "state format or table ordering is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SecretToken([u8; SECRET_BYTES]);

impl SecretToken {
    pub fn generate() -> Result<Self, StateError> {
        let mut bytes = [0_u8; SECRET_BYTES];
        getrandom::fill(&mut bytes).map_err(|_| StateError::RandomnessUnavailable)?;
        Ok(Self(bytes))
    }

    pub(crate) fn from_urlsafe(value: &str) -> Result<Self, StateError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| StateError::InvalidSecrets)?;
        let bytes: [u8; SECRET_BYTES] = bytes.try_into().map_err(|_| StateError::InvalidSecrets)?;
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; SECRET_BYTES] {
        &self.0
    }

    pub(crate) fn to_urlsafe(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretToken([REDACTED])")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct LocalSecrets {
    pub cursor_secret: SecretToken,
    pub proof_secret: SecretToken,
    pub plan_secret: SecretToken,
    pub bearer_token: SecretToken,
}

impl LocalSecrets {
    pub fn generate() -> Result<Self, StateError> {
        Ok(Self {
            cursor_secret: SecretToken::generate()?,
            proof_secret: SecretToken::generate()?,
            plan_secret: SecretToken::generate()?,
            bearer_token: SecretToken::generate()?,
        })
    }
}

impl fmt::Debug for LocalSecrets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalSecrets")
            .field("redacted", &true)
            .finish()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SecretsEnvelope {
    format_version: u32,
    cursor_secret: String,
    proof_secret: String,
    plan_secret: String,
    bearer_token: String,
}

impl From<&LocalSecrets> for SecretsEnvelope {
    fn from(secrets: &LocalSecrets) -> Self {
        Self {
            format_version: SECRETS_FORMAT_VERSION,
            cursor_secret: secrets.cursor_secret.to_urlsafe(),
            proof_secret: secrets.proof_secret.to_urlsafe(),
            plan_secret: secrets.plan_secret.to_urlsafe(),
            bearer_token: secrets.bearer_token.to_urlsafe(),
        }
    }
}

impl TryFrom<SecretsEnvelope> for LocalSecrets {
    type Error = StateError;

    fn try_from(value: SecretsEnvelope) -> Result<Self, Self::Error> {
        if value.format_version != SECRETS_FORMAT_VERSION {
            return Err(StateError::InvalidSecrets);
        }
        Ok(Self {
            cursor_secret: SecretToken::from_urlsafe(&value.cursor_secret)?,
            proof_secret: SecretToken::from_urlsafe(&value.proof_secret)?,
            plan_secret: SecretToken::from_urlsafe(&value.plan_secret)?,
            bearer_token: SecretToken::from_urlsafe(&value.bearer_token)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct StateStore {
    root: PathBuf,
}

impl StateStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StateError> {
        let path = path.as_ref();
        match path.symlink_metadata() {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(StateError::Symlink(path.to_path_buf()));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(StateError::NotDirectory(path.to_path_buf()));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(path).map_err(io_at(path))?;
            }
            Err(error) => return Err(io_at(path)(error)),
        }
        set_directory_permissions(path)?;
        let root = path.canonicalize().map_err(io_at(path))?;
        for directory in [
            root.join("objects"),
            root.join(SNAPSHOT_BUILD_DIRECTORY),
            snapshot_journal_root(&root),
            recut_journal_root(&root),
            snapshot_workspace_root(&root),
        ] {
            match directory.symlink_metadata() {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(StateError::Symlink(directory));
                }
                Ok(metadata) if !metadata.is_dir() => {
                    return Err(StateError::NotDirectory(directory));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&directory).map_err(io_at(&directory))?;
                }
                Err(error) => return Err(io_at(&directory)(error)),
            }
            set_directory_permissions(&directory)?;
        }
        sync_directory(&root.join(SNAPSHOT_BUILD_DIRECTORY))?;
        sync_directory(&root)?;
        Ok(Self { root })
    }

    /// Opens a state directory without creating files, child directories, or
    /// repairing permissions. Lifecycle inspection and deletion use this path
    /// so a failed read cannot turn a missing or incomplete input into state.
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, StateError> {
        let path = path.as_ref();
        match path.symlink_metadata() {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(StateError::Symlink(path.to_path_buf()));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(StateError::NotDirectory(path.to_path_buf()));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(StateError::Missing(path.to_path_buf()));
            }
            Err(error) => return Err(io_at(path)(error)),
        }
        Ok(Self {
            root: path.canonicalize().map_err(io_at(path))?,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn state_path(&self) -> PathBuf {
        self.root.join(STATE_FILE)
    }

    pub fn registry_path(&self) -> PathBuf {
        self.root.join(REGISTRY_FILE)
    }

    pub(crate) fn secrets_path(&self) -> PathBuf {
        self.root.join(SECRETS_FILE)
    }

    pub fn client_env_path(&self) -> PathBuf {
        self.root.join(CLIENT_ENV_FILE)
    }

    pub fn write_state(&self, state: &LocalState) -> Result<(), StateError> {
        state.validate()?;
        let bytes = serde_json::to_vec_pretty(state)?;
        enforce_limit(bytes.len(), MAX_STATE_FILE_BYTES)?;
        self.atomic_replace(STATE_FILE, &bytes)
    }

    pub fn read_state(&self) -> Result<LocalState, StateError> {
        let path = self.state_path();
        let bytes = self.read_private_file(STATE_FILE, MAX_STATE_FILE_BYTES)?;
        let state: LocalState = serde_json::from_slice(&bytes)
            .map_err(|error| in_file(&path)(StateError::Decode(error)))?;
        state.validate().map_err(in_file(&path))?;
        Ok(state)
    }

    pub fn write_registry(&self, registry: &SchemaRegistry) -> Result<(), StateError> {
        let bytes = registry.encode()?;
        enforce_limit(bytes.len(), MAX_REGISTRY_FILE_BYTES)?;
        self.atomic_replace(REGISTRY_FILE, &bytes)
    }

    pub fn read_registry(&self) -> Result<SchemaRegistry, StateError> {
        let bytes = self.read_private_file(REGISTRY_FILE, MAX_REGISTRY_FILE_BYTES)?;
        SchemaRegistry::decode(&bytes).map_err(|error| in_file(&self.registry_path())(error.into()))
    }

    pub fn load_or_create_secrets(&self) -> Result<LocalSecrets, StateError> {
        let destination = self.secrets_path();
        if destination.exists() {
            return self.read_secrets();
        }
        let secrets = LocalSecrets::generate()?;
        let bytes = serde_json::to_vec_pretty(&SecretsEnvelope::from(&secrets))?;
        let temporary = self.write_temporary(SECRETS_FILE, &bytes)?;
        match fs::hard_link(&temporary, &destination) {
            Ok(()) => {
                fs::remove_file(&temporary).map_err(io_at(&temporary))?;
                sync_directory(&self.root)?;
                Ok(secrets)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                fs::remove_file(&temporary).map_err(io_at(&temporary))?;
                self.read_secrets()
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                Err(io_at(&destination)(error))
            }
        }
    }

    pub(crate) fn read_secrets(&self) -> Result<LocalSecrets, StateError> {
        let path = self.secrets_path();
        let bytes = self.read_private_file(SECRETS_FILE, MAX_STATE_FILE_BYTES)?;
        let envelope: SecretsEnvelope = serde_json::from_slice(&bytes)
            .map_err(|error| in_file(&path)(StateError::Decode(error)))?;
        LocalSecrets::try_from(envelope).map_err(in_file(&path))
    }

    pub fn write_client_env(
        &self,
        binding: &HttpBinding,
        bearer_token: &SecretToken,
    ) -> Result<(), StateError> {
        let endpoint = format!("http://{}", binding.expected_host());
        let contents = format!(
            "CALQUE_ENDPOINT={endpoint}\nCALQUE_BEARER_TOKEN={}\n",
            bearer_token.to_urlsafe()
        );
        self.atomic_replace(CLIENT_ENV_FILE, contents.as_bytes())
    }

    fn atomic_replace(&self, name: &str, bytes: &[u8]) -> Result<(), StateError> {
        let temporary = self.write_temporary(name, bytes)?;
        let destination = self.root.join(name);
        if let Err(error) = fs::rename(&temporary, &destination) {
            let _ = fs::remove_file(&temporary);
            return Err(io_at(&destination)(error));
        }
        sync_directory(&self.root)
    }

    fn write_temporary(&self, name: &str, bytes: &[u8]) -> Result<PathBuf, StateError> {
        for _ in 0..16 {
            let mut nonce = [0_u8; 8];
            getrandom::fill(&mut nonce).map_err(|_| StateError::RandomnessUnavailable)?;
            let temporary = self
                .root
                .join(format!(".{name}.{}.tmp", hex::encode(nonce)));
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            match options.open(&temporary) {
                Ok(mut file) => {
                    file.write_all(bytes).map_err(io_at(&temporary))?;
                    file.sync_all().map_err(io_at(&temporary))?;
                    return Ok(temporary);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(io_at(&temporary)(error)),
            }
        }
        Err(StateError::TemporaryNameExhausted)
    }

    fn read_private_file(&self, name: &str, maximum: usize) -> Result<Vec<u8>, StateError> {
        let path = self.root.join(name);
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(o_nofollow());
        let file = options.open(&path).map_err(io_at(&path))?;
        let metadata = file.metadata().map_err(io_at(&path))?;
        if !metadata.is_file() {
            return Err(StateError::NotFile(path));
        }
        require_private_file_permissions(&path, &metadata)?;
        let length = usize::try_from(metadata.len()).map_err(|_| {
            in_file(&path)(StateError::TooLarge {
                actual: usize::MAX,
                maximum,
            })
        })?;
        enforce_limit(length, maximum).map_err(in_file(&path))?;
        let mut bytes = Vec::with_capacity(length);
        file.take((maximum + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(io_at(&path))?;
        enforce_limit(bytes.len(), maximum).map_err(in_file(&path))?;
        Ok(bytes)
    }
}

pub(crate) fn snapshot_journal_root(state_root: &Path) -> PathBuf {
    state_root
        .join(SNAPSHOT_BUILD_DIRECTORY)
        .join(SNAPSHOT_JOURNAL_DIRECTORY)
}

/// Where a re-cut journals its build: apart from the first cut's, which
/// startup recovers before anything is served.
pub(crate) fn recut_journal_root(state_root: &Path) -> PathBuf {
    state_root
        .join(SNAPSHOT_BUILD_DIRECTORY)
        .join(RECUT_JOURNAL_DIRECTORY)
}

pub(crate) fn snapshot_workspace_root(state_root: &Path) -> PathBuf {
    state_root
        .join(SNAPSHOT_BUILD_DIRECTORY)
        .join(SNAPSHOT_WORKSPACE_DIRECTORY)
}

#[cfg(unix)]
fn update_path_identity(digest: &mut Sha256, path: &Path) -> Result<(), StateError> {
    digest.update(path.as_os_str().as_bytes());
    Ok(())
}

#[cfg(not(unix))]
fn update_path_identity(digest: &mut Sha256, path: &Path) -> Result<(), StateError> {
    let value = path.to_str().ok_or(StateError::NonUtf8StatePath)?;
    digest.update(value.as_bytes());
    Ok(())
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
const fn o_nofollow() -> i32 {
    0x0000_0100
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const fn o_nofollow() -> i32 {
    0x0002_0000
}

#[cfg(all(
    unix,
    not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "linux",
        target_os = "android"
    ))
))]
compile_error!("calque-engine must define O_NOFOLLOW for this Unix target");

fn enforce_limit(actual: usize, maximum: usize) -> Result<(), StateError> {
    if actual > maximum {
        Err(StateError::TooLarge { actual, maximum })
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn set_directory_permissions(path: &Path) -> Result<(), StateError> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(io_at(path))
}

#[cfg(not(unix))]
fn set_directory_permissions(_path: &Path) -> Result<(), StateError> {
    Err(StateError::UnsupportedPlatform)
}

#[cfg(unix)]
fn require_private_file_permissions(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), StateError> {
    if metadata.permissions().mode() & 0o777 != 0o600 {
        Err(StateError::InsecurePermissions(path.to_path_buf()))
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
fn require_private_file_permissions(
    _path: &Path,
    _metadata: &fs::Metadata,
) -> Result<(), StateError> {
    Err(StateError::UnsupportedPlatform)
}

fn sync_directory(path: &Path) -> Result<(), StateError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(io_at(path))
}

/// Turns an I/O failure on `path` into a `StateError` that names it.
fn io_at(path: &Path) -> impl FnOnce(std::io::Error) -> StateError + '_ {
    move |source| StateError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Names `path` in a refusal of what it holds, which states only the reason.
fn in_file(path: &Path) -> impl FnOnce(StateError) -> StateError + '_ {
    move |reason| StateError::File {
        path: path.to_path_buf(),
        reason: Box::new(reason),
    }
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("local state I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("local state serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    /// What a state or secrets file holds is not JSON of its format.
    #[error("local state JSON is invalid: {0}")]
    Decode(serde_json::Error),
    /// A state file refused for its size or for holding something other than
    /// what Calque writes there.
    #[error("{path}: {reason}")]
    File {
        path: PathBuf,
        reason: Box<StateError>,
    },
    #[error("local state identifier is invalid: {0}")]
    Identifier(#[from] sync_types::IdentifierError),
    #[error("local state path cannot be a symbolic link: {0}")]
    Symlink(PathBuf),
    #[error("local state path is not a directory: {0}")]
    NotDirectory(PathBuf),
    #[error("local state directory does not exist: {0}")]
    Missing(PathBuf),
    #[error("local state path is not a regular file: {0}")]
    NotFile(PathBuf),
    #[error("local state file permissions are not 0600: {0}")]
    InsecurePermissions(PathBuf),
    #[error("local state file is {actual} bytes, exceeding {maximum}")]
    TooLarge { actual: usize, maximum: usize },
    #[error("local state is invalid: {0}")]
    InvalidState(&'static str),
    #[error("local secrets are invalid")]
    InvalidSecrets,
    #[error("secure randomness is unavailable")]
    RandomnessUnavailable,
    #[error("local state path is not valid UTF-8 on this platform")]
    NonUtf8StatePath,
    #[error("could not allocate a temporary state filename")]
    TemporaryNameExhausted,
    #[error("local state permissions are unsupported on this platform")]
    UnsupportedPlatform,
    #[error("schema registry is invalid: {0}")]
    Registry(#[from] source_control::SchemaRegistryError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        path.metadata().unwrap().permissions().mode() & 0o777
    }

    fn identity() -> SourceIdentity {
        SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        }
    }

    #[test]
    fn identifiers_are_deterministic_and_postgres_safe() {
        let temporary = tempfile::tempdir().unwrap();
        let store = StateStore::open(temporary.path().join("state")).unwrap();
        let first = LocalNames::derive(store.root(), &identity()).unwrap();
        let second = LocalNames::derive(store.root(), &identity()).unwrap();
        assert_eq!(first, second);
        assert!(first.publication_name.len() <= source_control::MAX_POSTGRES_IDENTIFIER_BYTES);
        assert!(first
            .publication_name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'));
    }

    #[test]
    fn opening_existing_state_never_creates_or_repairs_input() {
        let temporary = tempfile::tempdir().unwrap();
        let missing = temporary.path().join("missing");
        assert!(StateStore::open_existing(&missing).is_err());
        assert!(!missing.exists());

        let incomplete = temporary.path().join("incomplete");
        fs::create_dir(&incomplete).unwrap();
        assert!(StateStore::open_existing(&incomplete).is_ok());
        assert!(incomplete.is_dir());
        assert_eq!(fs::read_dir(&incomplete).unwrap().count(), 0);
    }

    #[test]
    fn state_registry_secrets_and_client_env_are_durable_and_private() {
        let temporary = tempfile::tempdir().unwrap();
        let store = StateStore::open(temporary.path().join("state")).unwrap();
        let names = LocalNames::derive(store.root(), &identity()).unwrap();
        let state = LocalState::new(
            identity(),
            SourceEpoch::new("epoch-1").unwrap(),
            names,
            vec![QualifiedTable {
                schema: "public".to_owned(),
                table: "tasks".to_owned(),
            }],
        )
        .unwrap();
        store.write_state(&state).unwrap();
        let registry = crate::test_support::registry();
        store.write_registry(&registry).unwrap();
        let secrets = store.load_or_create_secrets().unwrap();
        store
            .write_client_env(&HttpBinding::loopback(8787).unwrap(), &secrets.bearer_token)
            .unwrap();

        assert_eq!(store.read_state().unwrap(), state);
        assert_eq!(store.read_registry().unwrap(), registry);
        assert_eq!(store.read_secrets().unwrap(), secrets);
        assert_eq!(store.load_or_create_secrets().unwrap(), secrets);
        assert!(!format!("{secrets:?}").contains(&secrets.bearer_token.to_urlsafe()));
        let persisted_secrets = fs::read_to_string(store.secrets_path()).unwrap();
        assert!(!persisted_secrets.contains("artifactCapability"));
        let client_env = fs::read_to_string(store.client_env_path()).unwrap();
        let client_env_keys: Vec<&str> = client_env
            .lines()
            .map(|line| line.split_once('=').unwrap().0)
            .collect();
        assert_eq!(client_env_keys, ["CALQUE_ENDPOINT", "CALQUE_BEARER_TOKEN"]);

        #[cfg(unix)]
        {
            assert_eq!(mode(store.root()), 0o700);
            for path in [
                store.state_path(),
                store.registry_path(),
                store.secrets_path(),
                store.client_env_path(),
            ] {
                assert_eq!(mode(&path), 0o600, "{}", path.display());
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn insecure_or_symlinked_state_fails_closed() {
        let temporary = tempfile::tempdir().unwrap();
        let store = StateStore::open(temporary.path().join("state")).unwrap();
        fs::write(store.state_path(), b"{}").unwrap();
        fs::set_permissions(store.state_path(), fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            store.read_state(),
            Err(StateError::InsecurePermissions(_))
        ));

        let target = temporary.path().join("target");
        fs::create_dir(&target).unwrap();
        let link = temporary.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(matches!(
            StateStore::open(link),
            Err(StateError::Symlink(_))
        ));

        let child_root = temporary.path().join("child-state");
        fs::create_dir(&child_root).unwrap();
        std::os::unix::fs::symlink(&target, child_root.join("objects")).unwrap();
        assert!(matches!(
            StateStore::open(child_root),
            Err(StateError::Symlink(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn private_reads_do_not_follow_symlinks() {
        let temporary = tempfile::tempdir().unwrap();
        let store = StateStore::open(temporary.path().join("state")).unwrap();
        let target = temporary.path().join("outside-state");
        fs::write(&target, b"{}").unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        std::os::unix::fs::symlink(&target, store.state_path()).unwrap();
        assert!(store.read_state().is_err());
    }

    #[test]
    fn registry_storage_rejects_checksum_corruption() {
        let temporary = tempfile::tempdir().unwrap();
        let store = StateStore::open(temporary.path().join("state")).unwrap();
        store
            .write_registry(&crate::test_support::registry())
            .unwrap();
        let mut envelope: serde_json::Value =
            serde_json::from_slice(&fs::read(store.registry_path()).unwrap()).unwrap();
        envelope["registrySha256"] = serde_json::Value::String("0".repeat(64));
        fs::write(
            store.registry_path(),
            serde_json::to_vec(&envelope).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            store.read_registry(),
            Err(StateError::File { path, reason })
                if path == store.registry_path() && matches!(*reason, StateError::Registry(_))
        ));
    }

    #[cfg(unix)]
    fn write_private(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    /// A state or secrets file the store refuses is named, before the reason,
    /// whether it is too large, not JSON, or not what Calque writes there.
    #[cfg(unix)]
    #[test]
    fn a_refused_state_file_is_named_in_the_refusal() {
        let temporary = tempfile::tempdir().unwrap();
        let store = StateStore::open(temporary.path().join("state")).unwrap();
        let refusal = |path: PathBuf, reason: &str| format!("{}: {reason}", path.display());
        let not_json =
            "local state JSON is invalid: EOF while parsing an object at line 1 column 1";

        write_private(&store.state_path(), b"{");
        assert_eq!(
            store.read_state().unwrap_err().to_string(),
            refusal(store.state_path(), not_json)
        );
        let names = LocalNames::derive(store.root(), &identity()).unwrap();
        let state = LocalState::new(
            identity(),
            SourceEpoch::new("epoch-1").unwrap(),
            names,
            vec![QualifiedTable {
                schema: "public".to_owned(),
                table: "tasks".to_owned(),
            }],
        )
        .unwrap();
        let mut encoded = serde_json::to_value(&state).unwrap();
        encoded["formatVersion"] = (STATE_FORMAT_VERSION + 1).into();
        write_private(&store.state_path(), &serde_json::to_vec(&encoded).unwrap());
        assert_eq!(
            store.read_state().unwrap_err().to_string(),
            refusal(
                store.state_path(),
                "local state is invalid: state format or table ordering is invalid"
            )
        );
        write_private(&store.state_path(), &vec![b' '; MAX_STATE_FILE_BYTES + 1]);
        assert_eq!(
            store.read_state().unwrap_err().to_string(),
            refusal(
                store.state_path(),
                &format!(
                    "local state file is {} bytes, exceeding {MAX_STATE_FILE_BYTES}",
                    MAX_STATE_FILE_BYTES + 1
                )
            )
        );

        write_private(&store.secrets_path(), b"{");
        assert_eq!(
            store.read_secrets().unwrap_err().to_string(),
            refusal(store.secrets_path(), not_json)
        );
        let mut envelope =
            serde_json::to_value(SecretsEnvelope::from(&LocalSecrets::generate().unwrap()))
                .unwrap();
        envelope["bearerToken"] = "not-a-token".into();
        write_private(
            &store.secrets_path(),
            &serde_json::to_vec(&envelope).unwrap(),
        );
        assert_eq!(
            store.read_secrets().unwrap_err().to_string(),
            refusal(store.secrets_path(), "local secrets are invalid")
        );
        // An existing secrets file is read, never replaced, so it is refused
        // the same way.
        assert_eq!(
            store.load_or_create_secrets().unwrap_err().to_string(),
            refusal(store.secrets_path(), "local secrets are invalid")
        );
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_state_paths_have_distinct_identifier_inputs() {
        use std::os::unix::ffi::OsStringExt;

        let first = PathBuf::from(std::ffi::OsString::from_vec(vec![b'a', 0x80]));
        let second = PathBuf::from(std::ffi::OsString::from_vec(vec![b'a', 0x81]));
        let mut first_digest = Sha256::new();
        let mut second_digest = Sha256::new();
        update_path_identity(&mut first_digest, &first).unwrap();
        update_path_identity(&mut second_digest, &second).unwrap();
        assert_ne!(first_digest.finalize(), second_digest.finalize());
    }
}

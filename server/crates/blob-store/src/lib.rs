//! Provider-neutral immutable object storage and a crash-safe local adapter.
//!
//! Object IDs are semantic identifiers, never filesystem paths. The local
//! adapter maps them through SHA-256, stages verified bytes outside the visible
//! object namespace, then publishes with a no-replace hard link. Its `head`
//! reads an object's bytes again only when the file's stat identity has moved
//! since they were last hashed.

mod s3;

pub use s3::{S3BlobStore, S3BlobStoreConfig, S3Endpoint};

use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sync_types::ObjectId;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const OBJECT_ID_DOMAIN: &[u8] = b"calque.blob-store.object-path.v1\0";
const IO_BUFFER_BYTES: usize = 64 * 1024;
/// Hashing a file already on disk reads in larger pieces, on a blocking
/// thread: at 64 KiB through Tokio, each read is a thread-pool round trip.
const FILE_HASH_BUFFER_BYTES: usize = 1024 * 1024;
/// How far a file's recorded modification time must precede the read that
/// hashed it before that read's digest is trusted under the file's stat
/// identity. Linux stamps files from a clock that advances once per kernel
/// tick, at most 10 ms, and APFS to the nanosecond; this is ten times the
/// coarser of the two.
const TIMESTAMP_TICK: Duration = Duration::from_millis(100);
/// The same margin for a file whose timestamps are whole seconds, as on HFS+,
/// ext3 and FAT, which counts in two-second steps.
const WHOLE_SECOND_TIMESTAMP_TICK: Duration = Duration::from_secs(2);

pub type ByteStream = Pin<Box<dyn AsyncRead + Send + Unpin + 'static>>;
pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub bytes: u64,
    pub sha256: String,
}

impl ObjectMetadata {
    pub fn new(bytes: u64, sha256: impl Into<String>) -> Result<Self> {
        let metadata = Self {
            bytes,
            sha256: sha256.into(),
        };
        if metadata.sha256.len() != 64
            || !metadata
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(StorageError::InvalidDigest);
        }
        Ok(metadata)
    }

    /// The metadata of a complete in-memory body, such as a startup probe or a
    /// fixture. An artifact-sized body is hashed as it streams instead.
    pub fn for_bytes(body: &[u8]) -> Self {
        let mut digest = Sha256::new();
        digest.update(body);
        Self {
            bytes: body.len() as u64,
            sha256: hex::encode(digest.finalize()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    AlreadyPresent,
}

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put_if_absent(
        &self,
        object: &ObjectId,
        body: ByteStream,
        metadata: &ObjectMetadata,
    ) -> Result<PutOutcome>;

    /// `put_if_absent` for a complete file the caller will not change while
    /// this runs. An adapter that can store a file without streaming it
    /// through memory overrides this, and still verifies the bytes it stored
    /// against `metadata` before the object becomes visible.
    async fn put_file_if_absent(
        &self,
        object: &ObjectId,
        path: &Path,
        metadata: &ObjectMetadata,
    ) -> Result<PutOutcome> {
        let file = tokio::fs::File::open(path).await?;
        self.put_if_absent(object, Box::pin(file), metadata).await
    }

    async fn head(&self, object: &ObjectId) -> Result<Option<ObjectMetadata>>;

    async fn get(&self, object: &ObjectId) -> Result<Option<ByteStream>>;

    async fn delete(&self, object: &ObjectId) -> Result<()>;
}

/// A store one server process owns for the whole of its life, and can
/// therefore sweep after a crash.
///
/// Both adapters stage bytes in a private directory outside the visible
/// namespace, and an unclean exit leaves never-visible bytes there. Sweeping
/// it is process-global, so only a process that knows no publication is
/// running may call it.
pub trait ManagedBlobStore: BlobStore {
    /// Removes abandoned, never-visible staging entries left by a crash, and
    /// returns how many were removed.
    fn cleanup_staging(&self) -> Result<usize>;
}

#[derive(Debug, Clone)]
pub struct LocalFilesystemBlobStore {
    root: PathBuf,
    objects: PathBuf,
    staging: PathBuf,
    max_object_bytes: u64,
    /// The digest of each object this store has hashed, by path, with the
    /// stat identity its bytes were read under. Shared by clones, so the
    /// publication that stores an object and the bootstrap that heads it
    /// read one record.
    verified: Arc<Mutex<HashMap<PathBuf, VerifiedObject>>>,
    /// How many times `head` has read an object to hash it.
    #[cfg(test)]
    heads_hashed: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Debug)]
struct VerifiedObject {
    identity: FileIdentity,
    metadata: ObjectMetadata,
}

/// What `stat` says about a file that tells whether its bytes can have
/// changed: device, inode, length, and modification and change times. Writing
/// the bytes moves the modification time; any write, truncation, `chmod`,
/// `utimes`, link or rename moves the change time, which no call can set
/// back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    bytes: u64,
    modified: SystemTime,
    changed: (i64, i64),
}

impl FileIdentity {
    #[cfg(unix)]
    fn of(metadata: &std::fs::Metadata) -> Option<Self> {
        use std::os::unix::fs::MetadataExt as _;
        Some(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.len(),
            modified: metadata.modified().ok()?,
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }

    /// Without a change time and an inode, no identity stands for a file's
    /// bytes, and `head` reads them every time.
    #[cfg(not(unix))]
    fn of(_: &std::fs::Metadata) -> Option<Self> {
        None
    }

    /// Whether a write at `started` or later would have moved the
    /// modification time off this one. Timestamps come from a coarser clock
    /// than `SystemTime::now()`, so a write within the same tick as the last
    /// one can leave the modification time where it was; only a modification
    /// time at least [`TIMESTAMP_TICK`] older than the read rules that out.
    fn settled_before(&self, started: SystemTime) -> bool {
        let whole_seconds = self.changed.1 == 0
            && self
                .modified
                .duration_since(UNIX_EPOCH)
                .is_ok_and(|since| since.subsec_nanos() == 0);
        let tick = if whole_seconds {
            WHOLE_SECOND_TIMESTAMP_TICK
        } else {
            TIMESTAMP_TICK
        };
        self.modified
            .checked_add(tick)
            .is_some_and(|settled| settled <= started)
    }

    /// The same file with the same bytes, whatever has happened to its change
    /// time.
    fn same_bytes_as(&self, other: &Self) -> bool {
        self.device == other.device
            && self.inode == other.inode
            && self.bytes == other.bytes
            && self.modified == other.modified
    }
}

/// Synchronous cancellation guard for one never-visible staging file. Async
/// futures can be dropped at any await point, so cleanup cannot live only in
/// the normal `Result` path.
struct StagingFileGuard {
    path: PathBuf,
    armed: bool,
}

impl StagingFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StagingFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl LocalFilesystemBlobStore {
    pub fn open(root: impl AsRef<Path>, max_object_bytes: u64) -> Result<Self> {
        if max_object_bytes == 0 {
            return Err(StorageError::InvalidLimit);
        }
        std::fs::create_dir_all(root.as_ref())?;
        let root = std::fs::canonicalize(root)?;
        let objects = root.join("objects");
        let staging = root.join("staging");
        std::fs::create_dir_all(&objects)?;
        std::fs::create_dir_all(&staging)?;
        reject_symlink(&objects)?;
        reject_symlink(&staging)?;
        Ok(Self {
            root,
            objects,
            staging,
            max_object_bytes,
            verified: Arc::default(),
            #[cfg(test)]
            heads_hashed: Arc::default(),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Removes abandoned, never-visible staging files after a crash.
    pub fn cleanup_staging(&self) -> Result<usize> {
        let mut removed = 0;
        for entry in std::fs::read_dir(&self.staging)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if !metadata.is_file() || entry.file_type()?.is_symlink() {
                return Err(StorageError::UnsafeFilesystemEntry(entry.path()));
            }
            std::fs::remove_file(entry.path())?;
            removed += 1;
        }
        sync_directory(&self.staging)?;
        Ok(removed)
    }

    fn object_path(&self, object: &ObjectId) -> PathBuf {
        let digest = object_storage_digest(object);
        self.objects.join(&digest[..2]).join(digest)
    }

    async fn hash_path(&self, path: &Path) -> Result<(ObjectMetadata, Option<FileIdentity>)> {
        #[cfg(test)]
        self.heads_hashed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = path.to_path_buf();
        let maximum = self.max_object_bytes;
        tokio::task::spawn_blocking(move || hash_file(&path, maximum))
            .await
            .map_err(|error| StorageError::Io(io::Error::other(error)))?
    }

    /// The digest recorded for `path`, if the file still has the identity it
    /// was read under.
    fn verified_under(&self, path: &Path, identity: &FileIdentity) -> Option<ObjectMetadata> {
        let verified = self.verified.lock().ok()?;
        let object = verified.get(path)?;
        (object.identity == *identity).then(|| object.metadata.clone())
    }

    /// Records `metadata` as the digest of `path` under `identity`, or, with
    /// no identity to trust, forgets whatever was recorded for it.
    fn record(&self, path: PathBuf, identity: Option<FileIdentity>, metadata: &ObjectMetadata) {
        let Some(identity) = identity else {
            self.forget(&path);
            return;
        };
        if let Ok(mut verified) = self.verified.lock() {
            verified.insert(
                path,
                VerifiedObject {
                    identity,
                    metadata: metadata.clone(),
                },
            );
        }
    }

    /// A poisoned record is never read, so failing to forget is safe.
    fn forget(&self, path: &Path) {
        if let Ok(mut verified) = self.verified.lock() {
            verified.remove(path);
        }
    }

    /// Stages a copy of `source` and verifies the staged bytes, not the
    /// source's, against `expected`. The returned guard still owns the
    /// staging file, so a caller that stops before publishing removes it.
    /// Also returns the identity the staged bytes were verified under, if it
    /// can be trusted.
    fn stage_file(
        &self,
        source: &Path,
        expected: &ObjectMetadata,
    ) -> Result<(PathBuf, StagingFileGuard, Option<FileIdentity>)> {
        let source_metadata = std::fs::metadata(source)?;
        let length = source_metadata.len();
        if length != expected.bytes {
            return Err(StorageError::BodyMismatch {
                expected: expected.clone(),
                actual: ObjectMetadata {
                    bytes: length,
                    sha256: String::new(),
                },
            });
        }
        let temporary = self.staging.join(format!("{}.tmp", Uuid::new_v4()));
        let guard = StagingFileGuard::new(temporary.clone());
        // On APFS `std::fs::copy` clones: the staged file shares the source's
        // blocks instead of copying them. The source is flushed first, so the
        // staged file's own F_FULLFSYNC below leaves both its blocks and its
        // metadata on stable storage, as a written copy's would be. Elsewhere
        // it copies.
        std::fs::File::open(source)?.sync_all()?;
        std::fs::copy(source, &temporary)?;
        // A clone keeps the source's modification time and a copy takes the
        // time it was made. Giving the copy the source's lets the identity
        // hashed below be trusted as soon as the source's would be. If that
        // fails, the staged file keeps a fresh time and nothing is recorded.
        if let Ok(modified) = source_metadata.modified() {
            let _ = std::fs::File::open(&temporary).and_then(|file| file.set_modified(modified));
        }
        let (actual, identity) = hash_file(&temporary, self.max_object_bytes)?;
        if &actual != expected {
            return Err(StorageError::BodyMismatch {
                expected: expected.clone(),
                actual,
            });
        }
        std::fs::File::open(&temporary)?.sync_all()?;
        Ok((temporary, guard, identity))
    }

    /// Makes a verified, synced staging file visible under `object` with a
    /// no-replace hard link. An object already there is kept, and is an error
    /// unless its bytes are `expected`. `staged` is the identity the staging
    /// file's bytes were verified under, if one can be trusted.
    async fn publish_staged(
        &self,
        object: &ObjectId,
        temporary: &Path,
        mut staging_guard: StagingFileGuard,
        expected: &ObjectMetadata,
        staged: Option<FileIdentity>,
    ) -> Result<PutOutcome> {
        let destination = self.object_path(object);
        let parent = destination
            .parent()
            .ok_or_else(|| StorageError::UnsafeFilesystemEntry(destination.clone()))?;
        tokio::fs::create_dir_all(parent).await?;
        reject_symlink_async(parent).await?;
        match tokio::fs::hard_link(temporary, &destination).await {
            Ok(()) => {
                tokio::fs::remove_file(temporary).await?;
                staging_guard.disarm();
                sync_directory_async(parent).await?;
                sync_directory_async(&self.staging).await?;
                // Linking the staged file into place and unlinking its staging
                // name move only its change time, so the object has the bytes
                // verified in staging if nothing else about it moved.
                let published = FileIdentity::of(&tokio::fs::symlink_metadata(&destination).await?);
                let identity = staged.zip(published).and_then(|(staged, published)| {
                    staged.same_bytes_as(&published).then_some(published)
                });
                self.record(destination, identity, expected);
                Ok(PutOutcome::Created)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                tokio::fs::remove_file(temporary).await?;
                staging_guard.disarm();
                sync_directory_async(&self.staging).await?;
                let actual = self
                    .head(object)
                    .await?
                    .ok_or_else(|| StorageError::ObjectDisappeared(object.clone()))?;
                if &actual == expected {
                    Ok(PutOutcome::AlreadyPresent)
                } else {
                    Err(StorageError::ImmutableConflict {
                        object: object.clone(),
                        expected: expected.clone(),
                        actual,
                    })
                }
            }
            Err(error) => {
                let _ = tokio::fs::remove_file(temporary).await;
                staging_guard.disarm();
                Err(StorageError::Io(error))
            }
        }
    }
}

/// The length and SHA-256 of the regular file at `path`, read on the calling
/// thread, and the stat identity those bytes can later be trusted under:
/// none if the file changed while it was read, or if its modification time is
/// too recent to tell a later write from the last one.
fn hash_file(path: &Path, maximum: u64) -> Result<(ObjectMetadata, Option<FileIdentity>)> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(StorageError::UnsafeFilesystemEntry(path.to_path_buf()));
    }
    if metadata.len() > maximum {
        return Err(StorageError::ObjectTooLarge { maximum });
    }
    let started = SystemTime::now();
    let mut file = std::fs::File::open(path)?;
    let before = FileIdentity::of(&file.metadata()?);
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; FILE_HASH_BUFFER_BYTES];
    loop {
        let read = io::Read::read(&mut file, &mut buffer)?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(read as u64)
            .ok_or(StorageError::ObjectSizeOverflow)?;
        if bytes > maximum {
            return Err(StorageError::ObjectTooLarge { maximum });
        }
        digest.update(&buffer[..read]);
    }
    if bytes != metadata.len() {
        return Err(StorageError::ObjectChangedDuringRead);
    }
    let after = FileIdentity::of(&file.metadata()?);
    let identity =
        before.filter(|before| after.as_ref() == Some(before) && before.settled_before(started));
    Ok((
        ObjectMetadata::new(bytes, hex::encode(digest.finalize()))?,
        identity,
    ))
}

pub(crate) fn object_storage_digest(object: &ObjectId) -> String {
    let mut digest = Sha256::new();
    digest.update(OBJECT_ID_DOMAIN);
    digest.update(object.as_str().as_bytes());
    hex::encode(digest.finalize())
}

impl ManagedBlobStore for LocalFilesystemBlobStore {
    fn cleanup_staging(&self) -> Result<usize> {
        LocalFilesystemBlobStore::cleanup_staging(self)
    }
}

#[async_trait]
impl BlobStore for LocalFilesystemBlobStore {
    async fn put_if_absent(
        &self,
        object: &ObjectId,
        mut body: ByteStream,
        expected: &ObjectMetadata,
    ) -> Result<PutOutcome> {
        if expected.bytes > self.max_object_bytes {
            return Err(StorageError::ObjectTooLarge {
                maximum: self.max_object_bytes,
            });
        }
        let temporary = self.staging.join(format!("{}.tmp", Uuid::new_v4()));
        let staging_guard = StagingFileGuard::new(temporary.clone());
        // Create synchronously before the first cancellable await. Tokio's
        // background open can otherwise finish after a dropped future has
        // already run the staging guard, leaving a late orphan.
        let output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        let mut output = tokio::fs::File::from_std(output);
        let result = async {
            let mut digest = Sha256::new();
            let mut bytes = 0_u64;
            let mut buffer = [0_u8; IO_BUFFER_BYTES];
            loop {
                let read = body.read(&mut buffer).await?;
                if read == 0 {
                    break;
                }
                bytes = bytes
                    .checked_add(read as u64)
                    .ok_or(StorageError::ObjectSizeOverflow)?;
                if bytes > expected.bytes || bytes > self.max_object_bytes {
                    return Err(StorageError::BodyMismatch {
                        expected: expected.clone(),
                        actual: ObjectMetadata {
                            bytes,
                            sha256: String::new(),
                        },
                    });
                }
                digest.update(&buffer[..read]);
                output.write_all(&buffer[..read]).await?;
            }
            output.flush().await?;
            output.sync_all().await?;
            let digest = digest.finalize();
            let actual = ObjectMetadata {
                bytes,
                sha256: hex::encode(digest),
            };
            if &actual != expected {
                return Err(StorageError::BodyMismatch {
                    expected: expected.clone(),
                    actual,
                });
            }
            Ok(())
        }
        .await;
        drop(output);
        if let Err(error) = result {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error);
        }
        // Written as it was hashed, so its modification time is too recent to
        // trust; the first `head` reads it again.
        self.publish_staged(object, &temporary, staging_guard, expected, None)
            .await
    }

    /// Stages the file by `std::fs::copy`, which clones it on APFS, then
    /// hashes the staged file itself. The bytes checked against `expected`
    /// are the ones the object will hold, read back after they were stored,
    /// and `head` trusts that digest while the object's stat identity holds.
    async fn put_file_if_absent(
        &self,
        object: &ObjectId,
        path: &Path,
        expected: &ObjectMetadata,
    ) -> Result<PutOutcome> {
        if expected.bytes > self.max_object_bytes {
            return Err(StorageError::ObjectTooLarge {
                maximum: self.max_object_bytes,
            });
        }
        let store = self.clone();
        let source = path.to_path_buf();
        let staged_expected = expected.clone();
        // A dropped caller leaves this task running; its guard then removes
        // the staging file when the task's unread result is dropped.
        let (temporary, staging_guard, identity) =
            tokio::task::spawn_blocking(move || store.stage_file(&source, &staged_expected))
                .await
                .map_err(|error| StorageError::Io(io::Error::other(error)))??;
        self.publish_staged(object, &temporary, staging_guard, expected, identity)
            .await
    }

    /// The object's length and SHA-256, read from its bytes unless this store
    /// already hashed them, at `put_file_if_absent` or an earlier `head`,
    /// under the stat identity the file still has. A file rewritten,
    /// truncated, replaced, `chmod`ed or re-timed since has another identity
    /// and is read again.
    async fn head(&self, object: &ObjectId) -> Result<Option<ObjectMetadata>> {
        let path = self.object_path(object);
        let metadata = match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.forget(&path);
                return Ok(None);
            }
            Err(error) => return Err(StorageError::Io(error)),
        };
        if let Some(verified) =
            FileIdentity::of(&metadata).and_then(|identity| self.verified_under(&path, &identity))
        {
            return Ok(Some(verified));
        }
        let (metadata, identity) = self.hash_path(&path).await?;
        self.record(path, identity, &metadata);
        Ok(Some(metadata))
    }

    async fn get(&self, object: &ObjectId) -> Result<Option<ByteStream>> {
        let path = self.object_path(object);
        match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(StorageError::UnsafeFilesystemEntry(path));
                }
                if metadata.len() > self.max_object_bytes {
                    return Err(StorageError::ObjectTooLarge {
                        maximum: self.max_object_bytes,
                    });
                }
                let file = tokio::fs::File::open(path).await?;
                Ok(Some(Box::pin(file)))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(StorageError::Io(error)),
        }
    }

    async fn delete(&self, object: &ObjectId) -> Result<()> {
        let path = self.object_path(object);
        self.forget(&path);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                if let Some(parent) = path.parent() {
                    sync_directory_async(parent).await?;
                }
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StorageError::Io(error)),
        }
    }
}

fn reject_symlink(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(StorageError::UnsafeFilesystemEntry(path.to_path_buf()));
    }
    Ok(())
}

async fn reject_symlink_async(path: &Path) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(path).await?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(StorageError::UnsafeFilesystemEntry(path.to_path_buf()));
    }
    Ok(())
}

async fn sync_directory_async(path: &Path) -> Result<()> {
    tokio::fs::File::open(path).await?.sync_all().await?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    FileSync::sync(path).map_err(StorageError::Io)
}

struct FileSync;

impl FileSync {
    fn sync(path: &Path) -> io::Result<()> {
        std::fs::File::open(path)?.sync_all()
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("blob store I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("blob-store object limit must be positive")]
    InvalidLimit,
    #[error("object digest must be 64 lowercase hexadecimal characters")]
    InvalidDigest,
    #[error("object exceeds configured maximum of {maximum} bytes")]
    ObjectTooLarge { maximum: u64 },
    #[error("object byte count overflowed u64")]
    ObjectSizeOverflow,
    #[error("object body differs from declared metadata: expected {expected:?}, found {actual:?}")]
    BodyMismatch {
        expected: ObjectMetadata,
        actual: ObjectMetadata,
    },
    #[error("object {object} already exists with different immutable bytes")]
    ImmutableConflict {
        object: ObjectId,
        expected: ObjectMetadata,
        actual: ObjectMetadata,
    },
    #[error("object disappeared during immutable put: {0}")]
    ObjectDisappeared(ObjectId),
    #[error("unsafe filesystem entry in blob store: {0}")]
    UnsafeFilesystemEntry(PathBuf),
    #[error("object changed while its metadata was being read")]
    ObjectChangedDuringRead,
    #[error("invalid S3 blob-store configuration: {0}")]
    InvalidProviderConfiguration(&'static str),
    #[error("S3 {operation} failed ({code})")]
    Provider {
        operation: &'static str,
        code: String,
    },
    #[error("object {object} has invalid provider metadata ({reason})")]
    InvalidProviderMetadata {
        object: ObjectId,
        reason: &'static str,
    },
    #[error("S3 multipart cleanup failed after {operation} ({code})")]
    MultipartCleanup {
        operation: &'static str,
        code: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn object_id() -> ObjectId {
        ObjectId::new("tenant/artifact/object-1").unwrap()
    }

    fn metadata(bytes: &[u8]) -> ObjectMetadata {
        let digest = Sha256::digest(bytes);
        ObjectMetadata::new(bytes.len() as u64, hex::encode(digest)).unwrap()
    }

    #[test]
    fn sha2_0_11_digests_equal_the_0_10_digests_objects_were_named_by() {
        use sha2_010::Digest as _;

        // Every length around the 64-byte block and padding boundaries, then
        // a body larger than the I/O buffer fed in uneven pieces, as a stream is.
        let body: Vec<u8> = (0..(3 * IO_BUFFER_BYTES + 17) as u32)
            .map(|index| (index.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        for length in (0..=200).chain([IO_BUFFER_BYTES, body.len()]) {
            let slice = &body[..length];
            assert_eq!(
                hex::encode(Sha256::digest(slice)),
                hex::encode(sha2_010::Sha256::digest(slice)),
                "length {length}"
            );
        }
        let mut streamed = Sha256::new();
        for piece in body.chunks(4_093) {
            streamed.update(piece);
        }
        assert_eq!(
            hex::encode(streamed.finalize()),
            hex::encode(sha2_010::Sha256::digest(&body))
        );
        // FIPS 180-2's two-block vector, and the storage path of an object.
        assert_eq!(
            ObjectMetadata::for_bytes(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")
                .sha256,
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        let mut legacy = sha2_010::Sha256::new();
        legacy.update(OBJECT_ID_DOMAIN);
        legacy.update(object_id().as_str().as_bytes());
        assert_eq!(
            object_storage_digest(&object_id()),
            hex::encode(legacy.finalize())
        );
    }

    fn stream(bytes: &[u8]) -> ByteStream {
        let (mut writer, reader) = tokio::io::duplex(bytes.len().max(1));
        let bytes = bytes.to_vec();
        tokio::spawn(async move {
            writer.write_all(&bytes).await.unwrap();
        });
        Box::pin(reader)
    }

    #[tokio::test]
    async fn local_store_put_head_get_retry_and_delete_are_immutable() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path(), 1024).unwrap();
        let bytes = b"immutable artifact bytes";
        let expected = metadata(bytes);

        assert_eq!(
            store
                .put_if_absent(&object_id(), stream(bytes), &expected)
                .await
                .unwrap(),
            PutOutcome::Created
        );
        assert_eq!(
            store.head(&object_id()).await.unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            store
                .put_if_absent(&object_id(), stream(bytes), &expected)
                .await
                .unwrap(),
            PutOutcome::AlreadyPresent
        );

        let mut body = store.get(&object_id()).await.unwrap().unwrap();
        let mut actual = Vec::new();
        body.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, bytes);

        store.delete(&object_id()).await.unwrap();
        store.delete(&object_id()).await.unwrap();
        assert_eq!(store.head(&object_id()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn body_mismatch_never_enters_the_visible_namespace() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path(), 1024).unwrap();
        let expected = metadata(b"complete");
        assert!(matches!(
            store
                .put_if_absent(&object_id(), stream(b"cut"), &expected)
                .await,
            Err(StorageError::BodyMismatch { .. })
        ));
        assert_eq!(store.head(&object_id()).await.unwrap(), None);
        assert_eq!(store.cleanup_staging().unwrap(), 0);
    }

    #[tokio::test]
    async fn a_stored_file_is_its_own_object_and_a_mismatched_one_leaves_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("store"), 1024).unwrap();
        let source = directory.path().join("source");
        let bytes = b"immutable artifact bytes";
        let expected = metadata(bytes);
        std::fs::write(&source, bytes).unwrap();
        assert_eq!(
            store
                .put_file_if_absent(&object_id(), &source, &expected)
                .await
                .unwrap(),
            PutOutcome::Created
        );
        assert_eq!(store.cleanup_staging().unwrap(), 0);

        // The object is not the source's inode: rewriting the source in place
        // leaves the stored bytes as they were verified.
        std::fs::write(&source, b"IMMUTABLE ARTIFACT BYTES").unwrap();
        assert_eq!(
            store.head(&object_id()).await.unwrap(),
            Some(expected.clone())
        );
        assert!(matches!(
            store
                .put_file_if_absent(
                    &object_id(),
                    &source,
                    &metadata(b"IMMUTABLE ARTIFACT BYTES")
                )
                .await,
            Err(StorageError::ImmutableConflict { .. })
        ));

        let other = ObjectId::new("tenant/artifact/object-2").unwrap();
        assert!(matches!(
            store.put_file_if_absent(&other, &source, &expected).await,
            Err(StorageError::BodyMismatch { .. })
        ));
        assert_eq!(store.head(&other).await.unwrap(), None);
        assert_eq!(store.cleanup_staging().unwrap(), 0);
    }

    fn heads_hashed(store: &LocalFilesystemBlobStore) -> u64 {
        store
            .heads_hashed
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set_modified(path: &Path, modified: SystemTime) {
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(modified)
            .unwrap();
    }

    /// Overwrites the file's bytes from the start, keeping its inode.
    fn rewrite_in_place(path: &Path, bytes: &[u8]) {
        let mut file = std::fs::File::options().write(true).open(path).unwrap();
        io::Write::write_all(&mut file, bytes).unwrap();
    }

    #[tokio::test]
    async fn head_reads_an_object_again_only_when_its_stat_identity_moves() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("store"), 1024).unwrap();
        let source = directory.path().join("source");
        let bytes = b"immutable artifact bytes";
        let expected = metadata(bytes);
        std::fs::write(&source, bytes).unwrap();
        // Written an hour ago, so any write from here on moves the
        // modification time.
        let written = SystemTime::now() - Duration::from_secs(3_600);
        set_modified(&source, written);
        store
            .put_file_if_absent(&object_id(), &source, &expected)
            .await
            .unwrap();

        // The put recorded the digest it verified: the unchanged object is
        // not read again.
        for _ in 0..2 {
            assert_eq!(
                store.head(&object_id()).await.unwrap(),
                Some(expected.clone())
            );
        }
        assert_eq!(heads_hashed(&store), 0);

        // The same length rewritten in place.
        let path = store.object_path(&object_id());
        let first = b"IMMUTABLE ARTIFACT BYTES";
        rewrite_in_place(&path, first);
        assert_eq!(
            store.head(&object_id()).await.unwrap(),
            Some(metadata(first))
        );
        assert_eq!(heads_hashed(&store), 1);
        set_modified(&path, written);
        for _ in 0..2 {
            assert_eq!(
                store.head(&object_id()).await.unwrap(),
                Some(metadata(first))
            );
        }
        assert_eq!(heads_hashed(&store), 2);

        // Rewritten again with its modification time put back, a timestamp
        // tick later: only the change time moved.
        tokio::time::sleep(TIMESTAMP_TICK).await;
        let second = b"Immutable Artifact Bytes";
        rewrite_in_place(&path, second);
        set_modified(&path, written);
        assert_eq!(
            store.head(&object_id()).await.unwrap(),
            Some(metadata(second))
        );
        assert_eq!(heads_hashed(&store), 3);

        std::fs::remove_file(&path).unwrap();
        assert_eq!(store.head(&object_id()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_digest_read_before_the_last_write_settled_is_read_again() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("store"), 1024).unwrap();
        let source = directory.path().join("source");
        let bytes = b"immutable artifact bytes";
        let expected = metadata(bytes);
        std::fs::write(&source, bytes).unwrap();
        // A modification time no older than the reads below, as a write in
        // the same timestamp tick as one of them would leave it.
        set_modified(&source, SystemTime::now() + Duration::from_secs(3_600));
        store
            .put_file_if_absent(&object_id(), &source, &expected)
            .await
            .unwrap();

        for hashed in 1..=2 {
            assert_eq!(
                store.head(&object_id()).await.unwrap(),
                Some(expected.clone())
            );
            assert_eq!(heads_hashed(&store), hashed);
        }
    }

    /// `head` trusts a digest while the object's stat identity holds; a put
    /// never does. It hashes the bytes it stages from its source every time,
    /// so a source changed in place with its length and modification time
    /// kept is refused, even for an object already stored and trusted.
    #[tokio::test]
    async fn put_hashes_its_source_even_when_the_object_is_stored_and_trusted() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("store"), 1024).unwrap();
        let source = directory.path().join("source");
        let bytes = b"immutable artifact bytes";
        let expected = metadata(bytes);
        std::fs::write(&source, bytes).unwrap();
        let written = SystemTime::now() - Duration::from_secs(3_600);
        set_modified(&source, written);
        assert_eq!(
            store
                .put_file_if_absent(&object_id(), &source, &expected)
                .await
                .unwrap(),
            PutOutcome::Created
        );

        rewrite_in_place(&source, b"IMMUTABLE ARTIFACT BYTES");
        set_modified(&source, written);
        let absent = ObjectId::new("tenant/artifact/object-2").unwrap();
        for object in [object_id(), absent.clone()] {
            assert!(
                matches!(
                    store.put_file_if_absent(&object, &source, &expected).await,
                    Err(StorageError::BodyMismatch { .. })
                ),
                "{object:?}"
            );
        }
        assert_eq!(store.head(&absent).await.unwrap(), None);
        assert_eq!(store.head(&object_id()).await.unwrap(), Some(expected));
        assert_eq!(heads_hashed(&store), 0);
    }

    #[tokio::test]
    async fn cancelling_a_suspended_put_removes_its_private_staging_file() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path(), 1024).unwrap();
        let task_store = store.clone();
        let object = object_id();
        let expected = metadata(b"complete");
        let (mut writer, reader) = tokio::io::duplex(16);
        writer.write_all(b"c").await.unwrap();
        let task = tokio::spawn(async move {
            task_store
                .put_if_absent(&object, Box::pin(reader), &expected)
                .await
        });

        let mut staged = false;
        for _ in 0..10_000 {
            staged = std::fs::read_dir(&store.staging)
                .unwrap()
                .filter_map(std::result::Result::ok)
                .any(|entry| entry.metadata().is_ok_and(|metadata| metadata.len() == 1));
            if staged {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            staged,
            "put never reached its suspended staged-write boundary"
        );

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(store.cleanup_staging().unwrap(), 0);
        assert_eq!(store.head(&object_id()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn concurrent_puts_publish_one_identical_object() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path(), 1024).unwrap();
        let bytes = b"same bytes";
        let expected = metadata(bytes);
        let object = object_id();
        let left = store.put_if_absent(&object, stream(bytes), &expected);
        let right = store.put_if_absent(&object, stream(bytes), &expected);
        let (left, right) = tokio::join!(left, right);
        let outcomes = [left.unwrap(), right.unwrap()];
        assert!(outcomes.contains(&PutOutcome::Created));
        assert!(outcomes.contains(&PutOutcome::AlreadyPresent));
        assert_eq!(store.head(&object_id()).await.unwrap(), Some(expected));
    }

    #[tokio::test]
    async fn conflicting_reuse_and_corrupt_existing_bytes_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path(), 1024).unwrap();
        let original = b"original";
        let expected = metadata(original);
        store
            .put_if_absent(&object_id(), stream(original), &expected)
            .await
            .unwrap();

        let changed = b"different";
        assert!(matches!(
            store
                .put_if_absent(&object_id(), stream(changed), &metadata(changed))
                .await,
            Err(StorageError::ImmutableConflict { .. })
        ));

        std::fs::write(store.object_path(&object_id()), b"corrupt!").unwrap();
        assert_ne!(
            store.head(&object_id()).await.unwrap(),
            Some(expected.clone())
        );
        assert!(matches!(
            store
                .put_if_absent(&object_id(), stream(original), &expected)
                .await,
            Err(StorageError::ImmutableConflict { .. })
        ));
    }

    #[test]
    fn object_ids_never_become_paths_and_crash_staging_is_collectable() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path(), 1024).unwrap();
        let adversarial = ObjectId::new("tenant/../../outside").unwrap();
        let object_path = store.object_path(&adversarial);
        assert!(object_path.starts_with(store.root().join("objects")));
        assert!(!object_path.to_string_lossy().contains(".."));

        std::fs::write(store.staging.join("abandoned.tmp"), b"partial").unwrap();
        assert_eq!(store.cleanup_staging().unwrap(), 1);
        assert_eq!(store.cleanup_staging().unwrap(), 0);
    }
}

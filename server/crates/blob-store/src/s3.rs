use std::{
    cmp,
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use aws_sdk_s3::{
    error::{ProvideErrorMetadata, SdkError},
    operation::{
        complete_multipart_upload::CompleteMultipartUploadError, get_object::GetObjectError,
        head_object::HeadObjectError, put_object::PutObjectError,
    },
    primitives::{ByteStream as S3ByteStream, Length},
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use sha2::{Digest, Sha256};
use sync_types::ObjectId;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf, Take};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    object_storage_digest, BlobStore, ByteStream, ObjectMetadata, PutOutcome, Result, StorageError,
    IO_BUFFER_BYTES,
};

const MIN_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024;
const MAX_SINGLE_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MAX_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MAX_MULTIPART_PARTS: u64 = 10_000;
const DEFAULT_MULTIPART_PART_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_MULTIPART_THRESHOLD_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_MULTIPART_ATTEMPTS: u32 = 3;
const METADATA_BYTES: &str = "calque-bytes";
const METADATA_SHA256: &str = "calque-sha256";

/// Configuration for an immutable S3-compatible object namespace.
///
/// Credentials, endpoints, retry policy, and TLS policy belong to the injected
/// AWS SDK client. Keeping them out of this value prevents accidental secret
/// disclosure through configuration diagnostics.
#[derive(Debug, Clone)]
pub struct S3BlobStoreConfig {
    bucket: String,
    prefix: String,
    staging_root: PathBuf,
    max_object_bytes: u64,
    multipart_threshold_bytes: u64,
    multipart_part_bytes: u64,
    multipart_attempts: u32,
}

impl S3BlobStoreConfig {
    pub fn new(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        staging_root: impl Into<PathBuf>,
        max_object_bytes: u64,
    ) -> Result<Self> {
        let config = Self {
            bucket: bucket.into(),
            prefix: normalize_prefix(&prefix.into())?,
            staging_root: staging_root.into(),
            max_object_bytes,
            multipart_threshold_bytes: DEFAULT_MULTIPART_THRESHOLD_BYTES,
            multipart_part_bytes: DEFAULT_MULTIPART_PART_BYTES,
            multipart_attempts: DEFAULT_MULTIPART_ATTEMPTS,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn with_multipart(
        mut self,
        threshold_bytes: u64,
        part_bytes: u64,
        attempts: u32,
    ) -> Result<Self> {
        self.multipart_threshold_bytes = threshold_bytes;
        self.multipart_part_bytes = part_bytes;
        self.multipart_attempts = attempts;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<()> {
        if self.bucket.is_empty() {
            return Err(StorageError::InvalidProviderConfiguration(
                "bucket must not be empty",
            ));
        }
        if self.max_object_bytes == 0 || self.max_object_bytes > i64::MAX as u64 {
            return Err(StorageError::InvalidProviderConfiguration(
                "maximum object size must fit a positive signed 64-bit length",
            ));
        }
        if self.multipart_threshold_bytes == 0
            || self.multipart_threshold_bytes > MAX_SINGLE_PUT_BYTES
        {
            return Err(StorageError::InvalidProviderConfiguration(
                "multipart threshold must be between 1 byte and the 5 GiB single-PUT limit",
            ));
        }
        if !(MIN_MULTIPART_PART_BYTES..=MAX_MULTIPART_PART_BYTES)
            .contains(&self.multipart_part_bytes)
        {
            return Err(StorageError::InvalidProviderConfiguration(
                "multipart part size must be between 5 MiB and 5 GiB",
            ));
        }
        if self.multipart_attempts == 0 || self.multipart_attempts > 10 {
            return Err(StorageError::InvalidProviderConfiguration(
                "multipart attempts must be between 1 and 10",
            ));
        }
        let maximum_multipart_bytes = self
            .multipart_part_bytes
            .checked_mul(MAX_MULTIPART_PARTS)
            .ok_or(StorageError::InvalidProviderConfiguration(
                "multipart size limit overflowed",
            ))?;
        if self.max_object_bytes > maximum_multipart_bytes {
            return Err(StorageError::InvalidProviderConfiguration(
                "maximum object size exceeds 10,000 configured multipart parts",
            ));
        }
        Ok(())
    }
}

/// S3-compatible implementation of the provider-neutral immutable blob store.
///
/// Incoming streams are first verified into a private local staging file. The
/// official AWS SDK can then replay a byte-exact file for transport retries and
/// for the complete multipart restart required after an S3 409 conflict.
#[derive(Clone)]
pub struct S3BlobStore {
    client: Client,
    config: S3BlobStoreConfig,
    staging: PathBuf,
    /// Where requests go, for the errors that name it: the endpoint's scheme,
    /// host and port, never its credentials, path or query.
    endpoint_label: String,
}

/// One S3-compatible endpoint and the static credentials used to reach it, so
/// a binary can build a client without depending on the AWS SDK. It has no
/// `Debug`, so the secret halves cannot reach a diagnostic.
///
/// `endpoint_url` is `None` for AWS itself and `Some` for any other
/// S3-compatible provider. `force_path_style` is required by providers that do
/// not serve virtual-host-style bucket names, as is usual for a self-hosted
/// one reached by address.
pub struct S3Endpoint {
    pub region: String,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl S3BlobStore {
    /// Builds the SDK client for `endpoint` and opens the store on it.
    ///
    /// Nothing here reaches the network: an unreachable endpoint, a missing
    /// bucket or a rejected credential is discovered by the first request, so
    /// a caller that must fail closed at startup has to make one.
    pub fn connect(endpoint: S3Endpoint, config: S3BlobStoreConfig) -> Result<Self> {
        if endpoint.region.is_empty() {
            return Err(StorageError::InvalidProviderConfiguration(
                "region must not be empty",
            ));
        }
        if endpoint.access_key_id.is_empty() || endpoint.secret_access_key.is_empty() {
            return Err(StorageError::InvalidProviderConfiguration(
                "access key id and secret access key must not be empty",
            ));
        }
        let endpoint_label = endpoint_label(endpoint.endpoint_url.as_deref(), &endpoint.region);
        let mut builder = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(endpoint.region))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                endpoint.access_key_id,
                endpoint.secret_access_key,
                endpoint.session_token,
                None,
                "calque-configured-object-store",
            ))
            .force_path_style(endpoint.force_path_style);
        if let Some(url) = endpoint.endpoint_url {
            builder = builder.endpoint_url(url);
        }
        let mut store = Self::open(Client::from_conf(builder.build()), config)?;
        store.endpoint_label = endpoint_label;
        Ok(store)
    }

    pub fn open(client: Client, config: S3BlobStoreConfig) -> Result<Self> {
        config.validate()?;
        std::fs::create_dir_all(&config.staging_root)?;
        let staging_root = std::fs::canonicalize(&config.staging_root)?;
        crate::reject_symlink(&staging_root)?;
        let staging = staging_root.join("s3-staging");
        std::fs::create_dir_all(&staging)?;
        crate::reject_symlink(&staging)?;
        Ok(Self {
            client,
            config,
            staging,
            endpoint_label: "its configured endpoint".to_owned(),
        })
    }

    /// `error` with the bucket and endpoint a provider failure was made
    /// against.
    fn located(&self, error: StorageError) -> StorageError {
        match error {
            error @ (StorageError::Provider { .. } | StorageError::MultipartCleanup { .. }) => {
                StorageError::ProviderAt {
                    bucket: self.config.bucket.clone(),
                    endpoint: self.endpoint_label.clone(),
                    source: Box::new(error),
                }
            }
            error => error,
        }
    }

    pub fn object_key(&self, object: &ObjectId) -> String {
        let digest = object_storage_digest(object);
        let suffix = format!("objects/{}/{}", &digest[..2], digest);
        if self.config.prefix.is_empty() {
            suffix
        } else {
            format!("{}/{}", self.config.prefix, suffix)
        }
    }

    /// Removes abandoned local input staging left by a process crash.
    /// Incomplete remote multipart uploads need a bucket lifecycle rule that
    /// aborts them; normal errors and task cancellation abort eagerly.
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
        crate::sync_directory(&self.staging)?;
        Ok(removed)
    }

    async fn stage_body(
        &self,
        mut body: ByteStream,
        expected: &ObjectMetadata,
    ) -> Result<StagedObject> {
        if expected.bytes > self.config.max_object_bytes {
            return Err(StorageError::ObjectTooLarge {
                maximum: self.config.max_object_bytes,
            });
        }
        let path = self.staging.join(format!("{}.tmp", Uuid::new_v4()));
        let guard = StagingGuard::new(path.clone());
        // Keep file creation outside a cancellable background-open future so
        // the guard cannot run before the file appears.
        let output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut output = tokio::fs::File::from_std(output);
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
            if bytes > expected.bytes || bytes > self.config.max_object_bytes {
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
        drop(output);
        let actual = ObjectMetadata {
            bytes,
            sha256: hex::encode(digest.finalize()),
        };
        if &actual != expected {
            return Err(StorageError::BodyMismatch {
                expected: expected.clone(),
                actual,
            });
        }
        Ok(StagedObject {
            path,
            _guard: guard,
        })
    }

    async fn put_single(
        &self,
        object: &ObjectId,
        key: &str,
        staged: &Path,
        expected: &ObjectMetadata,
    ) -> Result<PutOutcome> {
        for attempt in 1..=self.config.multipart_attempts {
            let body = file_body(staged, 0, expected.bytes).await?;
            let result = self
                .client
                .put_object()
                .bucket(&self.config.bucket)
                .key(key)
                .body(body)
                .content_length(expected.bytes as i64)
                .metadata(METADATA_BYTES, expected.bytes.to_string())
                .metadata(METADATA_SHA256, expected.sha256.clone())
                .if_none_match("*")
                .send()
                .await;
            match result {
                Ok(_) => return self.verify_created(object, key, expected).await,
                Err(error) if put_precondition_failed(&error) => {
                    return self.resolve_existing(object, key, expected).await;
                }
                Err(error)
                    if provider_conflict(&error) && attempt < self.config.multipart_attempts =>
                {
                    continue;
                }
                Err(error) => return Err(provider_error("put-object", &error)),
            }
        }
        Err(StorageError::Provider {
            operation: "put-object",
            code: "retry-exhausted".to_owned(),
        })
    }

    async fn put_multipart(
        &self,
        object: &ObjectId,
        key: &str,
        staged: &Path,
        expected: &ObjectMetadata,
    ) -> Result<PutOutcome> {
        let api: Arc<dyn MultipartApi> = Arc::new(AwsMultipartApi {
            client: self.client.clone(),
            bucket: self.config.bucket.clone(),
        });
        self.put_multipart_with_api(api, object, key, staged, expected)
            .await
    }

    async fn put_multipart_with_api(
        &self,
        api: Arc<dyn MultipartApi>,
        object: &ObjectId,
        key: &str,
        staged: &Path,
        expected: &ObjectMetadata,
    ) -> Result<PutOutcome> {
        for attempt in 1..=self.config.multipart_attempts {
            match self
                .put_multipart_attempt(api.clone(), object, key, staged, expected)
                .await
            {
                Err(MultipartAttemptError::Conflict)
                    if attempt < self.config.multipart_attempts =>
                {
                    continue;
                }
                Err(MultipartAttemptError::Conflict) => {
                    return Err(StorageError::Provider {
                        operation: "complete-multipart-upload",
                        code: "ConditionalRequestConflict".to_owned(),
                    });
                }
                Err(MultipartAttemptError::Storage(error)) => return Err(error),
                Ok(outcome) => return Ok(outcome),
            }
        }
        Err(StorageError::Provider {
            operation: "complete-multipart-upload",
            code: "retry-exhausted".to_owned(),
        })
    }

    async fn put_multipart_attempt(
        &self,
        api: Arc<dyn MultipartApi>,
        object: &ObjectId,
        key: &str,
        staged: &Path,
        expected: &ObjectMetadata,
    ) -> std::result::Result<PutOutcome, MultipartAttemptError> {
        let mut guard = MultipartGuard::create(api.clone(), key.to_owned(), expected.clone())
            .await
            .map_err(MultipartAttemptError::Storage)?;
        let upload_id = guard.upload_id().to_owned();

        let mut completed = Vec::new();
        let mut offset = 0_u64;
        let mut part_number = 1_i32;
        while offset < expected.bytes {
            let part_bytes = cmp::min(self.config.multipart_part_bytes, expected.bytes - offset);
            let body = match file_body(staged, offset, part_bytes).await {
                Ok(body) => body,
                Err(error) => {
                    return Err(MultipartAttemptError::Storage(
                        guard.abort_after("stage-part", error).await,
                    ));
                }
            };
            let e_tag = match api
                .upload_part(key, &upload_id, part_number, part_bytes, body)
                .await
            {
                Ok(e_tag) => e_tag,
                Err(error) => {
                    return Err(MultipartAttemptError::Storage(
                        guard.abort_after("upload-part", error).await,
                    ));
                }
            };
            completed.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(e_tag)
                    .build(),
            );
            offset += part_bytes;
            part_number += 1;
        }

        match api.complete(key, &upload_id, completed).await {
            Ok(MultipartCompletion::Created) => {
                guard
                    .preserve()
                    .await
                    .map_err(MultipartAttemptError::Storage)?;
                self.verify_created(object, key, expected)
                    .await
                    .map_err(MultipartAttemptError::Storage)
            }
            Ok(MultipartCompletion::AlreadyPresent) => {
                let cleanup = guard.abort().await;
                if let Err(error) = cleanup {
                    return Err(MultipartAttemptError::Storage(error));
                }
                self.resolve_existing(object, key, expected)
                    .await
                    .map_err(MultipartAttemptError::Storage)
            }
            Ok(MultipartCompletion::Conflict) => {
                let cleanup = guard.abort().await;
                if let Err(error) = cleanup {
                    return Err(MultipartAttemptError::Storage(error));
                }
                Err(MultipartAttemptError::Conflict)
            }
            Err(error) => Err(MultipartAttemptError::Storage(
                guard.abort_after("complete-multipart-upload", error).await,
            )),
        }
    }

    async fn verify_created(
        &self,
        object: &ObjectId,
        key: &str,
        expected: &ObjectMetadata,
    ) -> Result<PutOutcome> {
        let actual = self
            .head_key(key, object)
            .await?
            .ok_or_else(|| StorageError::ObjectDisappeared(object.clone()))?;
        if &actual == expected {
            Ok(PutOutcome::Created)
        } else {
            Err(StorageError::ImmutableConflict {
                object: object.clone(),
                expected: expected.clone(),
                actual,
            })
        }
    }

    async fn resolve_existing(
        &self,
        object: &ObjectId,
        key: &str,
        expected: &ObjectMetadata,
    ) -> Result<PutOutcome> {
        let actual = self
            .head_key(key, object)
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

    async fn head_key(&self, key: &str, object: &ObjectId) -> Result<Option<ObjectMetadata>> {
        match self
            .client
            .head_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => parse_provider_metadata(
                object,
                output.content_length(),
                output.metadata(),
                self.config.max_object_bytes,
            )
            .map(Some),
            Err(error) if head_not_found(&error) => Ok(None),
            Err(error) => Err(provider_error("head-object", &error)),
        }
    }
}

impl crate::ManagedBlobStore for S3BlobStore {
    fn cleanup_staging(&self) -> Result<usize> {
        S3BlobStore::cleanup_staging(self)
    }
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn put_if_absent(
        &self,
        object: &ObjectId,
        body: ByteStream,
        expected: &ObjectMetadata,
    ) -> Result<PutOutcome> {
        self.put_if_absent_unlocated(object, body, expected)
            .await
            .map_err(|error| self.located(error))
    }

    async fn head(&self, object: &ObjectId) -> Result<Option<ObjectMetadata>> {
        self.head_key(&self.object_key(object), object)
            .await
            .map_err(|error| self.located(error))
    }

    async fn get(&self, object: &ObjectId) -> Result<Option<ByteStream>> {
        self.get_unlocated(object)
            .await
            .map_err(|error| self.located(error))
    }

    async fn delete(&self, object: &ObjectId) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(self.object_key(object))
            .send()
            .await
            .map_err(|error| self.located(provider_error("delete-object", &error)))?;
        Ok(())
    }
}

impl S3BlobStore {
    async fn put_if_absent_unlocated(
        &self,
        object: &ObjectId,
        body: ByteStream,
        expected: &ObjectMetadata,
    ) -> Result<PutOutcome> {
        let staged = self.stage_body(body, expected).await?;
        let key = self.object_key(object);
        if expected.bytes < self.config.multipart_threshold_bytes
            && expected.bytes <= MAX_SINGLE_PUT_BYTES
        {
            self.put_single(object, &key, &staged.path, expected).await
        } else {
            self.put_multipart(object, &key, &staged.path, expected)
                .await
        }
    }

    async fn get_unlocated(&self, object: &ObjectId) -> Result<Option<ByteStream>> {
        let key = self.object_key(object);
        match self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => {
                let expected = parse_provider_metadata(
                    object,
                    output.content_length(),
                    output.metadata(),
                    self.config.max_object_bytes,
                )?;
                let reader = VerifiedReader::new(output.body.into_async_read(), expected);
                Ok(Some(Box::pin(reader)))
            }
            Err(error) if get_not_found(&error) => Ok(None),
            Err(error) => Err(provider_error("get-object", &error)),
        }
    }
}

struct StagedObject {
    path: PathBuf,
    _guard: StagingGuard,
}

struct StagingGuard {
    path: PathBuf,
}

impl StagingGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for StagingGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultipartCompletion {
    Created,
    AlreadyPresent,
    Conflict,
}

#[async_trait]
trait MultipartApi: Send + Sync {
    async fn create(&self, key: &str, metadata: &ObjectMetadata) -> Result<String>;

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        bytes: u64,
        body: S3ByteStream,
    ) -> Result<String>;

    async fn complete(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<MultipartCompletion>;

    async fn abort(&self, key: &str, upload_id: &str) -> Result<()>;
}

struct AwsMultipartApi {
    client: Client,
    bucket: String,
}

#[async_trait]
impl MultipartApi for AwsMultipartApi {
    async fn create(&self, key: &str, metadata: &ObjectMetadata) -> Result<String> {
        let output = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .metadata(METADATA_BYTES, metadata.bytes.to_string())
            .metadata(METADATA_SHA256, metadata.sha256.clone())
            .send()
            .await
            .map_err(|error| provider_error("create-multipart-upload", &error))?;
        output
            .upload_id()
            .map(str::to_owned)
            .ok_or_else(|| StorageError::Provider {
                operation: "create-multipart-upload",
                code: "missing-upload-id".to_owned(),
            })
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        bytes: u64,
        body: S3ByteStream,
    ) -> Result<String> {
        let output = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .content_length(bytes as i64)
            .body(body)
            .send()
            .await
            .map_err(|error| provider_error("upload-part", &error))?;
        output
            .e_tag()
            .map(str::to_owned)
            .ok_or_else(|| StorageError::Provider {
                operation: "upload-part",
                code: "missing-etag".to_owned(),
            })
    }

    async fn complete(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<MultipartCompletion> {
        let multipart = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        match self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(multipart)
            .if_none_match("*")
            .send()
            .await
        {
            Ok(_) => Ok(MultipartCompletion::Created),
            Err(error) if multipart_precondition_failed(&error) => {
                Ok(MultipartCompletion::AlreadyPresent)
            }
            Err(error) if provider_conflict(&error) => Ok(MultipartCompletion::Conflict),
            Err(error) => Err(provider_error("complete-multipart-upload", &error)),
        }
    }

    async fn abort(&self, key: &str, upload_id: &str) -> Result<()> {
        match self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error)
                if error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("NoSuchUpload") =>
            {
                Ok(())
            }
            Err(error) => Err(provider_error("abort-multipart-upload", &error)),
        }
    }
}

struct MultipartGuard {
    upload_id: String,
    disposition: Option<oneshot::Sender<MultipartDisposition>>,
    cleanup: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl MultipartGuard {
    async fn create(
        api: Arc<dyn MultipartApi>,
        key: String,
        metadata: ObjectMetadata,
    ) -> Result<Self> {
        let (created, creation) = oneshot::channel();
        let (disposition, decision) = oneshot::channel();
        let cleanup = tokio::spawn(async move {
            let upload_id = match api.create(&key, &metadata).await {
                Ok(upload_id) => upload_id,
                Err(error) => {
                    let _ = created.send(Err(error));
                    return Ok(());
                }
            };
            if created.send(Ok(upload_id.clone())).is_err() {
                return api.abort(&key, &upload_id).await;
            }
            match decision.await {
                Ok(MultipartDisposition::Preserve) => Ok(()),
                Ok(MultipartDisposition::Abort) | Err(_) => api.abort(&key, &upload_id).await,
            }
        });
        let upload_id = match creation.await {
            Ok(Ok(upload_id)) => upload_id,
            Ok(Err(error)) => {
                cleanup.await.map_err(|_| StorageError::MultipartCleanup {
                    operation: "multipart-create-coordinator",
                    code: "task-join".to_owned(),
                })??;
                return Err(error);
            }
            Err(_) => {
                cleanup.await.map_err(|_| StorageError::MultipartCleanup {
                    operation: "multipart-create-coordinator",
                    code: "task-join".to_owned(),
                })??;
                return Err(StorageError::MultipartCleanup {
                    operation: "multipart-create-coordinator",
                    code: "missing-result".to_owned(),
                });
            }
        };
        Ok(Self {
            upload_id,
            disposition: Some(disposition),
            cleanup: Some(cleanup),
        })
    }

    fn upload_id(&self) -> &str {
        &self.upload_id
    }

    async fn preserve(&mut self) -> Result<()> {
        self.finish(MultipartDisposition::Preserve).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.finish(MultipartDisposition::Abort).await
    }

    async fn finish(&mut self, disposition: MultipartDisposition) -> Result<()> {
        let Some(sender) = self.disposition.take() else {
            return Ok(());
        };
        let _ = sender.send(disposition);
        let Some(cleanup) = self.cleanup.take() else {
            return Ok(());
        };
        cleanup.await.map_err(|_| StorageError::MultipartCleanup {
            operation: "multipart-cleanup-coordinator",
            code: "task-join".to_owned(),
        })?
    }

    async fn abort_after(
        &mut self,
        operation: &'static str,
        primary: StorageError,
    ) -> StorageError {
        match self.abort().await {
            Ok(()) => primary,
            Err(StorageError::Provider { code, .. }) => {
                StorageError::MultipartCleanup { operation, code }
            }
            Err(error) => error,
        }
    }
}

enum MultipartDisposition {
    Preserve,
    Abort,
}

enum MultipartAttemptError {
    Conflict,
    Storage(StorageError),
}

async fn file_body(path: &Path, offset: u64, bytes: u64) -> Result<S3ByteStream> {
    S3ByteStream::read_from()
        .path(path)
        .offset(offset)
        .length(Length::Exact(bytes))
        .buffer_size(IO_BUFFER_BYTES)
        .build()
        .await
        .map_err(|error| StorageError::Io(io::Error::other(error)))
}

fn parse_provider_metadata(
    object: &ObjectId,
    content_length: Option<i64>,
    metadata: Option<&HashMap<String, String>>,
    max_object_bytes: u64,
) -> Result<ObjectMetadata> {
    let content_length = content_length.ok_or_else(|| StorageError::InvalidProviderMetadata {
        object: object.clone(),
        reason: "missing content length",
    })?;
    let content_length =
        u64::try_from(content_length).map_err(|_| StorageError::InvalidProviderMetadata {
            object: object.clone(),
            reason: "negative content length",
        })?;
    if content_length > max_object_bytes {
        return Err(StorageError::ObjectTooLarge {
            maximum: max_object_bytes,
        });
    }
    let metadata = metadata.ok_or_else(|| StorageError::InvalidProviderMetadata {
        object: object.clone(),
        reason: "missing Calque metadata",
    })?;
    let declared_bytes = metadata
        .get(METADATA_BYTES)
        .ok_or_else(|| StorageError::InvalidProviderMetadata {
            object: object.clone(),
            reason: "missing Calque byte count",
        })?
        .parse::<u64>()
        .map_err(|_| StorageError::InvalidProviderMetadata {
            object: object.clone(),
            reason: "invalid Calque byte count",
        })?;
    if declared_bytes != content_length {
        return Err(StorageError::InvalidProviderMetadata {
            object: object.clone(),
            reason: "content length disagrees with Calque metadata",
        });
    }
    let sha256 =
        metadata
            .get(METADATA_SHA256)
            .ok_or_else(|| StorageError::InvalidProviderMetadata {
                object: object.clone(),
                reason: "missing Calque SHA-256",
            })?;
    ObjectMetadata::new(content_length, sha256.clone()).map_err(|_| {
        StorageError::InvalidProviderMetadata {
            object: object.clone(),
            reason: "invalid Calque SHA-256",
        }
    })
}

struct VerifiedReader<R> {
    inner: Take<R>,
    expected: ObjectMetadata,
    digest: Sha256,
    bytes: u64,
    done: bool,
}

impl<R: AsyncRead + Unpin> VerifiedReader<R> {
    fn new(inner: R, expected: ObjectMetadata) -> Self {
        Self {
            inner: inner.take(expected.bytes),
            expected,
            digest: Sha256::new(),
            bytes: 0,
            done: false,
        }
    }

    fn verify_eof(&mut self) -> io::Result<()> {
        let actual = ObjectMetadata {
            bytes: self.bytes,
            sha256: hex::encode(self.digest.clone().finalize()),
        };
        if actual != self.expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "S3 body differs from immutable object metadata",
            ));
        }
        self.done = true;
        Ok(())
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for VerifiedReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.done {
            return Poll::Ready(Ok(()));
        }
        if self.bytes < self.expected.bytes {
            let before = buf.filled().len();
            match Pin::new(&mut self.inner).poll_read(cx, buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {
                    let added = &buf.filled()[before..];
                    if added.is_empty() {
                        return Poll::Ready(self.verify_eof());
                    }
                    self.bytes += added.len() as u64;
                    self.digest.update(added);
                    return Poll::Ready(Ok(()));
                }
            }
        }

        let mut probe = [0_u8; 1];
        let mut probe_buf = ReadBuf::new(&mut probe);
        match Pin::new(self.inner.get_mut()).poll_read(cx, &mut probe_buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) if probe_buf.filled().is_empty() => Poll::Ready(self.verify_eof()),
            Poll::Ready(Ok(())) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "S3 body exceeds immutable object metadata",
            ))),
        }
    }
}

fn normalize_prefix(prefix: &str) -> Result<String> {
    let prefix = prefix.trim_matches('/');
    if prefix
        .bytes()
        .any(|byte| byte.is_ascii_control() || byte == b'\\')
        || prefix
            .split('/')
            .any(|segment| segment == "." || segment == "..")
    {
        return Err(StorageError::InvalidProviderConfiguration(
            "prefix contains an unsafe segment",
        ));
    }
    Ok(prefix.to_owned())
}

#[cfg(test)]
fn hex_digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn sanitized_code(code: Option<&str>) -> String {
    let Some(code) = code else {
        return "transport".to_owned();
    };
    if code.len() <= 64
        && !code.is_empty()
        && code
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        code.to_owned()
    } else {
        "unclassified".to_owned()
    }
}

/// Where requests go, without anything a URL can carry that is secret: the
/// scheme, host and port of `endpoint_url`, or the AWS region when there is
/// none.
fn endpoint_label(endpoint_url: Option<&str>, region: &str) -> String {
    let Some(url) = endpoint_url else {
        return format!("AWS S3 in region {region}");
    };
    let (scheme, rest) = url.split_once("://").unwrap_or(("", url));
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host = authority.rsplit('@').next().unwrap_or_default();
    if scheme.is_empty() {
        host.to_owned()
    } else {
        format!("{scheme}://{host}")
    }
}

fn provider_error<E, R>(operation: &'static str, error: &SdkError<E, R>) -> StorageError
where
    E: ProvideErrorMetadata,
{
    StorageError::Provider {
        operation,
        code: sanitized_code(
            error
                .as_service_error()
                .and_then(ProvideErrorMetadata::code),
        ),
    }
}

fn provider_conflict<E, R>(error: &SdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .is_some_and(|code| code == "ConditionalRequestConflict")
}

fn put_precondition_failed(error: &SdkError<PutObjectError>) -> bool {
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .is_some_and(|code| code == "PreconditionFailed")
}

fn multipart_precondition_failed(error: &SdkError<CompleteMultipartUploadError>) -> bool {
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .is_some_and(|code| code == "PreconditionFailed")
}

fn head_not_found(error: &SdkError<HeadObjectError>) -> bool {
    error
        .as_service_error()
        .is_some_and(HeadObjectError::is_not_found)
}

fn get_not_found(error: &SdkError<GetObjectError>) -> bool {
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .is_some_and(|code| matches!(code, "NoSuchKey" | "NotFound"))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashSet, VecDeque},
        sync::Mutex,
        time::Duration,
    };

    use super::*;
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use tokio::io::AsyncReadExt;
    use tokio::sync::Semaphore;

    struct TestGate {
        entered: Semaphore,
        release: Semaphore,
    }

    impl Default for TestGate {
        fn default() -> Self {
            Self {
                entered: Semaphore::new(0),
                release: Semaphore::new(0),
            }
        }
    }

    impl TestGate {
        async fn cross(&self) {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }

        async fn wait_until_entered(&self) {
            tokio::time::timeout(Duration::from_secs(2), self.entered.acquire())
                .await
                .expect("provider boundary was not reached")
                .unwrap()
                .forget();
        }

        fn release(&self) {
            self.release.add_permits(1);
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum FakeCompletion {
        Conflict,
        AcceptedThenLost,
    }

    #[derive(Default)]
    struct FakeMultipartState {
        create_calls: usize,
        upload_calls: usize,
        complete_calls: usize,
        abort_calls: usize,
        active: HashSet<String>,
        part_numbers: Vec<i32>,
        complete_part_counts: Vec<usize>,
        completions: VecDeque<FakeCompletion>,
    }

    struct FakeMultipartApi {
        state: Mutex<FakeMultipartState>,
        create_gate: Option<Arc<TestGate>>,
        upload_gate: Option<Arc<TestGate>>,
        complete_gate: Option<Arc<TestGate>>,
        abort_gate: Option<Arc<TestGate>>,
        upload_fail_on: Option<usize>,
        abort_fails: bool,
        abort_observed: Semaphore,
        abort_completed: Semaphore,
    }

    impl Default for FakeMultipartApi {
        fn default() -> Self {
            Self {
                state: Mutex::new(FakeMultipartState::default()),
                create_gate: None,
                upload_gate: None,
                complete_gate: None,
                abort_gate: None,
                upload_fail_on: None,
                abort_fails: false,
                abort_observed: Semaphore::new(0),
                abort_completed: Semaphore::new(0),
            }
        }
    }

    impl FakeMultipartApi {
        fn with_completions(completions: impl IntoIterator<Item = FakeCompletion>) -> Self {
            Self {
                state: Mutex::new(FakeMultipartState {
                    completions: completions.into_iter().collect(),
                    ..FakeMultipartState::default()
                }),
                ..Self::default()
            }
        }

        async fn wait_for_abort(&self) {
            tokio::time::timeout(Duration::from_secs(2), self.abort_observed.acquire())
                .await
                .expect("multipart cleanup did not reach abort")
                .unwrap()
                .forget();
        }

        async fn wait_for_abort_completion(&self) {
            tokio::time::timeout(Duration::from_secs(2), self.abort_completed.acquire())
                .await
                .expect("detached multipart abort did not complete")
                .unwrap()
                .forget();
        }
    }

    #[async_trait]
    impl MultipartApi for FakeMultipartApi {
        async fn create(&self, _key: &str, _metadata: &ObjectMetadata) -> Result<String> {
            let upload_id = {
                let mut state = self.state.lock().unwrap();
                state.create_calls += 1;
                let upload_id = format!("upload-{}", state.create_calls);
                state.active.insert(upload_id.clone());
                upload_id
            };
            if let Some(gate) = &self.create_gate {
                gate.cross().await;
            }
            Ok(upload_id)
        }

        async fn upload_part(
            &self,
            _key: &str,
            upload_id: &str,
            part_number: i32,
            _bytes: u64,
            _body: S3ByteStream,
        ) -> Result<String> {
            let (call, active) = {
                let mut state = self.state.lock().unwrap();
                state.upload_calls += 1;
                let call = state.upload_calls;
                state.part_numbers.push(part_number);
                (call, state.active.contains(upload_id))
            };
            assert!(active, "part used an inactive upload ID");
            if let Some(gate) = &self.upload_gate {
                gate.cross().await;
            }
            if self.upload_fail_on == Some(call) {
                return Err(StorageError::Provider {
                    operation: "upload-part",
                    code: "injected-upload-failure".to_owned(),
                });
            }
            Ok(format!("etag-{upload_id}-{part_number}"))
        }

        async fn complete(
            &self,
            _key: &str,
            upload_id: &str,
            parts: Vec<CompletedPart>,
        ) -> Result<MultipartCompletion> {
            let completion = {
                let mut state = self.state.lock().unwrap();
                state.complete_calls += 1;
                state.complete_part_counts.push(parts.len());
                let completion = state
                    .completions
                    .pop_front()
                    .unwrap_or(FakeCompletion::Conflict);
                if matches!(completion, FakeCompletion::AcceptedThenLost) {
                    state.active.remove(upload_id);
                }
                completion
            };
            if let Some(gate) = &self.complete_gate {
                gate.cross().await;
            }
            match completion {
                FakeCompletion::Conflict => Ok(MultipartCompletion::Conflict),
                FakeCompletion::AcceptedThenLost => Err(StorageError::Provider {
                    operation: "complete-multipart-upload",
                    code: "response-lost".to_owned(),
                }),
            }
        }

        async fn abort(&self, _key: &str, upload_id: &str) -> Result<()> {
            {
                let mut state = self.state.lock().unwrap();
                state.abort_calls += 1;
                if !self.abort_fails {
                    state.active.remove(upload_id);
                }
            }
            self.abort_observed.add_permits(1);
            if let Some(gate) = &self.abort_gate {
                gate.cross().await;
            }
            self.abort_completed.add_permits(1);
            if self.abort_fails {
                Err(StorageError::Provider {
                    operation: "abort-multipart-upload",
                    code: "injected-abort-failure".to_owned(),
                })
            } else {
                Ok(())
            }
        }
    }

    fn metadata(bytes: &[u8]) -> ObjectMetadata {
        ObjectMetadata::new(bytes.len() as u64, hex_digest(bytes)).unwrap()
    }

    fn multipart_test_store(root: &Path, attempts: u32) -> S3BlobStore {
        let sdk = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "test-access",
                "test-secret",
                None,
                None,
                "multipart-unit-test",
            ))
            .endpoint_url("http://127.0.0.1:1")
            .force_path_style(true)
            .build();
        let config = S3BlobStoreConfig::new("bucket", "unit", root, 1024)
            .unwrap()
            .with_multipart(1, MIN_MULTIPART_PART_BYTES, attempts)
            .unwrap();
        let mut store = S3BlobStore::open(Client::from_conf(sdk), config).unwrap();
        // The provider-independent coordinator tests use tiny replayable files;
        // production configuration remains bounded to S3's 5 MiB minimum part.
        store.config.multipart_part_bytes = 4;
        store
    }

    fn staged_fixture(root: &Path) -> (PathBuf, ObjectId, ObjectMetadata) {
        let bytes = b"abcdefgh";
        let path = root.join("multipart-fixture");
        std::fs::write(&path, bytes).unwrap();
        (
            path,
            ObjectId::new("test/multipart-boundaries").unwrap(),
            metadata(bytes),
        )
    }

    #[test]
    fn the_endpoint_label_drops_credentials_paths_and_queries() {
        assert_eq!(
            endpoint_label(None, "eu-west-3"),
            "AWS S3 in region eu-west-3"
        );
        assert_eq!(
            endpoint_label(
                Some("https://key:secret@minio.internal:9000/base?token=t#f"),
                "r"
            ),
            "https://minio.internal:9000"
        );
        assert_eq!(endpoint_label(Some("minio:9000"), "r"), "minio:9000");
    }

    #[tokio::test]
    async fn a_provider_failure_names_the_bucket_and_endpoint_but_no_credential() {
        let root = tempfile::tempdir().unwrap();
        let store = S3BlobStore::connect(
            S3Endpoint {
                region: "us-east-1".to_owned(),
                endpoint_url: Some("http://user:urlsecret@127.0.0.1:1/path".to_owned()),
                force_path_style: true,
                access_key_id: "AKIAEXAMPLEKEY".to_owned(),
                secret_access_key: "examplesecretvalue".to_owned(),
                session_token: Some("exampletoken".to_owned()),
            },
            S3BlobStoreConfig::new("reports", "", root.path(), 1024).unwrap(),
        )
        .unwrap();
        let error = store
            .head(&ObjectId::new("test/unreachable").unwrap())
            .await
            .unwrap_err();
        let text = error.to_string();
        assert!(
            text.starts_with("S3 head-object failed (")
                && text.ends_with(", using bucket reports at http://127.0.0.1:1"),
            "{text}"
        );
        for secret in [
            "AKIAEXAMPLEKEY",
            "examplesecretvalue",
            "exampletoken",
            "urlsecret",
            "/path",
        ] {
            assert!(!text.contains(secret), "{text}");
        }
    }

    #[test]
    fn configuration_rejects_unsafe_or_unbounded_values() {
        assert!(S3BlobStoreConfig::new("", "", ".", 1).is_err());
        assert!(S3BlobStoreConfig::new("bucket", "../escape", ".", 1).is_err());
        assert!(S3BlobStoreConfig::new("bucket", "bad\\prefix", ".", 1).is_err());
        assert!(S3BlobStoreConfig::new("bucket", "", ".", 0).is_err());
        let config = S3BlobStoreConfig::new("bucket", "/safe/prefix/", ".", 1024).unwrap();
        assert_eq!(config.prefix, "safe/prefix");
        assert!(config
            .clone()
            .with_multipart(1, MIN_MULTIPART_PART_BYTES - 1, 1)
            .is_err());
        assert!(config
            .clone()
            .with_multipart(1, MIN_MULTIPART_PART_BYTES, 0)
            .is_err());
        assert!(config
            .with_multipart(MAX_SINGLE_PUT_BYTES + 1, MIN_MULTIPART_PART_BYTES, 1)
            .is_err());
    }

    #[tokio::test]
    async fn conflict_restarts_the_entire_upload_and_unknown_completion_is_retry_safe() {
        let directory = tempfile::tempdir().unwrap();
        let store = multipart_test_store(directory.path(), 2);
        let (path, object, expected) = staged_fixture(directory.path());
        let fake = Arc::new(FakeMultipartApi::with_completions([
            FakeCompletion::Conflict,
            FakeCompletion::AcceptedThenLost,
        ]));

        let error = store
            .put_multipart_with_api(fake.clone(), &object, "key", &path, &expected)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            StorageError::Provider {
                operation: "complete-multipart-upload",
                ref code,
            } if code == "response-lost"
        ));
        let state = fake.state.lock().unwrap();
        assert_eq!(state.create_calls, 2);
        assert_eq!(state.upload_calls, 4);
        assert_eq!(state.part_numbers, [1, 2, 1, 2]);
        assert_eq!(state.complete_calls, 2);
        assert_eq!(state.complete_part_counts, [2, 2]);
        assert_eq!(state.abort_calls, 2);
        assert!(state.active.is_empty());
    }

    #[tokio::test]
    async fn upload_failure_aborts_and_abort_failure_is_an_explicit_cleanup_error() {
        for abort_fails in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let store = multipart_test_store(directory.path(), 1);
            let (path, object, expected) = staged_fixture(directory.path());
            let fake = Arc::new(FakeMultipartApi {
                upload_fail_on: Some(1),
                abort_fails,
                ..FakeMultipartApi::default()
            });

            let error = store
                .put_multipart_attempt(fake.clone(), &object, "key", &path, &expected)
                .await
                .unwrap_err();
            match (abort_fails, error) {
                (
                    false,
                    MultipartAttemptError::Storage(StorageError::Provider {
                        operation: "upload-part",
                        code,
                    }),
                ) => assert_eq!(code, "injected-upload-failure"),
                (
                    true,
                    MultipartAttemptError::Storage(StorageError::MultipartCleanup {
                        operation: "upload-part",
                        code,
                    }),
                ) => assert_eq!(code, "injected-abort-failure"),
                (_, _) => panic!("unexpected multipart failure classification"),
            }
            let state = fake.state.lock().unwrap();
            assert_eq!(state.create_calls, 1);
            assert_eq!(state.upload_calls, 1);
            assert_eq!(state.abort_calls, 1);
            assert_eq!(state.active.len(), usize::from(abort_fails));
        }
    }

    #[derive(Clone, Copy)]
    enum CancellationBoundary {
        Create,
        UploadPart,
        Complete,
        Abort,
    }

    async fn prove_cancellation_boundary(boundary: CancellationBoundary) {
        let directory = tempfile::tempdir().unwrap();
        let store = multipart_test_store(directory.path(), 1);
        let (path, object, expected) = staged_fixture(directory.path());
        let gate = Arc::new(TestGate::default());
        let fake = Arc::new(FakeMultipartApi {
            state: Mutex::new(FakeMultipartState {
                completions: [FakeCompletion::AcceptedThenLost].into_iter().collect(),
                ..FakeMultipartState::default()
            }),
            create_gate: matches!(boundary, CancellationBoundary::Create).then(|| gate.clone()),
            upload_gate: matches!(boundary, CancellationBoundary::UploadPart).then(|| gate.clone()),
            complete_gate: matches!(boundary, CancellationBoundary::Complete).then(|| gate.clone()),
            abort_gate: matches!(boundary, CancellationBoundary::Abort).then(|| gate.clone()),
            upload_fail_on: matches!(boundary, CancellationBoundary::Abort).then_some(1),
            ..FakeMultipartApi::default()
        });
        let task_store = store.clone();
        let task_fake: Arc<dyn MultipartApi> = fake.clone();
        let task = tokio::spawn(async move {
            task_store
                .put_multipart_attempt(task_fake, &object, "key", &path, &expected)
                .await
        });

        gate.wait_until_entered().await;
        task.abort();
        match task.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("multipart task completed before cancellation"),
        }
        gate.release();
        fake.wait_for_abort().await;
        if matches!(boundary, CancellationBoundary::Abort) {
            fake.wait_for_abort_completion().await;
        }
        assert!(fake.state.lock().unwrap().active.is_empty());
    }

    #[tokio::test]
    async fn cancellation_after_each_provider_acceptance_finishes_remote_cleanup() {
        for boundary in [
            CancellationBoundary::Create,
            CancellationBoundary::UploadPart,
            CancellationBoundary::Complete,
            CancellationBoundary::Abort,
        ] {
            prove_cancellation_boundary(boundary).await;
        }
    }

    #[tokio::test]
    async fn verified_reader_accepts_exact_bytes_and_rejects_short_long_or_corrupt() {
        let exact = b"exact";
        let mut reader = VerifiedReader::new(&exact[..], metadata(exact));
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.unwrap();
        assert_eq!(output, exact);

        for invalid in [&b"shor"[..], &b"exact-long"[..], &b"exacx"[..]] {
            let mut reader = VerifiedReader::new(invalid, metadata(exact));
            let mut output = Vec::new();
            let error = reader.read_to_end(&mut output).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn provider_codes_are_sanitized_before_display() {
        assert_eq!(
            sanitized_code(Some("PreconditionFailed")),
            "PreconditionFailed"
        );
        assert_eq!(sanitized_code(None), "transport");
        assert_eq!(
            sanitized_code(Some("https://secret.example/?token=credential")),
            "unclassified"
        );
    }
}

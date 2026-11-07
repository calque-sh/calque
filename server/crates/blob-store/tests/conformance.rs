use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use async_trait::async_trait;
use aws_sdk_s3::{
    config::{BehaviorVersion, Credentials, Region},
    error::ProvideErrorMetadata,
    Client,
};
use blob_store::{
    BlobStore, ByteStream, LocalFilesystemBlobStore, ObjectMetadata, PutOutcome, S3BlobStore,
    S3BlobStoreConfig, StorageError,
};
use sha2::{Digest, Sha256};
use sync_types::ObjectId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const S3_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024;

#[derive(Default)]
struct MemoryBlobStore {
    objects: std::sync::Mutex<BTreeMap<String, (Vec<u8>, ObjectMetadata)>>,
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    async fn put_if_absent(
        &self,
        object: &ObjectId,
        mut body: ByteStream,
        expected: &ObjectMetadata,
    ) -> blob_store::Result<PutOutcome> {
        let mut bytes = Vec::new();
        body.read_to_end(&mut bytes).await?;
        let actual = metadata(&bytes);
        if &actual != expected {
            return Err(StorageError::BodyMismatch {
                expected: expected.clone(),
                actual,
            });
        }
        let mut objects = self.objects.lock().unwrap();
        match objects.get(object.as_str()) {
            Some((_, actual)) if actual == expected => Ok(PutOutcome::AlreadyPresent),
            Some((_, actual)) => Err(StorageError::ImmutableConflict {
                object: object.clone(),
                expected: expected.clone(),
                actual: actual.clone(),
            }),
            None => {
                objects.insert(object.as_str().to_owned(), (bytes, expected.clone()));
                Ok(PutOutcome::Created)
            }
        }
    }

    async fn head(&self, object: &ObjectId) -> blob_store::Result<Option<ObjectMetadata>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(object.as_str())
            .map(|(_, metadata)| metadata.clone()))
    }

    async fn get(&self, object: &ObjectId) -> blob_store::Result<Option<ByteStream>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(object.as_str())
            .map(|(bytes, _)| Box::pin(Cursor::new(bytes.clone())) as ByteStream))
    }

    async fn delete(&self, object: &ObjectId) -> blob_store::Result<()> {
        self.objects.lock().unwrap().remove(object.as_str());
        Ok(())
    }
}

fn object(namespace: &str, name: &str) -> ObjectId {
    ObjectId::new(format!("conformance/{namespace}/{name}")).unwrap()
}

fn metadata(bytes: &[u8]) -> ObjectMetadata {
    ObjectMetadata::new(bytes.len() as u64, hex::encode(Sha256::digest(bytes))).unwrap()
}

fn stream(bytes: impl Into<Vec<u8>>) -> ByteStream {
    Box::pin(Cursor::new(bytes.into()))
}

async fn read(store: &dyn BlobStore, object: &ObjectId) -> Vec<u8> {
    let mut body = store.get(object).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    body.read_to_end(&mut bytes).await.unwrap();
    bytes
}

async fn run_provider_neutral_conformance(store: Arc<dyn BlobStore>, namespace: &str) {
    let primary = object(namespace, "primary");
    let bytes = b"immutable provider-neutral bytes";
    let expected = metadata(bytes);

    assert_eq!(store.head(&primary).await.unwrap(), None);
    assert!(store.get(&primary).await.unwrap().is_none());
    assert_eq!(
        store
            .put_if_absent(&primary, stream(bytes), &expected)
            .await
            .unwrap(),
        PutOutcome::Created
    );
    assert_eq!(store.head(&primary).await.unwrap(), Some(expected.clone()));
    assert_eq!(read(store.as_ref(), &primary).await, bytes);
    assert_eq!(
        store
            .put_if_absent(&primary, stream(bytes), &expected)
            .await
            .unwrap(),
        PutOutcome::AlreadyPresent
    );

    let different = b"different immutable bytes";
    assert!(matches!(
        store
            .put_if_absent(&primary, stream(different), &metadata(different))
            .await,
        Err(StorageError::ImmutableConflict { .. })
    ));
    assert_eq!(read(store.as_ref(), &primary).await, bytes);

    let mismatch = object(namespace, "mismatch");
    assert!(matches!(
        store
            .put_if_absent(&mismatch, stream(b"short"), &metadata(b"complete"))
            .await,
        Err(StorageError::BodyMismatch { .. })
    ));
    assert_eq!(store.head(&mismatch).await.unwrap(), None);

    let concurrent = object(namespace, "concurrent");
    let concurrent_bytes = b"concurrent identical body";
    let concurrent_metadata = metadata(concurrent_bytes);
    let left = store.put_if_absent(&concurrent, stream(concurrent_bytes), &concurrent_metadata);
    let right = store.put_if_absent(&concurrent, stream(concurrent_bytes), &concurrent_metadata);
    let (left, right) = tokio::join!(left, right);
    let outcomes = [left.unwrap(), right.unwrap()];
    assert!(outcomes.contains(&PutOutcome::Created));
    assert!(outcomes.contains(&PutOutcome::AlreadyPresent));
    assert_eq!(read(store.as_ref(), &concurrent).await, concurrent_bytes);

    let files = tempfile::tempdir().unwrap();
    let file = object(namespace, "file");
    let file_bytes = b"a complete file stored by path";
    let file_path = files.path().join("complete");
    std::fs::write(&file_path, file_bytes).unwrap();
    assert_eq!(
        store
            .put_file_if_absent(&file, &file_path, &metadata(file_bytes))
            .await
            .unwrap(),
        PutOutcome::Created
    );
    assert_eq!(read(store.as_ref(), &file).await, file_bytes);
    assert_eq!(
        store
            .put_file_if_absent(&file, &file_path, &metadata(file_bytes))
            .await
            .unwrap(),
        PutOutcome::AlreadyPresent
    );
    let file_mismatch = object(namespace, "file-mismatch");
    for (name, body) in [
        ("same-length", b"a complete file stored by PATH".as_slice()),
        ("short", b"a complete file".as_slice()),
    ] {
        let path = files.path().join(name);
        std::fs::write(&path, body).unwrap();
        assert!(
            matches!(
                store
                    .put_file_if_absent(&file_mismatch, &path, &metadata(file_bytes))
                    .await,
                Err(StorageError::BodyMismatch { .. })
            ),
            "{name}"
        );
        assert_eq!(store.head(&file_mismatch).await.unwrap(), None, "{name}");
    }

    let empty = object(namespace, "empty");
    let empty_metadata = metadata(b"");
    assert_eq!(
        store
            .put_if_absent(&empty, stream(Vec::new()), &empty_metadata)
            .await
            .unwrap(),
        PutOutcome::Created
    );
    assert_eq!(read(store.as_ref(), &empty).await, b"");

    store.delete(&primary).await.unwrap();
    store.delete(&primary).await.unwrap();
    assert_eq!(store.head(&primary).await.unwrap(), None);
    assert!(store.get(&primary).await.unwrap().is_none());

    store.delete(&concurrent).await.unwrap();
    store.delete(&file).await.unwrap();
    store.delete(&empty).await.unwrap();
}

#[tokio::test]
async fn in_memory_adapter_passes_shared_conformance() {
    run_provider_neutral_conformance(Arc::new(MemoryBlobStore::default()), "memory").await;
}

#[tokio::test]
async fn filesystem_adapter_passes_shared_conformance() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalFilesystemBlobStore::open(directory.path(), 16 * 1024 * 1024).unwrap();
    run_provider_neutral_conformance(Arc::new(store), "filesystem").await;
}

struct S3Environment {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3Environment {
    fn from_env() -> Self {
        let endpoint = std::env::var("CALQUE_S3_ENDPOINT").ok();
        let bucket = required_env("CALQUE_S3_BUCKET");
        let region = std::env::var("CALQUE_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
        let access_key = required_env("CALQUE_S3_ACCESS_KEY");
        let secret_key = required_env("CALQUE_S3_SECRET_KEY");
        let session_token = std::env::var("CALQUE_S3_SESSION_TOKEN").ok();
        let prefix = std::env::var("CALQUE_S3_PREFIX")
            .unwrap_or_else(|_| format!("s3-conformance-{}", std::process::id()));
        let force_path_style = std::env::var("CALQUE_S3_FORCE_PATH_STYLE")
            .map(|value| value == "1")
            .unwrap_or(endpoint.is_some());
        let mut config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region))
            .credentials_provider(Credentials::new(
                access_key,
                secret_key,
                session_token,
                None,
                "calque-s3-conformance",
            ))
            .force_path_style(force_path_style);
        if let Some(endpoint) = endpoint {
            config = config.endpoint_url(endpoint);
        }
        Self {
            client: Client::from_conf(config.build()),
            bucket,
            prefix,
        }
    }

    fn store(&self, staging: &std::path::Path) -> S3BlobStore {
        let config = S3BlobStoreConfig::new(
            self.bucket.clone(),
            self.prefix.clone(),
            staging,
            64 * 1024 * 1024,
        )
        .unwrap()
        .with_multipart(1024, S3_MULTIPART_PART_BYTES, 3)
        .unwrap();
        S3BlobStore::open(self.client.clone(), config).unwrap()
    }
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for the explicit S3 test"))
}

fn service_code<E: ProvideErrorMetadata, R>(error: &aws_sdk_s3::error::SdkError<E, R>) -> String {
    let code = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .unwrap_or("transport");
    if !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        code.to_owned()
    } else {
        "unclassified".to_owned()
    }
}

async fn assert_bucket_exists(environment: &S3Environment) {
    let head = environment
        .client
        .head_bucket()
        .bucket(&environment.bucket)
        .send()
        .await;
    if head.is_ok() {
        return;
    }
    if std::env::var("CALQUE_S3_CREATE_BUCKET").as_deref() == Ok("1") {
        if let Err(error) = environment
            .client
            .create_bucket()
            .bucket(&environment.bucket)
            .send()
            .await
        {
            if matches!(
                service_code(&error).as_str(),
                "BucketAlreadyOwnedByYou" | "BucketAlreadyExists"
            ) {
                return;
            }
            panic!(
                "explicit S3 conformance bucket creation failed ({})",
                service_code(&error)
            );
        }
        return;
    }
    if let Err(error) = head {
        panic!(
            "explicit S3 conformance bucket is unavailable ({})",
            service_code(&error)
        );
    }
}

async fn incomplete_upload_count(environment: &S3Environment, key: &str) -> usize {
    let output = environment
        .client
        .list_multipart_uploads()
        .bucket(&environment.bucket)
        .prefix(key)
        .send()
        .await
        .unwrap_or_else(|error| panic!("list-multipart-uploads failed ({})", service_code(&error)));
    output
        .uploads()
        .iter()
        .filter(|upload| upload.key() == Some(key))
        .count()
}

#[tokio::test]
#[ignore = "requires an explicit isolated S3-compatible bucket"]
async fn s3_adapter_passes_shared_and_multipart_conformance() {
    let environment = S3Environment::from_env();
    assert_bucket_exists(&environment).await;
    let directory = tempfile::tempdir().unwrap();
    let store = environment.store(directory.path());
    run_provider_neutral_conformance(Arc::new(store.clone()), "s3").await;

    let multipart = object("s3", "multipart");
    let bytes = vec![0x5a; S3_MULTIPART_PART_BYTES as usize + 512 * 1024];
    let expected = metadata(&bytes);
    assert_eq!(
        store
            .put_if_absent(&multipart, stream(bytes.clone()), &expected)
            .await
            .unwrap(),
        PutOutcome::Created
    );
    assert_eq!(store.head(&multipart).await.unwrap(), Some(expected));
    assert_eq!(read(&store, &multipart).await, bytes);
    assert_eq!(
        incomplete_upload_count(&environment, &store.object_key(&multipart)).await,
        0
    );
    store.delete(&multipart).await.unwrap();

    let concurrent_multipart = object("s3", "concurrent-multipart");
    let concurrent_bytes = vec![0x7d; S3_MULTIPART_PART_BYTES as usize + 256 * 1024];
    let concurrent_metadata = metadata(&concurrent_bytes);
    let left = store.put_if_absent(
        &concurrent_multipart,
        stream(concurrent_bytes.clone()),
        &concurrent_metadata,
    );
    let right = store.put_if_absent(
        &concurrent_multipart,
        stream(concurrent_bytes.clone()),
        &concurrent_metadata,
    );
    let (left, right) = tokio::join!(left, right);
    let outcomes = [left.unwrap(), right.unwrap()];
    assert!(outcomes.contains(&PutOutcome::Created));
    assert!(outcomes.contains(&PutOutcome::AlreadyPresent));
    assert_eq!(read(&store, &concurrent_multipart).await, concurrent_bytes);
    assert_eq!(
        incomplete_upload_count(&environment, &store.object_key(&concurrent_multipart)).await,
        0
    );
    store.delete(&concurrent_multipart).await.unwrap();

    let raw = object("s3", "missing-metadata");
    environment
        .client
        .put_object()
        .bucket(&environment.bucket)
        .key(store.object_key(&raw))
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"raw"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("raw put failed ({})", service_code(&error)));
    assert!(matches!(
        store.head(&raw).await,
        Err(StorageError::InvalidProviderMetadata { .. })
    ));
    store.delete(&raw).await.unwrap();

    assert_eq!(store.cleanup_staging().unwrap(), 0);
}

#[tokio::test]
#[ignore = "requires an explicit isolated S3-compatible bucket"]
async fn cancelled_s3_multipart_is_unselectable_and_aborted() {
    let environment = S3Environment::from_env();
    assert_bucket_exists(&environment).await;
    let directory = tempfile::tempdir().unwrap();
    let store = environment.store(directory.path());
    let object = object("s3", "cancelled-multipart");
    let key = store.object_key(&object);
    let bytes = vec![0x3c; 48 * 1024 * 1024];
    let expected = metadata(&bytes);
    let task_store = store.clone();
    let task_object = object.clone();
    let task = tokio::spawn(async move {
        task_store
            .put_if_absent(&task_object, stream(bytes), &expected)
            .await
    });

    let mut observed = false;
    for _ in 0..2_000 {
        if incomplete_upload_count(&environment, &key).await > 0 {
            observed = true;
            break;
        }
        if task.is_finished() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        observed,
        "multipart upload completed before cancellation boundary"
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    let mut cleaned = false;
    for _ in 0..2_000 {
        if incomplete_upload_count(&environment, &key).await == 0 {
            cleaned = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(cleaned, "cancelled multipart upload was not aborted");
    assert_eq!(store.head(&object).await.unwrap(), None);
    assert_eq!(store.cleanup_staging().unwrap(), 0);
}

#[tokio::test]
async fn cancelled_input_stream_never_reaches_any_visible_namespace() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalFilesystemBlobStore::open(directory.path(), 1024).unwrap();
    let object = object("filesystem", "cancelled-input");
    let expected = metadata(b"complete");
    let (mut writer, reader) = tokio::io::duplex(8);
    writer.write_all(b"c").await.unwrap();
    let task_store = store.clone();
    let task_object = object.clone();
    let task = tokio::spawn(async move {
        task_store
            .put_if_absent(&task_object, Box::pin(reader), &expected)
            .await
    });
    tokio::task::yield_now().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(store.head(&object).await.unwrap(), None);
    assert_eq!(store.cleanup_staging().unwrap(), 0);
}

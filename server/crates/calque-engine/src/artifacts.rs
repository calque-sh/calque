use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use artifact_contract::RepresentationEncoding;
use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{Path, RawQuery, State},
    http::{header, HeaderValue, StatusCode},
    response::Response,
    routing::get,
    Router,
};
use blob_store::{ManagedBlobStore, ObjectMetadata};
use bootstrap_api::{
    DeliveryError, DeliveryLocation, DeliveryPrincipal, DeliveryPurpose, DeliveryResolver,
};
use serde::{Deserialize, Serialize};
use sync_types::{ObjectId, PlanId};
use thiserror::Error;
use time::OffsetDateTime;
use tokio_util::io::ReaderStream;
use url::Url;

use crate::signing::{PlanKey, ARTIFACT_CAPABILITY_DOMAIN};

/// The most bytes one read of an artifact object puts in one body frame.
/// `ReaderStream`'s default of 4 KiB would make each read of a filesystem
/// object, and each frame, about 4 KiB: thousands of each for a
/// multi-megabyte object. It matches the client's largest BYOB read.
const ARTIFACT_READ_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArtifactGrant {
    pub(crate) object_id: ObjectId,
    pub(crate) metadata: ObjectMetadata,
    pub(crate) encoding: RepresentationEncoding,
    /// The decoded artifact's media type from its manifest, served as the
    /// response `Content-Type`.
    pub(crate) media_type: String,
    pub(crate) expires_at_unix_micros: i64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ArtifactCapability {
    key_id: String,
    object_id: String,
    bytes: u64,
    sha256: String,
    encoding: RepresentationEncoding,
    media_type: String,
    plan_id: String,
    generation: u64,
    expires_at_unix_micros: i64,
}

/// Signs each artifact URL for one object of one plan, and opens what it
/// signed while the plan's generation is the one this process serves.
#[derive(Debug, Clone)]
pub(crate) struct ArtifactCapabilities {
    key: PlanKey,
    generation: Arc<AtomicU64>,
}

impl ArtifactCapabilities {
    pub(crate) fn new(key: PlanKey, generation: u64) -> Self {
        Self {
            key,
            generation: Arc::new(AtomicU64::new(generation)),
        }
    }

    /// Refuses every capability signed before `generation`: a fence retired
    /// the generation they were signed under.
    pub(crate) fn retire_before(&self, generation: u64) {
        self.generation.fetch_max(generation, Ordering::SeqCst);
    }

    /// `url` with the capability for `grant` in its query.
    pub(crate) fn sign_url(
        &self,
        url: &str,
        grant: &ArtifactGrant,
        plan_id: &PlanId,
        generation: u64,
    ) -> Result<String, ArtifactError> {
        let mut url = Url::parse(url).map_err(DeliveryError::InvalidUrl)?;
        let capability = self.key.sign(
            ARTIFACT_CAPABILITY_DOMAIN,
            &ArtifactCapability {
                key_id: self.key.key_id().to_owned(),
                object_id: grant.object_id.as_str().to_owned(),
                bytes: grant.metadata.bytes,
                sha256: grant.metadata.sha256.clone(),
                encoding: grant.encoding,
                media_type: grant.media_type.clone(),
                plan_id: plan_id.as_str().to_owned(),
                generation,
                expires_at_unix_micros: grant.expires_at_unix_micros,
            },
        );
        url.query_pairs_mut().append_pair("cap", &capability);
        Ok(url.into())
    }

    fn open(
        &self,
        capability: &str,
        object: &ObjectId,
        now_unix_micros: i64,
    ) -> Result<ArtifactGrant, ArtifactError> {
        let signed: ArtifactCapability = self
            .key
            .open(ARTIFACT_CAPABILITY_DOMAIN, capability)
            .ok_or(ArtifactError::NotAllowed)?;
        if signed.object_id != object.as_str()
            || signed.expires_at_unix_micros <= now_unix_micros
            || signed.generation != self.generation.load(Ordering::SeqCst)
        {
            return Err(ArtifactError::NotAllowed);
        }
        Ok(ArtifactGrant {
            object_id: object.clone(),
            metadata: ObjectMetadata::new(signed.bytes, signed.sha256)
                .map_err(|_| ArtifactError::NotAllowed)?,
            encoding: signed.encoding,
            media_type: signed.media_type,
            expires_at_unix_micros: signed.expires_at_unix_micros,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LocalArtifactResolver {
    base: Url,
}

impl LocalArtifactResolver {
    /// The browser-visible base every artifact and change URL is built on.
    ///
    /// Those URLs carry a capability in their query, so they go out in
    /// cleartext only on loopback: `http://127.0.0.1`, or `https` on any host,
    /// the base a deployment behind a TLS proxy advertises.
    pub fn new(base: impl AsRef<str>) -> Result<Self, ArtifactError> {
        let mut base = Url::parse(base.as_ref()).map_err(DeliveryError::InvalidUrl)?;
        let transport_is_safe = match base.scheme() {
            "http" => base.host_str() == Some("127.0.0.1"),
            "https" => base.has_host(),
            _ => false,
        };
        if !transport_is_safe
            || !base.username().is_empty()
            || base.password().is_some()
            || base.fragment().is_some()
        {
            return Err(ArtifactError::UnsafeBaseUrl);
        }
        base.set_query(None);
        base.set_fragment(None);
        Ok(Self { base })
    }

    pub fn change_endpoint(&self) -> Result<String, ArtifactError> {
        let mut url = self.base.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|()| DeliveryError::CannotBeBaseUrl)?;
        segments.pop_if_empty().push("v1").push("changes");
        drop(segments);
        Ok(url.into())
    }
}

#[async_trait]
impl DeliveryResolver for LocalArtifactResolver {
    async fn resolve(
        &self,
        _principal: &DeliveryPrincipal,
        object: &ObjectId,
        purpose: DeliveryPurpose,
        plan_expires_at: OffsetDateTime,
    ) -> Result<DeliveryLocation, DeliveryError> {
        if purpose != DeliveryPurpose::ArtifactRepresentation {
            return Err(DeliveryError::Unavailable(
                "unsupported local delivery purpose".to_owned(),
            ));
        }
        let mut url = self.base.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|()| DeliveryError::CannotBeBaseUrl)?;
        segments.pop_if_empty();
        segments
            .push("v1")
            .push("artifact-objects")
            .push(object.as_str());
        drop(segments);
        DeliveryLocation::new(url.as_str(), Some(plan_expires_at))
    }
}

#[derive(Clone)]
pub struct ArtifactService {
    store: Arc<dyn ManagedBlobStore>,
    capabilities: ArtifactCapabilities,
}

impl ArtifactService {
    pub(crate) fn new(
        store: Arc<dyn ManagedBlobStore>,
        capabilities: ArtifactCapabilities,
    ) -> Self {
        Self {
            store,
            capabilities,
        }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/v1/artifact-objects/{object_id}", get(get_artifact))
            .with_state(self)
    }
}

async fn get_artifact(
    State(service): State<ArtifactService>,
    Path(object_id): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    match serve_artifact(service, object_id, query).await {
        Ok(response) => response,
        Err(ArtifactError::InvalidCapability) => empty_response(StatusCode::UNAUTHORIZED),
        Err(ArtifactError::NotAllowed | ArtifactError::ObjectMissing) => {
            empty_response(StatusCode::NOT_FOUND)
        }
        Err(_) => empty_response(StatusCode::SERVICE_UNAVAILABLE),
    }
}

async fn serve_artifact(
    service: ArtifactService,
    object_id: String,
    query: Option<String>,
) -> Result<Response, ArtifactError> {
    let capability = exact_capability(query.as_deref()).ok_or(ArtifactError::InvalidCapability)?;
    let object = ObjectId::new(object_id).map_err(|_| ArtifactError::NotAllowed)?;
    let grant = service
        .capabilities
        .open(capability, &object, now_unix_micros()?)?;
    let observed = service.store.head(&object).await?;
    if observed.as_ref() != Some(&grant.metadata) {
        return Err(ArtifactError::ObjectMissing);
    }
    let stream = service
        .store
        .get(&object)
        .await?
        .ok_or(ArtifactError::ObjectMissing)?;
    let mut response = Response::new(Body::from_stream(ReaderStream::with_capacity(
        stream,
        ARTIFACT_READ_BYTES,
    )));
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&grant.media_type).map_err(|_| ArtifactError::InvalidGrant)?,
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&grant.metadata.bytes.to_string())
            .map_err(|_| ArtifactError::InvalidGrant)?,
    );
    if let Some(encoding) = content_encoding(grant.encoding) {
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static(encoding));
    }
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    Ok(response)
}

fn exact_capability(query: Option<&str>) -> Option<&str> {
    let query = query?;
    let (name, value) = query.split_once('=')?;
    (name == "cap" && !value.is_empty() && !value.contains('&')).then_some(value)
}

const fn content_encoding(encoding: RepresentationEncoding) -> Option<&'static str> {
    match encoding {
        RepresentationEncoding::Zstd => Some("zstd"),
        RepresentationEncoding::Identity => None,
        // Decoded by the page's worker, not by the browser, which also restores
        // a packed object's artifact.
        RepresentationEncoding::BrotliLargeWindow
        | RepresentationEncoding::PackedBrotliLargeWindow => None,
    }
}

fn now_unix_micros() -> Result<i64, ArtifactError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ArtifactError::Clock)?;
    i64::try_from(elapsed.as_micros()).map_err(|_| ArtifactError::Clock)
}

fn empty_response(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::empty())
        .expect("static response")
}

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("artifact base URL must be https://, or http:// on 127.0.0.1")]
    UnsafeBaseUrl,
    #[error("artifact capability is invalid")]
    InvalidCapability,
    #[error("artifact grant is invalid")]
    InvalidGrant,
    #[error("artifact capability does not allow this object")]
    NotAllowed,
    #[error("artifact object is unavailable or changed")]
    ObjectMissing,
    #[error("system clock cannot represent a valid artifact expiry")]
    Clock,
    #[error(transparent)]
    Storage(#[from] blob_store::StorageError),
    #[error(transparent)]
    Delivery(#[from] DeliveryError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use artifact_contract::DIRECT_COLUMNAR_MEDIA_TYPE;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use dataset_grant::{GrantPresentation, GrantVerifier};
    use dataset_grant_issuer::GrantIssuer;

    use crate::{signing::PLAN_DOMAIN, SecretToken};

    const GENERATION: u64 = 4;

    fn key(byte: u8) -> PlanKey {
        PlanKey::new(&SecretToken::from_urlsafe(&URL_SAFE_NO_PAD.encode([byte; 32])).unwrap())
    }

    fn capabilities() -> ArtifactCapabilities {
        ArtifactCapabilities::new(key(1), GENERATION)
    }

    fn grant(object_id: &ObjectId, metadata: &ObjectMetadata, expires_at: i64) -> ArtifactGrant {
        ArtifactGrant {
            object_id: object_id.clone(),
            metadata: metadata.clone(),
            encoding: RepresentationEncoding::Identity,
            media_type: DIRECT_COLUMNAR_MEDIA_TYPE.to_owned(),
            expires_at_unix_micros: expires_at,
        }
    }

    fn capability(
        capabilities: &ArtifactCapabilities,
        grant: &ArtifactGrant,
        generation: u64,
    ) -> String {
        let url = capabilities
            .sign_url(
                "http://127.0.0.1:8787/v1/artifact-objects/x",
                grant,
                &PlanId::new("plan-1").unwrap(),
                generation,
            )
            .unwrap();
        exact_capability(Url::parse(&url).unwrap().query())
            .unwrap()
            .to_owned()
    }

    fn metadata() -> ObjectMetadata {
        ObjectMetadata::new(12, "a".repeat(64)).unwrap()
    }

    #[tokio::test]
    async fn an_artifact_object_is_served_whole_in_reads_of_up_to_256_kib() {
        use tower::ServiceExt;

        let directory = tempfile::tempdir().unwrap();
        let store =
            blob_store::LocalFilesystemBlobStore::open(directory.path(), 4 * 1024 * 1024).unwrap();
        // Two full reads and a remainder.
        let object_bytes: Vec<u8> = (0..2 * ARTIFACT_READ_BYTES + 12_345)
            .map(|index| (index % 251) as u8)
            .collect();
        let metadata = ObjectMetadata::for_bytes(&object_bytes);
        let object = ObjectId::new(format!("sha256:{}", metadata.sha256)).unwrap();
        blob_store::BlobStore::put_if_absent(
            &store,
            &object,
            Box::pin(std::io::Cursor::new(object_bytes.clone())),
            &metadata,
        )
        .await
        .unwrap();
        let capabilities = capabilities();
        let now = now_unix_micros().unwrap();
        let mut granted = grant(&object, &metadata, now + 60_000_000);
        granted.media_type = "application/octet-stream".to_owned();
        let capability = capability(&capabilities, &granted, GENERATION);

        let response = ArtifactService::new(Arc::new(store), capabilities)
            .router()
            .oneshot(
                axum::http::Request::get(format!(
                    "/v1/artifact-objects/{}?cap={capability}",
                    object.as_str(),
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/octet-stream"
        );
        let mut body = response.into_body();
        let mut frames = Vec::new();
        while let Some(frame) = std::future::poll_fn(|context| {
            http_body::Body::poll_frame(std::pin::Pin::new(&mut body), context)
        })
        .await
        {
            frames.push(frame.unwrap().into_data().unwrap());
        }
        // At `ReaderStream`'s default 4 KiB this object takes 132 frames.
        assert!(frames.len() <= 3, "{} frames", frames.len());
        assert_eq!(frames.concat(), object_bytes);
    }

    /// The page's own worker decodes this object, so it goes without a
    /// Content-Encoding, at its length, behind the check every object is behind.
    #[tokio::test]
    async fn a_brotli_large_window_object_is_served_uncoded_and_only_behind_its_capability() {
        served_uncoded_and_only_behind_its_capability(RepresentationEncoding::BrotliLargeWindow)
            .await;
    }

    /// The worker also restores a packed object's artifact, so the packed
    /// object is served the same way.
    #[tokio::test]
    async fn a_packed_object_is_served_uncoded_and_only_behind_its_capability() {
        served_uncoded_and_only_behind_its_capability(
            RepresentationEncoding::PackedBrotliLargeWindow,
        )
        .await;
    }

    async fn served_uncoded_and_only_behind_its_capability(encoding: RepresentationEncoding) {
        use tower::ServiceExt;

        let directory = tempfile::tempdir().unwrap();
        let store =
            blob_store::LocalFilesystemBlobStore::open(directory.path(), 4 * 1024 * 1024).unwrap();
        let object_bytes: Vec<u8> = (0..70_000).map(|index| (index % 249) as u8).collect();
        let metadata = ObjectMetadata::for_bytes(&object_bytes);
        let object = ObjectId::new(format!("sha256:{}", metadata.sha256)).unwrap();
        // The same bytes under a second name, so that the store's metadata check
        // cannot tell the two objects apart and only the signed object id can.
        let twin = ObjectId::new(format!("twin:{}", metadata.sha256)).unwrap();
        for name in [&object, &twin] {
            blob_store::BlobStore::put_if_absent(
                &store,
                name,
                Box::pin(std::io::Cursor::new(object_bytes.clone())),
                &metadata,
            )
            .await
            .unwrap();
        }

        let capabilities = capabilities();
        let now = now_unix_micros().unwrap();
        let mut granted = grant(&object, &metadata, now + 60_000_000);
        granted.encoding = encoding;
        let signed = capability(&capabilities, &granted, GENERATION);
        let twin_capability = capability(
            &capabilities,
            &ArtifactGrant {
                object_id: twin.clone(),
                ..granted.clone()
            },
            GENERATION,
        );
        let other_key = capability(
            &ArtifactCapabilities::new(key(2), GENERATION),
            &granted,
            GENERATION,
        );
        let changed_bytes = capability(
            &capabilities,
            &ArtifactGrant {
                metadata: ObjectMetadata::new(metadata.bytes + 1, metadata.sha256.clone()).unwrap(),
                ..granted.clone()
            },
            GENERATION,
        );
        // A changed plan id: nothing but the tag refuses it.
        let (payload, tag) = signed.split_once('.').unwrap();
        let payload_bytes = URL_SAFE_NO_PAD.decode(payload).unwrap();
        let plan_at = payload_bytes
            .windows(6)
            .position(|window| window == b"plan-1")
            .unwrap();
        let mut tampered_bytes = payload_bytes.clone();
        tampered_bytes[plan_at + 5] = b'2';
        let tampered = format!("{}.{tag}", URL_SAFE_NO_PAD.encode(&tampered_bytes));
        let plan_domain = key(1).sign(
            PLAN_DOMAIN,
            &serde_json::from_slice::<ArtifactCapability>(&payload_bytes).unwrap(),
        );
        assert_eq!(plan_domain.split_once('.').unwrap().0, payload);

        let router = ArtifactService::new(Arc::new(store), capabilities.clone()).router();
        let get = |query: String| {
            let router = router.clone();
            let uri = format!("/v1/artifact-objects/{}{query}", object.as_str());
            async move {
                router
                    .oneshot(axum::http::Request::get(uri).body(Body::empty()).unwrap())
                    .await
                    .unwrap()
            }
        };

        let response = get(format!("?cap={signed}")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
        assert_eq!(
            response.headers()[header::CONTENT_LENGTH],
            object_bytes.len().to_string()
        );
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            DIRECT_COLUMNAR_MEDIA_TYPE
        );
        let body = axum::body::to_bytes(response.into_body(), object_bytes.len() + 1)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), object_bytes.as_slice());

        assert_eq!(get(String::new()).await.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            get("?cap=".to_owned()).await.status(),
            StatusCode::UNAUTHORIZED
        );
        for (what, capability) in [
            ("the twin's", &twin_capability),
            ("another key's", &other_key),
            ("a tampered", &tampered),
            ("a plan-domain", &plan_domain),
            ("another length's", &changed_bytes),
        ] {
            assert_eq!(
                get(format!("?cap={capability}")).await.status(),
                StatusCode::NOT_FOUND,
                "{what} capability"
            );
        }

        capabilities.retire_before(GENERATION + 1);
        assert_eq!(
            get(format!("?cap={signed}")).await.status(),
            StatusCode::NOT_FOUND,
            "a retired generation's capability"
        );
    }

    #[test]
    fn a_capability_opens_until_its_expiry_while_its_generation_is_current() {
        let capabilities = capabilities();
        let object = ObjectId::new("sha256:one").unwrap();
        let signed = capability(&capabilities, &grant(&object, &metadata(), 20), GENERATION);
        assert_eq!(
            capabilities.open(&signed, &object, 19).unwrap(),
            grant(&object, &metadata(), 20)
        );
        assert!(matches!(
            capabilities.open(&signed, &object, 20),
            Err(ArtifactError::NotAllowed)
        ));
        assert!(matches!(
            capabilities.open(&signed, &ObjectId::new("sha256:two").unwrap(), 19),
            Err(ArtifactError::NotAllowed)
        ));

        let future = capability(
            &capabilities,
            &grant(&object, &metadata(), 20),
            GENERATION + 1,
        );
        assert!(matches!(
            capabilities.open(&future, &object, 19),
            Err(ArtifactError::NotAllowed)
        ));
        capabilities.retire_before(GENERATION + 1);
        assert!(capabilities.open(&future, &object, 19).is_ok());
        assert!(matches!(
            capabilities.open(&signed, &object, 19),
            Err(ArtifactError::NotAllowed)
        ));
        capabilities.retire_before(GENERATION);
        assert!(capabilities.open(&future, &object, 19).is_ok());
    }

    #[test]
    fn artifact_capability_is_rejected_after_its_verified_grant_expired() {
        let (issuer, _) = GrantIssuer::generate("key-a", "dataset-a").unwrap();
        let verifier =
            GrantVerifier::new("dataset-a", [issuer.verification_key().unwrap()]).unwrap();
        let token = issuer
            .issue("user-1", BTreeMap::new(), 4, 1_000, 300)
            .unwrap();
        let verified = verifier
            .verify(&GrantPresentation {
                token: &token,
                now_unix_seconds: 1_001,
                authorization_generation: 4,
            })
            .unwrap();
        let grant_expiry = i64::try_from(verified.expires_at_unix_seconds()).unwrap() * 1_000_000;

        let capabilities = capabilities();
        let object = ObjectId::new("sha256:grant-bounded").unwrap();
        let signed = capability(
            &capabilities,
            &grant(&object, &metadata(), grant_expiry),
            GENERATION,
        );
        assert!(capabilities
            .open(&signed, &object, grant_expiry - 1)
            .is_ok());
        assert!(matches!(
            capabilities.open(&signed, &object, grant_expiry),
            Err(ArtifactError::NotAllowed)
        ));
    }

    #[test]
    fn a_signed_url_keeps_its_path_and_carries_only_the_capability() {
        let resolver = LocalArtifactResolver::new("http://127.0.0.1:5173/_calque/").unwrap();
        assert_eq!(
            resolver.change_endpoint().unwrap(),
            "http://127.0.0.1:5173/_calque/v1/changes"
        );
        let expires_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let location = runtime
            .block_on(resolver.resolve(
                &DeliveryPrincipal::new("local").unwrap(),
                &ObjectId::new("sha256:abc").unwrap(),
                DeliveryPurpose::ArtifactRepresentation,
                expires_at,
            ))
            .unwrap();
        assert_eq!(
            location.url().as_str(),
            "http://127.0.0.1:5173/_calque/v1/artifact-objects/sha256:abc"
        );
        assert_eq!(location.expires_at(), Some(expires_at));

        let signed = capabilities()
            .sign_url(
                location.url().as_str(),
                &grant(&ObjectId::new("sha256:abc").unwrap(), &metadata(), 20),
                &PlanId::new("plan-1").unwrap(),
                GENERATION,
            )
            .unwrap();
        let signed = Url::parse(&signed).unwrap();
        assert_eq!(signed.path(), "/_calque/v1/artifact-objects/sha256:abc");
        assert!(exact_capability(signed.query()).is_some());
    }

    /// Behind a TLS proxy the plan's URLs name the public HTTPS origin; a
    /// cleartext base is refused anywhere but loopback.
    #[test]
    fn resolver_builds_on_an_https_public_base_and_refuses_remote_cleartext() {
        let resolver = LocalArtifactResolver::new("https://sync.example.com:443/").unwrap();
        assert_eq!(
            resolver.change_endpoint().unwrap(),
            "https://sync.example.com/v1/changes"
        );
        let resolver = LocalArtifactResolver::new("https://localhost:8443/").unwrap();
        assert_eq!(
            resolver.change_endpoint().unwrap(),
            "https://localhost:8443/v1/changes"
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let location = runtime
            .block_on(resolver.resolve(
                &DeliveryPrincipal::new("local").unwrap(),
                &ObjectId::new("sha256:abc").unwrap(),
                DeliveryPurpose::ArtifactRepresentation,
                OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap(),
            ))
            .unwrap();
        assert!(location
            .url()
            .as_str()
            .starts_with("https://localhost:8443/v1/artifact-objects/"));

        for refused in [
            "http://sync.example.com/",
            "http://localhost:8787/",
            "http://10.0.0.4:8787/",
            "ftp://127.0.0.1/",
            "https://user@sync.example.com/",
            "https://sync.example.com/#fragment",
        ] {
            assert!(
                matches!(
                    LocalArtifactResolver::new(refused),
                    Err(ArtifactError::UnsafeBaseUrl)
                ),
                "{refused}"
            );
        }
    }

    #[test]
    fn capability_parser_rejects_extensions_and_duplicates() {
        assert_eq!(exact_capability(Some("cap=value")), Some("value"));
        assert_eq!(exact_capability(Some("cap=value&x=1")), None);
        assert_eq!(exact_capability(Some("x=value")), None);
        assert_eq!(exact_capability(None), None);
    }
}

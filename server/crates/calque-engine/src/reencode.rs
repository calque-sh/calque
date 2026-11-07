//! The background re-encode of a published artifact's zstd object.
//!
//! A publication encodes its zstd object at level 12, which keeps preparation
//! short. Once the artifact is published, one thread encodes the same decoded
//! object again at level 19 and publishes a second artifact for the same cut,
//! whose manifest differs from the first only in its id and its zstd
//! representation. Bootstrap's cost order then hands every later plan the
//! smaller object. A plan made before keeps its grant on the level-12 object,
//! and the level-12 artifact is retired once no plan pins it.
//!
//! The same thread then publishes a third artifact, which adds one
//! large-window brotli stream to the level-19 manifest: of the packer's body
//! where [`packs`] says to pack and the packer shortens it, and of the decoded
//! object otherwise. Both older artifacts are retired once no plan pins them.

use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex as StdMutex, PoisonError,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use artifact_catalog::{
    BuildLease, CatalogError, PublishOutcome, PublishedArtifactQuery, SqliteArtifactCatalog,
};
use artifact_contract::{
    ArtifactRepresentation, DecodedArtifactFormat, ManifestError, RepresentationEncoding,
    ValidatedArtifactManifest,
};
use blob_store::{ManagedBlobStore, ObjectMetadata, StorageError};
use snapshot_pipeline::{
    encode_brotli_large_window_controlled, encode_packed_brotli_large_window_controlled,
    pack_artifact_controlled, reencode_zstd_controlled, RepresentationError, BROTLI_QUALITY,
    REENCODED_ZSTD_LEVEL,
};
use sync_types::{ArtifactId, IdentifierError, ObjectId, OpaqueCursor};
use thiserror::Error;
use tokio::{io::AsyncReadExt, sync::RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::state::snapshot_workspace_root;

/// A re-encode's build lease, held from registration to publication: one
/// object's upload and the verification of up to three.
const REENCODE_LEASE_MS: i64 = 10 * 60 * 1000;
/// How long the cleanup of a refused or failed re-encode may hold its lease.
const CLEANUP_LEASE_MS: i64 = 60 * 1000;
/// The longest a retirement waits on a plan's pin before it asks again.
const RETIRE_RETRY: Duration = Duration::from_secs(60);
const DOWNLOAD_BUFFER_BYTES: usize = 1024 * 1024;

/// What `calque dev` and `calque serve` hand `LocalBackend` to re-encode each
/// artifact they publish.
pub struct ReencodeConfig {
    pub state_root: PathBuf,
    /// Opened afresh for each re-encode, beside the handle bootstrap plans
    /// from, as a fenced generation's publication opens it.
    pub catalog_path: PathBuf,
    /// The process's own: cancelling it stops any re-encode.
    pub cancellation: CancellationToken,
}

/// The one re-encode a process runs at a time.
pub(crate) struct Reencoder {
    pub(crate) config: ReencodeConfig,
    /// Held for a whole re-encode, so a second waits for the first to end.
    pub(crate) serial: StdMutex<()>,
    /// The running re-encode's token. A fence cancels it, and each new
    /// re-encode cancels the one before.
    current: StdMutex<CancellationToken>,
    /// Re-encodes spawned whose encodes are not over yet. While any is, the
    /// re-cut gauge reads in progress.
    encoding: Arc<AtomicUsize>,
}

/// One spawned re-encode's share of [`Reencoder::is_encoding`], given back
/// once, when its encodes are over or it ends.
pub(crate) struct EncodingSpan {
    encoding: Arc<AtomicUsize>,
    open: std::cell::Cell<bool>,
}

impl EncodingSpan {
    pub(crate) fn close(&self) {
        if self.open.replace(false) {
            self.encoding.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Drop for EncodingSpan {
    fn drop(&mut self) {
        self.close();
    }
}

impl Reencoder {
    pub(crate) fn new(config: ReencodeConfig) -> Self {
        let current = config.cancellation.child_token();
        Self {
            config,
            serial: StdMutex::new(()),
            current: StdMutex::new(current),
            encoding: Arc::default(),
        }
    }

    pub(crate) fn is_encoding(&self) -> bool {
        self.encoding.load(Ordering::Acquire) > 0
    }

    /// Counts a re-encode from the moment it is spawned; the span the spawned
    /// thread holds gives the count back.
    pub(crate) fn begin_encoding(&self) -> EncodingSpan {
        self.encoding.fetch_add(1, Ordering::AcqRel);
        EncodingSpan {
            encoding: Arc::clone(&self.encoding),
            open: std::cell::Cell::new(true),
        }
    }

    /// Cancels the running re-encode, if any.
    pub(crate) fn cancel(&self) {
        self.current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancel();
    }

    /// Cancels the running re-encode and returns the token of the next.
    pub(crate) fn replace(&self) -> CancellationToken {
        let next = self.config.cancellation.child_token();
        let previous = std::mem::replace(
            &mut *self.current.lock().unwrap_or_else(PoisonError::into_inner),
            next.clone(),
        );
        previous.cancel();
        next
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReencodeStage {
    /// The published decoded object is local, and the encode starts.
    Encoding,
    /// The re-encoded object is uploaded, and its publication waits for the
    /// fence lock.
    Admitting,
    /// The decoded object is on disk again, the rule packs it, and the pack is
    /// about to start.
    Packing,
    /// The brotli encode is about to start, of the packed body or of the
    /// decoded object.
    BrotliEncoding,
    /// The brotli object is uploaded and the catalog is about to ask `admit`.
    BrotliAdmitting,
    /// The encodes are over, and what they replace is about to be retired.
    Retiring,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReencodeOutcome {
    /// The re-encoded artifact is published, and every artifact it replaces
    /// is retired.
    Published { artifact: ArtifactId },
    /// This cut's re-encoded artifact was published before; what it replaces
    /// is now retired.
    AlreadyPublished,
    /// Nothing is published for this query at a publication's own level.
    NothingToReencode,
    /// The catalog refused the publication: the generation it was re-encoded
    /// under is retired. Nothing was published, and its object is deleted.
    Refused,
}

#[derive(Debug, Error)]
pub(crate) enum ReencodeError {
    #[error("the re-encode was cancelled")]
    Cancelled,
    #[error("the published artifact {0} has no {1:?} representation")]
    MissingRepresentation(ArtifactId, RepresentationEncoding),
    #[error("the store has no object {0}")]
    MissingObject(ObjectId),
    #[error("the re-encode's decoded chunks differ from the published manifest's")]
    ChunksDiffer,
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Encode(#[from] RepresentationError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Identifier(#[from] IdentifierError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("the system clock is before 1970 or past 2262")]
    Clock,
}

#[derive(Clone, Copy)]
pub(crate) struct ReencodeRequest<'a> {
    pub(crate) state_root: &'a Path,
    pub(crate) catalog_path: &'a Path,
    pub(crate) store: &'a dyn ManagedBlobStore,
    pub(crate) query: &'a PublishedArtifactQuery,
    /// Only the artifacts `query` publishes at this cut are re-encoded or
    /// retired; an older cut's go once a newer cut replaces them.
    pub(crate) cut: &'a OpaqueCursor,
    /// The backend's fence lock, held shared while the catalog asks `admit`.
    pub(crate) fence: &'a RwLock<()>,
    pub(crate) cancellation: &'a CancellationToken,
}

/// One stage's outcome, and the published artifacts its result replaces.
struct Staged {
    outcome: ReencodeOutcome,
    replaced: Vec<ValidatedArtifactManifest>,
}

impl Staged {
    fn nothing(outcome: ReencodeOutcome) -> Self {
        Self {
            outcome,
            replaced: Vec::new(),
        }
    }
}

/// Re-encodes the artifact published for `request.query` at
/// [`REENCODED_ZSTD_LEVEL`] and publishes the result if `admit` holds when the
/// catalog asks.
async fn zstd_stage(
    request: ReencodeRequest<'_>,
    admit: &impl Fn() -> bool,
    on_stage: &mut impl FnMut(ReencodeStage),
) -> Result<Staged, ReencodeError> {
    let ReencodeRequest {
        state_root,
        catalog_path,
        store,
        query,
        cut,
        fence,
        cancellation,
    } = request;
    let mut catalog = SqliteArtifactCatalog::open(catalog_path)?;
    let (reencoded, replaced): (Vec<_>, Vec<_>) = published_at(&catalog, query, cut)?
        .into_iter()
        .partition(|manifest| zstd_level(manifest) == Some(REENCODED_ZSTD_LEVEL));
    if !reencoded.is_empty() {
        return Ok(Staged {
            outcome: ReencodeOutcome::AlreadyPublished,
            replaced,
        });
    }
    // A signed manifest cannot be rewritten without its key, and local
    // publications are unsigned.
    let Some(source) = replaced
        .iter()
        .find(|manifest| manifest.manifest().signature.is_none())
    else {
        return Ok(Staged::nothing(ReencodeOutcome::NothingToReencode));
    };
    let identity = representation(source, RepresentationEncoding::Identity)?;
    let zstd = representation(source, RepresentationEncoding::Zstd)?;
    let decoded = &source.manifest().decoded;

    let work = WorkDirectory::create(
        &snapshot_workspace_root(state_root).join(format!("reencode-{}", Uuid::new_v4().simple())),
    )?;
    let decoded_path = work.path.join("decoded");
    download(store, &identity.object_id, &decoded_path, cancellation).await?;
    on_stage(ReencodeStage::Encoding);
    let started = Instant::now();
    // One byte under the object it replaces: an encode no smaller fails
    // rather than publishing a second copy of the same cost.
    let (encoded, chunks) = reencode_zstd_controlled(
        &decoded_path,
        decoded.bytes,
        &decoded.sha256,
        zstd.encoded_bytes.saturating_sub(1),
        cancellation,
    )
    .map_err(|error| cancelled_or(cancellation, error.into()))?;
    let encode_ms = started.elapsed().as_millis();
    let DecodedArtifactFormat::DirectColumnar {
        chunks: published_chunks,
        ..
    } = &decoded.format;
    if published_chunks.sha256 != chunks {
        return Err(ReencodeError::ChunksDiffer);
    }

    let mut manifest = source.manifest().clone();
    manifest.artifact_id = ArtifactId::new(format!(
        "{}.zstd-{REENCODED_ZSTD_LEVEL}",
        manifest.artifact_id.as_str()
    ))?;
    for representation in &mut manifest.representations {
        if representation.encoding == RepresentationEncoding::Zstd {
            *representation = encoded.manifest().clone();
        }
    }
    let manifest = ValidatedArtifactManifest::parse(&serde_json::to_vec(&manifest)?)?;
    if cancellation.is_cancelled() {
        return Err(ReencodeError::Cancelled);
    }

    let owner = format!("reencode-{}", Uuid::new_v4().simple());
    let lease = catalog.register_build(&manifest, &owner, now_ms()?, REENCODE_LEASE_MS)?;
    let published = upload_and_publish(
        &mut catalog,
        store,
        &lease,
        encoded.manifest(),
        encoded.path(),
        fence,
        admit,
        on_stage,
        ReencodeStage::Admitting,
    )
    .await;
    match published {
        Ok(_) => {}
        Err(error) => {
            abandon(&mut catalog, store, &lease, &owner).await?;
            return match error {
                ReencodeError::Catalog(CatalogError::PublicationRefused(_)) => {
                    Ok(Staged::nothing(ReencodeOutcome::Refused))
                }
                error => Err(cancelled_or(cancellation, error)),
            };
        }
    }
    tracing::info!(
        target: "calque::preparation",
        artifact = %manifest.manifest().artifact_id,
        replaced = %source.manifest().artifact_id,
        encoded_bytes = encoded.manifest().encoded_bytes,
        replaced_bytes = zstd.encoded_bytes,
        encode_ms = encode_ms as u64,
        "re-encoded artifact published"
    );
    drop(encoded);
    drop(work);
    let artifact = manifest.manifest().artifact_id.clone();
    Ok(Staged {
        outcome: ReencodeOutcome::Published { artifact },
        replaced,
    })
}

/// The published artifact a brotli representation is added to: the level-19
/// one, unless it is signed, which only its key could rewrite.
fn brotli_source(published: &[ValidatedArtifactManifest]) -> Option<&ValidatedArtifactManifest> {
    published.iter().find(|manifest| {
        manifest.manifest().signature.is_none()
            && zstd_level(manifest) == Some(REENCODED_ZSTD_LEVEL)
    })
}

/// An artifact is packed where its level-19 object is at least a tenth of its
/// decoded bytes. Below that the transfer is over before the client has
/// unpacked, and the plain brotli object is published.
fn packs(level_19_bytes: u64, decoded_bytes: u64) -> bool {
    level_19_bytes.saturating_mul(10) >= decoded_bytes
}

/// Publishes a further artifact that carries one large-window brotli object
/// beside the level-19 artifact's representations, admitted as the re-encode
/// is. A pack or an encode that fails publishes nothing.
async fn brotli_stage(
    request: ReencodeRequest<'_>,
    admit: &impl Fn() -> bool,
    on_stage: &mut impl FnMut(ReencodeStage),
) -> Result<Staged, ReencodeError> {
    let ReencodeRequest {
        state_root,
        catalog_path,
        store,
        query,
        cut,
        fence,
        cancellation,
    } = request;
    let mut catalog = SqliteArtifactCatalog::open(catalog_path)?;
    let (encoded_already, replaced): (Vec<_>, Vec<_>) = published_at(&catalog, query, cut)?
        .into_iter()
        .partition(|manifest| {
            manifest
                .manifest()
                .representations
                .iter()
                .any(|representation| {
                    matches!(
                        representation.encoding,
                        RepresentationEncoding::BrotliLargeWindow
                            | RepresentationEncoding::PackedBrotliLargeWindow
                    )
                })
        });
    if !encoded_already.is_empty() {
        return Ok(Staged {
            outcome: ReencodeOutcome::AlreadyPublished,
            replaced,
        });
    }
    let Some(source) = brotli_source(&replaced) else {
        return Ok(Staged::nothing(ReencodeOutcome::NothingToReencode));
    };
    let identity = representation(source, RepresentationEncoding::Identity)?;
    let zstd = representation(source, RepresentationEncoding::Zstd)?;
    let decoded = &source.manifest().decoded;

    let work = WorkDirectory::create(
        &snapshot_workspace_root(state_root).join(format!("brotli-{}", Uuid::new_v4().simple())),
    )?;
    let decoded_path = work.path.join("decoded");
    download(store, &identity.object_id, &decoded_path, cancellation).await?;
    let started = Instant::now();
    let packed = if packs(zstd.encoded_bytes, decoded.bytes) {
        on_stage(ReencodeStage::Packing);
        let packed = pack_artifact_controlled(&decoded_path, decoded.bytes, cancellation)
            .map_err(|error| cancelled_or(cancellation, error.into()))?;
        if packed.is_none() {
            tracing::info!(
                target: "calque::preparation",
                artifact = %source.manifest().artifact_id,
                "the packer transforms no column of the artifact, which is encoded as it is"
            );
        }
        packed
    } else {
        tracing::info!(
            target: "calque::preparation",
            artifact = %source.manifest().artifact_id,
            level_19_bytes = zstd.encoded_bytes,
            decoded_bytes = decoded.bytes,
            "the level-19 object is under a tenth of the artifact, which is encoded unpacked"
        );
        None
    };
    let pack_ms = started.elapsed().as_millis();
    on_stage(ReencodeStage::BrotliEncoding);
    let started = Instant::now();
    // One byte under the level-19 object: an encode that is not smaller fails,
    // so bootstrap's choice of this representation, for a client that names
    // it, stays the fewest-bytes choice.
    let limit = zstd.encoded_bytes.saturating_sub(1);
    let (encoded, chunks) = match &packed {
        Some(packed) => encode_packed_brotli_large_window_controlled(
            packed,
            decoded.bytes,
            &decoded.sha256,
            limit,
            cancellation,
        ),
        None => encode_brotli_large_window_controlled(
            &decoded_path,
            decoded.bytes,
            &decoded.sha256,
            limit,
            cancellation,
        ),
    }
    .map_err(|error| cancelled_or(cancellation, error.into()))?;
    let encode_ms = started.elapsed().as_millis();
    drop(packed);
    let DecodedArtifactFormat::DirectColumnar {
        chunks: published_chunks,
        ..
    } = &decoded.format;
    if published_chunks.sha256 != chunks {
        return Err(ReencodeError::ChunksDiffer);
    }

    let mut manifest = source.manifest().clone();
    let body = encoded.manifest().packed;
    manifest.artifact_id = ArtifactId::new(format!(
        "{}.{}brotli-{BROTLI_QUALITY}",
        manifest.artifact_id.as_str(),
        if body.is_some() { "packed-" } else { "" }
    ))?;
    manifest.representations.push(encoded.manifest().clone());
    let manifest = ValidatedArtifactManifest::parse(&serde_json::to_vec(&manifest)?)?;
    if cancellation.is_cancelled() {
        return Err(ReencodeError::Cancelled);
    }

    let owner = format!("brotli-{}", Uuid::new_v4().simple());
    let lease = catalog.register_build(&manifest, &owner, now_ms()?, REENCODE_LEASE_MS)?;
    let published = upload_and_publish(
        &mut catalog,
        store,
        &lease,
        encoded.manifest(),
        encoded.path(),
        fence,
        admit,
        on_stage,
        ReencodeStage::BrotliAdmitting,
    )
    .await;
    if let Err(error) = published {
        abandon(&mut catalog, store, &lease, &owner).await?;
        return match error {
            ReencodeError::Catalog(CatalogError::PublicationRefused(_)) => {
                Ok(Staged::nothing(ReencodeOutcome::Refused))
            }
            error => Err(cancelled_or(cancellation, error)),
        };
    }
    let window_bits = encoded.manifest().brotli.map(|brotli| brotli.window_bits);
    // The benchmark finds both log lines by the text their messages end in,
    // "brotli-large-window artifact published", so that ending must not
    // change.
    match body {
        Some(body) => tracing::info!(
            target: "calque::preparation",
            artifact = %manifest.manifest().artifact_id,
            replaced = %source.manifest().artifact_id,
            encoded_bytes = encoded.manifest().encoded_bytes,
            replaced_bytes = zstd.encoded_bytes,
            body_bytes = body.body_bytes,
            quality = BROTLI_QUALITY,
            window_bits,
            pack_ms = pack_ms as u64,
            encode_ms = encode_ms as u64,
            "packed-brotli-large-window artifact published"
        ),
        None => tracing::info!(
            target: "calque::preparation",
            artifact = %manifest.manifest().artifact_id,
            replaced = %source.manifest().artifact_id,
            encoded_bytes = encoded.manifest().encoded_bytes,
            replaced_bytes = zstd.encoded_bytes,
            quality = BROTLI_QUALITY,
            window_bits,
            encode_ms = encode_ms as u64,
            "brotli-large-window artifact published"
        ),
    }
    drop(encoded);
    drop(work);
    let artifact = manifest.manifest().artifact_id.clone();
    Ok(Staged {
        outcome: ReencodeOutcome::Published { artifact },
        replaced,
    })
}

/// What one run of the background encodes did.
#[derive(Debug)]
pub(crate) struct ReencodeOutcomes {
    pub(crate) zstd: ReencodeOutcome,
    /// `None` when no level-19 artifact is published to add brotli to. An
    /// error here leaves the level-19 artifact served.
    pub(crate) brotli: Option<Result<ReencodeOutcome, ReencodeError>>,
}

/// Re-encodes the artifact published for `request.query` at
/// [`REENCODED_ZSTD_LEVEL`], then publishes its brotli artifact, packed or
/// plain, each only if `admit` holds when the catalog asks, which it does while
/// `request.fence` is held shared. A fence holds that lock exclusively while
/// it retires a generation, so an `admit` that answers whether the generation
/// is still served never lets either artifact into a retired one. The
/// artifacts they replace are retired last, so a plan's pin on one never
/// delays a publication.
pub(crate) async fn reencode_and_publish_brotli(
    request: ReencodeRequest<'_>,
    admit: impl Fn() -> bool,
    mut on_stage: impl FnMut(ReencodeStage),
) -> Result<ReencodeOutcomes, ReencodeError> {
    let (outcomes, replaced) = encode_stages(request, admit, &mut on_stage).await?;
    // The store holds its own copies now; a retirement can wait an hour.
    on_stage(ReencodeStage::Retiring);
    let mut catalog = SqliteArtifactCatalog::open(request.catalog_path)?;
    retire(&mut catalog, &replaced, request.cancellation).await?;
    Ok(outcomes)
}

/// [`reencode_and_publish_brotli`]'s encodes, returning the artifacts they
/// replace for the caller to retire rather than waiting on their plans' pins.
pub(crate) async fn reencode_leaving_replaced(
    request: ReencodeRequest<'_>,
    admit: impl Fn() -> bool,
    on_stage: impl FnMut(ReencodeStage),
) -> Result<(ReencodeOutcomes, Vec<ArtifactId>), ReencodeError> {
    let (outcomes, replaced) = encode_stages(request, admit, on_stage).await?;
    Ok((
        outcomes,
        replaced
            .into_iter()
            .map(|manifest| manifest.manifest().artifact_id.clone())
            .collect(),
    ))
}

async fn encode_stages(
    request: ReencodeRequest<'_>,
    admit: impl Fn() -> bool,
    mut on_stage: impl FnMut(ReencodeStage),
) -> Result<(ReencodeOutcomes, Vec<ValidatedArtifactManifest>), ReencodeError> {
    let zstd = zstd_stage(request, &admit, &mut on_stage).await?;
    let mut replaced = zstd.replaced;
    let level_19_is_published = matches!(
        zstd.outcome,
        ReencodeOutcome::Published { .. } | ReencodeOutcome::AlreadyPublished
    );
    let brotli = if level_19_is_published {
        match brotli_stage(request, &admit, &mut on_stage).await {
            Err(ReencodeError::Cancelled) => return Err(ReencodeError::Cancelled),
            Err(error) => Some(Err(error)),
            Ok(staged) => {
                for manifest in staged.replaced {
                    let known = replaced.iter().any(|known| {
                        known.manifest().artifact_id == manifest.manifest().artifact_id
                    });
                    if !known {
                        replaced.push(manifest);
                    }
                }
                Some(Ok(staged.outcome))
            }
        }
    } else {
        None
    };
    Ok((
        ReencodeOutcomes {
            zstd: zstd.outcome,
            brotli,
        },
        replaced,
    ))
}

/// The level-19 re-encode alone, without the brotli artifact that follows it.
#[cfg(test)]
pub(crate) async fn reencode_published(
    request: ReencodeRequest<'_>,
    admit: impl Fn() -> bool,
    mut on_stage: impl FnMut(ReencodeStage),
) -> Result<ReencodeOutcome, ReencodeError> {
    let staged = zstd_stage(request, &admit, &mut on_stage).await?;
    let mut catalog = SqliteArtifactCatalog::open(request.catalog_path)?;
    retire(&mut catalog, &staged.replaced, request.cancellation).await?;
    Ok(staged.outcome)
}

#[allow(clippy::too_many_arguments)]
async fn upload_and_publish(
    catalog: &mut SqliteArtifactCatalog,
    store: &dyn ManagedBlobStore,
    lease: &BuildLease,
    representation: &ArtifactRepresentation,
    path: &Path,
    fence: &RwLock<()>,
    admit: &impl Fn() -> bool,
    on_stage: &mut impl FnMut(ReencodeStage),
    admitting: ReencodeStage,
) -> Result<PublishOutcome, ReencodeError> {
    catalog.mark_uploading(lease, now_ms()?)?;
    let metadata = ObjectMetadata::new(
        representation.encoded_bytes,
        representation.encoded_sha256.clone(),
    )?;
    store
        .put_file_if_absent(&representation.object_id, path, &metadata)
        .await?;
    catalog.mark_uploaded(lease, now_ms()?)?;
    on_stage(admitting);
    let _fence = fence.read().await;
    Ok(catalog
        .verify_and_publish_admitted(lease, store, now_ms()?, admit)
        .await?)
}

/// Removes a re-encode that will not be published, and the object only it
/// referenced.
async fn abandon(
    catalog: &mut SqliteArtifactCatalog,
    store: &dyn ManagedBlobStore,
    lease: &BuildLease,
    owner: &str,
) -> Result<(), ReencodeError> {
    let now = now_ms()?;
    let cleanup = catalog.abandon_build(
        lease,
        owner,
        now,
        0,
        CLEANUP_LEASE_MS,
        "re-encode not published",
    )?;
    catalog
        .cleanup_abandoned_build(&cleanup, store, now)
        .await?;
    Ok(())
}

/// Retires each of `replaced` once no plan pins it.
async fn retire(
    catalog: &mut SqliteArtifactCatalog,
    replaced: &[ValidatedArtifactManifest],
    cancellation: &CancellationToken,
) -> Result<(), ReencodeError> {
    for manifest in replaced {
        loop {
            let now = now_ms()?;
            match catalog.retire(
                &manifest.manifest().artifact_id,
                now,
                "replaced by its re-encode",
            ) {
                Ok(_) => break,
                Err(CatalogError::ArtifactPinnedByPlan { expires_ms }) => {
                    let wait = u64::try_from(expires_ms.saturating_sub(now)).unwrap_or(0);
                    let wait = Duration::from_millis(wait.max(1)).min(RETIRE_RETRY);
                    tokio::select! {
                        () = cancellation.cancelled() => return Err(ReencodeError::Cancelled),
                        () = tokio::time::sleep(wait) => {}
                    }
                }
                // Retired or quarantined since it was read.
                Err(CatalogError::InvalidTransition { .. }) => break,
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

async fn download(
    store: &dyn ManagedBlobStore,
    object: &ObjectId,
    path: &Path,
    cancellation: &CancellationToken,
) -> Result<(), ReencodeError> {
    let mut stream = store
        .get(object)
        .await?
        .ok_or_else(|| ReencodeError::MissingObject(object.clone()))?;
    let mut file = File::create(path)?;
    let mut buffer = vec![0_u8; DOWNLOAD_BUFFER_BYTES];
    loop {
        if cancellation.is_cancelled() {
            return Err(ReencodeError::Cancelled);
        }
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        file.write_all(&buffer[..read])?;
    }
    file.sync_all()?;
    Ok(())
}

fn published_at(
    catalog: &SqliteArtifactCatalog,
    query: &PublishedArtifactQuery,
    cut: &OpaqueCursor,
) -> Result<Vec<ValidatedArtifactManifest>, ReencodeError> {
    Ok(catalog
        .published(query)?
        .into_iter()
        .filter(|manifest| &manifest.manifest().pit.snapshot_cursor == cut)
        .collect())
}

fn representation(
    manifest: &ValidatedArtifactManifest,
    encoding: RepresentationEncoding,
) -> Result<&ArtifactRepresentation, ReencodeError> {
    manifest
        .manifest()
        .representations
        .iter()
        .find(|representation| representation.encoding == encoding)
        .ok_or_else(|| {
            ReencodeError::MissingRepresentation(manifest.manifest().artifact_id.clone(), encoding)
        })
}

fn zstd_level(manifest: &ValidatedArtifactManifest) -> Option<i32> {
    representation(manifest, RepresentationEncoding::Zstd)
        .ok()?
        .zstd
        .as_ref()
        .map(|parameters| parameters.level)
}

/// An encode that fails because it was cancelled reports the cancellation.
fn cancelled_or(cancellation: &CancellationToken, error: ReencodeError) -> ReencodeError {
    if cancellation.is_cancelled() {
        ReencodeError::Cancelled
    } else {
        error
    }
}

fn now_ms() -> Result<i64, ReencodeError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ReencodeError::Clock)?;
    i64::try_from(elapsed.as_millis()).map_err(|_| ReencodeError::Clock)
}

/// A directory removed with everything in it when dropped.
struct WorkDirectory {
    path: PathBuf,
}

impl WorkDirectory {
    fn create(path: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(path)?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }
}

impl Drop for WorkDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        cell::Cell,
        sync::atomic::{AtomicU64, Ordering},
    };

    use artifact_contract::{BrotliParameters, PackedParameters, PACKED_BODY_VERSION};
    use blob_store::{BlobStore, LocalFilesystemBlobStore};
    use direct_columnar::{
        pack::{pack, unpack},
        schema_hash, ArtifactIdentity, ColumnKind, ColumnSchema, Encoder, EncoderLimits,
        TableSchema, Value, WorkspaceSchema,
    };
    use sha2::{Digest, Sha256};
    use snapshot_builder::SnapshotBuildPlan;
    use snapshot_pipeline::PreparedRepresentations;
    use sync_types::{PlanId, SourceEpoch};

    use super::*;
    use crate::snapshot::{
        published_query_for,
        tests::{prepared_source, retry_manifest},
    };

    pub(crate) struct Fixture {
        pub(crate) directory: tempfile::TempDir,
        store: LocalFilesystemBlobStore,
        pub(crate) query: PublishedArtifactQuery,
        pub(crate) published: ValidatedArtifactManifest,
        decoded: Vec<u8>,
    }

    impl Fixture {
        pub(crate) fn catalog(&self) -> SqliteArtifactCatalog {
            SqliteArtifactCatalog::open(self.directory.path().join("catalog.sqlite3")).unwrap()
        }

        pub(crate) fn request<'a>(
            &'a self,
            catalog_path: &'a Path,
            fence: &'a RwLock<()>,
            cancellation: &'a CancellationToken,
        ) -> ReencodeRequest<'a> {
            ReencodeRequest {
                state_root: self.directory.path(),
                catalog_path,
                store: &self.store,
                query: &self.query,
                cut: &self.published.manifest().pit.snapshot_cursor,
                fence,
                cancellation,
            }
        }

        pub(crate) fn reencoded_id(&self) -> ArtifactId {
            ArtifactId::new(format!(
                "{}.zstd-{REENCODED_ZSTD_LEVEL}",
                self.published.manifest().artifact_id.as_str()
            ))
            .unwrap()
        }

        fn brotli_id(&self) -> ArtifactId {
            ArtifactId::new(format!(
                "{}.brotli-{BROTLI_QUALITY}",
                self.reencoded_id().as_str()
            ))
            .unwrap()
        }

        pub(crate) fn packed_id(&self) -> ArtifactId {
            ArtifactId::new(format!(
                "{}.packed-brotli-{BROTLI_QUALITY}",
                self.reencoded_id().as_str()
            ))
            .unwrap()
        }
    }

    struct Random(u64);

    impl Random {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut value = self.0;
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^ (value >> 31)
        }

        fn below(&mut self, bound: usize) -> usize {
            (self.next() % bound as u64) as usize
        }

        fn uuid(&mut self) -> [u8; 16] {
            let mut value = [0; 16];
            value[..8].copy_from_slice(&self.next().to_le_bytes());
            value[8..].copy_from_slice(&(self.next() | 1).to_le_bytes());
            value
        }

        /// Word-like text, which level 19 writes smaller than level 12.
        fn words(&mut self, count: usize) -> String {
            const WORDS: [&str; 12] = [
                "incident", "latency", "deploy", "rollback", "checkout", "timeout", "p95",
                "region", "eu-west", "retry", "upstream", "degraded",
            ];
            (0..count)
                .map(|_| WORDS[self.below(WORDS.len())])
                .collect::<Vec<_>>()
                .join(" ")
        }
    }

    /// A table keyed by its first column, none of its columns nullable.
    fn table(name: &str, columns: &[(&str, ColumnKind)]) -> TableSchema {
        TableSchema {
            name: name.to_owned(),
            primary_key: vec![columns[0].0.to_owned()],
            columns: columns
                .iter()
                .map(|&(name, kind)| ColumnSchema {
                    name: name.to_owned(),
                    kind,
                    nullable: false,
                })
                .collect(),
        }
    }

    /// The v4 artifact the encoder writes for `tables`.
    fn artifact(tables: Vec<(TableSchema, Vec<Vec<Value>>)>) -> Vec<u8> {
        let schema = WorkspaceSchema {
            tables: tables.iter().map(|(table, _)| table.clone()).collect(),
        };
        let mut encoder = Encoder::new(
            ArtifactIdentity {
                source_epoch: "source-epoch-1".to_owned(),
                manifest_version: "manifest-1".to_owned(),
                collection_set_hash: format!("sha256:{}", "1".repeat(64)),
                schema_hash: schema_hash(&schema).unwrap(),
                authorization_scope_version: "scope-1".to_owned(),
                authorization_scope_hash: format!("sha256:{}", "2".repeat(64)),
                cursor: "cursor-1".to_owned(),
            },
            schema,
            EncoderLimits::default(),
        )
        .unwrap();
        for (table, rows) in &tables {
            for row in rows {
                encoder.push_row(&table.name, row).unwrap();
            }
        }
        let mut bytes = Vec::new();
        encoder.finish(&mut bytes).unwrap();
        bytes
    }

    /// The artifact a fixture publishes.
    enum Data {
        /// 1,200 items with a random key, one of 40 owners, a title and a
        /// time, and the owners. Level 19 is about a third of the artifact, so
        /// the rule packs it, and the packed brotli object is the smaller one.
        Ordinary,
        /// 600 log lines under a counting key. Level 19 is under a tenth of
        /// the artifact, so the rule leaves it unpacked, though the packer
        /// would transform its text column.
        Compressible,
        /// 800 rows of a random key and bytes, of which the packer transforms
        /// neither.
        Untransformed,
    }

    impl Data {
        fn artifact(self) -> Vec<u8> {
            match self {
                Self::Ordinary => {
                    const OWNERS: usize = 40;
                    let mut random = Random(7);
                    let owners: Vec<[u8; 16]> = (0..OWNERS).map(|_| random.uuid()).collect();
                    let mut at = 1_760_000_000_000_000_i64;
                    let items = (0..1_200)
                        .map(|_| {
                            at += 1_000 * (1 + random.below(90_000) as i64);
                            let title = 3 + random.below(6);
                            vec![
                                Value::Uuid(random.uuid()),
                                Value::Uuid(owners[random.below(OWNERS)]),
                                Value::Text(random.words(title)),
                                Value::TimestampMicros(at),
                            ]
                        })
                        .collect();
                    let owners = owners
                        .iter()
                        .map(|id| vec![Value::Uuid(*id), Value::Text(random.words(2))])
                        .collect();
                    artifact(vec![
                        (
                            table(
                                "items",
                                &[
                                    ("id", ColumnKind::Uuid),
                                    ("owner", ColumnKind::Uuid),
                                    ("title", ColumnKind::Utf8),
                                    ("at", ColumnKind::Timestamp),
                                ],
                            ),
                            items,
                        ),
                        (
                            table(
                                "owners",
                                &[("id", ColumnKind::Uuid), ("name", ColumnKind::Utf8)],
                            ),
                            owners,
                        ),
                    ])
                }
                Self::Compressible => {
                    let mut random = Random(11);
                    let lines = (0..600)
                        .map(|line| {
                            vec![
                                Value::Int64(line as i64 + 1),
                                Value::Text(format!(
                                    "2026-10-11T08:{:02}:{:02}Z service=checkout region=eu-west \
                                     level=warn msg=\"{}\" attempt={} \
                                     upstream=payments-gateway.internal:8443 status=degraded",
                                    line / 60 % 60,
                                    line % 60,
                                    random.words(3),
                                    random.below(4)
                                )),
                            ]
                        })
                        .collect();
                    artifact(vec![(
                        table(
                            "lines",
                            &[("id", ColumnKind::Int64), ("line", ColumnKind::Utf8)],
                        ),
                        lines,
                    )])
                }
                Self::Untransformed => {
                    // Words, then bytes over two alternating eight-symbol
                    // alphabets, which brotli's context modelling writes
                    // smaller than level 19 does.
                    let mut random = Random(13);
                    let mut state = 0x9e37_79b9_u32;
                    let blobs = (0..800)
                        .map(|row| {
                            let words = 3 + random.below(4);
                            let mut body = random.words(words).into_bytes();
                            let alphabet = if (row / 8) % 2 == 0 { 0x40 } else { 0x60 };
                            for _ in 0..64 {
                                state ^= state << 13;
                                state ^= state >> 17;
                                state ^= state << 5;
                                body.push(alphabet + (state >> 29) as u8);
                            }
                            vec![Value::Uuid(random.uuid()), Value::Bytes(body)]
                        })
                        .collect();
                    artifact(vec![(
                        table(
                            "blobs",
                            &[("id", ColumnKind::Uuid), ("body", ColumnKind::Bytes)],
                        ),
                        blobs,
                    )])
                }
            }
        }
    }

    /// An ordinary artifact published at a publication's own level, its
    /// objects in the store.
    pub(crate) async fn published() -> Fixture {
        published_of(Data::Ordinary).await
    }

    /// One artifact of `data` published at a publication's own level, its
    /// objects in the store.
    async fn published_of(data: Data) -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let prepared = prepared_source(SourceEpoch::new("reencode-epoch").unwrap());
        let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope).unwrap();
        let decoded = data.artifact();
        let decoded_path = directory.path().join("decoded");
        std::fs::write(&decoded_path, &decoded).unwrap();
        let digest = format!("{:x}", Sha256::digest(&decoded));
        let representations = PreparedRepresentations::prepare_controlled(
            &decoded_path,
            decoded.len() as u64,
            &digest,
            decoded.len() as u64 + 1024 * 1024,
            &CancellationToken::new(),
        )
        .unwrap();
        let artifact = ArtifactId::new("local-columnar-reencode").unwrap();
        let mut value =
            serde_json::to_value(retry_manifest(&decoded, &artifact, &prepared, &plan).manifest())
                .unwrap();
        value["representations"] = serde_json::to_value(representations.manifests()).unwrap();
        value["decoded"]["chunks"] =
            serde_json::to_value(representations.decoded_chunks()).unwrap();
        let published =
            ValidatedArtifactManifest::parse(&serde_json::to_vec(&value).unwrap()).unwrap();
        let store =
            LocalFilesystemBlobStore::open(directory.path().join("objects"), 64 * 1024 * 1024)
                .unwrap();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let lease = catalog
            .register_build(&published, "builder", now_ms().unwrap(), 60_000)
            .unwrap();
        catalog.mark_uploading(&lease, now_ms().unwrap()).unwrap();
        for entry in representations.entries() {
            let representation = entry.manifest();
            let metadata = ObjectMetadata::new(
                representation.encoded_bytes,
                representation.encoded_sha256.clone(),
            )
            .unwrap();
            store
                .put_file_if_absent(&representation.object_id, entry.path(), &metadata)
                .await
                .unwrap();
        }
        catalog.mark_uploaded(&lease, now_ms().unwrap()).unwrap();
        catalog
            .verify_and_publish(&lease, &store, now_ms().unwrap())
            .await
            .unwrap();
        let query = published_query_for(&prepared).unwrap();
        assert_eq!(
            catalog.published(&query).unwrap(),
            std::slice::from_ref(&published)
        );
        Fixture {
            directory,
            store,
            query,
            published,
            decoded,
        }
    }

    /// The manifest less its id and its zstd representation.
    fn shared_fields(manifest: &ValidatedArtifactManifest) -> serde_json::Value {
        let mut value = serde_json::to_value(manifest.manifest()).unwrap();
        value.as_object_mut().unwrap().remove("artifactId");
        value["representations"]
            .as_array_mut()
            .unwrap()
            .retain(|representation| representation["encoding"] != "zstd");
        value
    }

    async fn object_bytes(store: &LocalFilesystemBlobStore, object: &ObjectId) -> Vec<u8> {
        let mut bytes = Vec::new();
        store
            .get(object)
            .await
            .unwrap()
            .unwrap()
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        bytes
    }

    #[tokio::test]
    async fn the_reencode_replaces_the_published_artifact_with_a_smaller_one() {
        let fixture = published().await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite3");
        let fence = RwLock::new(());
        let cancellation = CancellationToken::new();
        let outcome = reencode_published(
            fixture.request(&catalog_path, &fence, &cancellation),
            || true,
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            ReencodeOutcome::Published {
                artifact: fixture.reencoded_id()
            }
        );

        let catalog = fixture.catalog();
        let [reencoded] = <[_; 1]>::try_from(catalog.published(&fixture.query).unwrap()).unwrap();
        assert_eq!(reencoded.manifest().artifact_id, fixture.reencoded_id());
        assert_eq!(shared_fields(&reencoded), shared_fields(&fixture.published));
        let before = representation(&fixture.published, RepresentationEncoding::Zstd).unwrap();
        let after = representation(&reencoded, RepresentationEncoding::Zstd).unwrap();
        assert_eq!(
            after.zstd.as_ref().unwrap().level,
            REENCODED_ZSTD_LEVEL,
            "the level the manifest records"
        );
        assert!(after.encoded_bytes < before.encoded_bytes);
        let bytes = object_bytes(&fixture.store, &after.object_id).await;
        assert_eq!(bytes.len() as u64, after.encoded_bytes);
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            after.encoded_sha256
        );
        let decoded = zstd_decode(&bytes);
        assert!(
            decoded == fixture.decoded,
            "the level-19 object decodes to the published bytes"
        );
        reencoded.verify_decoded_bytes(&decoded).unwrap();
        assert_eq!(
            catalog
                .state(&fixture.published.manifest().artifact_id)
                .unwrap(),
            Some(artifact_catalog::ArtifactState::Retired)
        );
        // A second run finds the work done.
        let again = reencode_published(
            fixture.request(&catalog_path, &fence, &cancellation),
            || true,
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(again, ReencodeOutcome::AlreadyPublished);
    }

    /// A fence that retires the generation between the upload and the
    /// publication leaves the re-encode unpublished and ungrantable, and the
    /// catalog asks `admit` while the fence lock is held shared, so no fence
    /// can move between the answer and the publication.
    #[tokio::test]
    async fn a_fence_during_the_reencode_keeps_it_out_of_the_retired_generation() {
        let fixture = published().await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite3");
        let fence = RwLock::new(());
        let cancellation = CancellationToken::new();
        let served = AtomicU64::new(1);
        let asked_under_fence = Cell::new(None);
        let outcome = reencode_published(
            fixture.request(&catalog_path, &fence, &cancellation),
            || {
                asked_under_fence.set(Some(fence.try_write().is_err()));
                served.load(Ordering::SeqCst) == 1
            },
            |stage| {
                if stage == ReencodeStage::Admitting {
                    let _fence = fence.try_write().expect("nothing else holds the fence");
                    served.store(2, Ordering::SeqCst);
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome, ReencodeOutcome::Refused);
        assert_eq!(asked_under_fence.get(), Some(true));

        let mut catalog = fixture.catalog();
        assert_eq!(
            catalog.published(&fixture.query).unwrap(),
            std::slice::from_ref(&fixture.published)
        );
        assert_ne!(
            catalog.state(&fixture.reencoded_id()).unwrap(),
            Some(artifact_catalog::ArtifactState::Published)
        );
        let now = now_ms().unwrap();
        assert!(catalog
            .pin_published_for_plan(
                &PlanId::new("plan-of-the-retired-generation").unwrap(),
                &[fixture.reencoded_id()],
                now + 60_000,
                now,
            )
            .is_err());
    }

    #[tokio::test]
    async fn a_cancelled_reencode_registers_nothing() {
        let fixture = published().await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite3");
        let fence = RwLock::new(());
        let cancellation = CancellationToken::new();
        let outcome = reencode_published(
            fixture.request(&catalog_path, &fence, &cancellation),
            || true,
            |stage| {
                if stage == ReencodeStage::Encoding {
                    cancellation.cancel();
                }
            },
        )
        .await;
        assert!(
            matches!(outcome, Err(ReencodeError::Cancelled)),
            "{outcome:?}"
        );
        let catalog = fixture.catalog();
        assert_eq!(catalog.state(&fixture.reencoded_id()).unwrap(), None);
        assert_eq!(
            catalog.published(&fixture.query).unwrap(),
            std::slice::from_ref(&fixture.published)
        );
    }

    /// A plan made before the swap still downloads the level-12 object and
    /// verifies it while its pin lasts; the level-12 artifact is retired once
    /// the pin expires, and its object stays where the plan found it.
    #[tokio::test]
    async fn a_plan_made_before_the_swap_keeps_its_object_until_its_pin_expires() {
        let fixture = published().await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite3");
        let fence = RwLock::new(());
        let cancellation = CancellationToken::new();
        let replaced = fixture.published.manifest().artifact_id.clone();
        let now = now_ms().unwrap();
        fixture
            .catalog()
            .pin_published_for_plan(
                &PlanId::new("plan-before-the-swap").unwrap(),
                std::slice::from_ref(&replaced),
                now + 1_500,
                now,
            )
            .unwrap();
        let level_12 = representation(&fixture.published, RepresentationEncoding::Zstd)
            .unwrap()
            .clone();

        let reencode = reencode_published(
            fixture.request(&catalog_path, &fence, &cancellation),
            || true,
            |_| {},
        );
        let pinned = async {
            let catalog = fixture.catalog();
            while catalog.state(&fixture.reencoded_id()).unwrap()
                != Some(artifact_catalog::ArtifactState::Published)
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(
                catalog.state(&replaced).unwrap(),
                Some(artifact_catalog::ArtifactState::Published),
                "retired while a plan pins it"
            );
            let bytes = object_bytes(&fixture.store, &level_12.object_id).await;
            assert_eq!(
                format!("{:x}", Sha256::digest(&bytes)),
                level_12.encoded_sha256
            );
            let decoded = zstd_decode(&bytes);
            assert!(decoded == fixture.decoded);
            fixture.published.verify_decoded_bytes(&decoded).unwrap();
        };
        let (outcome, ()) = tokio::join!(reencode, pinned);
        assert_eq!(
            outcome.unwrap(),
            ReencodeOutcome::Published {
                artifact: fixture.reencoded_id()
            }
        );
        assert!(now_ms().unwrap() >= now + 1_500);
        assert_eq!(
            fixture.catalog().state(&replaced).unwrap(),
            Some(artifact_catalog::ArtifactState::Retired)
        );
        assert!(fixture
            .store
            .head(&level_12.object_id)
            .await
            .unwrap()
            .is_some());
    }

    fn zstd_decode(bytes: &[u8]) -> Vec<u8> {
        zstd::stream::decode_all(bytes).unwrap()
    }

    fn brotli_decode(bytes: &[u8]) -> Vec<u8> {
        let mut decoded = Vec::new();
        std::io::Read::read_to_end(
            &mut brotli::Decompressor::new(bytes, 64 * 1024),
            &mut decoded,
        )
        .unwrap();
        decoded
    }

    fn published_ids(fixture: &Fixture) -> Vec<ArtifactId> {
        fixture
            .catalog()
            .published(&fixture.query)
            .unwrap()
            .iter()
            .map(|manifest| manifest.manifest().artifact_id.clone())
            .collect()
    }

    /// The work directories a brotli stage left behind.
    fn brotli_work_directories(fixture: &Fixture) -> usize {
        std::fs::read_dir(snapshot_workspace_root(fixture.directory.path()))
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|entry| entry.file_name().to_string_lossy().starts_with("brotli-"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// What both encodes left of one artifact of `data`.
    struct Encoded {
        fixture: Fixture,
        stages: Vec<ReencodeStage>,
        /// The one artifact still published.
        served: ValidatedArtifactManifest,
        /// The representation it adds to the level-19 artifact's, and that
        /// object's bytes.
        added: ArtifactRepresentation,
        object: Vec<u8>,
        level_19_bytes: u64,
    }

    /// Runs both encodes over one artifact of `data` and checks what every
    /// brotli artifact has in common: it is the only one left published, its
    /// manifest is the level-19 one under another id with one representation
    /// more, that object is smaller than the level-19 one and is in the store,
    /// and a second run finds nothing to do.
    async fn encode_both(data: Data) -> Encoded {
        let fixture = published_of(data).await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite3");
        let fence = RwLock::new(());
        let cancellation = CancellationToken::new();
        let mut stages = Vec::new();
        let outcomes = reencode_and_publish_brotli(
            fixture.request(&catalog_path, &fence, &cancellation),
            || true,
            |stage| stages.push(stage),
        )
        .await
        .unwrap();
        assert_eq!(
            outcomes.zstd,
            ReencodeOutcome::Published {
                artifact: fixture.reencoded_id()
            }
        );

        let catalog = fixture.catalog();
        let [served] = <[_; 1]>::try_from(catalog.published(&fixture.query).unwrap()).unwrap();
        assert!(matches!(
            &outcomes.brotli,
            Some(Ok(ReencodeOutcome::Published { artifact }))
                if *artifact == served.manifest().artifact_id
        ));
        for retired in [
            fixture.published.manifest().artifact_id.clone(),
            fixture.reencoded_id(),
        ] {
            assert_eq!(
                catalog.state(&retired).unwrap(),
                Some(artifact_catalog::ArtifactState::Retired)
            );
        }
        // The level-19 manifest with another id and one more representation.
        let level_19 = catalog.manifest(&fixture.reencoded_id()).unwrap().unwrap();
        let mut joined = serde_json::to_value(served.manifest()).unwrap();
        joined["representations"]
            .as_array_mut()
            .unwrap()
            .pop()
            .unwrap();
        let mut expected = serde_json::to_value(level_19.manifest()).unwrap();
        joined.as_object_mut().unwrap().remove("artifactId");
        expected.as_object_mut().unwrap().remove("artifactId");
        assert_eq!(joined, expected);

        let added = served.manifest().representations.last().unwrap().clone();
        let level_19_bytes = representation(&served, RepresentationEncoding::Zstd)
            .unwrap()
            .encoded_bytes;
        assert!(added.encoded_bytes < level_19_bytes);
        let object = object_bytes(&fixture.store, &added.object_id).await;
        assert_eq!(object.len() as u64, added.encoded_bytes);
        assert_eq!(
            format!("{:x}", Sha256::digest(&object)),
            added.encoded_sha256
        );
        for representation in &served.manifest().representations {
            assert!(fixture
                .store
                .head(&representation.object_id)
                .await
                .unwrap()
                .is_some());
        }
        assert_eq!(brotli_work_directories(&fixture), 0);

        let again = reencode_and_publish_brotli(
            fixture.request(&catalog_path, &fence, &cancellation),
            || true,
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(again.zstd, ReencodeOutcome::AlreadyPublished);
        assert!(matches!(
            again.brotli,
            Some(Ok(ReencodeOutcome::AlreadyPublished))
        ));
        assert_eq!(
            published_ids(&fixture),
            [served.manifest().artifact_id.clone()]
        );
        Encoded {
            fixture,
            stages,
            served,
            added,
            object,
            level_19_bytes,
        }
    }

    /// An ordinary artifact's third artifact carries the packer's body as one
    /// brotli stream, and that body restores the artifact.
    #[tokio::test]
    async fn the_packed_artifact_joins_the_level_19_one_and_replaces_both() {
        let Encoded {
            fixture,
            stages,
            served,
            added,
            object,
            level_19_bytes,
        } = encode_both(Data::Ordinary).await;
        assert_eq!(fixture.decoded.len(), 99_518);
        assert!(
            level_19_bytes * 10 >= 99_518,
            "the rule packs this artifact"
        );
        assert_eq!(
            stages,
            [
                ReencodeStage::Encoding,
                ReencodeStage::Admitting,
                ReencodeStage::Packing,
                ReencodeStage::BrotliEncoding,
                ReencodeStage::BrotliAdmitting,
                ReencodeStage::Retiring,
            ]
        );
        assert_eq!(served.manifest().artifact_id, fixture.packed_id());
        assert_eq!(fixture.catalog().state(&fixture.brotli_id()).unwrap(), None);
        assert_eq!(
            added.encoding,
            RepresentationEncoding::PackedBrotliLargeWindow
        );
        assert_eq!(
            added.brotli,
            Some(BrotliParameters {
                quality: BROTLI_QUALITY,
                window_bits: 17
            })
        );
        assert_eq!(
            added.packed,
            Some(PackedParameters {
                version: PACKED_BODY_VERSION,
                body_bytes: 82_818
            })
        );

        let body = brotli_decode(&object);
        assert!(
            body == pack(&fixture.decoded).unwrap().body,
            "the packed object decodes to the packer's body"
        );
        let restored = unpack(&body, fixture.decoded.len() as u64).unwrap();
        assert!(
            restored == fixture.decoded,
            "the body restores the published bytes"
        );
        served.verify_decoded_bytes(&restored).unwrap();
    }

    /// An artifact whose level-19 object is under a tenth of it is not packed,
    /// though the packer would shorten it, and its third artifact carries the
    /// plain brotli object.
    #[tokio::test]
    async fn an_artifact_that_compresses_under_a_tenth_is_published_unpacked() {
        let Encoded {
            fixture,
            stages,
            served,
            added,
            object,
            level_19_bytes,
        } = encode_both(Data::Compressible).await;
        assert!(
            level_19_bytes * 10 < fixture.decoded.len() as u64,
            "the rule leaves this artifact unpacked"
        );
        assert!(
            !pack(&fixture.decoded).unwrap().stored,
            "and the packer would have packed it"
        );
        assert_eq!(
            stages,
            [
                ReencodeStage::Encoding,
                ReencodeStage::Admitting,
                ReencodeStage::BrotliEncoding,
                ReencodeStage::BrotliAdmitting,
                ReencodeStage::Retiring,
            ]
        );
        assert_eq!(served.manifest().artifact_id, fixture.brotli_id());
        assert_eq!(fixture.catalog().state(&fixture.packed_id()).unwrap(), None);
        assert_eq!(added.encoding, RepresentationEncoding::BrotliLargeWindow);
        assert_eq!(
            added.brotli,
            Some(BrotliParameters {
                quality: BROTLI_QUALITY,
                window_bits: 17
            })
        );
        assert_eq!(added.packed, None);
        let decoded = brotli_decode(&object);
        assert!(
            decoded == fixture.decoded,
            "the brotli object decodes to the published bytes"
        );
        served.verify_decoded_bytes(&decoded).unwrap();
    }

    /// The rule packs this artifact and the packer transforms no column of
    /// it, so the run packs, finds nothing shorter, and publishes the plain
    /// object with the one encode.
    #[tokio::test]
    async fn an_artifact_the_packer_does_not_shorten_is_published_unpacked() {
        let Encoded {
            fixture,
            stages,
            served,
            added,
            object,
            level_19_bytes,
        } = encode_both(Data::Untransformed).await;
        assert!(
            level_19_bytes * 10 >= fixture.decoded.len() as u64,
            "the rule packs this artifact"
        );
        assert!(
            pack(&fixture.decoded).unwrap().stored,
            "and the packer leaves it as it is"
        );
        assert_eq!(
            stages,
            [
                ReencodeStage::Encoding,
                ReencodeStage::Admitting,
                ReencodeStage::Packing,
                ReencodeStage::BrotliEncoding,
                ReencodeStage::BrotliAdmitting,
                ReencodeStage::Retiring,
            ]
        );
        assert_eq!(served.manifest().artifact_id, fixture.brotli_id());
        assert_eq!(fixture.catalog().state(&fixture.packed_id()).unwrap(), None);
        assert_eq!(added.encoding, RepresentationEncoding::BrotliLargeWindow);
        assert_eq!(added.packed, None);
        assert!(brotli_decode(&object) == fixture.decoded);
    }

    /// The rule packs from a tenth up, and a product past `u64` still
    /// compares as one.
    #[test]
    fn an_artifact_is_packed_where_its_level_19_object_is_a_tenth_of_it_or_more() {
        for (level_19_bytes, decoded_bytes, packed) in [
            (10, 100, true),
            (9, 100, false),
            (99, 990, true),
            (99, 991, false),
            (100, 999, true),
            (0, 1, false),
            (1, 10, true),
            (1, 11, false),
            (u64::MAX, u64::MAX, true),
            (u64::MAX / 10, u64::MAX, false),
            (u64::MAX / 10 + 1, u64::MAX, true),
        ] {
            assert_eq!(
                packs(level_19_bytes, decoded_bytes),
                packed,
                "{level_19_bytes} of {decoded_bytes}"
            );
        }
    }

    /// The packed artifact is admitted under the same rule as the level-19 and
    /// plain brotli ones: asked under the fence lock held shared, and only into
    /// the generation still served.
    #[tokio::test]
    async fn a_fence_during_the_brotli_encode_keeps_it_out_of_the_retired_generation() {
        let fixture = published().await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite3");
        let fence = RwLock::new(());
        let cancellation = CancellationToken::new();
        let served = AtomicU64::new(1);
        let asked_under_fence = Cell::new(None);
        let outcomes = reencode_and_publish_brotli(
            fixture.request(&catalog_path, &fence, &cancellation),
            || {
                asked_under_fence.set(Some(fence.try_write().is_err()));
                served.load(Ordering::SeqCst) == 1
            },
            |stage| {
                if stage == ReencodeStage::BrotliAdmitting {
                    // What the level-19 admission recorded does not count.
                    asked_under_fence.set(None);
                    let _fence = fence.try_write().expect("nothing else holds the fence");
                    served.store(2, Ordering::SeqCst);
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(
            outcomes.zstd,
            ReencodeOutcome::Published {
                artifact: fixture.reencoded_id()
            }
        );
        assert!(matches!(
            outcomes.brotli,
            Some(Ok(ReencodeOutcome::Refused))
        ));
        assert_eq!(asked_under_fence.get(), Some(true));
        assert_eq!(published_ids(&fixture), [fixture.reencoded_id()]);
        let mut catalog = fixture.catalog();
        // The refused build is abandoned, and its registration removed.
        for id in [fixture.packed_id(), fixture.brotli_id()] {
            assert_eq!(catalog.state(&id).unwrap(), None, "{id}");
        }
        let now = now_ms().unwrap();
        assert!(catalog
            .pin_published_for_plan(
                &PlanId::new("plan-of-the-retired-generation").unwrap(),
                &[fixture.packed_id()],
                now + 60_000,
                now,
            )
            .is_err());
        // The abandoned build's cleanup leaves the objects the level-19
        // artifact still names.
        let level_19 = catalog.manifest(&fixture.reencoded_id()).unwrap().unwrap();
        for representation in &level_19.manifest().representations {
            assert!(fixture
                .store
                .head(&representation.object_id)
                .await
                .unwrap()
                .is_some());
        }
    }

    /// What a cancelled brotli stage leaves: the level-12 and level-19
    /// artifacts published, neither brotli artifact registered, and no work
    /// directory.
    fn assert_no_brotli_artifact(fixture: &Fixture) {
        let catalog = fixture.catalog();
        for id in [fixture.packed_id(), fixture.brotli_id()] {
            assert_eq!(catalog.state(&id).unwrap(), None, "{id}");
        }
        assert_eq!(
            published_ids(fixture),
            [
                fixture.published.manifest().artifact_id.clone(),
                fixture.reencoded_id()
            ]
        );
        assert_eq!(brotli_work_directories(fixture), 0);
    }

    /// A fence cancels the running re-encode through [`Reencoder::cancel`]
    /// (`LocalBackend::fence_generation`). One that lands as the pack starts
    /// ends the run in the pack: the encode never starts, nothing is
    /// registered, and the work directory is gone.
    #[tokio::test]
    async fn a_fence_s_cancel_ends_the_pack_and_registers_nothing() {
        let fixture = published().await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite3");
        let fence = RwLock::new(());
        let reencoder = Reencoder::new(ReencodeConfig {
            state_root: fixture.directory.path().to_path_buf(),
            catalog_path: catalog_path.clone(),
            cancellation: CancellationToken::new(),
        });
        let cancellation = reencoder.replace();
        let mut stages = Vec::new();
        let result = reencode_and_publish_brotli(
            fixture.request(&catalog_path, &fence, &cancellation),
            || true,
            |stage| {
                stages.push(stage);
                if stage == ReencodeStage::Packing {
                    reencoder.cancel();
                }
            },
        )
        .await;
        assert!(
            matches!(result, Err(ReencodeError::Cancelled)),
            "{result:?}"
        );
        assert_eq!(
            stages,
            [
                ReencodeStage::Encoding,
                ReencodeStage::Admitting,
                ReencodeStage::Packing,
            ]
        );
        assert_no_brotli_artifact(&fixture);
    }

    /// The next publication's re-encode cancels the one still running, as a
    /// fence does, and its own token is live.
    #[test]
    fn a_new_re_encode_cancels_the_one_before_it() {
        let process = CancellationToken::new();
        let reencoder = Reencoder::new(ReencodeConfig {
            state_root: PathBuf::new(),
            catalog_path: PathBuf::new(),
            cancellation: process.clone(),
        });
        let first = reencoder.replace();
        assert!(!first.is_cancelled());
        let second = reencoder.replace();
        assert!(first.is_cancelled());
        assert!(!second.is_cancelled());
        reencoder.cancel();
        assert!(second.is_cancelled());
        assert!(!process.is_cancelled());
        let third = reencoder.replace();
        assert!(!third.is_cancelled());
        process.cancel();
        assert!(third.is_cancelled());
    }

    #[tokio::test]
    async fn a_cancel_during_the_brotli_encode_registers_nothing_and_leaves_no_work() {
        let fixture = published().await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite3");
        let fence = RwLock::new(());
        let cancellation = CancellationToken::new();
        let mut stages = Vec::new();
        let result = reencode_and_publish_brotli(
            fixture.request(&catalog_path, &fence, &cancellation),
            || true,
            |stage| {
                stages.push(stage);
                if stage == ReencodeStage::BrotliEncoding {
                    cancellation.cancel();
                }
            },
        )
        .await;
        assert!(matches!(result, Err(ReencodeError::Cancelled)));
        // The pack finished, and its body went with the work directory.
        assert_eq!(
            stages,
            [
                ReencodeStage::Encoding,
                ReencodeStage::Admitting,
                ReencodeStage::Packing,
                ReencodeStage::BrotliEncoding,
            ]
        );
        assert_no_brotli_artifact(&fixture);
    }

    /// A plan made before the swap pins the level-12 artifact for as long as
    /// the plan lives. The brotli artifact does not wait for it.
    #[tokio::test]
    async fn a_pin_on_the_level_12_artifact_does_not_hold_back_the_brotli_one() {
        let fixture = published().await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite3");
        let fence = RwLock::new(());
        let cancellation = CancellationToken::new();
        let level_12 = fixture.published.manifest().artifact_id.clone();
        let now = now_ms().unwrap();
        fixture
            .catalog()
            .pin_published_for_plan(
                &PlanId::new("plan-made-before-the-swap").unwrap(),
                std::slice::from_ref(&level_12),
                now + 600_000,
                now,
            )
            .unwrap();
        let run = reencode_and_publish_brotli(
            fixture.request(&catalog_path, &fence, &cancellation),
            || true,
            |_| {},
        );
        let watch = async {
            let catalog = fixture.catalog();
            let deadline = Instant::now() + Duration::from_secs(120);
            let published = loop {
                if catalog.state(&fixture.packed_id()).unwrap()
                    == Some(artifact_catalog::ArtifactState::Published)
                {
                    break true;
                }
                if Instant::now() > deadline {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            };
            let pinned = catalog.state(&level_12).unwrap();
            cancellation.cancel();
            (published, pinned)
        };
        let (outcome, (published, pinned)) = tokio::join!(run, watch);
        assert!(
            published,
            "the brotli artifact waited on the level-12 artifact's pin"
        );
        assert_eq!(pinned, Some(artifact_catalog::ArtifactState::Published));
        assert!(matches!(outcome, Err(ReencodeError::Cancelled)));
    }

    #[tokio::test]
    async fn brotli_is_added_only_to_an_unsigned_level_19_artifact() {
        let fixture = published().await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite3");
        let fence = RwLock::new(());
        let cancellation = CancellationToken::new();
        reencode_published(
            fixture.request(&catalog_path, &fence, &cancellation),
            || true,
            |_| {},
        )
        .await
        .unwrap();
        let catalog = fixture.catalog();
        let level_19 = catalog.manifest(&fixture.reencoded_id()).unwrap().unwrap();
        assert!(brotli_source(std::slice::from_ref(&fixture.published)).is_none());
        // A signed manifest is not re-encoded: its key is not here.
        let mut signed = serde_json::to_value(level_19.manifest()).unwrap();
        signed["signature"] = serde_json::json!({
            "algorithm": "Ed25519",
            "keyId": "key-1",
            "signatureBase64": "A".repeat(44),
        });
        let signed =
            ValidatedArtifactManifest::parse(&serde_json::to_vec(&signed).unwrap()).unwrap();
        assert!(brotli_source(std::slice::from_ref(&signed)).is_none());
        let level_12 = catalog
            .manifest(&fixture.published.manifest().artifact_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            brotli_source(&[level_12, level_19])
                .map(|manifest| manifest.manifest().artifact_id.clone()),
            Some(fixture.reencoded_id())
        );
    }
}

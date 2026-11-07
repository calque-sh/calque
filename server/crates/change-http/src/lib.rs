//! Bounded Axum transport for the authenticated canonical change protocol.
//!
//! Authentication and source routing remain application policy. Callers
//! implement [`ChangeHttpBackend`] and return only batches built from a
//! server-resolved [`change_api::ChangeAuthorization`].
//!
//! A fenced request's response is zstd-encoded when the client accepts it;
//! every other response goes uncompressed.

use std::{
    collections::VecDeque,
    convert::Infallible,
    future::Future,
    io::{self, Write},
    pin::Pin,
    task::{Context, Poll},
};

use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::Response,
    routing::post,
    Router,
};
use change_api::{parse_change_request, ChangeBatch, ChangeBatchError, IncomingChangeRequest};
use futures_util::stream::{self, StreamExt};
use http_body::{Frame, SizeHint};
use http_headers::has_media_type;
use prost::Message;
use sync_protocol::{
    change::{change_stream_frame, ChangeRequest, ChangeStreamFrame, StreamError, StreamErrorCode},
    change_frame_body_sha256, encode_frame_with_limit,
};
use sync_types::CursorError;
use tokio::sync::mpsc;
use tracing::{info_span, Instrument};

const CHANGE_PATH: &str = "/v1/changes";
const CHANGE_CONTENT_TYPE: &str = "application/vnd.calque.change.v1+protobuf";
const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_ERROR_FRAME_BYTES: usize = 16 * 1024;
/// The zstd level a fenced response is compressed at.
const ZSTD_LEVEL: i32 = 3;
const MAX_ACCEPT_ENCODING_BYTES: usize = 1024;

type ZstdEncoder = zstd::stream::write::Encoder<'static, Vec<u8>>;

/// Application-owned authentication, plan resolution, long-polling, and
/// source routing behind the transport boundary.
pub trait ChangeHttpBackend: Clone + Send + Sync + 'static {
    fn execute(
        &self,
        request: IncomingChangeRequest,
    ) -> impl Future<Output = Result<ChangeBatch, ChangeBatchError>> + Send;

    /// Answers a request that set `stream`. The first batch decides the
    /// response's status, as a one-batch request's does. The default answers
    /// with that batch alone, which a streaming reader reads as a stream that
    /// ended.
    fn execute_stream(
        &self,
        request: IncomingChangeRequest,
    ) -> impl Future<Output = Result<ChangeStream, ChangeBatchError>> + Send {
        let first = self.execute(request);
        async move {
            Ok(ChangeStream {
                first: Some(first.await?),
                rest: None,
            })
        }
    }
}

/// The batches of one streamed response.
pub struct ChangeStream {
    /// The batch the response opens with. `None` opens it with none, once the
    /// request has been authorized; the backend then sends at least one batch
    /// on `rest`, so every response carries one.
    pub first: Option<ChangeBatch>,
    /// Each later batch's frames, built and authorized as a request of its
    /// own. The body ends when the sender drops, and dropping the receiver is
    /// how the backend learns the client left.
    pub rest: Option<mpsc::Receiver<Vec<Bytes>>>,
}

/// Marks a response that stays open for batches built after it was sent.
/// What the backend holds for it is its own open-stream bound, not a request
/// permit.
#[derive(Debug, Clone, Copy)]
pub struct OpenChangeStream;

/// Builds the versioned change endpoint with an allocation limit applied
/// before protobuf decoding.
pub fn router<B>(backend: B) -> Router
where
    B: ChangeHttpBackend,
{
    Router::new()
        .route(CHANGE_PATH, post(post_changes::<B>))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(backend)
}

async fn post_changes<B>(State(backend): State<B>, headers: HeaderMap, body: Bytes) -> Response
where
    B: ChangeHttpBackend,
{
    let span = info_span!(
        "change_http_request",
        client_instance_id = tracing::field::Empty,
        plan_id = tracing::field::Empty,
        request_bytes = body.len(),
    );
    async move {
        if !has_media_type(&headers, CHANGE_CONTENT_TYPE) {
            return protocol_error_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                StreamErrorCode::RequestInvalid,
                "change request Content-Type must be protobuf",
                false,
                0,
            );
        }
        let wire = match ChangeRequest::decode(body) {
            Ok(request) => request,
            Err(_) => {
                return protocol_error_response(
                    StatusCode::BAD_REQUEST,
                    StreamErrorCode::RequestInvalid,
                    "change request protobuf is malformed",
                    false,
                    0,
                );
            }
        };
        let request = match parse_change_request(wire) {
            Ok(request) => request,
            Err(error) => return batch_error_response(&error),
        };
        tracing::Span::current().record("client_instance_id", request.client_instance_id());
        tracing::Span::current().record("plan_id", request.request().plan_id.as_str());

        // Only a fenced catch-up is compressed: a writer can time a live
        // batch, and a compressed one's length would tell it more.
        let encoder = (request.request().through_cursor_inclusive.is_some()
            && accepts_zstd(&headers))
        .then(|| ZstdEncoder::new(Vec::new(), ZSTD_LEVEL).ok())
        .flatten();
        if request.request().stream {
            return match backend.execute_stream(request).await {
                Ok(stream) => stream_response(stream, encoder),
                Err(error) => batch_error_response(&error),
            };
        }
        match backend.execute(request).await {
            Ok(batch) => success_response(batch, encoder),
            Err(error) => batch_error_response(&error),
        }
    }
    .instrument(span)
    .await
}

fn success_response(batch: ChangeBatch, encoder: Option<ZstdEncoder>) -> Response {
    let zstd = encoder.is_some();
    let body = match encoder {
        Some(encoder) => zstd_body(encoder, Some(batch.chunks), None),
        None => Body::new(Chunks::from(batch.chunks)),
    };
    batch_response(body, zstd)
}

/// One batch's buffers as a body of known length, written as they are rather
/// than copied into one: a frame the batch shares with other clients stays
/// one buffer, and whatever holds a buffer to a bound holds it until it is
/// written.
struct Chunks {
    chunks: VecDeque<Bytes>,
    remaining: u64,
}

impl From<Vec<Bytes>> for Chunks {
    fn from(chunks: Vec<Bytes>) -> Self {
        Self {
            remaining: chunks.iter().map(|chunk| chunk.len() as u64).sum(),
            chunks: chunks.into(),
        }
    }
}

impl http_body::Body for Chunks {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        Poll::Ready(this.chunks.pop_front().map(|chunk| {
            this.remaining -= chunk.len() as u64;
            Ok(Frame::data(chunk))
        }))
    }

    fn is_end_stream(&self) -> bool {
        self.chunks.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}

fn stream_response(stream: ChangeStream, encoder: Option<ZstdEncoder>) -> Response {
    let ChangeStream { first, rest } = stream;
    let open = rest.is_some();
    let first = first.map(|batch| batch.chunks);
    let zstd = encoder.is_some();
    let body = match encoder {
        Some(encoder) => zstd_body(encoder, first, rest),
        None => {
            let head = stream::iter(
                first
                    .unwrap_or_default()
                    .into_iter()
                    .map(Ok::<_, Infallible>),
            );
            match rest {
                Some(rest) => Body::from_stream(
                    head.chain(
                        stream::unfold(rest, |mut rest| async move {
                            let batch = rest.recv().await?;
                            Some((stream::iter(batch.into_iter().map(Ok)), rest))
                        })
                        .flatten(),
                    ),
                ),
                None => Body::from_stream(head),
            }
        }
    };
    let mut response = batch_response(body, zstd);
    // A proxy that buffers the body would hold every batch until it ends.
    response.headers_mut().insert(
        header::HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    if open {
        response.extensions_mut().insert(OpenChangeStream);
    }
    response
}

fn batch_response(body: Body, zstd: bool) -> Response {
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    set_common_headers(response.headers_mut());
    let headers = response.headers_mut();
    headers.insert(header::VARY, HeaderValue::from_static("accept-encoding"));
    if zstd {
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("zstd"));
    }
    response
}

/// Whether `Accept-Encoding` names `zstd` once, with a weight above zero. A
/// header this does not read plainly reads as not accepting it, and the
/// response goes uncompressed, as every response did before.
fn accepts_zstd(headers: &HeaderMap) -> bool {
    let mut total_bytes = 0_usize;
    let mut zstd = None;
    for value in &headers.get_all(header::ACCEPT_ENCODING) {
        let Ok(value) = value.to_str() else {
            return false;
        };
        total_bytes += value.len();
        if total_bytes > MAX_ACCEPT_ENCODING_BYTES {
            return false;
        }
        for item in value.split(',') {
            let mut parts = item.split(';');
            if !parts
                .next()
                .unwrap_or_default()
                .trim()
                .eq_ignore_ascii_case("zstd")
            {
                continue;
            }
            let weighted = match (parts.next(), parts.next()) {
                (None, _) => Some(true),
                (Some(parameter), None) => parameter
                    .trim()
                    .split_once('=')
                    .filter(|(name, _)| name.trim().eq_ignore_ascii_case("q"))
                    .and_then(|(_, value)| qvalue_above_zero(value.trim())),
                _ => None,
            };
            if zstd.is_some() {
                return false;
            }
            zstd = Some(weighted);
        }
    }
    zstd.flatten().unwrap_or(false)
}

/// RFC 9110's `qvalue`, read as whether it is above zero.
fn qvalue_above_zero(value: &str) -> Option<bool> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if fraction.len() > 3 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    match whole {
        "0" => Some(fraction.bytes().any(|byte| byte != b'0')),
        "1" if fraction.bytes().all(|byte| byte == b'0') => Some(true),
        _ => None,
    }
}

/// One zstd frame for the whole body, flushed after every batch so the
/// browser decodes each batch as it arrives, and ended after the last. Each
/// batch is compressed on a blocking thread. A compression failure ends the
/// body short, which the page reads as a transport failure and retries.
fn zstd_body(
    encoder: ZstdEncoder,
    first: Option<Vec<Bytes>>,
    rest: Option<mpsc::Receiver<Vec<Bytes>>>,
) -> Body {
    let state = (Some(encoder), first, rest);
    Body::from_stream(stream::unfold(
        state,
        |(encoder, mut first, mut rest)| async move {
            let encoder = encoder?;
            let batch = match first.take() {
                Some(batch) => Some(batch),
                None => match rest.as_mut() {
                    Some(rest) => rest.recv().await,
                    None => None,
                },
            };
            let compressed = tokio::task::spawn_blocking(move || compress(encoder, batch))
                .await
                .unwrap_or_else(|error| Err(io::Error::other(error)));
            Some(match compressed {
                Ok((encoder, bytes)) => (Ok(bytes), (encoder, first, rest)),
                Err(error) => (Err(error), (None, first, rest)),
            })
        },
    ))
}

/// Writes and flushes one batch, or with none left ends the frame. The
/// compressed bytes hold the batch they were made from until they are
/// written, so whatever bounds the batch's buffers bounds them too.
fn compress(
    mut encoder: ZstdEncoder,
    batch: Option<Vec<Bytes>>,
) -> io::Result<(Option<ZstdEncoder>, Bytes)> {
    let Some(batch) = batch else {
        return Ok((None, Bytes::from(encoder.finish()?)));
    };
    for chunk in &batch {
        encoder.write_all(chunk)?;
    }
    encoder.flush()?;
    let bytes = std::mem::take(encoder.get_mut());
    Ok((
        Some(encoder),
        Bytes::from_owner(Compressed {
            bytes,
            _source: batch,
        }),
    ))
}

struct Compressed {
    bytes: Vec<u8>,
    _source: Vec<Bytes>,
}

impl AsRef<[u8]> for Compressed {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn batch_error_response(error: &ChangeBatchError) -> Response {
    let (status, code, message, resnapshot_required, retry_after_ms) = match error {
        ChangeBatchError::UnsupportedProtocol => (
            StatusCode::BAD_REQUEST,
            StreamErrorCode::ProtocolVersionUnsupported,
            "change protocol version is unsupported",
            false,
            0,
        ),
        // A barrier this client cannot be told about, and one the log cannot
        // place, both get 401 with `resnapshot_required` rather than serving
        // past the barrier.
        ChangeBatchError::AuthorizationExpired
        | ChangeBatchError::RevocationBarrierUnplaceable
        | ChangeBatchError::RevocationBarrierNotNegotiated
        | ChangeBatchError::InvalidAuthorizationToken
        | ChangeBatchError::InvalidPlanBinding => (
            StatusCode::UNAUTHORIZED,
            StreamErrorCode::AuthorizationChanged,
            "change authorization is invalid or expired",
            true,
            0,
        ),
        ChangeBatchError::BeforeRetention { .. } => (
            StatusCode::GONE,
            StreamErrorCode::CursorBeforeRetention,
            "change cursor is before the retention floor",
            true,
            0,
        ),
        ChangeBatchError::SourceEpochMismatch
        | ChangeBatchError::Cursor(CursorError::EpochMismatch { .. }) => (
            StatusCode::CONFLICT,
            StreamErrorCode::CursorEpochMismatch,
            "change cursor belongs to a different source epoch",
            true,
            0,
        ),
        ChangeBatchError::SchemaGenerationMismatch => (
            StatusCode::CONFLICT,
            StreamErrorCode::SchemaGenerationMismatch,
            "change authorization belongs to a different schema generation",
            true,
            0,
        ),
        ChangeBatchError::PartitionMoved => (
            StatusCode::CONFLICT,
            StreamErrorCode::PartitionMoved,
            "a row moved between partitions; take a fresh base",
            true,
            0,
        ),
        ChangeBatchError::InvalidTableSet
        | ChangeBatchError::UnauthorizedTable(_)
        | ChangeBatchError::InvalidSnapshotCursor => (
            StatusCode::FORBIDDEN,
            StreamErrorCode::TableSetInvalid,
            "change table set is invalid",
            false,
            0,
        ),
        ChangeBatchError::InvalidLimits
        | ChangeBatchError::BatchLimitTooSmall
        | ChangeBatchError::OperationFrameTooLarge
        | ChangeBatchError::FragmentationRequired
        | ChangeBatchError::TransactionOperationLimitExceeded => (
            StatusCode::UNPROCESSABLE_ENTITY,
            StreamErrorCode::RequestInvalid,
            "change request limits cannot represent the requested range",
            false,
            0,
        ),
        ChangeBatchError::InvalidClientInstance
        | ChangeBatchError::InvalidMutationAcknowledgementProof
        | ChangeBatchError::InvalidWireCursor
        | ChangeBatchError::FenceOutsideDurableRange
        | ChangeBatchError::Cursor(_) => (
            StatusCode::BAD_REQUEST,
            StreamErrorCode::RequestInvalid,
            "change request is invalid",
            false,
            0,
        ),
        ChangeBatchError::OpenStreamLimit => (
            StatusCode::SERVICE_UNAVAILABLE,
            StreamErrorCode::InternalRetryable,
            "this process holds as many open change streams as it may",
            false,
            1_000,
        ),
        error if error.is_retryable_source_unavailable() => (
            StatusCode::SERVICE_UNAVAILABLE,
            StreamErrorCode::InternalRetryable,
            "change source is temporarily unavailable",
            false,
            1_000,
        ),
        ChangeBatchError::CanonicalRangeIncomplete
        | ChangeBatchError::SourceMismatch
        | ChangeBatchError::Authorization(_)
        | ChangeBatchError::InvalidServerTime
        | ChangeBatchError::CountOverflow
        | ChangeBatchError::NonFiniteFloat
        | ChangeBatchError::Log(_)
        | ChangeBatchError::Integrity(_)
        | ChangeBatchError::Frame(_)
        | ChangeBatchError::SharedEncodingMismatch => {
            tracing::error!(error = %error, "change source requires operator action");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                StreamErrorCode::RequestInvalid,
                "change source failed a terminal safety check; operator action is required",
                false,
                0,
            )
        }
    };
    protocol_error_response(status, code, message, resnapshot_required, retry_after_ms)
}

fn protocol_error_response(
    status: StatusCode,
    code: StreamErrorCode,
    message: &'static str,
    resnapshot_required: bool,
    retry_after_ms: u64,
) -> Response {
    let mut frame = ChangeStreamFrame {
        protocol_version: 1,
        body: Some(change_stream_frame::Body::Error(StreamError {
            code: code as i32,
            message: message.to_owned(),
            resnapshot_required,
            minimum_retained_cursor: None,
            durable_head: None,
            retry_after_ms,
        })),
        body_sha256: Vec::new(),
    };
    let bytes = change_frame_body_sha256(&frame)
        .ok()
        .and_then(|digest| {
            frame.body_sha256 = digest.to_vec();
            encode_frame_with_limit(&frame, MAX_ERROR_FRAME_BYTES).ok()
        })
        .unwrap_or_default();
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    set_common_headers(response.headers_mut());
    if retry_after_ms > 0 {
        let retry_after_seconds = retry_after_ms.div_ceil(1_000);
        insert_u64_header(
            response.headers_mut(),
            header::RETRY_AFTER.as_str(),
            retry_after_seconds,
        );
    }
    response
}

fn set_common_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(CHANGE_CONTENT_TYPE),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
}

fn insert_u64_header(headers: &mut HeaderMap, name: &'static str, value: u64) {
    let Ok(value) = HeaderValue::from_str(&value.to_string()) else {
        return;
    };
    headers.insert(header::HeaderName::from_static(name), value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::{BTreeMap, BTreeSet},
        future,
        io::Cursor,
        sync::{Arc, Mutex},
    };

    use authz::{
        AuthorizationPolicy, CanonicalAuthorizationError, PolicyDefinition, TablePermitDefinition,
    };
    use axum::{
        body::{to_bytes, Body, BodyDataStream},
        http::Request,
    };
    use canonical_log::{
        CanonicalOperation, CanonicalOperationKind, CanonicalTransaction, FileCanonicalLog,
        LogError, TransactionKey,
    };
    use change_api::{
        ActivatedChangeScope, ChangeAuthorization, ChangeBatchBuilder, ScopedChangePlan,
    };
    use source_control::{ColumnInspection, QualifiedTable, RegisteredTable, TableInspection};
    use sync_protocol::{
        change::{self, change_stream_frame},
        verify_change_frame_body_sha256, FrameDecoder,
    };
    use sync_types::{
        CursorCodec, CursorPosition, OpaqueCursor, PlanId, SourceEpoch, SourceId, TableId,
    };
    use tempfile::TempDir;
    use tower::ServiceExt;

    #[derive(Clone)]
    struct TestBackend {
        log: Arc<FileCanonicalLog>,
        codec: CursorCodec,
        authorization: ChangeAuthorization,
    }

    impl ChangeHttpBackend for TestBackend {
        fn execute(
            &self,
            request: IncomingChangeRequest,
        ) -> impl Future<Output = Result<ChangeBatch, ChangeBatchError>> + Send {
            let result = if request.authorization_scope_token() != b"scope-secret" {
                Err(ChangeBatchError::InvalidAuthorizationToken)
            } else {
                ChangeBatchBuilder::new(self.log.as_ref(), &self.authorization)
                    .and_then(|builder| builder.build_at(request.request(), 1_000_000))
            };
            future::ready(result)
        }
    }

    fn source() -> SourceId {
        SourceId::new("source-main").unwrap()
    }

    fn epoch() -> SourceEpoch {
        SourceEpoch::new("epoch-1").unwrap()
    }

    fn table() -> TableId {
        activated_scope().2
    }

    fn activated_scope() -> (
        source_control::ActivatedSource,
        authz::AuthorizedScope,
        TableId,
    ) {
        let identity = source_control::SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        };
        let qualified_table = QualifiedTable {
            schema: "public".to_owned(),
            table: "tasks".to_owned(),
        };
        let snapshot = source_control::EnrollmentSnapshot {
            identity: identity.clone(),
            settings: source_control::SourceSettings {
                server_version_num: 180_004,
                server_encoding: "UTF8".to_owned(),
                date_style: "ISO, MDY".to_owned(),
                time_zone: "UTC".to_owned(),
                bytea_output: "hex".to_owned(),
                wal_level: "logical".to_owned(),
                max_replication_slots: 10,
                max_wal_senders: 10,
                current_user: "sync".to_owned(),
                current_user_is_superuser: false,
                current_user_has_replication: true,
            },
            publication: source_control::PublicationSettings {
                name: "sync_publication".to_owned(),
                exists: true,
                publishes_insert: true,
                publishes_update: true,
                publishes_delete: true,
                publishes_truncate: true,
                publishes_via_partition_root: false,
            },
            tables: vec![TableInspection {
                table: QualifiedTable {
                    schema: "public".to_owned(),
                    table: "tasks".to_owned(),
                },
                relation_oid: 16_384,
                in_publication: true,
                relation_kind: 'r',
                persistence: 'p',
                replica_identity: 'd',
                is_partition: false,
                has_publication_row_filter: false,
                has_publication_column_list: false,
                columns: vec![ColumnInspection {
                    attribute_number: 1,
                    name: "id".to_owned(),
                    type_oid: 20,
                    type_modifier: -1,
                    type_schema: "pg_catalog".to_owned(),
                    type_name: "int8".to_owned(),
                    nullable: false,
                    primary_key_ordinal: Some(1),
                    generated: false,
                    nondeterministic_collation: false,
                }],
            }],
        };
        let requirements = source_control::EnrollmentRequirements {
            expected_identity: identity,
            publication_name: "sync_publication".to_owned(),
            tables: BTreeSet::from([qualified_table]),
            pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
        };
        let registry =
            source_control::SchemaRegistry::initialize(&snapshot, &requirements).unwrap();
        let registered: &RegisteredTable = &registry.tables()[0];
        let table_id = registered.table_id.clone();
        let scope = AuthorizationPolicy::compile_for_registry(
            PolicyDefinition {
                generation: 7,
                tables: vec![TablePermitDefinition {
                    table_id: registered.table_id.clone(),
                    permitted_columns: registered.column_ids.clone(),
                }],
            },
            &registry,
        )
        .unwrap()
        .scope();
        let activated = source_control::ActivatedSource::new(
            &registry,
            epoch(),
            CursorCodec::new([7; 32]).unwrap(),
            source_control::PgoutputLaunchProfile::V1,
        );
        (activated, scope, table_id)
    }

    fn cursor(codec: &CursorCodec, sequence: u64) -> OpaqueCursor {
        codec.encode(&CursorPosition {
            source_epoch: epoch(),
            sequence,
        })
    }

    fn backend() -> (TempDir, TestBackend) {
        let directory = tempfile::tempdir().unwrap();
        let (activated_source, scope, table_id) = activated_scope();
        let mut log = FileCanonicalLog::open(directory.path(), source(), epoch()).unwrap();
        log.append(CanonicalTransaction {
            key: TransactionKey {
                source_epoch: epoch(),
                end_lsn: "0/10".to_owned(),
                postgres_xid: 1,
            },
            transaction_id: "tx-1".to_owned(),
            begin_lsn: "0/F".to_owned(),
            commit_lsn: "0/10".to_owned(),
            commit_time_unix_micros: 1,
            operations: vec![CanonicalOperation {
                table_id: table_id.clone(),
                primary_key: Vec::new(),
                kind: CanonicalOperationKind::TruncateAuthorizedTable {
                    reason: "test".to_owned(),
                },
            }],
            mutation_ids: Vec::new(),
        })
        .unwrap();
        let codec = activated_source.cursor_codec().clone();
        let plan_id = PlanId::new("plan-1").unwrap();
        let authorization = ChangeAuthorization::new(ScopedChangePlan {
            plan_id,
            activated_scope: ActivatedChangeScope::new(source(), &activated_source, scope).unwrap(),
            expires_at_unix_micros: 2_000_000,
            from_cursor_exclusive: cursor(&codec, 0),
            through_cursor_inclusive: cursor(&codec, 1),
            table_starts: BTreeMap::from([(table_id, None)]),
            acknowledgeable_mutation_ids: Default::default(),
            revocation_barrier: None,
        })
        .unwrap();
        (
            directory,
            TestBackend {
                log: Arc::new(log),
                codec,
                authorization,
            },
        )
    }

    fn request(codec: &CursorCodec, token: &[u8]) -> change::ChangeRequest {
        change::ChangeRequest {
            protocol_version: 1,
            from_cursor_exclusive: Some(change::Cursor {
                token: cursor(codec, 0).as_str().as_bytes().to_vec(),
            }),
            through_cursor_inclusive: Some(change::Cursor {
                token: cursor(codec, 1).as_str().as_bytes().to_vec(),
            }),
            tables: vec![change::TableSubscription {
                table_id: table().to_string(),
                shard_snapshot_cursor: None,
            }],
            authorization_scope_token: token.to_vec(),
            max_batch_bytes: 1024 * 1024,
            max_transactions: 100,
            long_poll_timeout_ms: 30_000,
            client_instance_id: "client-1".to_owned(),
            device_id: "device-1".to_owned(),
            pending_mutation_ids: Vec::new(),
            accept_transaction_fragmentation: true,
            accept_revocation_barrier: true,
            plan_id: "plan-1".to_owned(),
            stream: false,
        }
    }

    fn stream_request(codec: &CursorCodec) -> change::ChangeRequest {
        let mut request = request(codec, b"scope-secret");
        request.from_cursor_exclusive = Some(change::Cursor {
            token: cursor(codec, 1).as_str().as_bytes().to_vec(),
        });
        request.through_cursor_inclusive = None;
        request.stream = true;
        request
    }

    /// Streams the batch `TestBackend` builds, unless `opens_empty`, then
    /// each of `later` as a batch of its own.
    #[derive(Clone)]
    struct StreamingBackend {
        inner: TestBackend,
        opens_empty: bool,
        later: Vec<Vec<Bytes>>,
    }

    impl ChangeHttpBackend for StreamingBackend {
        fn execute(
            &self,
            request: IncomingChangeRequest,
        ) -> impl Future<Output = Result<ChangeBatch, ChangeBatchError>> + Send {
            self.inner.execute(request)
        }

        fn execute_stream(
            &self,
            request: IncomingChangeRequest,
        ) -> impl Future<Output = Result<ChangeStream, ChangeBatchError>> + Send {
            let backend = self.clone();
            async move {
                let first = backend.inner.execute(request).await?;
                let (sender, receiver) = mpsc::channel(backend.later.len().max(1));
                for batch in backend.later {
                    sender.try_send(batch).unwrap();
                }
                Ok(ChangeStream {
                    first: (!backend.opens_empty).then_some(first),
                    rest: Some(receiver),
                })
            }
        }
    }

    async fn call(backend: TestBackend, request: change::ChangeRequest) -> Response {
        router(backend)
            .oneshot(
                Request::post(CHANGE_PATH)
                    .header(header::CONTENT_TYPE, CHANGE_CONTENT_TYPE)
                    .body(Body::from(request.encode_to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    fn decode_frames(bytes: &[u8], max_payload_bytes: usize) -> Vec<ChangeStreamFrame> {
        let mut decoder =
            FrameDecoder::with_max_payload_bytes(Cursor::new(bytes), max_payload_bytes);
        let mut frames = Vec::new();
        while let Some(frame) = decoder.read_message().unwrap() {
            frames.push(frame);
        }
        frames
    }

    /// Each refusal's status, its frame's code, and whether it tells the
    /// client to rebuild from a new snapshot; a client acts on those three,
    /// not on the message.
    #[tokio::test]
    async fn each_refusal_maps_to_its_status_code_and_resnapshot_signal() {
        use change::StreamErrorCode as Code;
        let cases = [
            (
                ChangeBatchError::UnsupportedProtocol,
                StatusCode::BAD_REQUEST,
                Code::ProtocolVersionUnsupported,
                false,
            ),
            (
                ChangeBatchError::AuthorizationExpired,
                StatusCode::UNAUTHORIZED,
                Code::AuthorizationChanged,
                true,
            ),
            (
                ChangeBatchError::RevocationBarrierNotNegotiated,
                StatusCode::UNAUTHORIZED,
                Code::AuthorizationChanged,
                true,
            ),
            (
                ChangeBatchError::BeforeRetention { minimum: 3 },
                StatusCode::GONE,
                Code::CursorBeforeRetention,
                true,
            ),
            (
                ChangeBatchError::SourceEpochMismatch,
                StatusCode::CONFLICT,
                Code::CursorEpochMismatch,
                true,
            ),
            (
                ChangeBatchError::Cursor(CursorError::EpochMismatch {
                    expected: epoch(),
                    actual: SourceEpoch::new("epoch-2").unwrap(),
                }),
                StatusCode::CONFLICT,
                Code::CursorEpochMismatch,
                true,
            ),
            (
                ChangeBatchError::SchemaGenerationMismatch,
                StatusCode::CONFLICT,
                Code::SchemaGenerationMismatch,
                true,
            ),
            (
                ChangeBatchError::PartitionMoved,
                StatusCode::CONFLICT,
                Code::PartitionMoved,
                true,
            ),
            (
                ChangeBatchError::UnauthorizedTable(table()),
                StatusCode::FORBIDDEN,
                Code::TableSetInvalid,
                false,
            ),
            (
                ChangeBatchError::BatchLimitTooSmall,
                StatusCode::UNPROCESSABLE_ENTITY,
                Code::RequestInvalid,
                false,
            ),
            (
                ChangeBatchError::InvalidWireCursor,
                StatusCode::BAD_REQUEST,
                Code::RequestInvalid,
                false,
            ),
            (
                ChangeBatchError::Cursor(CursorError::InvalidSignature),
                StatusCode::BAD_REQUEST,
                Code::RequestInvalid,
                false,
            ),
            (
                ChangeBatchError::Log(LogError::PostgresUnavailable),
                StatusCode::SERVICE_UNAVAILABLE,
                Code::InternalRetryable,
                false,
            ),
            (
                ChangeBatchError::CanonicalRangeIncomplete,
                StatusCode::INTERNAL_SERVER_ERROR,
                Code::RequestInvalid,
                false,
            ),
        ];
        for (error, status, code, resnapshot_required) in cases {
            let case = error.to_string();
            let response = batch_error_response(&error);
            assert_eq!(response.status(), status, "{case}");
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
            assert_eq!(
                response.headers().get(header::RETRY_AFTER).is_some(),
                status == StatusCode::SERVICE_UNAVAILABLE,
                "{case}"
            );
            let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
            let frames = decode_frames(bytes.as_ref(), 64 * 1024);
            assert_eq!(frames.len(), 1, "{case}");
            assert!(
                verify_change_frame_body_sha256(&frames[0]).is_ok(),
                "{case}"
            );
            let Some(change_stream_frame::Body::Error(frame)) = &frames[0].body else {
                panic!("{case}: the frame is not an error");
            };
            assert_eq!(frame.code, code as i32, "{case}");
            assert_eq!(frame.resnapshot_required, resnapshot_required, "{case}");
        }
    }

    #[tokio::test]
    async fn serves_a_real_bounded_canonical_batch() {
        let (_directory, backend) = backend();
        let response = call(backend.clone(), request(&backend.codec, b"scope-secret")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert!(response.extensions().get::<OpenChangeStream>().is_none());
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let frames = decode_frames(bytes.as_ref(), 4 * 1024 * 1024);
        assert!(frames
            .iter()
            .all(|frame| verify_change_frame_body_sha256(frame).is_ok()));
        assert!(matches!(
            frames.first().and_then(|frame| frame.body.as_ref()),
            Some(change_stream_frame::Body::BatchStart(_))
        ));
        assert!(matches!(
            frames.last().and_then(|frame| frame.body.as_ref()),
            Some(change_stream_frame::Body::BatchEnd(_))
        ));
    }

    #[tokio::test]
    async fn a_stream_carries_its_batches_in_order_and_is_not_buffered() {
        let (_directory, backend) = backend();
        let codec = backend.codec.clone();
        let later = vec![
            vec![Bytes::from_static(b"second-"), Bytes::from_static(b"batch")],
            vec![Bytes::from_static(b"third-batch")],
        ];
        let response = router(StreamingBackend {
            inner: backend,
            opens_empty: false,
            later,
        })
        .oneshot(
            Request::post(CHANGE_PATH)
                .header(header::CONTENT_TYPE, CHANGE_CONTENT_TYPE)
                .body(Body::from(stream_request(&codec).encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-accel-buffering"], "no");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert!(response.extensions().get::<OpenChangeStream>().is_some());
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let rest = b"second-batchthird-batch";
        assert!(body.ends_with(rest));
        let first = decode_frames(&body[..body.len() - rest.len()], 4 * 1024 * 1024);
        assert!(matches!(
            first.first().and_then(|frame| frame.body.as_ref()),
            Some(change_stream_frame::Body::BatchStart(_))
        ));
        assert!(matches!(
            first.last().and_then(|frame| frame.body.as_ref()),
            Some(change_stream_frame::Body::BatchEnd(_))
        ));
    }

    #[tokio::test]
    async fn a_stream_opened_with_no_batch_carries_the_ones_sent_later() {
        let (_directory, backend) = backend();
        let codec = backend.codec.clone();
        let response = router(StreamingBackend {
            inner: backend,
            opens_empty: true,
            later: vec![vec![Bytes::from_static(b"later-batch")]],
        })
        .oneshot(
            Request::post(CHANGE_PATH)
                .header(header::CONTENT_TYPE, CHANGE_CONTENT_TYPE)
                .body(Body::from(stream_request(&codec).encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.extensions().get::<OpenChangeStream>().is_some());
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(body.as_ref(), b"later-batch");
    }

    #[tokio::test]
    async fn a_backend_that_cannot_stream_answers_with_one_batch() {
        let (_directory, backend) = backend();
        let response = call(backend.clone(), stream_request(&backend.codec)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-accel-buffering"], "no");
        assert!(response.extensions().get::<OpenChangeStream>().is_none());
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let frames = decode_frames(&bytes, 4 * 1024 * 1024);
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(frame.body, Some(change_stream_frame::Body::BatchEnd(_))))
                .count(),
            1
        );
    }

    /// Answers every request that reaches it as unavailable.
    #[derive(Clone)]
    struct Unavailable;

    impl ChangeHttpBackend for Unavailable {
        fn execute(
            &self,
            _request: IncomingChangeRequest,
        ) -> impl Future<Output = Result<ChangeBatch, ChangeBatchError>> + Send {
            future::ready(Err(ChangeBatchError::Log(LogError::PostgresUnavailable)))
        }
    }

    /// A limit out of range is refused before the backend could reserve
    /// anything for the batch it names.
    #[tokio::test]
    async fn a_request_whose_limits_are_out_of_range_never_reaches_the_backend() {
        let codec = backend().1.codec;
        let answer = |request: change::ChangeRequest| async move {
            router(Unavailable)
                .oneshot(
                    Request::post(CHANGE_PATH)
                        .header(header::CONTENT_TYPE, CHANGE_CONTENT_TYPE)
                        .body(Body::from(request.encode_to_vec()))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
        };
        assert_eq!(
            answer(request(&codec, b"scope-secret")).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        for (max_batch_bytes, max_transactions, stream) in [
            (300_000_000, 100, false),
            (300_000_000, 100, true),
            (16 * 1024 * 1024 + 1, 100, false),
            (1023, 100, false),
            (1024 * 1024, 0, false),
            (1024 * 1024, 10_001, true),
        ] {
            let mut request = if stream {
                stream_request(&codec)
            } else {
                request(&codec, b"scope-secret")
            };
            request.max_batch_bytes = max_batch_bytes;
            request.max_transactions = max_transactions;
            assert_eq!(answer(request).await, StatusCode::UNPROCESSABLE_ENTITY);
        }
    }

    #[tokio::test]
    async fn a_stream_with_no_wait_is_refused() {
        let (_directory, backend) = backend();
        let mut immediate = stream_request(&backend.codec);
        immediate.long_poll_timeout_ms = 0;
        let response = call(backend.clone(), immediate).await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(!response.headers().contains_key("x-accel-buffering"));
    }

    async fn call_accepting(
        backend: TestBackend,
        request: change::ChangeRequest,
        accept_encoding: &str,
    ) -> Response {
        router(backend)
            .oneshot(
                Request::post(CHANGE_PATH)
                    .header(header::CONTENT_TYPE, CHANGE_CONTENT_TYPE)
                    .header(header::ACCEPT_ENCODING, accept_encoding)
                    .body(Body::from(request.encode_to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_fenced_answer_is_zstd_encoded_when_the_client_accepts_it() {
        let (_directory, backend) = backend();
        let plain = call(backend.clone(), request(&backend.codec, b"scope-secret")).await;
        assert!(!plain.headers().contains_key(header::CONTENT_ENCODING));
        let plain = to_bytes(plain.into_body(), 1024 * 1024).await.unwrap();

        let response = call_accepting(
            backend.clone(),
            request(&backend.codec, b"scope-secret"),
            "gzip, deflate, br, zstd",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_ENCODING], "zstd");
        assert_eq!(response.headers()[header::VARY], "accept-encoding");
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let decoded = zstd::stream::decode_all(body.as_ref()).unwrap();
        let frames = decode_frames(&decoded, 4 * 1024 * 1024);
        assert!(frames
            .iter()
            .all(|frame| verify_change_frame_body_sha256(frame).is_ok()));
        // The same batch, but for the stream id and server times each answer
        // draws for itself.
        let bodies = |frames: &[ChangeStreamFrame]| {
            frames
                .iter()
                .map(|frame| std::mem::discriminant(frame.body.as_ref().unwrap()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            bodies(&frames),
            bodies(&decode_frames(&plain, 4 * 1024 * 1024))
        );
        assert_eq!(decoded.len(), plain.len());
    }

    /// Streams the batch `TestBackend` builds, then whatever the test sends
    /// on `later` while the body is open.
    #[derive(Clone)]
    struct HeldStreamBackend {
        inner: TestBackend,
        later: Arc<Mutex<Option<mpsc::Receiver<Vec<Bytes>>>>>,
    }

    impl ChangeHttpBackend for HeldStreamBackend {
        fn execute(
            &self,
            request: IncomingChangeRequest,
        ) -> impl Future<Output = Result<ChangeBatch, ChangeBatchError>> + Send {
            self.inner.execute(request)
        }

        fn execute_stream(
            &self,
            request: IncomingChangeRequest,
        ) -> impl Future<Output = Result<ChangeStream, ChangeBatchError>> + Send {
            let backend = self.clone();
            async move {
                Ok(ChangeStream {
                    first: Some(backend.inner.execute(request).await?),
                    rest: backend.later.lock().unwrap().take(),
                })
            }
        }
    }

    /// The next chunk of `body`, decoded as far as it goes.
    async fn decode_next(
        body: &mut BodyDataStream,
        decoder: &mut zstd::stream::write::Decoder<'static, Vec<u8>>,
    ) -> Vec<u8> {
        let chunk = body.next().await.unwrap().unwrap();
        decoder.write_all(&chunk).unwrap();
        decoder.flush().unwrap();
        std::mem::take(decoder.get_mut())
    }

    #[tokio::test]
    async fn each_batch_of_a_fenced_stream_decodes_before_the_body_ends() {
        let (_directory, backend) = backend();
        let codec = backend.codec.clone();
        let (sender, receiver) = mpsc::channel(1);
        let mut fenced = request(&codec, b"scope-secret");
        fenced.stream = true;
        let response = router(HeldStreamBackend {
            inner: backend,
            later: Arc::new(Mutex::new(Some(receiver))),
        })
        .oneshot(
            Request::post(CHANGE_PATH)
                .header(header::CONTENT_TYPE, CHANGE_CONTENT_TYPE)
                .header(header::ACCEPT_ENCODING, "zstd")
                .body(Body::from(fenced.encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.headers()[header::CONTENT_ENCODING], "zstd");
        let mut body = response.into_body().into_data_stream();
        let mut decoder = zstd::stream::write::Decoder::new(Vec::new()).unwrap();

        // The first batch decodes whole while the stream is still open.
        let first = decode_next(&mut body, &mut decoder).await;
        let frames = decode_frames(&first, 4 * 1024 * 1024);
        assert!(matches!(
            frames.last().and_then(|frame| frame.body.as_ref()),
            Some(change_stream_frame::Body::BatchEnd(_))
        ));
        sender
            .send(vec![
                Bytes::from_static(b"second-"),
                Bytes::from_static(b"batch"),
            ])
            .await
            .unwrap();
        assert_eq!(decode_next(&mut body, &mut decoder).await, b"second-batch");
        sender
            .send(vec![Bytes::from_static(b"third-batch")])
            .await
            .unwrap();
        assert_eq!(decode_next(&mut body, &mut decoder).await, b"third-batch");
        drop(sender);
        // The frame ends with the batches, and the body with it.
        assert!(decode_next(&mut body, &mut decoder).await.is_empty());
        assert!(body.next().await.is_none());
    }

    #[tokio::test]
    async fn a_live_stream_a_refusal_and_an_unaccepted_coding_go_uncompressed() {
        let (_directory, backend) = backend();
        let live = call_accepting(backend.clone(), stream_request(&backend.codec), "zstd").await;
        assert_eq!(live.status(), StatusCode::OK);
        assert!(!live.headers().contains_key(header::CONTENT_ENCODING));
        let refused =
            call_accepting(backend.clone(), request(&backend.codec, b"wrong"), "zstd").await;
        assert_ne!(refused.status(), StatusCode::OK);
        assert!(!refused.headers().contains_key(header::CONTENT_ENCODING));
        for accept_encoding in [
            "gzip, br",
            "zstd;q=0",
            "zstd;q=0.000",
            "zstd;q=2",
            "zstd, zstd",
        ] {
            let response = call_accepting(
                backend.clone(),
                request(&backend.codec, b"scope-secret"),
                accept_encoding,
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "{accept_encoding}");
            assert!(
                !response.headers().contains_key(header::CONTENT_ENCODING),
                "{accept_encoding}"
            );
        }
    }

    #[test]
    fn reads_zstd_from_accept_encoding_only_when_plainly_weighted_above_zero() {
        let accepts = |values: &[&str]| {
            let mut headers = HeaderMap::new();
            for value in values {
                headers.append(
                    header::ACCEPT_ENCODING,
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            accepts_zstd(&headers)
        };
        assert!(accepts(&["gzip, deflate, br, zstd"]));
        assert!(accepts(&["ZSTD;q=0.5"]));
        assert!(accepts(&["gzip", "zstd; q=1.000"]));
        assert!(accepts(&["zstd;q=0.001"]));
        assert!(!accepts(&[]));
        assert!(!accepts(&["gzip, br"]));
        assert!(!accepts(&["*"]));
        assert!(!accepts(&["zstd;q=0"]));
        assert!(!accepts(&["zstd;q=1.5"]));
        assert!(!accepts(&["zstd;q=0.0001"]));
        assert!(!accepts(&["zstd;level=3"]));
        assert!(!accepts(&["zstd;q=1;x=2"]));
        assert!(!accepts(&["zstd", "zstd"]));
        assert!(!accepts(&[&format!(
            "zstd, {}",
            "x".repeat(MAX_ACCEPT_ENCODING_BYTES)
        )]));
    }

    #[tokio::test]
    async fn returns_framed_sanitized_protocol_errors() {
        let (_directory, backend) = backend();
        let response = call(backend.clone(), request(&backend.codec, b"wrong")).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let bytes = to_bytes(response.into_body(), MAX_ERROR_FRAME_BYTES)
            .await
            .unwrap();
        let frames = decode_frames(bytes.as_ref(), MAX_ERROR_FRAME_BYTES);
        let frame = frames.first().unwrap();
        verify_change_frame_body_sha256(frame).unwrap();
        let Some(change_stream_frame::Body::Error(error)) = frame.body.as_ref() else {
            panic!("expected stream error");
        };
        assert_eq!(error.code, StreamErrorCode::AuthorizationChanged as i32);
        assert!(!error.message.contains("scope-secret"));
    }

    #[tokio::test]
    async fn a_barrier_that_cannot_be_delivered_still_retires_the_scope() {
        // A client that did not negotiate the barrier body, and a barrier
        // cursor the log cannot place, both get 401 with `resnapshot_required`,
        // so the client rebuilds under a fresh grant instead of retrying.
        for error in [
            ChangeBatchError::RevocationBarrierNotNegotiated,
            ChangeBatchError::RevocationBarrierUnplaceable,
        ] {
            let response = batch_error_response(&error);
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{error}");
            let bytes = to_bytes(response.into_body(), MAX_ERROR_FRAME_BYTES)
                .await
                .unwrap();
            let frames = decode_frames(&bytes, MAX_ERROR_FRAME_BYTES);
            let Some(change_stream_frame::Body::Error(refusal)) = frames[0].body.as_ref() else {
                panic!("expected a stream error for {error}");
            };
            assert_eq!(
                refusal.code,
                StreamErrorCode::AuthorizationChanged as i32,
                "{error}"
            );
            assert!(refusal.resnapshot_required, "{error}");
            // Neither refusal is worth retrying against the same authorization.
            assert_eq!(refusal.retry_after_ms, 0, "{error}");
        }
    }

    #[tokio::test]
    async fn classifies_rebuild_terminal_and_retryable_source_failures() {
        let schema = batch_error_response(&ChangeBatchError::SchemaGenerationMismatch);
        assert_eq!(schema.status(), StatusCode::CONFLICT);
        assert!(!schema.headers().contains_key(header::RETRY_AFTER));
        let schema_bytes = to_bytes(schema.into_body(), MAX_ERROR_FRAME_BYTES)
            .await
            .unwrap();
        let schema_frames = decode_frames(&schema_bytes, MAX_ERROR_FRAME_BYTES);
        let Some(change_stream_frame::Body::Error(schema_error)) = schema_frames[0].body.as_ref()
        else {
            panic!("expected schema stream error");
        };
        assert_eq!(
            schema_error.code,
            StreamErrorCode::SchemaGenerationMismatch as i32
        );
        assert!(schema_error.resnapshot_required);
        assert_eq!(schema_error.retry_after_ms, 0);

        let policy = batch_error_response(&ChangeBatchError::Authorization(
            CanonicalAuthorizationError::DuplicateChangedColumn,
        ));
        assert_eq!(policy.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!policy.headers().contains_key(header::RETRY_AFTER));
        let policy_bytes = to_bytes(policy.into_body(), MAX_ERROR_FRAME_BYTES)
            .await
            .unwrap();
        let policy_frames = decode_frames(&policy_bytes, MAX_ERROR_FRAME_BYTES);
        let Some(change_stream_frame::Body::Error(policy_error)) = policy_frames[0].body.as_ref()
        else {
            panic!("expected policy stream error");
        };
        assert_eq!(policy_error.code, StreamErrorCode::RequestInvalid as i32);
        assert!(!policy_error.resnapshot_required);
        assert_eq!(policy_error.retry_after_ms, 0);

        let unavailable = batch_error_response(&ChangeBatchError::Log(
            canonical_log::LogError::Io(std::io::Error::other("temporary test failure")),
        ));
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unavailable.headers()[header::RETRY_AFTER], "1");
        let unavailable_bytes = to_bytes(unavailable.into_body(), MAX_ERROR_FRAME_BYTES)
            .await
            .unwrap();
        let unavailable_frames = decode_frames(&unavailable_bytes, MAX_ERROR_FRAME_BYTES);
        let Some(change_stream_frame::Body::Error(unavailable_error)) =
            unavailable_frames[0].body.as_ref()
        else {
            panic!("expected unavailable stream error");
        };
        assert_eq!(
            unavailable_error.code,
            StreamErrorCode::InternalRetryable as i32
        );
        assert!(!unavailable_error.resnapshot_required);
        assert_eq!(unavailable_error.retry_after_ms, 1_000);
    }

    /// The route reads one media type. The generic protobuf types get the
    /// refusal any other type gets; the same body under the change type is
    /// the control that the media type is what is refused.
    #[tokio::test]
    async fn generic_protobuf_media_types_are_refused_like_any_other() {
        let (_directory, backend) = backend();
        let body = request(&backend.codec, b"scope-secret").encode_to_vec();
        for (content_type, expected) in [
            (CHANGE_CONTENT_TYPE, StatusCode::OK),
            ("application/protobuf", StatusCode::UNSUPPORTED_MEDIA_TYPE),
            ("application/x-protobuf", StatusCode::UNSUPPORTED_MEDIA_TYPE),
        ] {
            let response = router(backend.clone())
                .oneshot(
                    Request::post(CHANGE_PATH)
                        .header(header::CONTENT_TYPE, content_type)
                        .body(Body::from(body.clone()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "content-type {content_type}");
        }
    }

    #[tokio::test]
    async fn rejects_wrong_media_types_and_oversized_bodies_before_decode() {
        let (_directory, backend) = backend();
        let wrong_type = router(backend.clone())
            .oneshot(
                Request::post(CHANGE_PATH)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_type.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let oversized = router(backend)
            .oneshot(
                Request::post(CHANGE_PATH)
                    .header(header::CONTENT_TYPE, CHANGE_CONTENT_TYPE)
                    .body(Body::from(vec![0_u8; MAX_REQUEST_BYTES + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}

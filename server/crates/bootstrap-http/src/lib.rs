//! Bounded authenticated HTTP adapter for bootstrap planning over the tables
//! a client names.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    time::Instant,
};

use artifact_contract::RepresentationEncoding;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use bootstrap_api::BootstrapPlan;
use http_headers::has_media_type;
pub use http_headers::BearerToken;
use serde::{Deserialize, Serialize};
use sync_types::{ArtifactId, OpaqueCursor, TableId};
use tracing::{info_span, Instrument};

const BOOTSTRAP_PATH: &str = "/v1/bootstrap";
const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_RESIDENT_PROOF_BYTES: usize = 1024 * 1024;
const MAX_CACHE_HINTS: usize = 4096;
const MAX_CLIENT_INSTANCE_ID_BYTES: usize = 1024;
const MAX_DEVICE_ID_BYTES: usize = 1024;
const MAX_ACCEPT_ENCODING_BYTES: usize = 1024;
const MAX_ACCEPT_ENCODING_ITEMS: usize = 32;
const ARTIFACT_PROFILE_HEADER: &str = "x-calque-artifact-profile";
const CHROMIUM_NATIVE_ARTIFACT_V1: &str = "chromium-native-artifact-v1";

#[derive(Debug, Clone)]
pub struct BootstrapHttpRequest {
    pub required_tables: BTreeSet<TableId>,
    pub resident_state_proof: Option<String>,
    pub resident_tables: BTreeSet<TableId>,
    /// Where the resident tables stand; named exactly when they are.
    pub resident_cursor: Option<OpaqueCursor>,
    pub cached_artifact_hints: BTreeSet<ArtifactId>,
    pub accepted_encodings: Vec<RepresentationEncoding>,
    pub memory_budget_bytes: u64,
    pub client_heap_reserve_bytes: u64,
    pub client_instance_id: String,
    /// The device the plan is bound to beside the client instance; every
    /// change request and write of the plan must name both.
    pub device_id: String,
}

pub trait BootstrapHttpBackend: Clone + Send + Sync + 'static {
    fn execute(
        &self,
        bearer: BearerToken,
        request: BootstrapHttpRequest,
    ) -> impl Future<Output = Result<BootstrapPlan, BootstrapHttpError>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapHttpError {
    Unauthorized,
    Forbidden,
    InvalidRequest,
    CapacityExceeded,
    ResnapshotRequired,
    Unavailable,
    /// The scope's artifact is not ready to serve yet. The response carries
    /// no plan, scope token, artifact URL, or change capability -- only
    /// bounded retry guidance.
    ArtifactPreparing {
        retry_after_seconds: u32,
    },
}

/// Upper bound on the `Retry-After` a preparing artifact may advertise.
/// Anything longer is a deployment problem the client cannot poll its way out
/// of, and a client that sleeps for it looks hung.
pub const MAX_ARTIFACT_PREPARING_RETRY_AFTER_SECONDS: u32 = 300;

pub fn router<B>(backend: B) -> Router
where
    B: BootstrapHttpBackend,
{
    Router::new()
        .route(BOOTSTRAP_PATH, post(post_bootstrap::<B>))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(backend)
}

async fn post_bootstrap<B>(State(backend): State<B>, headers: HeaderMap, body: Bytes) -> Response
where
    B: BootstrapHttpBackend,
{
    let span = info_span!(
        "bootstrap_http_request",
        client_instance_id = tracing::field::Empty,
        required_table_count = tracing::field::Empty,
        plan_id = tracing::field::Empty,
        result = tracing::field::Empty,
        request_bytes = body.len(),
        latency_micros = tracing::field::Empty,
    );
    async move {
        let started = Instant::now();
        let outcome = async {
            let bearer =
                BearerToken::from_headers(&headers).ok_or(BootstrapHttpError::Unauthorized)?;
            if !has_media_type(&headers, "application/json") {
                return Err(BootstrapHttpError::InvalidRequest);
            }
            let mut request: BootstrapHttpRequest =
                serde_json::from_slice::<WireBootstrapRequest>(&body)
                    .map_err(|_| BootstrapHttpError::InvalidRequest)
                    .and_then(TryInto::try_into)?;
            request.accepted_encodings =
                accepted_artifact_encodings(&headers, &request.accepted_encodings)?;
            tracing::Span::current()
                .record("client_instance_id", request.client_instance_id.as_str());
            tracing::Span::current().record("required_table_count", request.required_tables.len());
            backend.execute(bearer, request).await
        }
        .await;

        tracing::Span::current().record(
            "latency_micros",
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        );
        match outcome {
            Ok(plan) => {
                tracing::Span::current().record("result", "ok");
                tracing::Span::current().record("plan_id", plan.plan_id.to_string());
                let mut response = Json(plan).into_response();
                response.headers_mut().insert(
                    header::CACHE_CONTROL,
                    HeaderValue::from_static("no-store, private"),
                );
                response.headers_mut().insert(
                    header::HeaderName::from_static("x-content-type-options"),
                    HeaderValue::from_static("nosniff"),
                );
                response
            }
            Err(error) => {
                tracing::Span::current().record("result", error_class(error));
                error_response(error)
            }
        }
    }
    .instrument(span)
    .await
}

fn error_class(error: BootstrapHttpError) -> &'static str {
    match error {
        BootstrapHttpError::Unauthorized => "unauthorized",
        BootstrapHttpError::Forbidden => "forbidden",
        BootstrapHttpError::InvalidRequest => "invalid_request",
        BootstrapHttpError::CapacityExceeded => "capacity_exceeded",
        BootstrapHttpError::ResnapshotRequired => "resnapshot_required",
        BootstrapHttpError::Unavailable => "unavailable",
        BootstrapHttpError::ArtifactPreparing { .. } => "artifact_preparing",
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireBootstrapRequest {
    protocol_version: u32,
    required_tables: Vec<String>,
    resident_state_proof: Option<String>,
    #[serde(default)]
    resident_tables: Vec<String>,
    resident_cursor: Option<String>,
    #[serde(default)]
    cached_artifact_hints: Vec<String>,
    accepted_encodings: Vec<RepresentationEncoding>,
    memory_budget_bytes: u64,
    client_heap_reserve_bytes: u64,
    client_instance_id: String,
    device_id: String,
}

impl TryFrom<WireBootstrapRequest> for BootstrapHttpRequest {
    type Error = BootstrapHttpError;

    fn try_from(wire: WireBootstrapRequest) -> Result<Self, Self::Error> {
        if wire.protocol_version != 1
            || wire.required_tables.is_empty()
            || wire.required_tables.len() > 256
            || wire.cached_artifact_hints.len() > MAX_CACHE_HINTS
            || wire.resident_tables.len() > 1_024
            || wire.accepted_encodings.is_empty()
            || wire.memory_budget_bytes == 0
            || wire.memory_budget_bytes > 9_007_199_254_740_991
            || wire.client_heap_reserve_bytes == 0
            || wire.client_heap_reserve_bytes > wire.memory_budget_bytes
            || wire.client_instance_id.is_empty()
            || wire.client_instance_id.len() > MAX_CLIENT_INSTANCE_ID_BYTES
            || wire.client_instance_id.chars().any(char::is_control)
            || wire.device_id.is_empty()
            || wire.device_id.len() > MAX_DEVICE_ID_BYTES
            || wire.device_id.chars().any(char::is_control)
            || wire
                .resident_state_proof
                .as_ref()
                .is_some_and(|proof| proof.is_empty() || proof.len() > MAX_RESIDENT_PROOF_BYTES)
        {
            return Err(BootstrapHttpError::InvalidRequest);
        }
        let required_table_count = wire.required_tables.len();
        let cache_hint_count = wire.cached_artifact_hints.len();
        let resident_table_count = wire.resident_tables.len();
        let required_tables = wire
            .required_tables
            .into_iter()
            .map(TableId::new)
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(|_| BootstrapHttpError::InvalidRequest)?;
        let cached_artifact_hints = wire
            .cached_artifact_hints
            .into_iter()
            .map(ArtifactId::new)
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(|_| BootstrapHttpError::InvalidRequest)?;
        let resident_tables = wire
            .resident_tables
            .into_iter()
            .map(TableId::new)
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(|_| BootstrapHttpError::InvalidRequest)?;
        let resident_cursor = wire
            .resident_cursor
            .map(OpaqueCursor::parse)
            .transpose()
            .map_err(|_| BootstrapHttpError::InvalidRequest)?;
        let accepted_encodings = wire.accepted_encodings;
        if required_tables.len() != required_table_count
            || cached_artifact_hints.len() != cache_hint_count
            || resident_tables.len() != resident_table_count
            || (wire.resident_state_proof.is_none() && !resident_tables.is_empty())
            || resident_cursor.is_some() == resident_tables.is_empty()
            || accepted_encodings
                .iter()
                .enumerate()
                .any(|(index, encoding)| accepted_encodings[..index].contains(encoding))
        {
            return Err(BootstrapHttpError::InvalidRequest);
        }
        Ok(Self {
            required_tables,
            resident_state_proof: wire.resident_state_proof,
            resident_tables,
            resident_cursor,
            cached_artifact_hints,
            accepted_encodings,
            memory_budget_bytes: wire.memory_budget_bytes,
            client_heap_reserve_bytes: wire.client_heap_reserve_bytes,
            client_instance_id: wire.client_instance_id,
            device_id: wire.device_id,
        })
    }
}

/// What a client names when it reads only what the browser decodes.
const NATIVE_ARTIFACT_ENCODINGS: [RepresentationEncoding; 2] = [
    RepresentationEncoding::Zstd,
    RepresentationEncoding::Identity,
];
/// What a client names when it can also run the brotli decoder worker, which
/// restores a packed object's artifact too. No list names one brotli encoding
/// without the other: a client that could not restore a packed object would be
/// planned the level-19 one for a packed cut, and the client package is not
/// such a client.
const DECODING_ARTIFACT_ENCODINGS: [RepresentationEncoding; 4] = [
    RepresentationEncoding::PackedBrotliLargeWindow,
    RepresentationEncoding::BrotliLargeWindow,
    RepresentationEncoding::Zstd,
    RepresentationEncoding::Identity,
];

fn accepted_artifact_encodings(
    headers: &HeaderMap,
    client_encodings: &[RepresentationEncoding],
) -> Result<Vec<RepresentationEncoding>, BootstrapHttpError> {
    let profile = headers
        .get(ARTIFACT_PROFILE_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or(BootstrapHttpError::InvalidRequest)?;
    let decodes = client_encodings == DECODING_ARTIFACT_ENCODINGS;
    if profile != CHROMIUM_NATIVE_ARTIFACT_V1
        || !(decodes || client_encodings == NATIVE_ARTIFACT_ENCODINGS)
    {
        return Err(BootstrapHttpError::InvalidRequest);
    }

    let codings = parse_accept_encoding(headers)?;
    if !coding_accepted(&codings, "identity", true) {
        return Err(BootstrapHttpError::InvalidRequest);
    }
    let mut accepted = Vec::with_capacity(4);
    // Sent without a Content-Encoding and decoded by the page's own worker,
    // so the browser's Accept-Encoding does not gate them.
    if decodes {
        accepted.push(RepresentationEncoding::PackedBrotliLargeWindow);
        accepted.push(RepresentationEncoding::BrotliLargeWindow);
    }
    if coding_accepted(&codings, "zstd", false) {
        accepted.push(RepresentationEncoding::Zstd);
    }
    accepted.push(RepresentationEncoding::Identity);
    Ok(accepted)
}

fn parse_accept_encoding(
    headers: &HeaderMap,
) -> Result<BTreeMap<String, bool>, BootstrapHttpError> {
    // Hyper/http already owns HTTP wire decoding. This does not negotiate the
    // general header grammar: it derives a bounded yes/no policy for three
    // known artifact codings and rejects every ambiguous q-value, duplicate, or
    // extension. Reviewed typed-header crates either erase q=0 or accept
    // out-of-range/over-precise floats, which is unsafe for this gate.
    let values = headers.get_all(header::ACCEPT_ENCODING);
    let mut total_bytes = 0_usize;
    let mut codings = BTreeMap::new();
    let mut items = 0_usize;
    for value in values.iter() {
        let value = value
            .to_str()
            .map_err(|_| BootstrapHttpError::InvalidRequest)?;
        total_bytes = total_bytes
            .checked_add(value.len())
            .ok_or(BootstrapHttpError::InvalidRequest)?;
        if total_bytes > MAX_ACCEPT_ENCODING_BYTES {
            return Err(BootstrapHttpError::InvalidRequest);
        }
        for item in value.split(',') {
            items = items
                .checked_add(1)
                .ok_or(BootstrapHttpError::InvalidRequest)?;
            if items > MAX_ACCEPT_ENCODING_ITEMS {
                return Err(BootstrapHttpError::InvalidRequest);
            }
            let mut parts = item.trim().split(';');
            let coding = parts
                .next()
                .filter(|coding| !coding.is_empty() && coding.bytes().all(is_token_byte))
                .ok_or(BootstrapHttpError::InvalidRequest)?
                .to_ascii_lowercase();
            let mut accepted = true;
            if let Some(parameter) = parts.next() {
                let (name, value) = parameter
                    .trim()
                    .split_once('=')
                    .ok_or(BootstrapHttpError::InvalidRequest)?;
                if !name.trim().eq_ignore_ascii_case("q") || parts.next().is_some() {
                    return Err(BootstrapHttpError::InvalidRequest);
                }
                accepted = parse_qvalue(value.trim())?;
            }
            if codings.insert(coding, accepted).is_some() {
                return Err(BootstrapHttpError::InvalidRequest);
            }
        }
    }
    if items == 0 {
        return Err(BootstrapHttpError::InvalidRequest);
    }
    Ok(codings)
}

fn coding_accepted(codings: &BTreeMap<String, bool>, coding: &str, identity: bool) -> bool {
    codings.get(coding).copied().unwrap_or_else(|| {
        if identity {
            codings.get("*").copied().unwrap_or(true)
        } else {
            false
        }
    })
}

fn parse_qvalue(value: &str) -> Result<bool, BootstrapHttpError> {
    let (whole, fraction) = value.split_once('.').map_or((value, ""), |parts| parts);
    if fraction.len() > 3 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(BootstrapHttpError::InvalidRequest);
    }
    match whole {
        "0" => Ok(fraction.bytes().any(|byte| byte != b'0')),
        "1" if fraction.bytes().all(|byte| byte == b'0') => Ok(true),
        _ => Err(BootstrapHttpError::InvalidRequest),
    }
}

fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
    resnapshot_required: bool,
}

fn error_response(error: BootstrapHttpError) -> Response {
    let (status, code, message, resnapshot_required) = match error {
        BootstrapHttpError::Unauthorized => (
            StatusCode::UNAUTHORIZED,
            "AUTHORIZATION_REQUIRED",
            "bootstrap authorization is missing or invalid",
            false,
        ),
        BootstrapHttpError::Forbidden => (
            StatusCode::FORBIDDEN,
            "TABLE_SET_FORBIDDEN",
            "bootstrap table set is not authorized",
            false,
        ),
        BootstrapHttpError::InvalidRequest => (
            StatusCode::BAD_REQUEST,
            "REQUEST_INVALID",
            "bootstrap request is invalid",
            false,
        ),
        BootstrapHttpError::CapacityExceeded => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "CAPACITY_EXCEEDED",
            "bootstrap cannot fit the declared client memory budget",
            false,
        ),
        BootstrapHttpError::ResnapshotRequired => (
            StatusCode::CONFLICT,
            "RESNAPSHOT_REQUIRED",
            "resident state is incompatible and must be rebuilt",
            true,
        ),
        BootstrapHttpError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "INTERNAL_RETRYABLE",
            "bootstrap planning is temporarily unavailable",
            false,
        ),
        BootstrapHttpError::ArtifactPreparing { .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            "ARTIFACT_PREPARING",
            "this authorization generation's artifact is still being published",
            false,
        ),
    };
    let mut response = (
        status,
        Json(ErrorBody {
            code,
            message,
            resnapshot_required,
        }),
    )
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, private"),
    );
    if let BootstrapHttpError::ArtifactPreparing {
        retry_after_seconds,
    } = error
    {
        // Bounded retry guidance, and the only field of this response that
        // says anything about server state. It never reveals which artifact is
        // preparing, or that one exists at all beyond this scope's own.
        let seconds = retry_after_seconds
            .clamp(1, MAX_ARTIFACT_PREPARING_RETRY_AFTER_SECONDS)
            .to_string();
        if let Ok(value) = HeaderValue::from_str(&seconds) {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future;

    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use tower::ServiceExt;

    #[derive(Clone)]
    struct Backend;

    impl BootstrapHttpBackend for Backend {
        fn execute(
            &self,
            bearer: BearerToken,
            request: BootstrapHttpRequest,
        ) -> impl Future<Output = Result<BootstrapPlan, BootstrapHttpError>> + Send {
            let result = if bearer.expose() != "app-token" {
                Err(BootstrapHttpError::Unauthorized)
            } else if !request
                .required_tables
                .contains(&TableId::new("table/tasks").unwrap())
            {
                Err(BootstrapHttpError::Forbidden)
            } else if request.memory_budget_bytes != 536_870_912
                || request.client_heap_reserve_bytes != 67_108_864
            {
                Err(BootstrapHttpError::InvalidRequest)
            } else {
                serde_json::from_slice::<BootstrapPlan>(include_bytes!(
                    "../../../../spec/bootstrap-plan.direct-columnar.example.json"
                ))
                .map_err(|_| BootstrapHttpError::Unavailable)
            };
            future::ready(result)
        }
    }

    fn request(body: impl Into<Body>) -> Request<Body> {
        Request::post(BOOTSTRAP_PATH)
            .header(header::AUTHORIZATION, "Bearer app-token")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT_ENCODING, "zstd, br, gzip, deflate")
            .header(ARTIFACT_PROFILE_HEADER, CHROMIUM_NATIVE_ARTIFACT_V1)
            .body(body.into())
            .unwrap()
    }

    #[tokio::test]
    async fn serves_a_private_schema_valid_plan_without_echoing_auth() {
        let response = router(Backend)
            .oneshot(request(
                serde_json::json!({
                    "protocolVersion": 1,
                    "requiredTables": ["table/tasks"],
                    "cachedArtifactHints": [],
                    "acceptedEncodings": ["zstd", "identity"],
                    "memoryBudgetBytes": 536870912,
                    "clientHeapReserveBytes": 67108864,
                    "clientInstanceId": "client-1",
                    "deviceId": "device-1"
                })
                .to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "no-store, private"
        );
        let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .unwrap();
        let plan: BootstrapPlan = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(plan.plan_id.to_string(), "plan_01JZCOLUMNAR");
        assert_eq!(plan.memory.client_heap_reserve_bytes, 67_108_864);
        assert!(!String::from_utf8(bytes.to_vec())
            .unwrap()
            .contains("app-token"));
    }

    #[tokio::test]
    async fn rejects_duplicates_malformed_auth_and_oversized_bodies() {
        // The backend answers 503 to whatever reaches it, so a 400 here is
        // the adapter's own refusal.
        let duplicate = router(RefusingBackend(BootstrapHttpError::Unavailable))
            .oneshot(request(
                serde_json::json!({
                    "protocolVersion": 1,
                    "requiredTables": ["table/tasks", "table/tasks"],
                    "acceptedEncodings": ["zstd", "identity"],
                    "memoryBudgetBytes": 536870912,
                    "clientHeapReserveBytes": 67108864,
                    "clientInstanceId": "client-1",
                    "deviceId": "device-1"
                })
                .to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(duplicate.status(), StatusCode::BAD_REQUEST);

        let offers_brotli = router(Backend)
            .oneshot(request(
                serde_json::json!({
                    "protocolVersion": 1,
                    "requiredTables": ["table/tasks"],
                    "acceptedEncodings": ["zstd", "br", "identity"],
                    "memoryBudgetBytes": 536870912,
                    "clientHeapReserveBytes": 67108864,
                    "clientInstanceId": "client-1",
                    "deviceId": "device-1"
                })
                .to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(offers_brotli.status(), StatusCode::BAD_REQUEST);

        let no_auth = router(Backend)
            .oneshot(
                Request::post(BOOTSTRAP_PATH)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(no_auth.status(), StatusCode::UNAUTHORIZED);

        let oversized = router(Backend)
            .oneshot(request(vec![b' '; MAX_REQUEST_BYTES + 1]))
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn rejects_missing_zero_or_over_budget_client_heap_reserve() {
        for body in [
            serde_json::json!({
                "protocolVersion": 1,
                "requiredTables": ["table/tasks"],
                "acceptedEncodings": ["zstd", "identity"],
                "memoryBudgetBytes": 1024,
                "clientInstanceId": "client-1",
                "deviceId": "device-1"
            }),
            serde_json::json!({
                "protocolVersion": 1,
                "requiredTables": ["table/tasks"],
                "acceptedEncodings": ["zstd", "identity"],
                "memoryBudgetBytes": 1024,
                "clientHeapReserveBytes": 0,
                "clientInstanceId": "client-1",
                "deviceId": "device-1"
            }),
            serde_json::json!({
                "protocolVersion": 1,
                "requiredTables": ["table/tasks"],
                "acceptedEncodings": ["zstd", "identity"],
                "memoryBudgetBytes": 1024,
                "clientHeapReserveBytes": 1025,
                "clientInstanceId": "client-1",
                "deviceId": "device-1"
            }),
        ] {
            // A reserve equal to the budget reaches the backend, which answers
            // 503; each body above is refused before it.
            let response = router(RefusingBackend(BootstrapHttpError::Unavailable))
                .oneshot(request(body.to_string()))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
        }
        let at_budget = serde_json::json!({
            "protocolVersion": 1,
            "requiredTables": ["table/tasks"],
            "acceptedEncodings": ["zstd", "identity"],
            "memoryBudgetBytes": 1024,
            "clientHeapReserveBytes": 1024,
            "clientInstanceId": "client-1",
            "deviceId": "device-1"
        });
        let response = router(RefusingBackend(BootstrapHttpError::Unavailable))
            .oneshot(request(at_budget.to_string()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn rejects_a_missing_empty_oversized_or_control_device() {
        let body = |device: Option<serde_json::Value>| {
            let mut body = serde_json::json!({
                "protocolVersion": 1,
                "requiredTables": ["table/tasks"],
                "acceptedEncodings": ["zstd", "identity"],
                "memoryBudgetBytes": 1024,
                "clientHeapReserveBytes": 512,
                "clientInstanceId": "client-1"
            });
            if let Some(device) = device {
                body["deviceId"] = device;
            }
            body.to_string()
        };
        for refused in [
            None,
            Some(serde_json::json!("")),
            Some(serde_json::json!("d".repeat(MAX_DEVICE_ID_BYTES + 1))),
            Some(serde_json::json!("device\n1")),
            Some(serde_json::json!(7)),
        ] {
            let response = router(RefusingBackend(BootstrapHttpError::Unavailable))
                .oneshot(request(body(refused.clone())))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{refused:?}");
        }
        let longest = serde_json::json!("d".repeat(MAX_DEVICE_ID_BYTES));
        let response = router(RefusingBackend(BootstrapHttpError::Unavailable))
            .oneshot(request(body(Some(longest))))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// A renewal names the cursor its resident tables stand at, and a request
    /// that names no resident tables names no cursor.
    #[tokio::test]
    async fn a_resident_cursor_comes_exactly_with_resident_tables() {
        let body = |tables: &[&str], cursor: Option<&str>| {
            let mut body: serde_json::Value = serde_json::from_str(&valid_body()).unwrap();
            if !tables.is_empty() {
                body["residentStateProof"] = serde_json::json!("proof");
                body["residentTables"] = serde_json::json!(tables);
            }
            if let Some(cursor) = cursor {
                body["residentCursor"] = serde_json::json!(cursor);
            }
            body.to_string()
        };
        for (tables, cursor, status) in [
            (&["table/tasks"][..], None, StatusCode::BAD_REQUEST),
            (&[][..], Some("cursor-7"), StatusCode::BAD_REQUEST),
            (&["table/tasks"][..], Some(""), StatusCode::BAD_REQUEST),
            (
                &["table/tasks"][..],
                Some("cursor-7"),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (&[][..], None, StatusCode::SERVICE_UNAVAILABLE),
        ] {
            let response = router(RefusingBackend(BootstrapHttpError::Unavailable))
                .oneshot(request(body(tables, cursor)))
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{tables:?} {cursor:?}");
        }
    }

    /// A backend that answers every request with one refusal, so the response
    /// body and headers are the whole observable result: no plan, no scope
    /// token, no artifact URL.
    #[derive(Clone)]
    struct RefusingBackend(BootstrapHttpError);

    impl BootstrapHttpBackend for RefusingBackend {
        fn execute(
            &self,
            _bearer: BearerToken,
            _request: BootstrapHttpRequest,
        ) -> impl Future<Output = Result<BootstrapPlan, BootstrapHttpError>> + Send {
            future::ready(Err(self.0))
        }
    }

    fn valid_body() -> String {
        serde_json::json!({
            "protocolVersion": 1,
            "requiredTables": ["table/tasks"],
            "acceptedEncodings": ["zstd", "identity"],
            "memoryBudgetBytes": 536870912,
            "clientHeapReserveBytes": 67108864,
            "clientInstanceId": "client-1",
            "deviceId": "device-1"
        })
        .to_string()
    }

    #[tokio::test]
    async fn each_refusal_maps_to_its_status_code_and_resnapshot_flag() {
        for (error, status, code, resnapshot_required) in [
            (
                BootstrapHttpError::Unauthorized,
                StatusCode::UNAUTHORIZED,
                "AUTHORIZATION_REQUIRED",
                false,
            ),
            (
                BootstrapHttpError::Forbidden,
                StatusCode::FORBIDDEN,
                "TABLE_SET_FORBIDDEN",
                false,
            ),
            (
                BootstrapHttpError::InvalidRequest,
                StatusCode::BAD_REQUEST,
                "REQUEST_INVALID",
                false,
            ),
            (
                BootstrapHttpError::CapacityExceeded,
                StatusCode::UNPROCESSABLE_ENTITY,
                "CAPACITY_EXCEEDED",
                false,
            ),
            (
                BootstrapHttpError::ResnapshotRequired,
                StatusCode::CONFLICT,
                "RESNAPSHOT_REQUIRED",
                true,
            ),
            (
                BootstrapHttpError::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                "INTERNAL_RETRYABLE",
                false,
            ),
            (
                BootstrapHttpError::ArtifactPreparing {
                    retry_after_seconds: 5,
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "ARTIFACT_PREPARING",
                false,
            ),
        ] {
            let response = router(RefusingBackend(error))
                .oneshot(request(valid_body()))
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{error:?}");
            assert_eq!(
                response.headers()[header::CACHE_CONTROL],
                "no-store, private",
                "{error:?}"
            );
            let body = to_bytes(response.into_body(), MAX_REQUEST_BYTES)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["code"], code, "{error:?}");
            assert_eq!(body["resnapshotRequired"], resnapshot_required, "{error:?}");
        }
    }

    #[tokio::test]
    async fn a_body_that_is_not_declared_json_is_refused() {
        for content_type in [None, Some("text/plain"), Some("application/jsonx")] {
            let mut request = request(valid_body());
            match content_type {
                Some(value) => {
                    request
                        .headers_mut()
                        .insert(header::CONTENT_TYPE, HeaderValue::from_static(value));
                }
                None => {
                    request.headers_mut().remove(header::CONTENT_TYPE);
                }
            }
            let response = router(RefusingBackend(BootstrapHttpError::Unavailable))
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{content_type:?}"
            );
        }
    }

    #[tokio::test]
    async fn artifact_preparing_refusal_matches_its_frozen_vector() {
        let preparing = router(RefusingBackend(BootstrapHttpError::ArtifactPreparing {
            retry_after_seconds: 5,
        }))
        .oneshot(request(valid_body()))
        .await
        .unwrap();
        assert_eq!(preparing.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            preparing.headers()[header::CACHE_CONTROL],
            "no-store, private"
        );
        assert_eq!(preparing.headers()[header::RETRY_AFTER], "5");
        let body = to_bytes(preparing.into_body(), MAX_REQUEST_BYTES)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::from_slice::<serde_json::Value>(include_bytes!(
                "../../../../spec/bootstrap-error.artifact-preparing.example.json"
            ))
            .unwrap()
        );
        // The refusal carries no capability the client could use.
        let text = String::from_utf8(body.to_vec()).unwrap();
        for field in ["planId", "artifacts", "authorizationScopeToken", "changes"] {
            assert!(!text.contains(field), "{field} must not appear: {text}");
        }
    }

    #[tokio::test]
    async fn preparing_retry_after_is_bounded_in_both_directions() {
        for (requested, expected) in [
            (0_u32, "1"),
            (1, "1"),
            (MAX_ARTIFACT_PREPARING_RETRY_AFTER_SECONDS, "300"),
            (u32::MAX, "300"),
        ] {
            let response = router(RefusingBackend(BootstrapHttpError::ArtifactPreparing {
                retry_after_seconds: requested,
            }))
            .oneshot(request(valid_body()))
            .await
            .unwrap();
            assert_eq!(response.headers()[header::RETRY_AFTER], expected);
        }
    }

    #[test]
    fn profile_intersects_only_explicit_native_codings_and_identity() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ARTIFACT_PROFILE_HEADER,
            HeaderValue::from_static(CHROMIUM_NATIVE_ARTIFACT_V1),
        );
        let client = [
            RepresentationEncoding::Zstd,
            RepresentationEncoding::Identity,
        ];
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("gzip, br;q=0.5, zstd;q=0, identity;q=1"),
        );
        assert_eq!(
            accepted_artifact_encodings(&headers, &client).unwrap(),
            vec![RepresentationEncoding::Identity]
        );

        // Chrome's own header. Its `br` is HTTP brotli, which no
        // representation uses.
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("gzip, deflate, br, zstd"),
        );
        assert_eq!(
            accepted_artifact_encodings(&headers, &client).unwrap(),
            vec![
                RepresentationEncoding::Zstd,
                RepresentationEncoding::Identity
            ]
        );
    }

    #[test]
    fn a_client_that_runs_the_decoder_worker_is_offered_both_brotli_objects_whatever_the_browser_accepts(
    ) {
        use RepresentationEncoding::{
            BrotliLargeWindow, Identity, PackedBrotliLargeWindow as Packed, Zstd,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            ARTIFACT_PROFILE_HEADER,
            HeaderValue::from_static(CHROMIUM_NATIVE_ARTIFACT_V1),
        );
        // Either object goes without a Content-Encoding and the page's worker
        // decodes it, so Accept-Encoding says nothing about it. zstd stays
        // the browser's to accept.
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("gzip, deflate, br, zstd"),
        );
        assert_eq!(
            accepted_artifact_encodings(&headers, &DECODING_ARTIFACT_ENCODINGS).unwrap(),
            vec![Packed, BrotliLargeWindow, Zstd, Identity]
        );
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("gzip, br;q=0.5, zstd;q=0, identity;q=1"),
        );
        assert_eq!(
            accepted_artifact_encodings(&headers, &DECODING_ARTIFACT_ENCODINGS).unwrap(),
            vec![Packed, BrotliLargeWindow, Identity]
        );
        assert_eq!(
            accepted_artifact_encodings(&headers, &NATIVE_ARTIFACT_ENCODINGS).unwrap(),
            vec![Identity]
        );
        // Any other list that names either is refused, as any other list is:
        // one brotli encoding without the other, or the two in another order.
        for other in [
            vec![BrotliLargeWindow, Zstd, Identity],
            vec![Packed, Zstd, Identity],
            vec![BrotliLargeWindow, Packed, Zstd, Identity],
            vec![Zstd, Packed, BrotliLargeWindow, Identity],
            vec![Packed, BrotliLargeWindow, Identity],
            vec![Packed, BrotliLargeWindow, Zstd],
            vec![Packed, BrotliLargeWindow],
            vec![Packed],
            vec![Packed, BrotliLargeWindow, Zstd, Identity, Identity],
        ] {
            assert_eq!(
                accepted_artifact_encodings(&headers, &other),
                Err(BootstrapHttpError::InvalidRequest),
                "{other:?}"
            );
        }
    }

    #[tokio::test]
    async fn the_route_takes_the_list_that_names_both_brotli_encodings() {
        for (accepted, status) in [
            (
                serde_json::json!([
                    "packed-brotli-large-window",
                    "brotli-large-window",
                    "zstd",
                    "identity"
                ]),
                StatusCode::OK,
            ),
            (
                serde_json::json!(["brotli-large-window", "zstd", "identity"]),
                StatusCode::BAD_REQUEST,
            ),
            (
                serde_json::json!(["packed-brotli-large-window", "zstd", "identity"]),
                StatusCode::BAD_REQUEST,
            ),
        ] {
            let response = router(Backend)
                .oneshot(request(
                    serde_json::json!({
                        "protocolVersion": 1,
                        "requiredTables": ["table/tasks"],
                        "cachedArtifactHints": [],
                        "acceptedEncodings": accepted,
                        "memoryBudgetBytes": 536870912,
                        "clientHeapReserveBytes": 67108864,
                        "clientInstanceId": "client-1",
                        "deviceId": "device-1"
                    })
                    .to_string(),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{accepted}");
        }
    }

    #[test]
    fn malformed_or_unbounded_profile_claims_fail_closed() {
        let client = [
            RepresentationEncoding::Zstd,
            RepresentationEncoding::Identity,
        ];
        for value in [
            "zstd;q=2, br",
            "zstd;q=0.0000, br",
            "zstd, zstd",
            "zstd;level=3, br",
            "zstd,,br",
            "*;q=0",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                ARTIFACT_PROFILE_HEADER,
                HeaderValue::from_static(CHROMIUM_NATIVE_ARTIFACT_V1),
            );
            headers.insert(
                header::ACCEPT_ENCODING,
                HeaderValue::from_str(value).unwrap(),
            );
            assert_eq!(
                accepted_artifact_encodings(&headers, &client),
                Err(BootstrapHttpError::InvalidRequest),
                "{value}"
            );
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("zstd, br"),
        );
        assert_eq!(
            accepted_artifact_encodings(&headers, &client),
            Err(BootstrapHttpError::InvalidRequest)
        );
        headers.insert(
            ARTIFACT_PROFILE_HEADER,
            HeaderValue::from_static(CHROMIUM_NATIVE_ARTIFACT_V1),
        );
        assert_eq!(
            accepted_artifact_encodings(&headers, &[RepresentationEncoding::Identity],),
            Err(BootstrapHttpError::InvalidRequest)
        );
    }
}

//! Bounded authenticated Axum transport for registered mutation commands.
//!
//! Applications resolve authentication, authorization scope, PostgreSQL
//! routing, and the command registry behind [`MutationHttpBackend`]. This
//! crate owns only HTTP admission, protobuf framing, and sanitized failures.

use std::{future::Future, time::Instant};

use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::Response,
    routing::post,
    Router,
};
use http_headers::has_media_type;
pub use http_headers::BearerToken;
use prost::Message;
use sync_protocol::mutation::{MutationBatchRequest, MutationBatchResponse};
use tracing::{info_span, Instrument};

const MUTATION_PATH: &str = "/v1/mutations";
const MUTATION_CONTENT_TYPE: &str = "application/vnd.calque.mutation.v1+protobuf";
const MAX_REQUEST_BYTES: usize = 5 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

pub trait MutationHttpBackend: Clone + Send + Sync + 'static {
    /// Whether this bearer may reach the write surface at all.
    ///
    /// Called before the body is read and the media type checked, so a caller
    /// that holds no command capability gets the same
    /// [`MutationHttpError::NotFound`] an unmounted route gives. A bearer that
    /// fails verification still gets `Unauthorized`, because the client resets
    /// on 401 but retries on 404; mount state is therefore not a secret. There
    /// is no default, so a new mount cannot inherit a permissive gate.
    fn admit(
        &self,
        bearer: &BearerToken,
    ) -> impl Future<Output = Result<(), MutationHttpError>> + Send;

    fn execute(
        &self,
        bearer: BearerToken,
        request: MutationBatchRequest,
    ) -> impl Future<Output = Result<MutationBatchResponse, MutationHttpError>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationHttpError {
    Unauthorized,
    Forbidden,
    /// The write surface does not exist for this caller. Byte-identical to the
    /// answer an unmounted route gives.
    NotFound,
    /// The body exceeded the transport's memory bound. Reached only after
    /// admission, so it discloses nothing to a caller that holds no capability.
    PayloadTooLarge,
    InvalidRequest,
    Unavailable,
}

pub fn router<B>(backend: B) -> Router
where
    B: MutationHttpBackend,
{
    Router::new()
        .route(
            MUTATION_PATH,
            // Any other method answers 404 rather than axum's 405, which would
            // tell an anonymous caller that the path exists.
            post(post_mutations::<B>)
                .fallback(|| async { error_response(MutationHttpError::NotFound) }),
        )
        .with_state(backend)
}

/// The request body is bounded inside the handler rather than by a
/// `DefaultBodyLimit` layer, whose 413 before admission would tell an
/// unadmitted caller that this route exists.
async fn post_mutations<B>(State(backend): State<B>, headers: HeaderMap, body: Body) -> Response
where
    B: MutationHttpBackend,
{
    let span = info_span!(
        "mutation_http_request",
        client_instance_id = tracing::field::Empty,
        mutation_count = tracing::field::Empty,
        result = tracing::field::Empty,
        request_bytes = tracing::field::Empty,
        latency_micros = tracing::field::Empty,
    );
    async move {
        let started = Instant::now();
        let outcome = async {
            // An anonymous caller holds no command capability, so it gets the
            // 404 an absent route gives. The client sets `authorization` on
            // every mutation request, so no legitimate submission lands here.
            let bearer = BearerToken::from_headers(&headers).ok_or(MutationHttpError::NotFound)?;
            // The body is drained first so a refusal answers instead of
            // resetting the connection, but its outcome is consulted only after
            // admission, so an unadmitted caller gets the same answer whatever
            // it sent.
            let body = to_bytes(body, MAX_REQUEST_BYTES).await;
            backend.admit(&bearer).await?;
            if !has_media_type(&headers, MUTATION_CONTENT_TYPE) {
                return Err(MutationHttpError::InvalidRequest);
            }
            let body = body.map_err(|_| MutationHttpError::PayloadTooLarge)?;
            tracing::Span::current().record("request_bytes", body.len());
            let request = MutationBatchRequest::decode(body)
                .map_err(|_| MutationHttpError::InvalidRequest)?;
            if request.protocol_version != 1
                || request.mutations.is_empty()
                || request.mutations.len() > 100
            {
                return Err(MutationHttpError::InvalidRequest);
            }
            tracing::Span::current()
                .record("client_instance_id", request.client_instance_id.as_str());
            tracing::Span::current().record("mutation_count", request.mutations.len());
            backend.execute(bearer, request).await
        }
        .await;
        tracing::Span::current().record(
            "latency_micros",
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        );
        match outcome {
            Ok(response) => {
                tracing::Span::current().record("result", "ok");
                success_response(response)
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

fn success_response(response: MutationBatchResponse) -> Response {
    if response.encoded_len() > MAX_RESPONSE_BYTES {
        return error_response(MutationHttpError::Unavailable);
    }
    let mut bytes = Vec::with_capacity(response.encoded_len());
    if response.encode(&mut bytes).is_err() {
        return error_response(MutationHttpError::Unavailable);
    }
    let mut result = Response::new(Body::from(bytes));
    *result.status_mut() = StatusCode::OK;
    result.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(MUTATION_CONTENT_TYPE),
    );
    set_common_headers(result.headers_mut());
    result
}

fn error_class(error: MutationHttpError) -> &'static str {
    match error {
        MutationHttpError::Unauthorized => "unauthorized",
        MutationHttpError::Forbidden => "forbidden",
        MutationHttpError::NotFound => "not_found",
        MutationHttpError::PayloadTooLarge => "payload_too_large",
        MutationHttpError::InvalidRequest => "invalid_request",
        MutationHttpError::Unavailable => "unavailable",
    }
}

fn error_response(error: MutationHttpError) -> Response {
    let (status, message) = match error {
        MutationHttpError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
        MutationHttpError::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
        MutationHttpError::NotFound => (StatusCode::NOT_FOUND, "not found"),
        MutationHttpError::PayloadTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "payload too large"),
        MutationHttpError::InvalidRequest => (StatusCode::BAD_REQUEST, "invalid request"),
        MutationHttpError::Unavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "temporarily unavailable")
        }
    };
    let mut response = Response::new(Body::from(message));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    if error == MutationHttpError::Unavailable {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    set_common_headers(response.headers_mut());
    response
}

fn set_common_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, private"),
    );
    headers.insert(
        header::HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use sync_protocol::mutation::{MutationEnvelope, MutationOutcome};
    use tower::ServiceExt;

    use super::*;

    #[derive(Clone)]
    struct Backend;

    impl MutationHttpBackend for Backend {
        async fn admit(&self, bearer: &BearerToken) -> Result<(), MutationHttpError> {
            if bearer.expose() == "uncapable-token" {
                return Err(MutationHttpError::NotFound);
            }
            Ok(())
        }

        async fn execute(
            &self,
            bearer: BearerToken,
            request: MutationBatchRequest,
        ) -> Result<MutationBatchResponse, MutationHttpError> {
            if bearer.expose() != "app-token" {
                return Err(MutationHttpError::Unauthorized);
            }
            Ok(MutationBatchResponse {
                protocol_version: 1,
                outcomes: request
                    .mutations
                    .into_iter()
                    .map(|mutation| MutationOutcome {
                        mutation_id: mutation.mutation_id,
                        code: 1,
                        message: "accepted".to_owned(),
                        server_transaction_id: vec![7; 16],
                        observed_canonical_cursor: None,
                        conflict: None,
                        retry_after_ms: 0,
                    })
                    .collect(),
                server_time_unix_micros: 1,
            })
        }
    }

    fn request() -> MutationBatchRequest {
        MutationBatchRequest {
            protocol_version: 1,
            client_instance_id: "client-1".to_owned(),
            device_id: "device-1".to_owned(),
            authorization_scope_token: b"scope".to_vec(),
            mutations: vec![MutationEnvelope {
                mutation_id: "mutation-1".to_owned(),
                ..Default::default()
            }],
        }
    }

    #[tokio::test]
    async fn admits_bounded_authenticated_protobuf_and_encodes_response() {
        let response = router(Backend)
            .oneshot(
                Request::post(MUTATION_PATH)
                    .header(header::AUTHORIZATION, "Bearer app-token")
                    .header(header::CONTENT_TYPE, MUTATION_CONTENT_TYPE)
                    .body(Body::from(request().encode_to_vec()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store, private"))
        );
        let body = to_bytes(response.into_body(), MAX_RESPONSE_BYTES)
            .await
            .expect("body");
        let decoded = MutationBatchResponse::decode(body).expect("protobuf");
        assert_eq!(decoded.outcomes[0].mutation_id, "mutation-1");
    }

    /// The route reads only the mutation type, which the test above shows
    /// accepting this same body; the generic protobuf types are refused like
    /// any other.
    #[tokio::test]
    async fn rejects_the_wrong_media_type_without_backend_data() {
        for content_type in [
            "application/json",
            "application/protobuf",
            "application/x-protobuf",
        ] {
            let wrong_media = router(Backend)
                .oneshot(
                    Request::post(MUTATION_PATH)
                        .header(header::AUTHORIZATION, "Bearer app-token")
                        .header(header::CONTENT_TYPE, content_type)
                        .body(Body::from(request().encode_to_vec()))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(
                wrong_media.status(),
                StatusCode::BAD_REQUEST,
                "content-type {content_type}"
            );
        }
    }

    /// No usable `Authorization` header, and any method other than POST, both
    /// answer the 404 an absent route answers, not a 401 or axum's 405.
    #[tokio::test]
    async fn an_anonymous_probe_cannot_tell_this_route_from_an_absent_one() {
        for header_value in [None, Some("app-token"), Some("Bearer "), Some("Basic x")] {
            let mut builder =
                Request::post(MUTATION_PATH).header(header::CONTENT_TYPE, MUTATION_CONTENT_TYPE);
            if let Some(value) = header_value {
                builder = builder.header(header::AUTHORIZATION, value);
            }
            let response = router(Backend)
                .oneshot(
                    builder
                        .body(Body::from(request().encode_to_vec()))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "authorization {header_value:?} told an anonymous caller the route is mounted"
            );
        }

        for method in ["GET", "PUT", "DELETE", "PATCH", "HEAD"] {
            let response = router(Backend)
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(MUTATION_PATH)
                        .header(header::AUTHORIZATION, "Bearer app-token")
                        .header(header::CONTENT_TYPE, MUTATION_CONTENT_TYPE)
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "{method} told a caller the route is mounted"
            );
        }
    }

    /// The admission gate runs before the media type, the body limit and the
    /// decoder, so every request an unadmitted caller can shape gets the same
    /// 404 an unmounted route gives.
    #[tokio::test]
    async fn an_unadmitted_bearer_cannot_tell_this_route_from_an_absent_one() {
        for (content_type, body) in [
            (MUTATION_CONTENT_TYPE, request().encode_to_vec()),
            ("application/json", b"{}".to_vec()),
            (MUTATION_CONTENT_TYPE, vec![0xff, 0xff, 0xff]),
            (MUTATION_CONTENT_TYPE, vec![0; MAX_REQUEST_BYTES + 1]),
        ] {
            let response = router(Backend)
                .oneshot(
                    Request::post(MUTATION_PATH)
                        .header(header::AUTHORIZATION, "Bearer uncapable-token")
                        .header(header::CONTENT_TYPE, content_type)
                        .body(Body::from(body))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "content-type {content_type} leaked the mounted route"
            );
        }
    }

    #[tokio::test]
    async fn rejects_malformed_and_oversized_protobuf_before_execution() {
        let malformed = router(Backend)
            .oneshot(
                Request::post(MUTATION_PATH)
                    .header(header::AUTHORIZATION, "Bearer app-token")
                    .header(header::CONTENT_TYPE, MUTATION_CONTENT_TYPE)
                    .body(Body::from(vec![0xff, 0xff, 0xff]))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

        let oversized = router(Backend)
            .oneshot(
                Request::post(MUTATION_PATH)
                    .header(header::AUTHORIZATION, "Bearer app-token")
                    .header(header::CONTENT_TYPE, MUTATION_CONTENT_TYPE)
                    .body(Body::from(vec![0; MAX_REQUEST_BYTES + 1]))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}

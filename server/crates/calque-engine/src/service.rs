use std::{
    collections::BTreeSet,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use axum::{
    body::{Body, Bytes},
    extract::Request,
    http::{header, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::get,
    Router,
};
use http_body::{Body as _, Frame, SizeHint};
use pin_project_lite::pin_project;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    AllowedOrigin, ArtifactService, HttpBinding, LocalBackend, LocalMutationBackend,
    LocalSourceHealth, OperationErrorClass, OperationsMetrics,
};

type DependencyProbe = Arc<dyn Fn() -> Result<(), OperationErrorClass> + Send + Sync>;

const ALLOWED_REQUEST_HEADERS: &[&str] = &[
    "accept-encoding",
    "authorization",
    "content-type",
    "x-calque-artifact-profile",
];

/// Distinct refused values logged per kind. A flood of different values stops
/// at this bound, after one line saying so.
const MAX_LOGGED_REFUSALS: usize = 32;
/// Characters of a refused value that are kept and logged.
const MAX_LOGGED_VALUE_CHARS: usize = 100;

const HOST_REFUSAL_BODY: &str = "Host not served by this calque\n";
const ORIGIN_REFUSAL_BODY: &str = "Origin not allowed\n";

/// The refused values of one kind already logged, bounded in count and size.
#[derive(Default)]
struct RefusedValues {
    seen: Mutex<(BTreeSet<String>, bool)>,
}

enum Refusal {
    First,
    Repeated,
    FirstBeyondTheBound,
}

impl RefusedValues {
    fn note(&self, value: &str) -> Refusal {
        let value: String = value.chars().take(MAX_LOGGED_VALUE_CHARS).collect();
        let (seen, capped) = &mut *self
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if seen.contains(&value) {
            Refusal::Repeated
        } else if seen.len() < MAX_LOGGED_REFUSALS {
            seen.insert(value);
            Refusal::First
        } else if std::mem::replace(capped, true) {
            Refusal::Repeated
        } else {
            Refusal::FirstBeyondTheBound
        }
    }
}

/// How a refused `Host` or `Origin` is received: the value as text, with
/// anything that is not printable escaped.
fn received(value: Option<&HeaderValue>) -> String {
    match value {
        Some(value) => format!(
            "\"{}\"",
            String::from_utf8_lossy(value.as_bytes())
                .chars()
                .take(MAX_LOGGED_VALUE_CHARS)
                .flat_map(char::escape_debug)
                .collect::<String>()
        ),
        None => "missing".to_owned(),
    }
}

#[derive(Clone, Default)]
struct RefusedRequests {
    hosts: Arc<RefusedValues>,
    origins: Arc<RefusedValues>,
}

impl RefusedRequests {
    fn host(&self, request: &Request, binding: &HttpBinding, flag: &str) {
        let value = request.headers().get(header::HOST);
        let received = received(value);
        match self.hosts.note(&received) {
            Refusal::First => tracing::warn!(
                "refused a request whose Host is {received}: this listener answers only Host \"{}\". \
                 Send that Host, or, with calque serve, set {flag} to the Host your proxy forwards",
                binding.expected_host()
            ),
            Refusal::Repeated => {}
            Refusal::FirstBeyondTheBound => tracing::warn!(
                "refused requests with more than {MAX_LOGGED_REFUSALS} different Host values; \
                 further ones are refused without a log line"
            ),
        }
    }

    fn origin(&self, request: &Request, allowed: &BTreeSet<String>) {
        let received = received(request.headers().get(header::ORIGIN));
        match self.origins.note(&received) {
            Refusal::First => tracing::warn!(
                "refused a request from Origin {received}: it is not an allowed origin ({}). \
                 Add the page's origin, as scheme://host[:port] with no path, with --allow-origin",
                allowed
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Refusal::Repeated => {}
            Refusal::FirstBeyondTheBound => tracing::warn!(
                "refused requests with more than {MAX_LOGGED_REFUSALS} different Origin values; \
                 further ones are refused without a log line"
            ),
        }
    }
}

#[derive(Clone)]
struct LocalHttpPolicy {
    binding: HttpBinding,
    origins: Arc<BTreeSet<String>>,
    refused: RefusedRequests,
    health: LocalSourceHealth,
    request_permits: Arc<Semaphore>,
}

#[derive(Clone)]
struct MetricsPolicy {
    binding: HttpBinding,
    refused: RefusedRequests,
}

pin_project! {
    struct RequestPermitBody {
        #[pin]
        inner: Body,
        permit: Option<OwnedSemaphorePermit>,
    }
}

impl http_body::Body for RequestPermitBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        let result = this.inner.as_mut().poll_frame(context);
        let finished = matches!(&result, Poll::Ready(None | Some(Err(_))));
        if finished {
            this.permit.take();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Every application route, the mutation route included, inside one protection
/// layer. `/metrics` is not among them: it is served only by
/// [`metrics_router`], on a listener of its own.
#[allow(clippy::too_many_arguments)]
pub fn local_router(
    backend: LocalBackend,
    mutations: LocalMutationBackend,
    artifacts: ArtifactService,
    health: LocalSourceHealth,
    metrics: OperationsMetrics,
    binding: HttpBinding,
    origins: impl IntoIterator<Item = AllowedOrigin>,
    max_inflight_requests: usize,
) -> Router {
    let application = application_router(backend, artifacts, health.clone(), metrics)
        .merge(mutation_http::router(mutations));
    protect_local_router(application, health, binding, origins, max_inflight_requests)
}

fn application_router(
    backend: LocalBackend,
    artifacts: ArtifactService,
    health: LocalSourceHealth,
    metrics: OperationsMetrics,
) -> Router {
    let dependency_backend = backend.clone();
    let dependency_probe: DependencyProbe =
        Arc::new(move || dependency_backend.probe_operational_dependencies());
    control_router(health, metrics, dependency_probe)
        .merge(bootstrap_http::router(backend.clone()))
        .merge(change_http::router(backend.clone()))
        .merge(crate::revocation::router(backend.clone()))
        .merge(crate::tenants::router(backend))
        .merge(artifacts.router())
}

fn control_router(
    health: LocalSourceHealth,
    metrics: OperationsMetrics,
    dependency_probe: DependencyProbe,
) -> Router {
    Router::new()
        .route(
            "/healthz",
            get(|| async { empty_response(StatusCode::NO_CONTENT) }),
        )
        .route(
            "/readyz",
            get(move || {
                let health = health.clone();
                let metrics = metrics.clone();
                let dependency_probe = Arc::clone(&dependency_probe);
                async move {
                    if !health.is_current() {
                        metrics.record_error(OperationErrorClass::Cdc);
                        return empty_response(StatusCode::SERVICE_UNAVAILABLE);
                    }
                    let result = tokio::task::spawn_blocking(move || dependency_probe()).await;
                    match result {
                        Ok(Ok(())) => empty_response(StatusCode::NO_CONTENT),
                        Ok(Err(class)) => {
                            metrics.record_error(class);
                            empty_response(StatusCode::SERVICE_UNAVAILABLE)
                        }
                        Err(_) => {
                            metrics.record_error(OperationErrorClass::Internal);
                            empty_response(StatusCode::SERVICE_UNAVAILABLE)
                        }
                    }
                }
            }),
        )
}

/// The metrics listener's routes: `/metrics` and nothing else, answering only
/// the `Host` its own binding expects, with 421 for any other. A process
/// that is given no metrics listener serves no scrape at all, because the
/// application listener has no such route.
pub fn metrics_router(metrics: OperationsMetrics, binding: HttpBinding) -> Router {
    Router::new()
        .route(
            "/metrics",
            get(move || {
                let metrics = metrics.clone();
                async move { metrics_response(&metrics) }
            }),
        )
        .layer(middleware::from_fn_with_state(
            MetricsPolicy {
                binding,
                refused: RefusedRequests::default(),
            },
            enforce_metrics_host,
        ))
}

async fn enforce_metrics_host(
    axum::extract::State(policy): axum::extract::State<MetricsPolicy>,
    request: Request,
    next: Next,
) -> Response {
    if !has_expected_host(&request, &policy.binding) {
        policy
            .refused
            .host(&request, &policy.binding, "--metrics-expected-host");
        return text_response(StatusCode::MISDIRECTED_REQUEST, HOST_REFUSAL_BODY);
    }
    next.run(request).await
}

fn has_expected_host(request: &Request, binding: &HttpBinding) -> bool {
    request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| binding.validate_host_header(value).is_ok())
}

fn protect_local_router(
    router: Router,
    health: LocalSourceHealth,
    binding: HttpBinding,
    origins: impl IntoIterator<Item = AllowedOrigin>,
    max_inflight_requests: usize,
) -> Router {
    let policy = LocalHttpPolicy {
        binding,
        origins: Arc::new(
            origins
                .into_iter()
                .map(|origin| origin.as_str().to_owned())
                .collect(),
        ),
        refused: RefusedRequests::default(),
        health,
        request_permits: Arc::new(Semaphore::new(max_inflight_requests)),
    };
    router.layer(middleware::from_fn_with_state(
        policy,
        enforce_local_request,
    ))
}

async fn enforce_local_request(
    axum::extract::State(policy): axum::extract::State<LocalHttpPolicy>,
    request: Request,
    next: Next,
) -> Response {
    if !has_expected_host(&request, &policy.binding) {
        policy
            .refused
            .host(&request, &policy.binding, "--expected-host");
        return text_response(StatusCode::MISDIRECTED_REQUEST, HOST_REFUSAL_BODY);
    }
    let origin = match request.headers().get(header::ORIGIN) {
        Some(value) => match value.to_str() {
            Ok(value) if policy.origins.contains(value) => Some(value.to_owned()),
            _ => {
                policy.refused.origin(&request, &policy.origins);
                return text_response(StatusCode::FORBIDDEN, ORIGIN_REFUSAL_BODY);
            }
        },
        None => None,
    };

    let Ok(request_permit) = policy.request_permits.clone().try_acquire_owned() else {
        let response = overloaded_response();
        return match origin {
            Some(origin) => cors_response(response, &origin, false),
            None => response,
        };
    };

    let preflight = request.method() == Method::OPTIONS;
    let response = if preflight {
        let Some(origin) = origin.as_deref() else {
            return hold_request_permit(empty_response(StatusCode::FORBIDDEN), request_permit);
        };
        if !valid_preflight(&request) {
            empty_response(StatusCode::FORBIDDEN)
        } else {
            cors_response(empty_response(StatusCode::NO_CONTENT), origin, true)
        }
    } else if request.uri().path().starts_with("/v1/") && !policy.health.is_connected() {
        empty_response(StatusCode::SERVICE_UNAVAILABLE)
    } else {
        next.run(request).await
    };
    let response = match (origin, preflight) {
        (Some(_), true) => response,
        (Some(origin), false) => cors_response(response, &origin, false),
        (None, _) => response,
    };
    hold_request_permit(response, request_permit)
}

/// Holds `permit` until the body ends. An open change stream holds one of the
/// backend's open streams instead, so its request permit ends as it opens.
fn hold_request_permit(response: Response, permit: OwnedSemaphorePermit) -> Response {
    if response.body().is_end_stream()
        || response
            .extensions()
            .get::<change_http::OpenChangeStream>()
            .is_some()
    {
        drop(permit);
        return response;
    }
    let (parts, inner) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(RequestPermitBody {
            inner,
            permit: Some(permit),
        }),
    )
}

fn valid_preflight(request: &Request) -> bool {
    let method = request
        .headers()
        .get(header::ACCESS_CONTROL_REQUEST_METHOD)
        .and_then(|value| value.to_str().ok());
    if !matches!(method, Some("GET" | "POST")) {
        return false;
    }
    request
        .headers()
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|headers| {
            headers.split(',').all(|name| {
                let name = name.trim();
                !name.is_empty()
                    && ALLOWED_REQUEST_HEADERS
                        .iter()
                        .any(|allowed| name.eq_ignore_ascii_case(allowed))
            })
        })
}

fn cors_response(mut response: Response, origin: &str, preflight: bool) -> Response {
    let Ok(origin) = HeaderValue::from_str(origin) else {
        return empty_response(StatusCode::FORBIDDEN);
    };
    let headers = response.headers_mut();
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    headers.insert(header::VARY, HeaderValue::from_static("Origin"));
    headers.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("Content-Encoding, Content-Length, Content-Type, Retry-After"),
    );
    if preflight {
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST"),
        );
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static(
                "Accept-Encoding, Authorization, Content-Type, X-Calque-Artifact-Profile",
            ),
        );
        // Chrome's cap, two hours. A cached preflight admits nothing new:
        // enforce_local_request checks every request's Origin against the
        // allow list, preflighted or not, and a header outside the cached
        // list preflights again.
        headers.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            HeaderValue::from_static("7200"),
        );
    }
    response
}

fn text_response(status: StatusCode, body: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .header("x-content-type-options", "nosniff")
        .header("referrer-policy", "no-referrer")
        .body(Body::from(body))
        .expect("static local response")
}

fn empty_response(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .header(header::CACHE_CONTROL, "no-store")
        .header("x-content-type-options", "nosniff")
        .header("referrer-policy", "no-referrer")
        .body(Body::empty())
        .expect("static local response")
}

fn overloaded_response() -> Response {
    let mut response = empty_response(StatusCode::SERVICE_UNAVAILABLE);
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

fn metrics_response(metrics: &OperationsMetrics) -> Response {
    match metrics.encode() {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )
            .header(header::CACHE_CONTROL, "no-store")
            .header("x-content-type-options", "nosniff")
            .header("referrer-policy", "no-referrer")
            .body(Body::from(body))
            .expect("static metrics response"),
        Err(_) => empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_MAX_INFLIGHT_REQUESTS;
    use std::{convert::Infallible, time::Duration};

    use axum::body::to_bytes;
    use tower::ServiceExt;

    struct PendingBody;

    struct FinalFrameThenPending {
        emitted: bool,
    }

    impl http_body::Body for PendingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }
    }

    impl http_body::Body for FinalFrameThenPending {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if self.emitted {
                Poll::Pending
            } else {
                self.emitted = true;
                Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"last-frame")))))
            }
        }

        fn is_end_stream(&self) -> bool {
            self.emitted
        }
    }

    fn router(healthy: bool) -> Router {
        router_with_probe(healthy, Arc::new(|| Ok(())))
    }

    fn router_with_probe(healthy: bool, dependency_probe: DependencyProbe) -> Router {
        let metrics = OperationsMetrics::new();
        protect_local_router(
            control_router(
                LocalSourceHealth::for_test(healthy),
                metrics,
                dependency_probe,
            ),
            LocalSourceHealth::for_test(healthy),
            HttpBinding::loopback(8787).unwrap(),
            [AllowedOrigin::parse("http://127.0.0.1:5173").unwrap()],
            DEFAULT_MAX_INFLIGHT_REQUESTS,
        )
    }

    fn request(path: &str) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .uri(path)
            .header(header::HOST, "127.0.0.1:8787")
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn readiness_fails_closed_when_cdc_is_unhealthy() {
        let response = router(false).oneshot(request("/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = router(true).oneshot(request("/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = router(false)
            .oneshot({
                let mut request = request("/v1/bootstrap");
                request.headers_mut().insert(
                    header::ORIGIN,
                    HeaderValue::from_static("http://127.0.0.1:5173"),
                );
                request
            })
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("http://127.0.0.1:5173"))
        );
    }

    /// A source whose last CDC cycle did not catch up is behind: not ready,
    /// but its application requests still reach the routes, which serve the
    /// canonical log at its own cursor.
    #[tokio::test]
    async fn a_source_that_is_behind_is_unready_but_still_served() {
        let router = || {
            protect_local_router(
                control_router(
                    LocalSourceHealth::behind_for_test(),
                    OperationsMetrics::new(),
                    Arc::new(|| Ok(())),
                ),
                LocalSourceHealth::behind_for_test(),
                HttpBinding::loopback(8787).unwrap(),
                [AllowedOrigin::parse("http://127.0.0.1:5173").unwrap()],
                DEFAULT_MAX_INFLIGHT_REQUESTS,
            )
        };
        let response = router().oneshot(request("/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        // The control router has no `/v1/` routes, so a request the gate lets
        // through is answered 404 by the router behind it.
        let response = router().oneshot(request("/v1/bootstrap")).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn readiness_fails_closed_when_a_durable_dependency_is_unavailable() {
        let response =
            router_with_probe(true, Arc::new(|| Err(OperationErrorClass::ArtifactCatalog)))
                .oneshot(request("/readyz"))
                .await
                .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// The application listener has no scrape: `/metrics` is answered like
    /// any path it does not route.
    #[tokio::test]
    async fn the_application_router_serves_no_metrics() {
        let response = router(true).oneshot(request("/metrics")).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    fn metrics_listener() -> Router {
        metrics_router(
            OperationsMetrics::new(),
            HttpBinding::loopback(8788).unwrap(),
        )
    }

    fn metrics_request(path: &str, host: &'static str) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .uri(path)
            .header(header::HOST, host)
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn metrics_uses_openmetrics_content_type_and_eof() {
        let response = metrics_listener()
            .oneshot(metrics_request("/metrics", "127.0.0.1:8788"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static(
                "application/openmetrics-text; version=1.0.0; charset=utf-8"
            ))
        );
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(body.ends_with(b"# EOF\n"));
    }

    /// The metrics listener answers its own binding's `Host` and no other,
    /// the application listener's included, and routes nothing but the
    /// scrape.
    #[tokio::test]
    async fn the_metrics_listener_answers_only_its_own_host_and_only_the_scrape() {
        for host in ["127.0.0.1:8787", "localhost:8788", "127.0.0.1"] {
            let response = metrics_listener()
                .oneshot(metrics_request("/metrics", host))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::MISDIRECTED_REQUEST, "{host}");
        }
        let no_host = axum::http::Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            metrics_listener().oneshot(no_host).await.unwrap().status(),
            StatusCode::MISDIRECTED_REQUEST
        );
        for path in ["/healthz", "/readyz", "/v1/bootstrap"] {
            let response = metrics_listener()
                .oneshot(metrics_request(path, "127.0.0.1:8788"))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    async fn host_and_origin_are_exactly_allowlisted() {
        let wrong_host = axum::http::Request::builder()
            .uri("/healthz")
            .header(header::HOST, "localhost:8787")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router(true).oneshot(wrong_host).await.unwrap().status(),
            StatusCode::MISDIRECTED_REQUEST
        );

        let mut wrong_origin = request("/healthz");
        wrong_origin.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("http://localhost:5173"),
        );
        assert_eq!(
            router(true).oneshot(wrong_origin).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = Captured;

        fn make_writer(&'a self) -> Captured {
            self.clone()
        }
    }

    impl Captured {
        fn lines(&self) -> Vec<String> {
            String::from_utf8(self.0.lock().unwrap().clone())
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect()
        }
    }

    /// Logs on this thread, which a current-thread test runtime shares with
    /// the middleware it drives, until the guard drops.
    fn capture_logs() -> (Captured, tracing::subscriber::DefaultGuard) {
        let captured = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_ansi(false)
            .finish();
        (captured, tracing::subscriber::set_default(subscriber))
    }

    async fn body_text(response: Response) -> String {
        String::from_utf8(to_bytes(response.into_body(), 4096).await.unwrap().to_vec()).unwrap()
    }

    fn from_host(host: &str) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .uri("/healthz")
            .header(header::HOST, host)
            .body(Body::empty())
            .unwrap()
    }

    fn from_origin(origin: &str) -> axum::http::Request<Body> {
        let mut request = request("/healthz");
        request
            .headers_mut()
            .insert(header::ORIGIN, HeaderValue::from_str(origin).unwrap());
        request
    }

    #[tokio::test]
    async fn a_refused_host_or_origin_gets_a_short_text_body_that_hides_the_allow_list() {
        let (_logs, _guard) = capture_logs();
        let host = router(true)
            .oneshot(from_host("evil.example"))
            .await
            .unwrap();
        assert_eq!(host.status(), StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(
            host.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("text/plain; charset=utf-8"))
        );
        assert_eq!(body_text(host).await, "Host not served by this calque\n");

        let origin = router(true)
            .oneshot(from_origin("https://evil.example"))
            .await
            .unwrap();
        assert_eq!(origin.status(), StatusCode::FORBIDDEN);
        assert_eq!(body_text(origin).await, "Origin not allowed\n");

        let metrics = metrics_listener()
            .oneshot(metrics_request("/metrics", "evil.example"))
            .await
            .unwrap();
        assert_eq!(metrics.status(), StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(body_text(metrics).await, "Host not served by this calque\n");
    }

    /// The warning names what was received, what is expected and the flag
    /// that sets it, once per distinct value.
    #[tokio::test]
    async fn a_refused_value_is_logged_once_with_what_is_expected_and_its_flag() {
        let (logs, _guard) = capture_logs();
        let router = router(true);
        for _ in 0..3 {
            router
                .clone()
                .oneshot(from_host("evil.example"))
                .await
                .unwrap();
            router
                .clone()
                .oneshot(from_origin("https://evil.example"))
                .await
                .unwrap();
        }
        router
            .clone()
            .oneshot(from_host("other.example"))
            .await
            .unwrap();
        let lines = logs.lines();
        assert_eq!(lines.len(), 3, "{lines:#?}");
        assert!(
            lines[0].contains("Host is \"evil.example\"")
                && lines[0].contains("answers only Host \"127.0.0.1:8787\"")
                && lines[0].contains("--expected-host"),
            "{}",
            lines[0]
        );
        assert!(
            lines[1].contains("Origin \"https://evil.example\"")
                && lines[1].contains("http://127.0.0.1:5173")
                && lines[1].contains("--allow-origin"),
            "{}",
            lines[1]
        );
        assert!(
            lines[2].contains("Host is \"other.example\""),
            "{}",
            lines[2]
        );

        let (logs, _guard) = capture_logs();
        metrics_listener()
            .oneshot(metrics_request("/metrics", "evil.example"))
            .await
            .unwrap();
        let lines = logs.lines();
        assert_eq!(lines.len(), 1, "{lines:#?}");
        assert!(
            lines[0].contains("answers only Host \"127.0.0.1:8788\"")
                && lines[0].contains("--metrics-expected-host"),
            "{}",
            lines[0]
        );
    }

    /// A flood of different values grows neither memory nor the log without
    /// bound: past the bound one line says so, and nothing more is written.
    #[tokio::test]
    async fn a_flood_of_different_refused_values_is_logged_to_a_bound() {
        let (logs, _guard) = capture_logs();
        let router = router(true);
        let long = "h".repeat(10_000);
        for index in 0..(MAX_LOGGED_REFUSALS * 3) {
            router
                .clone()
                .oneshot(from_host(&format!("flood-{index}.example")))
                .await
                .unwrap();
        }
        router.clone().oneshot(from_host(&long)).await.unwrap();
        let lines = logs.lines();
        assert_eq!(lines.len(), MAX_LOGGED_REFUSALS + 1, "{}", lines.len());
        assert!(lines.last().unwrap().contains("without a log line"));
        assert!(lines.iter().all(|line| line.len() < 600));

        let values = RefusedValues::default();
        for index in 0..1_000 {
            values.note(&format!("{index}{long}"));
        }
        let (seen, capped) = &*values.seen.lock().unwrap();
        assert_eq!(seen.len(), MAX_LOGGED_REFUSALS);
        assert!(*capped);
        assert!(seen
            .iter()
            .all(|value| value.chars().count() <= MAX_LOGGED_VALUE_CHARS));
    }

    #[tokio::test]
    async fn a_refused_origin_that_is_not_utf8_is_logged_lossily_on_one_line() {
        let (logs, _guard) = capture_logs();
        let mut refused = request("/healthz");
        refused.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_bytes(b"https://a\xff.example").unwrap(),
        );
        router(true).oneshot(refused).await.unwrap();
        let lines = logs.lines();
        assert_eq!(lines.len(), 1, "{lines:#?}");
        assert!(lines[0].contains("Origin \"https://a"), "{}", lines[0]);
    }

    /// The shape `docker-compose.selfhost.yml` puts behind Caddy: a wildcard
    /// bind answering only the public host the proxy forwards, called from the
    /// application's HTTPS origin.
    #[tokio::test]
    async fn a_proxied_listener_answers_the_public_host_and_an_https_origin() {
        let router = protect_local_router(
            control_router(
                LocalSourceHealth::for_test(true),
                OperationsMetrics::new(),
                Arc::new(|| Ok(())),
            ),
            LocalSourceHealth::for_test(true),
            HttpBinding::configured(
                std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                8787,
                Some("sync.example.com"),
            )
            .unwrap(),
            [AllowedOrigin::parse_https_or_loopback("https://app.example.com").unwrap()],
            DEFAULT_MAX_INFLIGHT_REQUESTS,
        );
        let proxied = |host: &'static str, origin: &'static str| {
            axum::http::Request::builder()
                .uri("/readyz")
                .header(header::HOST, host)
                .header(header::ORIGIN, origin)
                .body(Body::empty())
                .unwrap()
        };

        let response = router
            .clone()
            .oneshot(proxied("sync.example.com", "https://app.example.com"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("https://app.example.com"))
        );
        for (host, origin, status) in [
            // What the proxy would forward had it kept the browser's port.
            (
                "sync.example.com:443",
                "https://app.example.com",
                StatusCode::MISDIRECTED_REQUEST,
            ),
            (
                "calque:8787",
                "https://app.example.com",
                StatusCode::MISDIRECTED_REQUEST,
            ),
            (
                "sync.example.com",
                "http://app.example.com",
                StatusCode::FORBIDDEN,
            ),
            (
                "sync.example.com",
                "https://evil.example.com",
                StatusCode::FORBIDDEN,
            ),
        ] {
            assert_eq!(
                router
                    .clone()
                    .oneshot(proxied(host, origin))
                    .await
                    .unwrap()
                    .status(),
                status,
                "{host} {origin}"
            );
        }
    }

    #[tokio::test]
    async fn preflight_is_bounded_and_echoes_only_the_allowed_origin() {
        let mut allowed = request("/v1/bootstrap");
        *allowed.method_mut() = Method::OPTIONS;
        allowed.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:5173"),
        );
        allowed.headers_mut().insert(
            header::ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_static("POST"),
        );
        allowed.headers_mut().insert(
            header::ACCESS_CONTROL_REQUEST_HEADERS,
            HeaderValue::from_static("authorization, content-type"),
        );
        let response = router(true).oneshot(allowed).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("http://127.0.0.1:5173"))
        );

        let mut forbidden = request("/v1/bootstrap");
        *forbidden.method_mut() = Method::OPTIONS;
        forbidden.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:5173"),
        );
        forbidden.headers_mut().insert(
            header::ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_static("POST"),
        );
        forbidden.headers_mut().insert(
            header::ACCESS_CONTROL_REQUEST_HEADERS,
            HeaderValue::from_static("x-unbounded"),
        );
        assert_eq!(
            router(true).oneshot(forbidden).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn only_an_allowed_preflight_may_be_cached_and_for_chromes_two_hours() {
        let preflight = |origin: &'static str, headers: &'static str| {
            let mut request = request("/v1/changes");
            *request.method_mut() = Method::OPTIONS;
            request
                .headers_mut()
                .insert(header::ORIGIN, HeaderValue::from_static(origin));
            request.headers_mut().insert(
                header::ACCESS_CONTROL_REQUEST_METHOD,
                HeaderValue::from_static("POST"),
            );
            request.headers_mut().insert(
                header::ACCESS_CONTROL_REQUEST_HEADERS,
                HeaderValue::from_static(headers),
            );
            request
        };

        let allowed = router(true)
            .oneshot(preflight("http://127.0.0.1:5173", "content-type"))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            allowed.headers().get(header::ACCESS_CONTROL_MAX_AGE),
            Some(&HeaderValue::from_static("7200"))
        );

        for (origin, headers) in [
            ("http://127.0.0.1:5173", "x-unbounded"),
            ("http://127.0.0.1:5174", "content-type"),
        ] {
            let refused = router(true)
                .oneshot(preflight(origin, headers))
                .await
                .unwrap();
            assert_eq!(
                refused.status(),
                StatusCode::FORBIDDEN,
                "{origin} {headers}"
            );
            assert_eq!(refused.headers().get(header::ACCESS_CONTROL_MAX_AGE), None);
        }

        let mut simple = request("/readyz");
        simple.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:5173"),
        );
        let simple = router(true).oneshot(simple).await.unwrap();
        assert_eq!(simple.status(), StatusCode::NO_CONTENT);
        assert_eq!(simple.headers().get(header::ACCESS_CONTROL_MAX_AGE), None);
    }

    #[tokio::test]
    async fn global_request_limit_follows_streaming_response_until_drop() {
        // Not the default, so a router that ignored its bound would fail.
        let bound = 3;
        let application = Router::new().route(
            "/hold",
            get(|| async {
                Response::builder()
                    .status(StatusCode::OK)
                    .header("x-held-response", "yes")
                    .body(Body::new(PendingBody))
                    .unwrap()
            }),
        );
        let router = protect_local_router(
            application,
            LocalSourceHealth::for_test(true),
            HttpBinding::loopback(8787).unwrap(),
            [AllowedOrigin::parse("http://127.0.0.1:5173").unwrap()],
            bound,
        );

        let mut responses = Vec::with_capacity(bound);
        for _ in 0..bound {
            let response = tokio::time::timeout(
                Duration::from_secs(1),
                router.clone().oneshot(request("/hold")),
            )
            .await
            .expect("admitted request should return its response promptly")
            .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get("x-held-response"),
                Some(&HeaderValue::from_static("yes"))
            );
            responses.push(response);
        }

        let mut excess = request("/hold");
        excess.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:5173"),
        );
        let response = tokio::time::timeout(Duration::from_secs(1), router.clone().oneshot(excess))
            .await
            .expect("overload must fail without waiting")
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER),
            Some(&HeaderValue::from_static("1"))
        );
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("http://127.0.0.1:5173"))
        );

        let mut preflight = request("/v1/bootstrap");
        *preflight.method_mut() = Method::OPTIONS;
        preflight.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:5173"),
        );
        preflight.headers_mut().insert(
            header::ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_static("POST"),
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), router.clone().oneshot(preflight))
                .await
                .expect("accepted preflight must share the immediate global limit")
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        drop(responses.pop());
        let replacement = tokio::time::timeout(
            Duration::from_secs(1),
            router.clone().oneshot(request("/hold")),
        )
        .await
        .expect("dropping a response body must release global capacity")
        .unwrap();
        assert_eq!(replacement.status(), StatusCode::OK);
        responses.push(replacement);
    }

    #[tokio::test]
    async fn global_request_limit_releases_at_eof_and_preserves_response() {
        let application = Router::new().route(
            "/finite",
            get(|| async {
                Response::builder()
                    .status(StatusCode::CREATED)
                    .header("x-finite-response", "preserved")
                    .body(Body::from("exact-body"))
                    .unwrap()
            }),
        );
        let router = protect_local_router(
            application,
            LocalSourceHealth::for_test(true),
            HttpBinding::loopback(8787).unwrap(),
            [AllowedOrigin::parse("http://127.0.0.1:5173").unwrap()],
            DEFAULT_MAX_INFLIGHT_REQUESTS,
        );

        let mut responses = Vec::with_capacity(DEFAULT_MAX_INFLIGHT_REQUESTS);
        for _ in 0..DEFAULT_MAX_INFLIGHT_REQUESTS {
            let response = router.clone().oneshot(request("/finite")).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
            responses.push(response);
        }
        assert_eq!(
            router
                .clone()
                .oneshot(request("/finite"))
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        let response = responses.pop().unwrap();
        assert_eq!(
            response.headers().get("x-finite-response"),
            Some(&HeaderValue::from_static("preserved"))
        );
        let body = to_bytes(response.into_body(), 64)
            .await
            .expect("finite response body should remain readable");
        assert_eq!(body, Bytes::from_static(b"exact-body"));

        assert_eq!(
            router.oneshot(request("/finite")).await.unwrap().status(),
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn final_ok_frame_does_not_release_capacity_before_eof_or_drop() {
        let application = Router::new().route(
            "/final-frame",
            get(|| async {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::new(FinalFrameThenPending { emitted: false }))
                    .unwrap()
            }),
        );
        let router = protect_local_router(
            application,
            LocalSourceHealth::for_test(true),
            HttpBinding::loopback(8787).unwrap(),
            [AllowedOrigin::parse("http://127.0.0.1:5173").unwrap()],
            DEFAULT_MAX_INFLIGHT_REQUESTS,
        );
        let mut responses = Vec::with_capacity(DEFAULT_MAX_INFLIGHT_REQUESTS);
        for _ in 0..DEFAULT_MAX_INFLIGHT_REQUESTS {
            let mut response = router
                .clone()
                .oneshot(request("/final-frame"))
                .await
                .unwrap();
            let frame =
                std::future::poll_fn(|context| Pin::new(response.body_mut()).poll_frame(context))
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(
                frame.into_data().unwrap(),
                Bytes::from_static(b"last-frame")
            );
            responses.push(response);
        }

        assert_eq!(
            router
                .clone()
                .oneshot(request("/final-frame"))
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        drop(responses.pop());
        assert_eq!(
            router
                .oneshot(request("/final-frame"))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
}

//! The request headers every Calque router reads the same way: the
//! `Authorization: Bearer` credential and the `Content-Type` media type.

use std::fmt;

use http::{header, HeaderMap};

/// The longest `Authorization` header value a router reads, in bytes.
pub const MAX_AUTHORIZATION_HEADER_BYTES: usize = 8 * 1024;

/// A bearer credential read from a request. Its `Debug` output never contains
/// the credential.
#[derive(Clone)]
pub struct BearerToken(String);

impl BearerToken {
    /// Reads the credential from `Authorization: Bearer <token>`.
    ///
    /// Returns `None` when the header is absent, longer than
    /// [`MAX_AUTHORIZATION_HEADER_BYTES`], or names another scheme, and when
    /// the token is empty or holds anything other than visible ASCII.
    pub fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let value = headers.get(header::AUTHORIZATION)?;
        if value.len() > MAX_AUTHORIZATION_HEADER_BYTES {
            return None;
        }
        let token = value.to_str().ok()?.strip_prefix("Bearer ")?;
        (!token.is_empty() && token.bytes().all(|byte| byte.is_ascii_graphic()))
            .then(|| Self(token.to_owned()))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BearerToken([REDACTED])")
    }
}

/// Whether the request's `Content-Type` is `media_type`, ignoring any
/// parameters after `;`.
pub fn has_media_type(headers: &HeaderMap, media_type: &str) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim() == media_type)
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::*;

    fn with(name: header::HeaderName, value: HeaderValue) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(name, value);
        headers
    }

    fn bearer(value: &[u8]) -> Option<String> {
        BearerToken::from_headers(&with(
            header::AUTHORIZATION,
            HeaderValue::from_bytes(value).unwrap(),
        ))
        .map(|token| token.expose().to_owned())
    }

    #[test]
    fn a_bearer_is_a_non_empty_run_of_visible_ascii_after_the_scheme() {
        assert_eq!(bearer(b"Bearer app-token").as_deref(), Some("app-token"));
        assert_eq!(
            BearerToken::from_headers(&HeaderMap::new()).map(|_| ()),
            None
        );
        for refused in [
            &b"Basic app-token"[..],
            b"bearer app-token",
            b"Bearerapp-token",
            b"Bearer ",
            b"Bearer  app-token",
            b"Bearer app token",
            b"Bearer app\ttoken",
            b"Bearer app-t\xc3\xb6ken",
        ] {
            assert_eq!(
                bearer(refused),
                None,
                "{}",
                String::from_utf8_lossy(refused)
            );
        }
    }

    #[test]
    fn a_bearer_header_longer_than_the_bound_is_refused() {
        let at_bound = format!(
            "Bearer {}",
            "a".repeat(MAX_AUTHORIZATION_HEADER_BYTES - "Bearer ".len())
        );
        assert!(bearer(at_bound.as_bytes()).is_some());
        assert_eq!(bearer(format!("{at_bound}a").as_bytes()), None);
    }

    #[test]
    fn a_bearer_debug_never_shows_the_credential() {
        let token = BearerToken::from_headers(&with(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer app-token"),
        ))
        .unwrap();
        assert_eq!(format!("{token:?}"), "BearerToken([REDACTED])");
    }

    #[test]
    fn a_media_type_matches_with_or_without_parameters() {
        let content_type = |value| with(header::CONTENT_TYPE, HeaderValue::from_static(value));
        assert!(has_media_type(
            &content_type("application/json"),
            "application/json"
        ));
        assert!(has_media_type(
            &content_type("application/json; charset=utf-8"),
            "application/json"
        ));
        assert!(!has_media_type(
            &content_type("text/plain"),
            "application/json"
        ));
        assert!(!has_media_type(
            &content_type("application/jsonx"),
            "application/json"
        ));
        assert!(!has_media_type(&HeaderMap::new(), "application/json"));
    }
}

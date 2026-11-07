//! Verification of the application backend's signed tenant order.
//!
//! A tenant order asks the deployment to warm one tenant now, to pin, unpin
//! or evict it, or to report its state. Only the application's backend may
//! ask, so the order is signed with the backend's grant key under a domain
//! separator of its own: a browser's dataset grant never verifies as an
//! order, nor a fence order as this one. Framing, size bounds and
//! verification order are the grant's. Each order carries an id of its own,
//! which the deployment acts on once.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use authz::PrincipalValue;

use crate::{GrantAttributeValue, GrantVerifier, MAX_PAYLOAD_BYTES, MAX_TOKEN_BYTES};

/// Domain separator prepended to a tenant-order payload before signing.
pub const TENANT_ORDER_SIGNING_DOMAIN: &[u8] = b"calque.tenant-order.v1\0";

/// The only tenant-order payload version this verifier accepts.
pub const TENANT_ORDER_VERSION: u32 = 1;

/// How long one order may be presented for.
pub const MAX_TENANT_ORDER_LIFETIME_SECONDS: u64 = 5 * 60;

/// The bounds on an order id's length, in base64url characters.
pub const TENANT_ORDER_ID_LENGTH: std::ops::RangeInclusive<usize> = 16..=64;

/// What a tenant order asks of the deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TenantAction {
    /// Prepare the tenant now if it has no current artifact.
    Warm,
    /// Keep the tenant warm whatever its recency.
    Pin,
    /// Let recency decide again whether the tenant stays warm.
    Unpin,
    /// Report the tenant's state and change nothing.
    State,
    /// Drop the tenant's artifact, pin and recency now, so its next open is
    /// cold. A build in progress is dropped with them.
    Evict,
}

/// The signed payload of a tenant order. An order that names anything beyond
/// its fields is refused rather than half-read.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TenantOrderClaims {
    pub version: u32,
    pub key_id: String,
    pub audience: String,
    /// Unique to this order: the deployment acts on an id once.
    pub order_id: String,
    pub action: TenantAction,
    /// The tenant, typed as a grant's partition attribute is.
    pub tenant: GrantAttributeValue,
    pub issued_at_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
}

/// An authenticated order for one tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedTenantOrder {
    pub order_id: String,
    pub action: TenantAction,
    pub tenant: PrincipalValue,
    pub expires_at_unix_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TenantOrderError {
    #[error("tenant order exceeds its size bound")]
    TokenTooLarge,
    #[error("tenant order is not exactly one payload and one signature")]
    InvalidFraming,
    #[error("tenant order is not valid unpadded base64url")]
    InvalidEncoding,
    #[error("tenant order payload is not a well-formed order")]
    MalformedPayload,
    #[error("tenant order version {0} is not supported")]
    UnsupportedVersion(u32),
    #[error("tenant order names no key this deployment trusts")]
    UnknownKeyId,
    #[error("tenant order signature verification failed")]
    InvalidSignature,
    #[error("tenant order was issued for a different audience")]
    AudienceMismatch,
    #[error("tenant order names a tenant value that does not parse as its type")]
    MalformedTenant,
    #[error("tenant order id is not 16 to 64 base64url characters")]
    MalformedOrderId,
    #[error("tenant order expiry does not follow its issue time, or exceeds the maximum window")]
    InvalidValidityWindow,
    #[error("tenant order is not yet valid")]
    NotYetValid,
    #[error("tenant order has expired")]
    Expired,
}

/// Builds the exact byte string a tenant-order signature covers.
pub fn tenant_order_signing_input(payload: &[u8]) -> Vec<u8> {
    let mut signed = Vec::with_capacity(TENANT_ORDER_SIGNING_DOMAIN.len() + payload.len());
    signed.extend_from_slice(TENANT_ORDER_SIGNING_DOMAIN);
    signed.extend_from_slice(payload);
    signed
}

impl GrantVerifier {
    /// Verifies one presented tenant order against this deployment's grant
    /// keys and audience.
    pub fn verify_tenant_order(
        &self,
        token: &str,
        now_unix_seconds: u64,
    ) -> Result<VerifiedTenantOrder, TenantOrderError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(TenantOrderError::TokenTooLarge);
        }
        let (encoded_payload, encoded_signature) = token
            .split_once('.')
            .ok_or(TenantOrderError::InvalidFraming)?;
        if encoded_signature.contains('.') {
            return Err(TenantOrderError::InvalidFraming);
        }
        let payload = URL_SAFE_NO_PAD
            .decode(encoded_payload)
            .map_err(|_| TenantOrderError::InvalidEncoding)?;
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(TenantOrderError::TokenTooLarge);
        }
        let signature = URL_SAFE_NO_PAD
            .decode(encoded_signature)
            .map_err(|_| TenantOrderError::InvalidEncoding)?;
        let claims: TenantOrderClaims =
            serde_json::from_slice(&payload).map_err(|_| TenantOrderError::MalformedPayload)?;
        if claims.version != TENANT_ORDER_VERSION {
            return Err(TenantOrderError::UnsupportedVersion(claims.version));
        }
        let key = self
            .keys
            .get(&claims.key_id)
            .ok_or(TenantOrderError::UnknownKeyId)?;
        key.verify(&tenant_order_signing_input(&payload), &signature)
            .map_err(|_| TenantOrderError::InvalidSignature)?;

        // Everything below here is authenticated.
        if claims.audience != self.audience {
            return Err(TenantOrderError::AudienceMismatch);
        }
        if claims.expires_at_unix_seconds <= claims.issued_at_unix_seconds
            || claims.expires_at_unix_seconds - claims.issued_at_unix_seconds
                > MAX_TENANT_ORDER_LIFETIME_SECONDS
        {
            return Err(TenantOrderError::InvalidValidityWindow);
        }
        if now_unix_seconds < claims.issued_at_unix_seconds {
            return Err(TenantOrderError::NotYetValid);
        }
        if now_unix_seconds >= claims.expires_at_unix_seconds {
            return Err(TenantOrderError::Expired);
        }
        if !TENANT_ORDER_ID_LENGTH.contains(&claims.order_id.len())
            || !claims
                .order_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(TenantOrderError::MalformedOrderId);
        }
        let tenant = claims
            .tenant
            .into_principal_value()
            .map_err(|_| TenantOrderError::MalformedTenant)?;
        Ok(VerifiedTenantOrder {
            order_id: claims.order_id,
            action: claims.action,
            tenant,
            expires_at_unix_seconds: claims.expires_at_unix_seconds,
        })
    }
}

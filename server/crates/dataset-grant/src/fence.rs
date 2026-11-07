//! Verification of the application's signed authorization-fence order.
//!
//! A fence order says "generation `revokedGeneration` is retired and
//! `effectiveGeneration` is now in force". Only the application backend may
//! say it, so it is signed with the backend's grant key under its own domain
//! separator: a fence-order signature must never verify as a grant, nor a
//! grant's as an order. Framing, size bounds and verification order are the
//! grant's.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{GrantVerifier, ED25519_PUBLIC_KEY_BYTES, MAX_PAYLOAD_BYTES, MAX_TOKEN_BYTES};

/// Domain separator prepended to a fence-order payload before signing.
pub const FENCE_ORDER_SIGNING_DOMAIN: &[u8] = b"calque.authorization-fence.v1\0";

/// The only fence-order payload version this verifier accepts.
pub const FENCE_ORDER_VERSION: u32 = 1;

/// How long one order may be presented for. This bounds how long a captured
/// order stays presentable; the fence itself is durable.
pub const MAX_FENCE_ORDER_LIFETIME_SECONDS: u64 = 5 * 60;

/// The signed payload of a fence order. An order that names anything beyond
/// its fields is refused rather than half-read.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FenceOrderClaims {
    pub version: u32,
    pub key_id: String,
    pub audience: String,
    pub revoked_generation: u64,
    pub effective_generation: u64,
    pub issued_at_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
}

/// An authenticated order: the application retired `revoked_generation` and
/// put `effective_generation` in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedFenceOrder {
    pub revoked_generation: u64,
    pub effective_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FenceOrderError {
    #[error("fence order exceeds its size bound")]
    TokenTooLarge,
    #[error("fence order is not exactly one payload and one signature")]
    InvalidFraming,
    #[error("fence order is not valid unpadded base64url")]
    InvalidEncoding,
    #[error("fence order payload is not a well-formed order")]
    MalformedPayload,
    #[error("fence order version {0} is not supported")]
    UnsupportedVersion(u32),
    #[error("fence order names no key this deployment trusts")]
    UnknownKeyId,
    #[error("fence order signature verification failed")]
    InvalidSignature,
    #[error("fence order was issued for a different audience")]
    AudienceMismatch,
    #[error("fence order must move to a later generation than the one it retires")]
    GenerationNotAdvanced,
    #[error("fence order expiry does not follow its issue time, or exceeds the maximum window")]
    InvalidValidityWindow,
    #[error("fence order is not yet valid")]
    NotYetValid,
    #[error("fence order has expired")]
    Expired,
}

/// Builds the exact byte string a fence-order signature covers.
pub fn fence_order_signing_input(payload: &[u8]) -> Vec<u8> {
    let mut signed = Vec::with_capacity(FENCE_ORDER_SIGNING_DOMAIN.len() + payload.len());
    signed.extend_from_slice(FENCE_ORDER_SIGNING_DOMAIN);
    signed.extend_from_slice(payload);
    signed
}

impl GrantVerifier {
    /// Verifies one presented fence order against this deployment's grant keys
    /// and audience.
    pub fn verify_fence_order(
        &self,
        token: &str,
        now_unix_seconds: u64,
    ) -> Result<VerifiedFenceOrder, FenceOrderError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(FenceOrderError::TokenTooLarge);
        }
        let (encoded_payload, encoded_signature) = token
            .split_once('.')
            .ok_or(FenceOrderError::InvalidFraming)?;
        if encoded_signature.contains('.') {
            return Err(FenceOrderError::InvalidFraming);
        }
        let payload = URL_SAFE_NO_PAD
            .decode(encoded_payload)
            .map_err(|_| FenceOrderError::InvalidEncoding)?;
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(FenceOrderError::TokenTooLarge);
        }
        let signature = URL_SAFE_NO_PAD
            .decode(encoded_signature)
            .map_err(|_| FenceOrderError::InvalidEncoding)?;
        let claims: FenceOrderClaims =
            serde_json::from_slice(&payload).map_err(|_| FenceOrderError::MalformedPayload)?;
        if claims.version != FENCE_ORDER_VERSION {
            return Err(FenceOrderError::UnsupportedVersion(claims.version));
        }
        let key = self
            .keys
            .get(&claims.key_id)
            .ok_or(FenceOrderError::UnknownKeyId)?;
        key.verify(&fence_order_signing_input(&payload), &signature)
            .map_err(|_| FenceOrderError::InvalidSignature)?;

        // Everything below here is authenticated.
        if claims.audience != self.audience {
            return Err(FenceOrderError::AudienceMismatch);
        }
        if claims.effective_generation <= claims.revoked_generation {
            return Err(FenceOrderError::GenerationNotAdvanced);
        }
        if claims.expires_at_unix_seconds <= claims.issued_at_unix_seconds
            || claims.expires_at_unix_seconds - claims.issued_at_unix_seconds
                > MAX_FENCE_ORDER_LIFETIME_SECONDS
        {
            return Err(FenceOrderError::InvalidValidityWindow);
        }
        if now_unix_seconds < claims.issued_at_unix_seconds {
            return Err(FenceOrderError::NotYetValid);
        }
        if now_unix_seconds >= claims.expires_at_unix_seconds {
            return Err(FenceOrderError::Expired);
        }
        Ok(VerifiedFenceOrder {
            revoked_generation: claims.revoked_generation,
            effective_generation: claims.effective_generation,
        })
    }

    /// The public half of one trusted key, for checking the revocation
    /// barrier the same application key signs.
    pub fn public_key(&self, key_id: &str) -> Option<[u8; ED25519_PUBLIC_KEY_BYTES]> {
        self.keys
            .get(key_id)
            .and_then(|key| key.as_ref().try_into().ok())
    }
}

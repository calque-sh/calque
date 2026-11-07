//! The v2 preimage and Ed25519 verification of the ordered authenticated
//! revocation barrier.
//!
//! The application backend signs the barrier with the key it signs grants
//! with, so a change server holding only the public half can neither forge a
//! barrier nor withhold one and mint a different one later. The barrier has
//! its own domain separator, so a signature over a barrier never verifies as a
//! grant.
//!
//! The signature covers every field of [`RevocationBarrier`] except itself.
//! The frame's `body_sha256` covers the signature and the stream's frame
//! sequence covers the frame, so a barrier cannot be moved inside a stream
//! without breaking the digest chain. A barrier that is never delivered goes
//! unnoticed; only grant expiry bounds that.

use ring::signature::{UnparsedPublicKey, ED25519};
use thiserror::Error;

use crate::change::RevocationBarrier;

/// Domain separator prefixed to the barrier preimage before signing.
pub(crate) const REVOCATION_BARRIER_SIGNING_DOMAIN: &[u8] =
    b"calque.change.revocation-barrier.v2\0";

/// Ed25519 public keys are 32 bytes and signatures are 64.
pub(crate) const REVOCATION_BARRIER_PUBLIC_KEY_BYTES: usize = 32;
pub(crate) const REVOCATION_BARRIER_SIGNATURE_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RevocationBarrierError {
    #[error("revocation barrier is missing its cursor")]
    MissingCursor,
    #[error("revocation barrier signature must contain exactly 64 bytes")]
    InvalidSignatureLength,
    #[error("revocation barrier signing key id does not match the configured key")]
    KeyIdMismatch,
    #[error("revocation barrier signature verification failed")]
    InvalidSignature,
}

/// The Ed25519 key a receiver accepts barriers from, and the identifier the
/// issuer stamps into them. A receiver with no key configured cannot call
/// [`verify_revocation_barrier`] at all, so "no key" fails closed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevocationBarrierVerificationKey {
    key_id: String,
    public_key: [u8; REVOCATION_BARRIER_PUBLIC_KEY_BYTES],
}

impl RevocationBarrierVerificationKey {
    pub fn new(
        key_id: impl Into<String>,
        public_key: [u8; REVOCATION_BARRIER_PUBLIC_KEY_BYTES],
    ) -> Self {
        Self {
            key_id: key_id.into(),
            public_key,
        }
    }
}

/// Builds the v2 signing preimage. A barrier names no plan: it states
/// something true of every plan of its generation and scope.
///
/// ```text
/// ASCII "calque.change.revocation-barrier.v2\0"
/// || u32 BE len(barrier_id)       || barrier_id
/// || u32 BE len(signing_key_id)   || signing_key_id
/// || u32 BE len(barrier_cursor)   || barrier_cursor.token
/// || u32 BE len(scope_hash)       || revoked_authorization_scope_hash
/// || u64 BE revoked_authorization_generation
/// || u64 BE effective_authorization_generation
/// || u64 BE issued_at_unix_micros
/// ```
///
/// Every variable-length field is length-prefixed, so no two distinct barriers
/// can produce the same bytes by moving a boundary between two fields.
pub fn revocation_barrier_signing_preimage(
    barrier: &RevocationBarrier,
) -> Result<Vec<u8>, RevocationBarrierError> {
    let cursor = barrier
        .barrier_cursor
        .as_ref()
        .ok_or(RevocationBarrierError::MissingCursor)?;
    let mut preimage = Vec::with_capacity(REVOCATION_BARRIER_SIGNING_DOMAIN.len() + 128);
    preimage.extend_from_slice(REVOCATION_BARRIER_SIGNING_DOMAIN);
    for field in [
        barrier.barrier_id.as_bytes(),
        barrier.signing_key_id.as_bytes(),
        cursor.token.as_slice(),
        barrier.revoked_authorization_scope_hash.as_slice(),
    ] {
        // A field longer than u32::MAX cannot exist inside a 4 MiB frame; the
        // saturating cast is unreachable rather than lossy, and a receiver
        // that somehow saw one would compute a preimage that fails to verify.
        preimage.extend_from_slice(&u32::try_from(field.len()).unwrap_or(u32::MAX).to_be_bytes());
        preimage.extend_from_slice(field);
    }
    preimage.extend_from_slice(&barrier.revoked_authorization_generation.to_be_bytes());
    preimage.extend_from_slice(&barrier.effective_authorization_generation.to_be_bytes());
    preimage.extend_from_slice(&barrier.issued_at_unix_micros.to_be_bytes());
    Ok(preimage)
}

/// Verifies one barrier against the configured authorization-authority key.
///
/// The key identifier is compared first so a barrier signed by a key this
/// deployment does not accept is refused as such rather than as a bad
/// signature.
pub fn verify_revocation_barrier(
    barrier: &RevocationBarrier,
    key: &RevocationBarrierVerificationKey,
) -> Result<(), RevocationBarrierError> {
    if barrier.signature.len() != REVOCATION_BARRIER_SIGNATURE_BYTES {
        return Err(RevocationBarrierError::InvalidSignatureLength);
    }
    if barrier.signing_key_id != key.key_id {
        return Err(RevocationBarrierError::KeyIdMismatch);
    }
    let preimage = revocation_barrier_signing_preimage(barrier)?;
    UnparsedPublicKey::new(&ED25519, key.public_key.as_slice())
        .verify(&preimage, &barrier.signature)
        .map_err(|_| RevocationBarrierError::InvalidSignature)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::Cursor;

    fn barrier() -> RevocationBarrier {
        RevocationBarrier {
            barrier_cursor: Some(Cursor {
                token: b"cursor-b".to_vec(),
            }),
            revoked_authorization_generation: 4,
            effective_authorization_generation: 5,
            revoked_authorization_scope_hash: vec![5; 32],
            barrier_id: "barrier-1".to_owned(),
            issued_at_unix_micros: 12,
            signing_key_id: "revocation-key-1".to_owned(),
            signature: vec![0; 64],
        }
    }

    #[test]
    fn preimage_is_domain_separated_and_unambiguous() {
        let preimage = revocation_barrier_signing_preimage(&barrier()).unwrap();
        assert!(preimage.starts_with(REVOCATION_BARRIER_SIGNING_DOMAIN));
        // Moving a byte from one adjacent field into the next must change the
        // preimage; without length prefixes these two would be identical.
        let mut moved = barrier();
        moved.barrier_id = "barrier-1r".to_owned();
        moved.signing_key_id = "evocation-key-1".to_owned();
        assert_ne!(
            preimage,
            revocation_barrier_signing_preimage(&moved).unwrap()
        );
    }

    #[test]
    fn missing_cursor_and_wrong_width_signature_fail_closed() {
        let mut without_cursor = barrier();
        without_cursor.barrier_cursor = None;
        assert_eq!(
            revocation_barrier_signing_preimage(&without_cursor),
            Err(RevocationBarrierError::MissingCursor)
        );
        let key = RevocationBarrierVerificationKey::new("revocation-key-1", [0; 32]);
        let mut short = barrier();
        short.signature.pop();
        assert_eq!(
            verify_revocation_barrier(&short, &key),
            Err(RevocationBarrierError::InvalidSignatureLength)
        );
        let mismatched = RevocationBarrierVerificationKey::new("other-key", [0; 32]);
        assert_eq!(
            verify_revocation_barrier(&barrier(), &mismatched),
            Err(RevocationBarrierError::KeyIdMismatch)
        );
        assert_eq!(
            verify_revocation_barrier(&barrier(), &key),
            Err(RevocationBarrierError::InvalidSignature)
        );
    }
}

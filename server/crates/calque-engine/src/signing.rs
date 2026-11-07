//! What the deployment signs for itself: plan tokens and artifact
//! capabilities, each `base64url(payload).base64url(tag)` with an
//! HMAC-SHA-256 tag over a domain and the payload.

use std::fmt;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};

use crate::SecretToken;

pub(crate) const PLAN_DOMAIN: &[u8] = b"calque.plan.v1\0";
pub(crate) const ARTIFACT_CAPABILITY_DOMAIN: &[u8] = b"calque.artifact-capability.v1\0";

/// The longest token `open` reads, well under the change request's bound.
const MAX_SIGNED_BYTES: usize = 4 * 1024;

#[derive(Clone)]
pub struct PlanKey {
    secret: [u8; 32],
    key_id: String,
}

impl PlanKey {
    pub fn new(secret: &SecretToken) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"calque.plan-key-id.v1\0");
        digest.update(secret.as_bytes());
        Self {
            secret: *secret.as_bytes(),
            key_id: hex::encode(&digest.finalize()[..8]),
        }
    }

    pub(crate) fn key_id(&self) -> &str {
        &self.key_id
    }

    pub(crate) fn sign(&self, domain: &[u8], payload: &impl Serialize) -> String {
        let payload = serde_json::to_vec(payload).expect("a signed payload serializes");
        let tag = self.mac(domain, &payload).finalize().into_bytes();
        format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(&payload),
            URL_SAFE_NO_PAD.encode(tag)
        )
    }

    /// The payload of a token this key signed under `domain`. The tag is
    /// checked before the payload is parsed.
    pub(crate) fn open<T: DeserializeOwned>(&self, domain: &[u8], token: &str) -> Option<T> {
        if token.len() > MAX_SIGNED_BYTES {
            return None;
        }
        let (payload, tag) = token.split_once('.')?;
        if tag.contains('.') {
            return None;
        }
        let payload = URL_SAFE_NO_PAD.decode(payload).ok()?;
        let tag = URL_SAFE_NO_PAD.decode(tag).ok()?;
        self.mac(domain, &payload).verify_slice(&tag).ok()?;
        serde_json::from_slice(&payload).ok()
    }

    fn mac(&self, domain: &[u8], payload: &[u8]) -> Hmac<Sha256> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.secret).expect("HMAC accepts a 32-byte key");
        mac.update(domain);
        mac.update(payload);
        mac
    }
}

impl fmt::Debug for PlanKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlanKey")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Payload {
        value: u64,
    }

    fn key(byte: u8) -> PlanKey {
        PlanKey::new(&SecretToken::from_urlsafe(&URL_SAFE_NO_PAD.encode([byte; 32])).unwrap())
    }

    #[test]
    fn a_token_opens_only_under_its_key_and_domain_and_only_whole() {
        let key = key(1);
        let token = key.sign(PLAN_DOMAIN, &Payload { value: 7 });
        assert_eq!(
            key.open::<Payload>(PLAN_DOMAIN, &token),
            Some(Payload { value: 7 })
        );
        assert_eq!(
            key.open::<Payload>(ARTIFACT_CAPABILITY_DOMAIN, &token),
            None
        );
        assert_eq!(
            super::tests::key(2).open::<Payload>(PLAN_DOMAIN, &token),
            None
        );

        let (payload, tag) = token.split_once('.').unwrap();
        let mut payload_bytes = URL_SAFE_NO_PAD.decode(payload).unwrap();
        let last = payload_bytes.len() - 2;
        payload_bytes[last] ^= 1;
        let flipped_payload = format!("{}.{tag}", URL_SAFE_NO_PAD.encode(&payload_bytes));
        assert_eq!(key.open::<Payload>(PLAN_DOMAIN, &flipped_payload), None);

        let mut tag_bytes = URL_SAFE_NO_PAD.decode(tag).unwrap();
        tag_bytes[0] ^= 1;
        let flipped_tag = format!("{payload}.{}", URL_SAFE_NO_PAD.encode(&tag_bytes));
        assert_eq!(key.open::<Payload>(PLAN_DOMAIN, &flipped_tag), None);

        for framing in [
            payload.to_owned(),
            format!("{token}.{tag}"),
            format!("{payload}.{}", &tag[1..]),
            "a".repeat(MAX_SIGNED_BYTES + 1),
        ] {
            assert_eq!(
                key.open::<Payload>(PLAN_DOMAIN, &framing),
                None,
                "{framing}"
            );
        }
    }

    #[test]
    fn a_signed_payload_with_an_unknown_field_is_refused() {
        #[derive(Serialize)]
        struct Wider {
            value: u64,
            extra: u64,
        }
        let key = key(1);
        let token = key.sign(PLAN_DOMAIN, &Wider { value: 7, extra: 1 });
        assert_eq!(key.open::<Payload>(PLAN_DOMAIN, &token), None);
    }

    #[test]
    fn the_key_id_names_the_key_and_not_its_bytes() {
        assert_eq!(key(1).key_id(), key(1).key_id());
        assert_ne!(key(1).key_id(), key(2).key_id());
        assert_eq!(key(1).key_id().len(), 16);
        assert!(!format!("{:?}", key(1)).contains(&URL_SAFE_NO_PAD.encode([1_u8; 32])));
    }
}

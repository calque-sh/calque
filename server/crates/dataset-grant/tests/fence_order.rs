//! The application's signed fence order: what verifies, and every
//! way an order that must not move a generation fails closed.
//!
//! Every negative vector is validly signed by a trusted key unless the case is
//! about the signature or the key, so each fails on the claim check it names.

use dataset_grant::{
    encode_grant_token, grant_signing_input, FenceOrderClaims, FenceOrderError, GrantVerifier,
    VerifiedFenceOrder, FENCE_ORDER_VERSION, MAX_FENCE_ORDER_LIFETIME_SECONDS,
};
use dataset_grant_issuer::{GrantIssuer, IssuerError};
use ring::signature::Ed25519KeyPair;

const AUDIENCE: &str = "calque.example.test";
const ISSUED_AT: u64 = 1_800_000_000;
const LIFETIME: u64 = 60;

fn issuer(key_id: &str) -> (GrantIssuer, Ed25519KeyPair) {
    let (issuer, pkcs8) = GrantIssuer::generate(key_id, AUDIENCE).unwrap();
    (issuer, Ed25519KeyPair::from_pkcs8(&pkcs8).unwrap())
}

fn verifier_for(issuer: &GrantIssuer) -> GrantVerifier {
    GrantVerifier::new(AUDIENCE, [issuer.verification_key().unwrap()]).unwrap()
}

fn claims(issuer: &GrantIssuer) -> FenceOrderClaims {
    FenceOrderClaims {
        version: FENCE_ORDER_VERSION,
        key_id: issuer.key_id().to_owned(),
        audience: AUDIENCE.to_owned(),
        revoked_generation: 3,
        effective_generation: 4,
        issued_at_unix_seconds: ISSUED_AT,
        expires_at_unix_seconds: ISSUED_AT + LIFETIME,
    }
}

#[test]
fn a_signed_order_verifies_to_exactly_its_two_generations() {
    let (issuer, _) = issuer("app-key-1");
    let token = issuer.issue_fence_order(3, 4, ISSUED_AT, LIFETIME).unwrap();
    assert_eq!(
        verifier_for(&issuer).verify_fence_order(&token, ISSUED_AT + 1),
        Ok(VerifiedFenceOrder {
            revoked_generation: 3,
            effective_generation: 4,
        })
    );
}

/// The order and the grant share one key, so the domain separator is the only
/// thing keeping a grant signature from verifying as an order. A payload with
/// the order's exact shape, signed under the grant's domain, must not verify.
#[test]
fn a_grant_signature_never_verifies_as_an_order() {
    let (issuer, key_pair) = issuer("app-key-1");
    let payload = serde_json::to_vec(&claims(&issuer)).unwrap();
    let signature = key_pair.sign(&grant_signing_input(&payload));
    let token = encode_grant_token(&payload, signature.as_ref());
    assert_eq!(
        verifier_for(&issuer).verify_fence_order(&token, ISSUED_AT + 1),
        Err(FenceOrderError::InvalidSignature)
    );
}

#[test]
fn an_order_signed_by_an_untrusted_key_or_naming_one_is_refused() {
    let (trusted, _) = issuer("app-key-1");
    let (stranger, _) = issuer("app-key-1");
    let forged = stranger
        .issue_fence_order(3, 4, ISSUED_AT, LIFETIME)
        .unwrap();
    assert_eq!(
        verifier_for(&trusted).verify_fence_order(&forged, ISSUED_AT + 1),
        Err(FenceOrderError::InvalidSignature)
    );
    let (unknown, _) = issuer("app-key-2");
    let token = unknown
        .issue_fence_order(3, 4, ISSUED_AT, LIFETIME)
        .unwrap();
    assert_eq!(
        verifier_for(&trusted).verify_fence_order(&token, ISSUED_AT + 1),
        Err(FenceOrderError::UnknownKeyId)
    );
}

#[test]
fn a_tampered_generation_breaks_the_signature() {
    let (issuer, _) = issuer("app-key-1");
    let token = issuer.issue_fence_order(3, 4, ISSUED_AT, LIFETIME).unwrap();
    let (_, signature) = token.split_once('.').unwrap();
    let mut widened = claims(&issuer);
    widened.effective_generation = 9;
    let tampered = format!(
        "{}.{signature}",
        base64_url(&serde_json::to_vec(&widened).unwrap())
    );
    assert_eq!(
        verifier_for(&issuer).verify_fence_order(&tampered, ISSUED_AT + 1),
        Err(FenceOrderError::InvalidSignature)
    );
}

#[test]
fn every_authenticated_claim_is_checked() {
    let (issuer, _) = issuer("app-key-1");
    let verifier = verifier_for(&issuer);
    let refused = |edit: fn(&mut FenceOrderClaims), now: u64| {
        let mut order = claims(&issuer);
        edit(&mut order);
        let token = issuer.issue_fence_order_claims(&order).unwrap();
        verifier.verify_fence_order(&token, now).unwrap_err()
    };
    assert_eq!(
        refused(|order| order.audience = "other.test".to_owned(), ISSUED_AT),
        FenceOrderError::AudienceMismatch
    );
    assert_eq!(
        refused(|order| order.effective_generation = 3, ISSUED_AT),
        FenceOrderError::GenerationNotAdvanced
    );
    assert_eq!(
        refused(|order| order.effective_generation = 2, ISSUED_AT),
        FenceOrderError::GenerationNotAdvanced
    );
    assert_eq!(
        refused(
            |order| order.expires_at_unix_seconds =
                ISSUED_AT + MAX_FENCE_ORDER_LIFETIME_SECONDS + 1,
            ISSUED_AT
        ),
        FenceOrderError::InvalidValidityWindow
    );
    assert_eq!(
        refused(|order| order.expires_at_unix_seconds = ISSUED_AT, ISSUED_AT),
        FenceOrderError::InvalidValidityWindow
    );
    assert_eq!(refused(|_| {}, ISSUED_AT - 1), FenceOrderError::NotYetValid);
    assert_eq!(
        refused(|_| {}, ISSUED_AT + LIFETIME),
        FenceOrderError::Expired
    );
    assert_eq!(
        refused(|order| order.version = 2, ISSUED_AT),
        FenceOrderError::UnsupportedVersion(2)
    );
}

#[test]
fn an_order_carrying_an_unknown_field_is_refused() {
    let (issuer, key_pair) = issuer("app-key-1");
    let mut value = serde_json::to_value(claims(&issuer)).unwrap();
    value["scope"] = serde_json::json!("everything");
    let payload = serde_json::to_vec(&value).unwrap();
    let signature = key_pair.sign(&dataset_grant::fence_order_signing_input(&payload));
    let token = encode_grant_token(&payload, signature.as_ref());
    assert_eq!(
        verifier_for(&issuer).verify_fence_order(&token, ISSUED_AT + 1),
        Err(FenceOrderError::MalformedPayload)
    );
}

#[test]
fn the_issuer_refuses_an_order_every_verifier_would_refuse() {
    let (issuer, _) = issuer("app-key-1");
    assert_eq!(
        issuer.issue_fence_order(4, 4, ISSUED_AT, LIFETIME),
        Err(IssuerError::GenerationNotAdvanced)
    );
    assert_eq!(
        issuer.issue_fence_order(3, 4, ISSUED_AT, MAX_FENCE_ORDER_LIFETIME_SECONDS + 1),
        Err(IssuerError::LifetimeOutOfRange)
    );
}

#[test]
fn the_public_key_is_exposed_only_for_a_trusted_key_id() {
    let (issuer, key_pair) = issuer("app-key-1");
    let verifier = verifier_for(&issuer);
    use ring::signature::KeyPair as _;
    assert_eq!(
        verifier
            .public_key("app-key-1")
            .as_ref()
            .map(|key| &key[..]),
        Some(key_pair.public_key().as_ref())
    );
    assert_eq!(verifier.public_key("app-key-2"), None);
}

fn base64_url(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

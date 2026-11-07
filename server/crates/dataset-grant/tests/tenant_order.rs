//! The application backend's signed tenant order: what verifies, and every
//! way an order a browser could forge or replay fails closed.
//!
//! Every negative vector is validly signed by a trusted key unless the case is
//! about the signature or the key, so each fails on the claim check it names.

use authz::PrincipalValue;
use dataset_grant::{
    encode_grant_token, fence_order_signing_input, grant_signing_input, GrantAttributeValue,
    GrantVerifier, TenantAction, TenantOrderClaims, TenantOrderError, VerifiedTenantOrder,
    MAX_TENANT_ORDER_LIFETIME_SECONDS, TENANT_ORDER_VERSION,
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

fn tenant() -> GrantAttributeValue {
    GrantAttributeValue::Int8("7".to_owned())
}

fn claims(issuer: &GrantIssuer) -> TenantOrderClaims {
    TenantOrderClaims {
        version: TENANT_ORDER_VERSION,
        key_id: issuer.key_id().to_owned(),
        audience: AUDIENCE.to_owned(),
        order_id: "order-0123456789ab".to_owned(),
        action: TenantAction::Pin,
        tenant: tenant(),
        issued_at_unix_seconds: ISSUED_AT,
        expires_at_unix_seconds: ISSUED_AT + LIFETIME,
    }
}

#[test]
fn a_signed_order_verifies_to_exactly_its_action_and_tenant() {
    let (issuer, _) = issuer("app-key-1");
    for action in [
        TenantAction::Warm,
        TenantAction::Pin,
        TenantAction::Unpin,
        TenantAction::State,
        TenantAction::Evict,
    ] {
        let token = issuer
            .issue_tenant_order(action, tenant(), ISSUED_AT, LIFETIME)
            .unwrap();
        let verified = verifier_for(&issuer)
            .verify_tenant_order(&token, ISSUED_AT + 1)
            .unwrap();
        assert_eq!(verified.order_id.len(), 22, "{verified:?}");
        assert_eq!(
            verified,
            VerifiedTenantOrder {
                order_id: verified.order_id.clone(),
                action,
                tenant: PrincipalValue::Int8(7),
                expires_at_unix_seconds: ISSUED_AT + LIFETIME,
            }
        );
    }
}

/// The deployment acts on an order id once, so each order the issuer signs
/// carries an id of its own.
#[test]
fn each_order_carries_an_id_of_its_own() {
    let (issuer, _) = issuer("app-key-1");
    let verifier = verifier_for(&issuer);
    let ids = (0..64)
        .map(|_| {
            let token = issuer
                .issue_tenant_order(TenantAction::Warm, tenant(), ISSUED_AT, LIFETIME)
                .unwrap();
            verifier
                .verify_tenant_order(&token, ISSUED_AT + 1)
                .unwrap()
                .order_id
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 64);
}

/// A browser holds a dataset grant signed by the same key, so the domain
/// separator is what keeps a grant, or a fence order, from verifying as a
/// tenant order. A payload of the order's exact shape signed under either
/// other domain must not verify.
#[test]
fn neither_a_grant_nor_a_fence_order_signature_verifies_as_a_tenant_order() {
    let (issuer, key_pair) = issuer("app-key-1");
    let payload = serde_json::to_vec(&claims(&issuer)).unwrap();
    for signing_input in [grant_signing_input, fence_order_signing_input] {
        let signature = key_pair.sign(&signing_input(&payload));
        let token = encode_grant_token(&payload, signature.as_ref());
        assert_eq!(
            verifier_for(&issuer).verify_tenant_order(&token, ISSUED_AT + 1),
            Err(TenantOrderError::InvalidSignature)
        );
    }
}

#[test]
fn a_dataset_grant_is_not_a_tenant_order() {
    let (issuer, _) = issuer("app-key-1");
    let grant = issuer
        .issue("user-1", Default::default(), 1, ISSUED_AT, LIFETIME)
        .unwrap();
    assert_eq!(
        verifier_for(&issuer).verify_tenant_order(&grant, ISSUED_AT + 1),
        Err(TenantOrderError::MalformedPayload)
    );
}

#[test]
fn an_order_signed_by_an_untrusted_key_or_naming_one_is_refused() {
    let (trusted, _) = issuer("app-key-1");
    let (stranger, _) = issuer("app-key-1");
    let forged = stranger
        .issue_tenant_order(TenantAction::Warm, tenant(), ISSUED_AT, LIFETIME)
        .unwrap();
    assert_eq!(
        verifier_for(&trusted).verify_tenant_order(&forged, ISSUED_AT + 1),
        Err(TenantOrderError::InvalidSignature)
    );
    let (unknown, _) = issuer("app-key-2");
    let token = unknown
        .issue_tenant_order(TenantAction::Warm, tenant(), ISSUED_AT, LIFETIME)
        .unwrap();
    assert_eq!(
        verifier_for(&trusted).verify_tenant_order(&token, ISSUED_AT + 1),
        Err(TenantOrderError::UnknownKeyId)
    );
}

#[test]
fn a_tampered_tenant_breaks_the_signature() {
    let (issuer, _) = issuer("app-key-1");
    let token = issuer
        .issue_tenant_order(TenantAction::Pin, tenant(), ISSUED_AT, LIFETIME)
        .unwrap();
    let (_, signature) = token.split_once('.').unwrap();
    let mut other = claims(&issuer);
    other.tenant = GrantAttributeValue::Int8("8".to_owned());
    let tampered = format!(
        "{}.{signature}",
        base64_url(&serde_json::to_vec(&other).unwrap())
    );
    assert_eq!(
        verifier_for(&issuer).verify_tenant_order(&tampered, ISSUED_AT + 1),
        Err(TenantOrderError::InvalidSignature)
    );
}

#[test]
fn every_authenticated_claim_is_checked() {
    let (issuer, _) = issuer("app-key-1");
    let verifier = verifier_for(&issuer);
    let refused = |edit: fn(&mut TenantOrderClaims), now: u64| {
        let mut order = claims(&issuer);
        edit(&mut order);
        let token = issuer.issue_tenant_order_claims(&order).unwrap();
        verifier.verify_tenant_order(&token, now).unwrap_err()
    };
    assert_eq!(
        refused(|order| order.audience = "other.test".to_owned(), ISSUED_AT),
        TenantOrderError::AudienceMismatch
    );
    assert_eq!(
        refused(
            |order| order.expires_at_unix_seconds =
                ISSUED_AT + MAX_TENANT_ORDER_LIFETIME_SECONDS + 1,
            ISSUED_AT
        ),
        TenantOrderError::InvalidValidityWindow
    );
    assert_eq!(
        refused(|order| order.expires_at_unix_seconds = ISSUED_AT, ISSUED_AT),
        TenantOrderError::InvalidValidityWindow
    );
    assert_eq!(
        refused(|_| {}, ISSUED_AT - 1),
        TenantOrderError::NotYetValid
    );
    assert_eq!(
        refused(|_| {}, ISSUED_AT + LIFETIME),
        TenantOrderError::Expired
    );
    assert_eq!(
        refused(|order| order.version = 2, ISSUED_AT),
        TenantOrderError::UnsupportedVersion(2)
    );
    assert_eq!(
        refused(
            |order| order.tenant = GrantAttributeValue::Int8("seven".to_owned()),
            ISSUED_AT
        ),
        TenantOrderError::MalformedTenant
    );
    for id in ["short", "an id with spaces 0123", &"x".repeat(65)] {
        let mut order = claims(&issuer);
        order.order_id = id.to_owned();
        let token = issuer.issue_tenant_order_claims(&order).unwrap();
        assert_eq!(
            verifier.verify_tenant_order(&token, ISSUED_AT),
            Err(TenantOrderError::MalformedOrderId),
            "{id}"
        );
    }
}

#[test]
fn an_order_without_an_id_is_refused() {
    let (issuer, key_pair) = issuer("app-key-1");
    let mut value = serde_json::to_value(claims(&issuer)).unwrap();
    value.as_object_mut().unwrap().remove("orderId");
    let payload = serde_json::to_vec(&value).unwrap();
    let signature = key_pair.sign(&dataset_grant::tenant_order_signing_input(&payload));
    let token = encode_grant_token(&payload, signature.as_ref());
    assert_eq!(
        verifier_for(&issuer).verify_tenant_order(&token, ISSUED_AT + 1),
        Err(TenantOrderError::MalformedPayload)
    );
}

#[test]
fn an_order_carrying_an_unknown_field_is_refused() {
    let (issuer, key_pair) = issuer("app-key-1");
    let mut value = serde_json::to_value(claims(&issuer)).unwrap();
    value["tenants"] = serde_json::json!("every");
    let payload = serde_json::to_vec(&value).unwrap();
    let signature = key_pair.sign(&dataset_grant::tenant_order_signing_input(&payload));
    let token = encode_grant_token(&payload, signature.as_ref());
    assert_eq!(
        verifier_for(&issuer).verify_tenant_order(&token, ISSUED_AT + 1),
        Err(TenantOrderError::MalformedPayload)
    );
}

#[test]
fn the_issuer_refuses_a_lifetime_every_verifier_would_refuse() {
    let (issuer, _) = issuer("app-key-1");
    for lifetime in [0, MAX_TENANT_ORDER_LIFETIME_SECONDS + 1] {
        assert_eq!(
            issuer.issue_tenant_order(TenantAction::Warm, tenant(), ISSUED_AT, lifetime),
            Err(IssuerError::LifetimeOutOfRange)
        );
    }
}

fn base64_url(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

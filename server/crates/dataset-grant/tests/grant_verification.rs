//! Signed dataset grant verification: exact signed grants, online generation,
//! rotation, and negative vectors that fail closed.
//!
//! Every negative case is signed by a trusted key unless it tests signature or
//! key rejection, so it fails on the claim it targets.

use std::collections::BTreeMap;

use authz::PrincipalError;
use dataset_grant::{
    encode_grant_token, grant_signing_input, DatasetGrantClaims, GrantAttributeValue,
    GrantCommandCapability, GrantCommandOperation, GrantError, GrantKeyError, GrantPresentation,
    GrantVerificationKey, GrantVerifier, GRANT_VERSION, MAX_GRANT_LIFETIME_SECONDS,
};
use dataset_grant_issuer::{GrantIssuer, IssuerError};
use ring::signature::Ed25519KeyPair;

const AUDIENCE: &str = "calque.example.test";
const GENERATION: u64 = 7;
const ISSUED_AT: u64 = 1_800_000_000;
const LIFETIME: u64 = 300;

/// An issuer plus its private key, so tests can also sign bytes the issuer's
/// own API refuses to sign.
struct TestIssuer {
    issuer: GrantIssuer,
    key_pair: Ed25519KeyPair,
}

impl TestIssuer {
    fn generate(key_id: &str) -> Self {
        let (issuer, pkcs8) = GrantIssuer::generate(key_id, AUDIENCE).unwrap();
        Self {
            issuer,
            key_pair: Ed25519KeyPair::from_pkcs8(&pkcs8).unwrap(),
        }
    }

    fn sign(&self, message: &[u8]) -> Vec<u8> {
        self.key_pair.sign(message).as_ref().to_vec()
    }
}

impl std::ops::Deref for TestIssuer {
    type Target = GrantIssuer;

    fn deref(&self) -> &GrantIssuer {
        &self.issuer
    }
}

fn issuer(key_id: &str) -> TestIssuer {
    TestIssuer::generate(key_id)
}

fn verifier_for(issuers: &[&TestIssuer]) -> GrantVerifier {
    GrantVerifier::new(
        AUDIENCE,
        issuers
            .iter()
            .map(|issuer| issuer.verification_key().unwrap()),
    )
    .unwrap()
}

fn tenant() -> BTreeMap<String, GrantAttributeValue> {
    BTreeMap::from([(
        "tenant".to_owned(),
        GrantAttributeValue::Uuid("11".repeat(16)),
    )])
}

fn presented<'a>(token: &'a str, now: u64) -> GrantPresentation<'a> {
    GrantPresentation {
        token,
        now_unix_seconds: now,
        authorization_generation: GENERATION,
    }
}

fn claims(key_id: &str) -> DatasetGrantClaims {
    DatasetGrantClaims {
        version: GRANT_VERSION,
        key_id: key_id.to_owned(),
        audience: AUDIENCE.to_owned(),
        subject: "user-1".to_owned(),
        attributes: tenant(),
        authorization_generation: GENERATION,
        issued_at_unix_seconds: ISSUED_AT,
        expires_at_unix_seconds: ISSUED_AT + LIFETIME,
        commands: Vec::new(),
    }
}

fn capability(collection: &str, operations: &[GrantCommandOperation]) -> GrantCommandCapability {
    GrantCommandCapability {
        collection: collection.to_owned(),
        operations: operations.to_vec(),
    }
}

#[test]
fn a_valid_grant_yields_exactly_the_principal_it_names() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let token = issuer
        .issue("user-1", tenant(), GENERATION, ISSUED_AT, LIFETIME)
        .unwrap();

    let principal = verifier.verify(&presented(&token, ISSUED_AT + 1)).unwrap();
    assert_eq!(principal.subject(), "user-1");
    assert!(principal.attribute("tenant").is_some());
    assert!(principal.attribute("not-granted").is_none());
}

#[test]
fn verification_retains_the_authenticated_grant_expiry() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let token = issuer
        .issue("user-1", tenant(), GENERATION, ISSUED_AT, LIFETIME)
        .unwrap();

    let verified = verifier.verify(&presented(&token, ISSUED_AT + 1)).unwrap();
    assert_eq!(verified.principal().subject(), "user-1");
    assert_eq!(verified.expires_at_unix_seconds(), ISSUED_AT + LIFETIME);
}

#[test]
fn every_principal_value_codec_survives_the_round_trip_exactly() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let attributes = BTreeMap::from([
        ("flag".to_owned(), GrantAttributeValue::Bool(true)),
        ("small".to_owned(), GrantAttributeValue::Int2(-32_768)),
        (
            "medium".to_owned(),
            GrantAttributeValue::Int4(-2_147_483_648),
        ),
        (
            // A JSON number would silently corrupt the extreme i64.
            "large".to_owned(),
            GrantAttributeValue::Int8("-9223372036854775808".to_owned()),
        ),
        (
            "raw".to_owned(),
            GrantAttributeValue::Bytes("00ff07".to_owned()),
        ),
        (
            "tenant".to_owned(),
            GrantAttributeValue::Uuid("ab".repeat(16)),
        ),
    ]);
    let token = issuer
        .issue("user-1", attributes, GENERATION, ISSUED_AT, LIFETIME)
        .unwrap();

    let principal = verifier.verify(&presented(&token, ISSUED_AT + 1)).unwrap();
    for name in ["flag", "small", "medium", "large", "raw", "tenant"] {
        assert!(principal.attribute(name).is_some(), "missing {name}");
    }
}

#[test]
fn rotation_accepts_both_keys_and_retirement_takes_effect_immediately() {
    let old = issuer("key-old");
    let new = issuer("key-new");
    let old_token = old
        .issue("user-1", tenant(), GENERATION, ISSUED_AT, LIFETIME)
        .unwrap();
    let new_token = new
        .issue("user-1", tenant(), GENERATION, ISSUED_AT, LIFETIME)
        .unwrap();

    // Overlap: both keys are trusted while the issuer cuts over.
    let overlapping = verifier_for(&[&old, &new]);
    assert!(overlapping
        .verify(&presented(&old_token, ISSUED_AT + 1))
        .is_ok());
    assert!(overlapping
        .verify(&presented(&new_token, ISSUED_AT + 1))
        .is_ok());

    // Retirement: the old key is dropped and its outstanding, unexpired,
    // otherwise-valid grant stops being accepted.
    let rotated = verifier_for(&[&new]);
    assert_eq!(
        rotated.verify(&presented(&old_token, ISSUED_AT + 1)),
        Err(GrantError::UnknownKeyId)
    );
    assert!(rotated
        .verify(&presented(&new_token, ISSUED_AT + 1))
        .is_ok());
}

#[test]
fn a_grant_from_an_untrusted_issuer_is_rejected_even_with_a_trusted_key_id() {
    // The forger names a key id the deployment trusts, but holds a different
    // private key. Because the key id selects the key the signature is checked
    // against, this is a signature failure rather than a way in.
    let trusted = issuer("key-a");
    let forger = issuer("key-a");
    let verifier = verifier_for(&[&trusted]);
    let forged = forger
        .issue("user-1", tenant(), GENERATION, ISSUED_AT, LIFETIME)
        .unwrap();

    assert_eq!(
        verifier.verify(&presented(&forged, ISSUED_AT + 1)),
        Err(GrantError::InvalidSignature)
    );
}

#[test]
fn a_superseded_authorization_generation_fails_closed() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let token = issuer
        .issue("user-1", tenant(), GENERATION, ISSUED_AT, LIFETIME)
        .unwrap();

    // The policy generation moved on, so an unexpired grant minted against the
    // old one authorizes nothing further.
    let after_revocation = GrantPresentation {
        token: &token,
        now_unix_seconds: ISSUED_AT + 1,
        authorization_generation: GENERATION + 1,
    };
    assert_eq!(
        verifier.verify(&after_revocation),
        Err(GrantError::AuthorizationGenerationMismatch {
            grant: GENERATION,
            current: GENERATION + 1,
        })
    );
}

#[test]
fn expiry_is_bounded_and_exclusive_at_the_boundary() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let token = issuer
        .issue("user-1", tenant(), GENERATION, ISSUED_AT, LIFETIME)
        .unwrap();

    assert!(verifier.verify(&presented(&token, ISSUED_AT)).is_ok());
    assert!(verifier
        .verify(&presented(&token, ISSUED_AT + LIFETIME - 1))
        .is_ok());
    assert_eq!(
        verifier.verify(&presented(&token, ISSUED_AT + LIFETIME)),
        Err(GrantError::Expired)
    );
    assert_eq!(
        verifier.verify(&presented(&token, ISSUED_AT - 1)),
        Err(GrantError::NotYetValid)
    );
}

#[test]
fn an_over_long_validity_window_is_refused_at_both_ends() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);

    // A correct issuer refuses to mint one.
    assert_eq!(
        issuer.issue(
            "user-1",
            tenant(),
            GENERATION,
            ISSUED_AT,
            MAX_GRANT_LIFETIME_SECONDS + 1,
        ),
        Err(IssuerError::LifetimeOutOfRange)
    );

    // A hostile one can, and the verifier still refuses it.
    let mut over_long = claims(issuer.key_id());
    over_long.expires_at_unix_seconds = ISSUED_AT + MAX_GRANT_LIFETIME_SECONDS + 1;
    let token = issuer.issue_claims(&over_long).unwrap();
    assert_eq!(
        verifier.verify(&presented(&token, ISSUED_AT + 1)),
        Err(GrantError::LifetimeTooLong)
    );
}

#[test]
fn an_inverted_validity_window_is_refused() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let mut inverted = claims(issuer.key_id());
    inverted.expires_at_unix_seconds = inverted.issued_at_unix_seconds;
    let token = issuer.issue_claims(&inverted).unwrap();

    assert_eq!(
        verifier.verify(&presented(&token, ISSUED_AT)),
        Err(GrantError::InvalidValidityWindow)
    );
}

#[test]
fn a_grant_for_another_audience_is_rejected() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let mut elsewhere = claims(issuer.key_id());
    elsewhere.audience = "calque.other.test".to_owned();
    let token = issuer.issue_claims(&elsewhere).unwrap();

    assert_eq!(
        verifier.verify(&presented(&token, ISSUED_AT + 1)),
        Err(GrantError::AudienceMismatch)
    );
}

#[test]
fn an_unsupported_version_is_rejected_before_it_is_interpreted() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let mut future = claims(issuer.key_id());
    future.version = GRANT_VERSION + 1;
    let token = issuer.issue_claims(&future).unwrap();

    assert_eq!(
        verifier.verify(&presented(&token, ISSUED_AT + 1)),
        Err(GrantError::UnsupportedVersion(GRANT_VERSION + 1))
    );
}

#[test]
fn a_grant_cannot_carry_a_scope_a_table_or_a_column() {
    // The payload is signed by a trusted key and is valid JSON; it is refused
    // only because the grant has no field in which to ask for data, and unknown
    // fields are denied rather than ignored.
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    for injected in [
        r#""tables":["public.secrets"]"#,
        r#""columns":["ssn"]"#,
        r#""scope":{"kind":"sourceWide"}"#,
        r#""permittedTables":["*"]"#,
    ] {
        let payload = format!(
            r#"{{"version":{GRANT_VERSION},"keyId":"{}","audience":"{AUDIENCE}",
                "subject":"user-1","attributes":{{}},
                "authorizationGeneration":{GENERATION},
                "issuedAtUnixSeconds":{ISSUED_AT},
                "expiresAtUnixSeconds":{},{injected}}}"#,
            issuer.key_id(),
            ISSUED_AT + LIFETIME,
        );
        let token = sign_raw_payload(&issuer, payload.as_bytes());
        assert_eq!(
            verifier.verify(&presented(&token, ISSUED_AT + 1)),
            Err(GrantError::MalformedPayload),
            "widening field was not refused: {injected}"
        );
    }
}

#[test]
fn a_tampered_payload_is_rejected() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let token = issuer
        .issue("user-1", tenant(), GENERATION, ISSUED_AT, LIFETIME)
        .unwrap();

    // Re-encode a payload that swaps the subject, keeping the original
    // signature: the classic "edit the claims" attempt.
    let (_, signature) = token.split_once('.').unwrap();
    let swapped = serde_json::to_vec(&claims(issuer.key_id())).unwrap();
    let mut altered: serde_json::Value = serde_json::from_slice(&swapped).unwrap();
    altered["subject"] = serde_json::Value::String("admin".to_owned());
    let altered = serde_json::to_vec(&altered).unwrap();
    let forged = format!("{}.{signature}", base64_url(&altered),);

    assert_eq!(
        verifier.verify(&presented(&forged, ISSUED_AT + 1)),
        Err(GrantError::InvalidSignature)
    );
}

#[test]
fn a_signature_over_the_bare_payload_is_rejected() {
    // Domain separation: a signature produced without GRANT_SIGNING_DOMAIN does
    // not verify here, so a token from another protocol that happens to share a
    // key cannot be replayed as a grant.
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let payload = serde_json::to_vec(&claims(issuer.key_id())).unwrap();

    let undomained = issuer_sign_without_domain(&issuer, &payload);
    assert_eq!(
        verifier.verify(&presented(&undomained, ISSUED_AT + 1)),
        Err(GrantError::InvalidSignature)
    );

    // The same payload signed correctly is accepted, so the rejection above is
    // the domain and not something else about the payload.
    let domained = encode_grant_token(&payload, &issuer.sign(&grant_signing_input(&payload)));
    assert!(verifier
        .verify(&presented(&domained, ISSUED_AT + 1))
        .is_ok());
}

#[test]
fn malformed_framing_and_encoding_are_rejected() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let valid = issuer
        .issue("user-1", tenant(), GENERATION, ISSUED_AT, LIFETIME)
        .unwrap();

    for (token, expected) in [
        ("", GrantError::InvalidFraming),
        ("no-separator", GrantError::InvalidFraming),
        (
            // A third segment is refused rather than ignored.
            &format!("{valid}.extra"),
            GrantError::InvalidFraming,
        ),
        ("!!!.!!!", GrantError::InvalidEncoding),
        (
            &format!("{}.{}", base64_url(b"not json"), base64_url(b"x")),
            GrantError::MalformedPayload,
        ),
    ] {
        assert_eq!(
            verifier.verify(&presented(token, ISSUED_AT + 1)),
            Err(expected.clone()),
            "framing case not refused: {token:?}"
        );
    }

    let oversized = "a".repeat(16 * 1024);
    assert_eq!(
        verifier.verify(&presented(&oversized, ISSUED_AT + 1)),
        Err(GrantError::TokenTooLarge)
    );
}

#[test]
fn a_malformed_attribute_value_is_rejected() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    for bad in [
        GrantAttributeValue::Uuid("ab".to_owned()),
        GrantAttributeValue::Uuid("zz".repeat(16)),
        GrantAttributeValue::Int8("not-a-number".to_owned()),
        GrantAttributeValue::Int8("9223372036854775808".to_owned()),
        GrantAttributeValue::Bytes("odd".to_owned()),
    ] {
        let mut with_bad = claims(issuer.key_id());
        with_bad.attributes = BTreeMap::from([("tenant".to_owned(), bad.clone())]);
        let token = issuer.issue_claims(&with_bad).unwrap();
        assert_eq!(
            verifier.verify(&presented(&token, ISSUED_AT + 1)),
            Err(GrantError::MalformedAttribute),
            "attribute was not refused: {bad:?}"
        );
    }
}

#[test]
fn a_grant_with_too_many_attributes_or_an_unnameable_principal_is_refused() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let verify = |claims: &DatasetGrantClaims| {
        verifier
            .verify(&presented(
                &issuer.issue_claims(claims).unwrap(),
                ISSUED_AT + 1,
            ))
            .map(|_| ())
    };
    let with_attributes = |count: usize| {
        let mut claims = claims(issuer.key_id());
        claims.attributes = (0..count)
            .map(|index| {
                (
                    format!("attribute-{index}"),
                    GrantAttributeValue::Bool(true),
                )
            })
            .collect();
        claims
    };
    assert_eq!(verify(&with_attributes(64)), Ok(()));
    assert_eq!(
        verify(&with_attributes(65)),
        Err(GrantError::TooManyAttributes)
    );

    for (subject, attribute, refusal) in [
        ("", "tenant", PrincipalError::InvalidSubject),
        ("user\u{7}", "tenant", PrincipalError::InvalidSubject),
        ("user-1", "", PrincipalError::InvalidAttributeName),
    ] {
        let mut unnameable = claims(issuer.key_id());
        unnameable.subject = subject.to_owned();
        unnameable.attributes =
            BTreeMap::from([(attribute.to_owned(), GrantAttributeValue::Bool(true))]);
        assert_eq!(
            verify(&unnameable),
            Err(GrantError::Principal(refusal)),
            "subject {subject:?}, attribute {attribute:?}"
        );
    }
}

#[test]
fn verifier_construction_rejects_unusable_key_sets() {
    let issuer = issuer("key-a");
    let key = issuer.verification_key().unwrap();
    assert_eq!(
        GrantVerifier::new(AUDIENCE, [key.clone(), key.clone()]).err(),
        Some(GrantKeyError::DuplicateKeyId)
    );
    assert_eq!(
        GrantVerifier::new(AUDIENCE, []).err(),
        Some(GrantKeyError::NoKeys)
    );
    assert_eq!(
        GrantVerifier::new("", [key]).err(),
        Some(GrantKeyError::InvalidAudience)
    );
    // A key id containing the framing separator could not be represented
    // unambiguously, so it is refused where it is introduced.
    assert_eq!(
        GrantVerificationKey::new("bad.id", [0; 32]).err(),
        Some(GrantKeyError::InvalidKeyId)
    );
    assert_eq!(
        GrantVerificationKey::new("", [0; 32]).err(),
        Some(GrantKeyError::InvalidKeyId)
    );
}

fn base64_url(bytes: &[u8]) -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    URL_SAFE_NO_PAD.encode(bytes)
}

fn decode_base64url(text: &str) -> Vec<u8> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    URL_SAFE_NO_PAD.decode(text).unwrap()
}

/// Signs bytes exactly as given, so a test can present a payload that a correct
/// issuer would never serialize.
fn sign_raw_payload(issuer: &TestIssuer, payload: &[u8]) -> String {
    encode_grant_token(payload, &issuer.sign(&grant_signing_input(payload)))
}

/// Signs the payload without the domain separator, as a foreign protocol
/// sharing this key would.
fn issuer_sign_without_domain(issuer: &TestIssuer, payload: &[u8]) -> String {
    encode_grant_token(payload, &issuer.sign(payload))
}

// --- The command capability ------------------------------------------------

/// The capability is authenticated data, exactly as signed: the collections
/// and the operations, and nothing either side of them.
#[test]
fn a_signed_command_capability_survives_verification_exactly() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let token = issuer
        .issue_with_commands(
            "user-1",
            tenant(),
            vec![
                capability("tasks", &[GrantCommandOperation::Update]),
                capability(
                    "projects",
                    &[GrantCommandOperation::Insert, GrantCommandOperation::Delete],
                ),
            ],
            GENERATION,
            ISSUED_AT,
            LIFETIME,
        )
        .unwrap();
    let verified = verifier.verify(&presented(&token, ISSUED_AT + 1)).unwrap();
    let commands = verified.commands();

    assert_eq!(commands.len(), 2);
    assert_eq!(
        commands.collections().collect::<Vec<_>>(),
        vec!["projects", "tasks"]
    );
    assert!(commands.permits("tasks", GrantCommandOperation::Update));
    // Per operation, not per collection: authorizing update authorizes update.
    assert!(!commands.permits("tasks", GrantCommandOperation::Insert));
    assert!(!commands.permits("tasks", GrantCommandOperation::Delete));
    assert!(commands.permits("projects", GrantCommandOperation::Insert));
    assert!(commands.permits("projects", GrantCommandOperation::Delete));
    assert!(!commands.permits("projects", GrantCommandOperation::Update));
    // Per collection, not per deployment: a collection the grant never named
    // is authorized for nothing at all.
    assert!(!commands.permits("incidents", GrantCommandOperation::Update));
    // And nothing here is a prefix or a pattern.
    assert!(!commands.permits("task", GrantCommandOperation::Update));
    assert!(!commands.permits("tasks ", GrantCommandOperation::Update));
}

/// A grant issued without a capability, and a payload with no `commands` key,
/// both authorize no write.
#[test]
fn a_grant_without_a_capability_authorizes_no_command_at_all() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);

    let minted = issuer
        .issue("user-1", tenant(), GENERATION, ISSUED_AT, LIFETIME)
        .unwrap();
    // No `commands` key at all.
    let payload = format!(
        r#"{{"version":{GRANT_VERSION},"keyId":"{}","audience":"{AUDIENCE}",
            "subject":"user-1","attributes":{{}},
            "authorizationGeneration":{GENERATION},
            "issuedAtUnixSeconds":{ISSUED_AT},
            "expiresAtUnixSeconds":{}}}"#,
        issuer.key_id(),
        ISSUED_AT + LIFETIME,
    );
    let legacy = sign_raw_payload(&issuer, payload.as_bytes());

    for token in [minted, legacy] {
        let verified = verifier.verify(&presented(&token, ISSUED_AT + 1)).unwrap();
        assert!(verified.commands().is_empty());
        assert_eq!(verified.commands().len(), 0);
        assert!(!verified
            .commands()
            .permits("tasks", GrantCommandOperation::Update));
        assert_eq!(verified.commands().operations("tasks"), None);
    }
}

/// An empty operation list is refused, never read as "every operation".
#[test]
fn an_empty_or_repeated_capability_is_refused_rather_than_narrowed() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);

    for (injected, expected) in [
        (
            r#"[{"collection":"tasks","operations":[]}]"#,
            GrantError::MalformedCommandCapability,
        ),
        (
            r#"[{"collection":"","operations":["update"]}]"#,
            GrantError::MalformedCommandCapability,
        ),
        (
            r#"[{"collection":"tasks","operations":["update","update"]}]"#,
            GrantError::MalformedCommandCapability,
        ),
        (
            r#"[{"collection":"ta sks","operations":["update"]}]"#,
            GrantError::MalformedCommandCapability,
        ),
        (
            r#"[{"collection":"tasks","operations":["update"]},
                {"collection":"tasks","operations":["delete"]}]"#,
            GrantError::DuplicateCommandCapability,
        ),
    ] {
        let token = sign_raw_payload(&issuer, capability_payload(&issuer, injected).as_bytes());
        assert_eq!(
            verifier.verify(&presented(&token, ISSUED_AT + 1)),
            Err(expected),
            "capability was not refused: {injected}"
        );
    }

    // The issuer refuses the same shapes rather than minting them.
    for unusable in [
        vec![capability("tasks", &[])],
        vec![capability("", &[GrantCommandOperation::Update])],
        vec![capability(
            "tasks",
            &[GrantCommandOperation::Update, GrantCommandOperation::Update],
        )],
        vec![
            capability("tasks", &[GrantCommandOperation::Update]),
            capability("tasks", &[GrantCommandOperation::Delete]),
        ],
    ] {
        assert_eq!(
            issuer.issue_with_commands(
                "user-1",
                tenant(),
                unusable,
                GENERATION,
                ISSUED_AT,
                LIFETIME
            ),
            Err(IssuerError::UnusableCommandCapability)
        );
    }
}

/// The capability vocabulary is closed: an unknown operation, an unknown field
/// inside a capability, and an oversized collection are refused rather than
/// ignored, so nothing a later reader might honour can be smuggled in.
#[test]
fn the_capability_vocabulary_is_closed_and_bounded() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);

    for injected in [
        r#"[{"collection":"tasks","operations":["truncate"]}]"#,
        r#"[{"collection":"tasks","operations":["*"]}]"#,
        r#"[{"collection":"tasks","operations":["update"],"columns":["ssn"]}]"#,
        r#"[{"collection":"tasks","operations":["update"],"table":"public.secrets"}]"#,
        r#"[{"collection":"tasks"}]"#,
        r#"[{"operations":["update"]}]"#,
        r#"{"tasks":["update"]}"#,
        r#""tasks:update""#,
    ] {
        let token = sign_raw_payload(&issuer, capability_payload(&issuer, injected).as_bytes());
        assert_eq!(
            verifier.verify(&presented(&token, ISSUED_AT + 1)),
            Err(GrantError::MalformedPayload),
            "capability shape was not refused: {injected}"
        );
    }

    let oversized = format!(
        r#"[{{"collection":"{}","operations":["update"]}}]"#,
        "c".repeat(129)
    );
    let token = sign_raw_payload(&issuer, capability_payload(&issuer, &oversized).as_bytes());
    assert_eq!(
        verifier.verify(&presented(&token, ISSUED_AT + 1)),
        Err(GrantError::MalformedCommandCapability)
    );

    let too_many = (0..65)
        .map(|index| format!(r#"{{"collection":"c{index}","operations":["update"]}}"#))
        .collect::<Vec<_>>()
        .join(",");
    let token = sign_raw_payload(
        &issuer,
        capability_payload(&issuer, &format!("[{too_many}]")).as_bytes(),
    );
    assert_eq!(
        verifier.verify(&presented(&token, ISSUED_AT + 1)),
        Err(GrantError::TooManyCommandCapabilities)
    );
}

/// A capability is only as good as the signature over it. Editing one byte of
/// an authorized collection, or grafting a wider one onto a validly signed
/// grant, fails the signature check before any claim is read.
#[test]
fn a_forged_or_widened_capability_fails_the_signature_check() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let token = issuer
        .issue_with_commands(
            "user-1",
            tenant(),
            vec![capability("tasks", &[GrantCommandOperation::Update])],
            GENERATION,
            ISSUED_AT,
            LIFETIME,
        )
        .unwrap();
    let (encoded_payload, encoded_signature) = token.split_once('.').unwrap();
    let text = String::from_utf8(decode_base64url(encoded_payload)).unwrap();
    assert!(text.contains(r#""commands":[{"collection":"tasks","operations":["update"]}]"#));

    for widened in [
        text.replace("\"update\"", "\"delete\""),
        text.replace("\"update\"", "\"update\",\"insert\""),
        text.replace("\"tasks\"", "\"secrets\""),
    ] {
        let forged = encode_grant_token(widened.as_bytes(), &decode_base64url(encoded_signature));
        assert_eq!(
            verifier.verify(&presented(&forged, ISSUED_AT + 1)),
            Err(GrantError::InvalidSignature),
            "a rewritten capability verified: {widened}"
        );
    }
}

/// A capability does not survive the fence, the clock, or the audience of the
/// grant that carries it.
#[test]
fn a_capability_is_bounded_by_the_grant_that_carries_it() {
    let issuer = issuer("key-a");
    let verifier = verifier_for(&[&issuer]);
    let token = issuer
        .issue_with_commands(
            "user-1",
            tenant(),
            vec![capability("tasks", &[GrantCommandOperation::Update])],
            GENERATION,
            ISSUED_AT,
            LIFETIME,
        )
        .unwrap();

    assert_eq!(
        verifier.verify(&presented(&token, ISSUED_AT + LIFETIME)),
        Err(GrantError::Expired)
    );
    assert_eq!(
        verifier.verify(&GrantPresentation {
            token: &token,
            now_unix_seconds: ISSUED_AT + 1,
            authorization_generation: GENERATION + 1,
        }),
        Err(GrantError::AuthorizationGenerationMismatch {
            grant: GENERATION,
            current: GENERATION + 1,
        })
    );
    let other = GrantVerifier::new("other.test", [issuer.verification_key().unwrap()]).unwrap();
    assert_eq!(
        other.verify(&presented(&token, ISSUED_AT + 1)),
        Err(GrantError::AudienceMismatch)
    );
}

/// The payload text a capability case needs, with everything but `commands`
/// held at the values every other test uses.
fn capability_payload(issuer: &TestIssuer, commands: &str) -> String {
    format!(
        r#"{{"version":{GRANT_VERSION},"keyId":"{}","audience":"{AUDIENCE}",
            "subject":"user-1","attributes":{{}},
            "authorizationGeneration":{GENERATION},
            "issuedAtUnixSeconds":{ISSUED_AT},
            "expiresAtUnixSeconds":{},"commands":{commands}}}"#,
        issuer.key_id(),
        ISSUED_AT + LIFETIME,
    )
}

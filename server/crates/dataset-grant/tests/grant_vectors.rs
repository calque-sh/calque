//! The frozen v1 dataset-grant payload vectors (`spec/fixtures/dataset-grant-v1`).
//!
//! Every negative vector is a payload signed in the test, so each refusal
//! proves the claim check rather than the signature check; framing and
//! signature negatives live in `grant_verification.rs`. Each vector's bytes are
//! pinned by SHA-256, and the issuer's serialization of the same claims must
//! reproduce them. Ed25519 is deterministic, so the test-only key signs each
//! positive payload to one token: the Rust issuer must mint it and the verifier
//! accept it, and `client/packages/grant` holds the Node issuer
//! to the same files.

use std::collections::BTreeMap;

use authz::PrincipalValue;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use dataset_grant::{
    grant_signing_input, DatasetGrantClaims, GrantAttributeValue, GrantCommandCapability,
    GrantCommandOperation, GrantError, GrantPresentation, GrantVerificationKey, GrantVerifier,
    GRANT_VERSION, MAX_GRANT_LIFETIME_SECONDS,
};
use dataset_grant_issuer::GrantIssuer;
use ring::signature::Ed25519KeyPair;

const AUDIENCE: &str = "calque.vectors.test";
const KEY_ID: &str = "grant-vectors-key";
const GENERATION: u64 = 9;
const ISSUED_AT: u64 = 1_800_000_000;
const VECTORS: &str = "../../../spec/fixtures/dataset-grant-v1";

/// Every frozen vector and the SHA-256 of its bytes, so a changed file fails on
/// its digest first.
const FROZEN: &[(&str, &str)] = &[
    (
        "read-only.payload.json",
        "5c76d16cae4f36d132d6a7e1add00d52158346760665bc33d241cfb31b46e398",
    ),
    (
        "command-capability.payload.json",
        "0309176ab77bd959d6add4aed8947a40bcb306fe81ba3542050a202b45cad990",
    ),
    (
        "invalid/truncated.payload.json",
        "e2fad41781e6fef6040064608ed260587047c999f13d6352cbbd37c877c1ef9c",
    ),
    (
        "invalid/oversized.payload.json",
        "d3221201fc296beab0cbc6621a4f0de14103c54551dd996689a45e1f032797c3",
    ),
    (
        "invalid/capability-empty-operations.payload.json",
        "b6899a5e79b2d1e6a1b4c3a3d6cd51cbcee96813089cd0f28dccde00f202d340",
    ),
    (
        "invalid/capability-unknown-operation.payload.json",
        "e95e40bf6f763feda3bfc5a80c62f3a5c2e7cd2867aead9020cb2e091a8b71cd",
    ),
    (
        "invalid/capability-unknown-field.payload.json",
        "8da87f5fca964bdf501177bd14a4333fdac8cf792d7b4280ea5b880debb20427",
    ),
    (
        "invalid/capability-duplicate-collection.payload.json",
        "b8ac60f9ecff681232f313ce6e156acfa221993bf68c7c4a775b461457256f9e",
    ),
    (
        "invalid/cross-generation.payload.json",
        "07886d99df22cf5f1dffdcaf1da4faddeba1a030e703f7e3b9d74c4f6f791eb1",
    ),
    (
        "invalid/duplicate-attribute.payload.json",
        "f0944951e05f68be1030d443050052dc3ecba16f55deaec06a5a5672b96ffa6c",
    ),
    (
        "attribute-types.payload.json",
        "572cfc79732be176834e1b0619fabcfbdfd5e9542aa25e2a82b7d93cd99ba060",
    ),
    (
        TEST_ONLY_KEY,
        "486b0a41b3e7059d6b4aefa78cfb21a8f2662b6258bdd53227468295d892705f",
    ),
    (
        "test-only-grant-key.public.raw",
        "21fe31dfa154a261626bf854046fd2271b7bed4b6abe45aa58877ef47f9721b9",
    ),
    (
        "test-only-verification-key.json",
        "7a55e6155e751ab14de0b25a342b1b83a2ffc1d1e624dbe3e6c61ef9318801d1",
    ),
    (
        "read-only.token",
        "f8b4f811ba282336055c5aeda9f078ed143fc7bd83669a4b479b6c8e6746a722",
    ),
    (
        "command-capability.token",
        "154c1dde5ca85d721f2992ef17dc03cef5f9a8071ade4a8d19d23690fd4eec43",
    ),
    (
        "attribute-types.token",
        "6b201d2c76aaeba4da34f73f58bab7c3aad7fcd14dd1e8be0a352c6006507cec",
    ),
];

/// The signing key the token vectors are made with: the RFC 8032 §7.1 TEST 1
/// key, public by construction, so no deployment may ever trust it.
const TEST_ONLY_KEY: &str = "test-only-grant-key.pk8";

/// Each token vector: the payload it signs and the exact token.
const TOKENS: &[(&str, &str)] = &[
    ("read-only.payload.json", "read-only.token"),
    (
        "command-capability.payload.json",
        "command-capability.token",
    ),
    ("attribute-types.payload.json", "attribute-types.token"),
];

fn vector(name: &str) -> Vec<u8> {
    std::fs::read(format!("{VECTORS}/{name}"))
        .unwrap_or_else(|error| panic!("{VECTORS}/{name}: {error}"))
}

fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    hex::encode(Sha256::digest(bytes))
}

struct Signer {
    verifier: GrantVerifier,
    key_pair: Ed25519KeyPair,
}

impl Signer {
    fn new() -> Self {
        let (issuer, pkcs8) = GrantIssuer::generate(KEY_ID, AUDIENCE).unwrap();
        Self {
            verifier: GrantVerifier::new(AUDIENCE, [issuer.verification_key().unwrap()]).unwrap(),
            key_pair: Ed25519KeyPair::from_pkcs8(&pkcs8).unwrap(),
        }
    }

    /// Signs the frozen payload exactly as given, without normalizing it.
    fn present(&self, payload: &[u8]) -> String {
        let signature = self.key_pair.sign(&grant_signing_input(payload));
        format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        )
    }

    fn verify(&self, payload: &[u8]) -> Result<dataset_grant::VerifiedDatasetGrant, GrantError> {
        let token = self.present(payload);
        self.verifier.verify(&GrantPresentation {
            token: &token,
            now_unix_seconds: ISSUED_AT + 1,
            authorization_generation: GENERATION,
        })
    }
}

#[test]
fn the_frozen_payload_bytes_have_not_changed() {
    for (name, expected) in FROZEN {
        assert_eq!(&digest(&vector(name)), expected, "{name}");
    }
}

/// The v1 grant with no capability: it parses, it authorizes no command, and
/// an issuer that carries no capability serializes exactly these bytes, which
/// holds only while `commands` is skipped when empty.
#[test]
fn the_read_only_vector_verifies_and_authorizes_no_command() {
    let signer = Signer::new();
    let payload = vector("read-only.payload.json");
    let verified = signer.verify(&payload).unwrap();
    assert_eq!(verified.principal().subject(), "member-1");
    assert!(verified.commands().is_empty());

    let text = String::from_utf8(payload).unwrap();
    assert!(
        !text.contains("commands"),
        "the pre-capability wire form names no commands key: {text}"
    );
    let claims = DatasetGrantClaims {
        version: GRANT_VERSION,
        key_id: KEY_ID.to_owned(),
        audience: AUDIENCE.to_owned(),
        subject: "member-1".to_owned(),
        attributes: BTreeMap::from([(
            "organization_id".to_owned(),
            GrantAttributeValue::Uuid("0a1b2c3d4e5f6071829304a5b6c7d8e9".to_owned()),
        )]),
        authorization_generation: GENERATION,
        issued_at_unix_seconds: ISSUED_AT,
        expires_at_unix_seconds: ISSUED_AT + 300,
        commands: Vec::new(),
    };
    assert_eq!(
        String::from_utf8(serde_json::to_vec(&claims).unwrap()).unwrap(),
        text,
        "an issuer carrying no capability no longer mints the pre-capability bytes"
    );
}

/// The v1 grant with a capability, and the bytes an issuer produces for it.
#[test]
fn the_capability_vector_verifies_and_is_what_an_issuer_serializes() {
    let signer = Signer::new();
    let payload = vector("command-capability.payload.json");
    let verified = signer.verify(&payload).unwrap();
    let commands = verified.commands();
    assert_eq!(
        commands.collections().collect::<Vec<_>>(),
        vec!["projects", "tasks"]
    );
    assert!(commands.permits("projects", GrantCommandOperation::Update));
    assert!(!commands.permits("projects", GrantCommandOperation::Delete));
    for operation in [
        GrantCommandOperation::Insert,
        GrantCommandOperation::Update,
        GrantCommandOperation::Delete,
    ] {
        assert!(commands.permits("tasks", operation));
    }

    let claims = DatasetGrantClaims {
        version: GRANT_VERSION,
        key_id: KEY_ID.to_owned(),
        audience: AUDIENCE.to_owned(),
        subject: "member-1".to_owned(),
        attributes: BTreeMap::from([(
            "organization_id".to_owned(),
            GrantAttributeValue::Uuid("0a1b2c3d4e5f6071829304a5b6c7d8e9".to_owned()),
        )]),
        authorization_generation: GENERATION,
        issued_at_unix_seconds: ISSUED_AT,
        expires_at_unix_seconds: ISSUED_AT + 300,
        commands: vec![
            GrantCommandCapability {
                collection: "projects".to_owned(),
                operations: vec![GrantCommandOperation::Update],
            },
            GrantCommandCapability {
                collection: "tasks".to_owned(),
                operations: vec![
                    GrantCommandOperation::Insert,
                    GrantCommandOperation::Update,
                    GrantCommandOperation::Delete,
                ],
            },
        ],
    };
    assert_eq!(
        String::from_utf8(serde_json::to_vec(&claims).unwrap()).unwrap(),
        String::from_utf8(payload).unwrap(),
        "an issuer no longer serializes the frozen v1 payload"
    );
}

/// The negative vectors, each validly signed, each refused for the reason its
/// name gives. A refusal is of the whole grant: nothing here parses into a
/// narrower capability that a later reader might honour.
#[test]
fn every_invalid_vector_is_refused_for_its_own_reason() {
    let signer = Signer::new();
    let expectations = [
        ("truncated.payload.json", GrantError::MalformedPayload),
        ("oversized.payload.json", GrantError::TokenTooLarge),
        (
            "capability-unknown-operation.payload.json",
            GrantError::MalformedPayload,
        ),
        (
            "capability-unknown-field.payload.json",
            GrantError::MalformedPayload,
        ),
        (
            "capability-empty-operations.payload.json",
            GrantError::MalformedCommandCapability,
        ),
        (
            "capability-duplicate-collection.payload.json",
            GrantError::DuplicateCommandCapability,
        ),
        (
            "cross-generation.payload.json",
            GrantError::AuthorizationGenerationMismatch {
                grant: 10,
                current: GENERATION,
            },
        ),
        (
            "duplicate-attribute.payload.json",
            GrantError::DuplicateAttribute,
        ),
    ];
    for (name, expected) in &expectations {
        assert_eq!(
            signer.verify(&vector(&format!("invalid/{name}"))),
            Err(expected.clone()),
            "{name}"
        );
    }

    // The manifest names each vector's refusal for a reader in another
    // language. It is held to the list above, whole, so it can neither name a
    // refusal the verifier does not return nor omit a vector this test drives.
    let manifest: serde_json::Value = serde_json::from_slice(&vector("manifest.json")).unwrap();
    let mut recorded: Vec<(String, String)> = manifest["fixtures"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["file"].as_str().unwrap().starts_with("invalid/"))
        .map(|entry| {
            (
                entry["file"].as_str().unwrap().to_owned(),
                entry["refusal"].as_str().unwrap_or("<none>").to_owned(),
            )
        })
        .collect();
    let mut expected: Vec<(String, String)> = expectations
        .iter()
        .map(|(name, error)| {
            let debug = format!("{error:?}");
            let variant = debug
                .split(|character: char| !character.is_ascii_alphanumeric())
                .next()
                .unwrap()
                .to_owned();
            (format!("invalid/{name}"), variant)
        })
        .collect();
    recorded.sort();
    expected.sort();
    assert_eq!(recorded, expected);
}

/// A repeated attribute name is refused with its own error, not resolved to
/// the last value. The vector repeats `organization_id` with a second value;
/// without the first entry the same payload verifies, so the repeat is the
/// only thing refused.
#[test]
fn a_repeated_attribute_name_is_refused_rather_than_resolved_to_the_last_value() {
    let signer = Signer::new();
    let repeated = text("invalid/duplicate-attribute.payload.json");
    assert_eq!(
        signer.verify(repeated.as_bytes()),
        Err(GrantError::DuplicateAttribute)
    );

    let first = r#""organization_id":{"uuid":"0a1b2c3d4e5f6071829304a5b6c7d8e9"},"#;
    assert_eq!(repeated.matches(first).count(), 1);
    let once = repeated.replacen(first, "", 1);
    let verified = signer.verify(once.as_bytes()).unwrap();
    assert_eq!(
        verified.principal().attribute("organization_id"),
        Some(&PrincipalValue::Uuid([0xff; 16]))
    );
}

/// Every other name a payload can carry twice is a struct field or an
/// externally tagged value, which serde refuses without help; these hold that.
#[test]
fn every_other_repeated_name_in_the_payload_does_not_parse() {
    let signer = Signer::new();
    let read_only = text("read-only.payload.json");
    let capability = text("command-capability.payload.json");
    for (case, original, payload) in [
        (
            "a repeated top-level field",
            &read_only,
            read_only.replacen(
                r#""subject":"member-1","#,
                r#""subject":"member-1","subject":"member-2","#,
                1,
            ),
        ),
        (
            "a repeated field inside one capability",
            &capability,
            capability.replacen(
                r#"{"collection":"projects","#,
                r#"{"collection":"projects","collection":"tasks","#,
                1,
            ),
        ),
        (
            "an attribute value naming its type twice",
            &read_only,
            read_only.replacen(
                r#"{"uuid":"0a1b2c3d4e5f6071829304a5b6c7d8e9"}"#,
                r#"{"uuid":"0a1b2c3d4e5f6071829304a5b6c7d8e9","uuid":"ffffffffffffffffffffffffffffffff"}"#,
                1,
            ),
        ),
    ] {
        assert_ne!(&payload, original, "{case}: the edit did not apply");
        assert_eq!(
            signer.verify(payload.as_bytes()),
            Err(GrantError::MalformedPayload),
            "{case}"
        );
    }
}

fn text(name: &str) -> String {
    String::from_utf8(vector(name)).unwrap_or_else(|error| panic!("{name}: {error}"))
}

fn test_only_issuer() -> GrantIssuer {
    GrantIssuer::from_pkcs8(KEY_ID, AUDIENCE, &vector(TEST_ONLY_KEY)).unwrap()
}

/// A verifier configured from the frozen raw public key rather than from the
/// issuer, so it trusts exactly the bytes the vectors publish.
fn frozen_verifier() -> GrantVerifier {
    let public: [u8; 32] = vector("test-only-grant-key.public.raw")
        .try_into()
        .expect("the raw public key is 32 bytes");
    GrantVerifier::new(
        AUDIENCE,
        [GrantVerificationKey::new(KEY_ID, public).unwrap()],
    )
    .unwrap()
}

fn present_frozen(token: &str) -> Result<dataset_grant::VerifiedDatasetGrant, GrantError> {
    frozen_verifier().verify(&GrantPresentation {
        token,
        now_unix_seconds: ISSUED_AT + 1,
        authorization_generation: GENERATION,
    })
}

/// The test-only key, loaded the way `calque-dev-grant` loads `grant-key.pk8`,
/// prints the frozen verification-key value over the frozen raw public key.
#[test]
fn the_test_only_key_prints_the_frozen_verification_key() {
    let environment = test_only_issuer().verification_key_environment().unwrap();
    assert_eq!(environment, text("test-only-verification-key.json"));
    let value: serde_json::Value = serde_json::from_str(&environment).unwrap();
    assert_eq!(
        URL_SAFE_NO_PAD
            .decode(value["publicKey"].as_str().unwrap())
            .unwrap(),
        vector("test-only-grant-key.public.raw")
    );
}

/// The issuer mints each frozen token exactly from the claims its payload
/// states, and the verifier accepts it.
#[test]
fn each_frozen_token_is_what_the_issuer_mints_and_the_verifier_accepts() {
    let issuer = test_only_issuer();
    for (payload, token) in TOKENS {
        let claims: DatasetGrantClaims = serde_json::from_slice(&vector(payload)).unwrap();
        assert_eq!(
            issuer.issue_claims(&claims).unwrap(),
            text(token),
            "the issuer no longer mints {token}"
        );
        present_frozen(&text(token)).unwrap_or_else(|error| panic!("{token}: {error}"));
    }
}

/// Every attribute type, in the order a Rust `BTreeMap` writes them, which is
/// UTF-8 byte order: integer-like names sort as text, upper case before lower,
/// and U+FF5A before U+1D467, the pair UTF-16 order reverses. An issuer whose
/// maps keep another order has to sort to produce these bytes.
#[test]
fn the_attribute_types_vector_is_what_an_issuer_serializes_and_verifies_exactly() {
    let uuid = |value: &str| GrantAttributeValue::Uuid(value.to_owned());
    let claims = DatasetGrantClaims {
        version: GRANT_VERSION,
        key_id: KEY_ID.to_owned(),
        audience: AUDIENCE.to_owned(),
        subject: "member-7 \"Zoë\" \\o/".to_owned(),
        attributes: BTreeMap::from([
            (
                "tenant".to_owned(),
                GrantAttributeValue::Int8("-9223372036854775808".to_owned()),
            ),
            ("active".to_owned(), GrantAttributeValue::Bool(true)),
            ("region".to_owned(), GrantAttributeValue::Int2(-32_768)),
            ("seat".to_owned(), GrantAttributeValue::Int4(2_147_483_647)),
            (
                "salt".to_owned(),
                GrantAttributeValue::Bytes("00FF10".to_owned()),
            ),
            (
                "organization_id".to_owned(),
                uuid("0a1b2c3d4e5f6071829304a5b6c7d8e9"),
            ),
            ("Zone".to_owned(), uuid("0A1B2C3D4E5F6071829304A5B6C7D8E9")),
            ("10".to_owned(), GrantAttributeValue::Int4(10)),
            ("9".to_owned(), GrantAttributeValue::Int4(9)),
            ("\u{1D467}".to_owned(), GrantAttributeValue::Bool(true)),
            ("\u{FF5A}".to_owned(), GrantAttributeValue::Bool(false)),
        ]),
        authorization_generation: GENERATION,
        issued_at_unix_seconds: ISSUED_AT,
        expires_at_unix_seconds: ISSUED_AT + MAX_GRANT_LIFETIME_SECONDS,
        commands: vec![
            GrantCommandCapability {
                collection: "tasks".to_owned(),
                operations: vec![GrantCommandOperation::Delete, GrantCommandOperation::Insert],
            },
            GrantCommandCapability {
                collection: "projects".to_owned(),
                operations: vec![GrantCommandOperation::Update],
            },
        ],
    };
    assert_eq!(
        String::from_utf8(serde_json::to_vec(&claims).unwrap()).unwrap(),
        text("attribute-types.payload.json"),
        "an issuer no longer serializes the frozen attribute-types payload"
    );

    let verified = present_frozen(&text("attribute-types.token")).unwrap();
    let principal = verified.principal();
    assert_eq!(principal.subject(), "member-7 \"Zoë\" \\o/");
    let organization = PrincipalValue::Uuid([
        0x0a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60, 0x71, 0x82, 0x93, 0x04, 0xa5, 0xb6, 0xc7, 0xd8,
        0xe9,
    ]);
    for (name, expected) in [
        ("tenant", PrincipalValue::Int8(i64::MIN)),
        ("active", PrincipalValue::Bool(true)),
        ("region", PrincipalValue::Int2(i16::MIN)),
        ("seat", PrincipalValue::Int4(i32::MAX)),
        ("salt", PrincipalValue::Bytes(vec![0x00, 0xff, 0x10])),
        ("organization_id", organization.clone()),
        ("Zone", organization),
        ("10", PrincipalValue::Int4(10)),
        ("9", PrincipalValue::Int4(9)),
        ("\u{1D467}", PrincipalValue::Bool(true)),
        ("\u{FF5A}", PrincipalValue::Bool(false)),
    ] {
        assert_eq!(principal.attribute(name), Some(&expected), "{name}");
    }
    let commands = verified.commands();
    assert!(commands.permits("tasks", GrantCommandOperation::Delete));
    assert!(commands.permits("tasks", GrantCommandOperation::Insert));
    assert!(!commands.permits("tasks", GrantCommandOperation::Update));
    assert!(commands.permits("projects", GrantCommandOperation::Update));
}

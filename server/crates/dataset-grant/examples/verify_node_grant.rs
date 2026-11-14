//! A dataset grant and a tenant order that `@calque/grant` signed with a key it
//! generated, verified by Calque's own verifier from the verification-key
//! value `calque-grant keygen` printed.
//!
//! `scripts/grant-package-cross-language.test.ts` writes the fixture and runs
//! `cargo run -p dataset-grant --example verify_node_grant -- <directory>`,
//! which panics on any mismatch and prints `verified` once both check out.

use std::{fs, path::PathBuf};

use authz::PrincipalValue;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use dataset_grant::{
    GrantCommandOperation, GrantPresentation, GrantVerificationKey, GrantVerifier, TenantAction,
};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    audience: String,
    generation: u64,
    now_unix_seconds: u64,
    verification_key: VerificationKey,
    grant: String,
    tenant_order: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VerificationKey {
    key_id: String,
    public_key: String,
}

fn main() {
    let directory = std::env::args()
        .nth(1)
        .expect("the first argument names the directory the Node test wrote");
    let fixture: Fixture = serde_json::from_slice(
        &fs::read(PathBuf::from(directory).join("fixture.json")).expect("the fixture is there"),
    )
    .expect("the fixture is JSON of the expected shape");

    let public: [u8; 32] = URL_SAFE_NO_PAD
        .decode(&fixture.verification_key.public_key)
        .expect("publicKey is unpadded base64url")
        .try_into()
        .expect("publicKey is 32 bytes");
    let verifier = GrantVerifier::new(
        fixture.audience.clone(),
        [GrantVerificationKey::new(fixture.verification_key.key_id.clone(), public).unwrap()],
    )
    .unwrap();

    let verified = verifier
        .verify(&GrantPresentation {
            token: &fixture.grant,
            now_unix_seconds: fixture.now_unix_seconds,
            authorization_generation: fixture.generation,
        })
        .unwrap();
    assert_eq!(verified.principal().subject(), "cross-language-subject");
    assert_eq!(
        verified.principal().attribute("tenant"),
        Some(&PrincipalValue::Int8(9_007_199_254_740_993))
    );
    assert_eq!(
        verified.expires_at_unix_seconds(),
        fixture.now_unix_seconds + 899
    );
    assert!(verified
        .commands()
        .permits("tasks", GrantCommandOperation::Update));
    assert!(verified
        .commands()
        .permits("tasks", GrantCommandOperation::Delete));
    assert!(!verified
        .commands()
        .permits("tasks", GrantCommandOperation::Insert));

    let order = verifier
        .verify_tenant_order(&fixture.tenant_order, fixture.now_unix_seconds)
        .unwrap();
    assert_eq!(order.action, TenantAction::Pin);
    assert_eq!(order.tenant, PrincipalValue::Int8(9_007_199_254_740_993));
    println!("verified");
}

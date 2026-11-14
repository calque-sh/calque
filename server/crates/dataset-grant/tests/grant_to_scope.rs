//! A verified grant becomes a principal, and the server-side policy, not the
//! grant, decides what that principal sees.

use std::collections::BTreeMap;

use authz::{AuthorizationPolicy, PolicyDefinition, PrincipalResolver, TablePermitDefinition};
use dataset_grant::{GrantAttributeValue, GrantPresentation, GrantVerifier};
use dataset_grant_issuer::GrantIssuer;
use source_control::{ColumnInspection, QualifiedTable, RegisteredTable, TableInspection};
use sync_types::{ColumnId, TableId};

const AUDIENCE: &str = "calque.example.test";
const GENERATION: u64 = 7;
const ISSUED_AT: u64 = 1_800_000_000;
const LIFETIME: u64 = 300;

fn column(
    attribute_number: i16,
    name: &str,
    type_name: &str,
    primary_key_ordinal: Option<u32>,
) -> ColumnInspection {
    ColumnInspection {
        attribute_number,
        name: name.to_owned(),
        type_oid: u32::try_from(attribute_number).unwrap() + 1_000,
        type_modifier: -1,
        type_schema: "pg_catalog".to_owned(),
        type_name: type_name.to_owned(),
        nullable: false,
        primary_key_ordinal,
        generated: false,
        nondeterministic_collation: false,
        carried_as: None,
    }
}

fn table() -> RegisteredTable {
    RegisteredTable {
        table_id: TableId::new("table:tasks:g1").unwrap(),
        column_ids: (1..=3).map(ColumnId::new).collect(),
        inspection: TableInspection {
            table: QualifiedTable {
                schema: "app".to_owned(),
                table: "tasks".to_owned(),
            },
            relation_oid: 16_384,
            in_publication: true,
            relation_kind: 'r',
            persistence: 'p',
            replica_identity: 'd',
            is_partition: false,
            has_publication_row_filter: false,
            has_publication_column_list: false,
            unlisted_columns: Vec::new(),
            columns: vec![
                column(1, "tenant_id", "uuid", Some(1)),
                column(2, "id", "uuid", Some(2)),
                // Not part of the primary key: a withheld column must be one
                // the policy is actually allowed to withhold.
                column(3, "title", "text", None),
            ],
        },
    }
}

/// A source-wide policy over columns 1 and 2 only, so column 3 is withheld.
fn policy() -> AuthorizationPolicy {
    AuthorizationPolicy::compile(
        PolicyDefinition {
            generation: GENERATION,
            tables: vec![TablePermitDefinition {
                table_id: table().table_id,
                permitted_columns: vec![ColumnId::new(1), ColumnId::new(2)],
            }],
        },
        &[table()],
    )
    .unwrap()
}

fn grant_for(issuer: &GrantIssuer, subject: &str, tenant_byte: u8) -> String {
    issuer
        .issue(
            subject,
            BTreeMap::from([(
                "tenant".to_owned(),
                GrantAttributeValue::Uuid(format!("{tenant_byte:02x}").repeat(16)),
            )]),
            GENERATION,
            ISSUED_AT,
            LIFETIME,
        )
        .unwrap()
}

fn verifier(issuer: &GrantIssuer) -> GrantVerifier {
    GrantVerifier::new(AUDIENCE, [issuer.verification_key().unwrap()]).unwrap()
}

#[test]
fn the_scope_is_the_policys_projection_and_not_the_grants() {
    let (issuer, _) = GrantIssuer::generate("key-a", AUDIENCE).unwrap();
    let verifier = verifier(&issuer);
    verifier
        .verify(&GrantPresentation {
            token: &grant_for(&issuer, "user-1", 0x11),
            now_unix_seconds: ISSUED_AT + 1,
            authorization_generation: GENERATION,
        })
        .unwrap();
    // The grant carries a tenant attribute, and `scope` takes nothing from it.
    let scope = policy().scope();

    // Columns 1 and 2 are configured server-side; column 3 is not, and no grant
    // can name it, because a grant has no column vocabulary at all.
    assert!(scope.permits_column(&table().table_id, ColumnId::new(1)));
    assert!(scope.permits_column(&table().table_id, ColumnId::new(2)));
    assert!(!scope.permits_column(&table().table_id, ColumnId::new(3)));
    assert_eq!(scope.permitted_tables().len(), 1);
}

#[test]
fn the_verifier_is_usable_through_the_principal_resolver_seam() {
    // Through the trait, so a server can resolve principals without knowing
    // grants exist.
    let (issuer, _) = GrantIssuer::generate("key-a", AUDIENCE).unwrap();
    let verifier = verifier(&issuer);
    let token = grant_for(&issuer, "user-1", 0x11);
    let resolved = PrincipalResolver::resolve(
        &verifier,
        &GrantPresentation {
            token: &token,
            now_unix_seconds: ISSUED_AT + 1,
            authorization_generation: GENERATION,
        },
    )
    .unwrap();
    assert_eq!(resolved.subject(), "user-1");
}

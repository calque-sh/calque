//! One scope per partition value: the router, the scope hash, the grant's
//! value and the write check.
//!
//! Every tenant here repeats every key value but the partition column, so a
//! router or a check that ignored the partition would hand one tenant another's
//! row and fail.

use std::collections::BTreeMap;

use authz::{
    AuthorizationPolicy, CanonicalAuthorizationError, PartitionDefinition, PartitionTouch,
    PartitionValue, PolicyDefinition, PolicyError, Principal, PrincipalValue, ScopeError,
    TablePermitDefinition,
};
use canonical_log::{CanonicalOperation, CanonicalOperationKind, CellValue, ColumnValue};
use source_control::{ColumnInspection, QualifiedTable, RegisteredTable, TableInspection};
use sync_protocol::{
    change::{cell_value::Kind, CellValue as WireCell, ColumnValue as WireColumn, PrimaryKey},
    encode_primary_key,
};
use sync_types::{ColumnId, TableId};

const TENANT_A: i64 = 7;
const TENANT_B: i64 = 8;
const TENANT_C: i64 = 9;

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
        nullable: primary_key_ordinal.is_none(),
        primary_key_ordinal,
        generated: false,
        nondeterministic_collation: false,
    }
}

fn registered(name: &str, columns: Vec<ColumnInspection>) -> RegisteredTable {
    RegisteredTable {
        table_id: TableId::new(format!("table:{name}:g1")).unwrap(),
        column_ids: (1..=u32::try_from(columns.len()).unwrap())
            .map(ColumnId::new)
            .collect(),
        inspection: TableInspection {
            table: QualifiedTable {
                schema: "app".to_owned(),
                table: name.to_owned(),
            },
            relation_oid: 16_384,
            in_publication: true,
            relation_kind: 'r',
            persistence: 'p',
            replica_identity: 'd',
            is_partition: false,
            has_publication_row_filter: false,
            has_publication_column_list: false,
            columns,
        },
    }
}

/// `issues (tenant_id int8, id int8, title text)` keyed by `(tenant_id, id)`.
fn issues() -> RegisteredTable {
    registered(
        "issues",
        vec![
            column(1, "tenant_id", "int8", Some(1)),
            column(2, "id", "int8", Some(2)),
            column(3, "title", "text", None),
        ],
    )
}

/// `plans (id int8, name text)`, shared by every tenant.
fn plans() -> RegisteredTable {
    registered(
        "plans",
        vec![
            column(1, "id", "int8", Some(1)),
            column(2, "name", "text", None),
        ],
    )
}

fn tables() -> Vec<RegisteredTable> {
    vec![issues(), plans()]
}

fn definition(generation: u64) -> PolicyDefinition {
    PolicyDefinition {
        generation,
        tables: tables()
            .into_iter()
            .map(|table| TablePermitDefinition {
                table_id: table.table_id,
                permitted_columns: table.column_ids,
            })
            .collect(),
    }
}

fn partition() -> PartitionDefinition {
    PartitionDefinition {
        attribute: "tenant".to_owned(),
        tables: BTreeMap::from([(issues().table_id, ColumnId::new(1))]),
    }
}

fn policy() -> AuthorizationPolicy {
    AuthorizationPolicy::compile_partitioned_detached(definition(3), partition(), &tables())
        .unwrap()
}

fn principal(attributes: &[(&str, PrincipalValue)]) -> Principal {
    Principal::new(
        "user",
        attributes
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone()))
            .collect(),
    )
    .unwrap()
}

fn tenant(value: i64) -> Principal {
    principal(&[("tenant", PrincipalValue::Int8(value))])
}

fn key(parts: &[(u32, i64)]) -> Vec<u8> {
    encode_primary_key(&PrimaryKey {
        codec_version: 1,
        parts: parts
            .iter()
            .map(|(column_id, value)| WireColumn {
                column_id: *column_id,
                value: Some(WireCell {
                    kind: Some(Kind::Int64Value(*value)),
                }),
            })
            .collect(),
    })
    .unwrap()
}

fn issue_key(tenant: i64, id: i64) -> Vec<u8> {
    key(&[(1, tenant), (2, id)])
}

fn cell(column_id: u32, value: CellValue) -> ColumnValue {
    ColumnValue {
        column_id: ColumnId::new(column_id),
        value,
    }
}

fn insert_issue(tenant: i64, id: i64) -> CanonicalOperation {
    CanonicalOperation {
        table_id: issues().table_id,
        primary_key: issue_key(tenant, id),
        kind: CanonicalOperationKind::InsertFullRow {
            columns: vec![
                cell(1, CellValue::Int64(tenant)),
                cell(2, CellValue::Int64(id)),
                cell(3, CellValue::Text(format!("issue {id}"))),
            ],
        },
    }
}

/// An update at `from`'s key whose new tuple names `to`, as the canonical log
/// records an update that changes the key.
fn update_issue(from: i64, to: i64, id: i64) -> CanonicalOperation {
    CanonicalOperation {
        table_id: issues().table_id,
        primary_key: issue_key(from, id),
        kind: CanonicalOperationKind::UpdatePatch {
            changed_columns: vec![
                cell(1, CellValue::Int64(to)),
                cell(2, CellValue::Int64(id)),
                cell(3, CellValue::Text("renamed".to_owned())),
            ],
        },
    }
}

fn delete_issue(tenant: i64, id: i64) -> CanonicalOperation {
    CanonicalOperation {
        table_id: issues().table_id,
        primary_key: issue_key(tenant, id),
        kind: CanonicalOperationKind::DeleteByKey,
    }
}

fn routed(policy: &AuthorizationPolicy, tenant_value: i64, operation: &CanonicalOperation) -> bool {
    policy
        .scope_for(&tenant(tenant_value))
        .unwrap()
        .route_operation(operation)
        .unwrap()
        .is_some()
}

#[test]
fn each_tenant_receives_its_own_rows_and_no_other_tenants() {
    let policy = policy();
    for operation in [
        insert_issue(TENANT_A, 1),
        update_issue(TENANT_A, TENANT_A, 1),
        delete_issue(TENANT_A, 1),
    ] {
        assert!(routed(&policy, TENANT_A, &operation), "{operation:?}");
        assert!(!routed(&policy, TENANT_B, &operation), "{operation:?}");
    }
    // The same key values under tenant B reach B and only B.
    let insert = insert_issue(TENANT_B, 1);
    assert!(routed(&policy, TENANT_B, &insert));
    assert!(!routed(&policy, TENANT_A, &insert));
}

#[test]
fn a_shared_table_and_a_truncate_reach_every_tenant() {
    let policy = policy();
    let shared = CanonicalOperation {
        table_id: plans().table_id,
        primary_key: key(&[(1, 1)]),
        kind: CanonicalOperationKind::InsertFullRow {
            columns: vec![
                cell(1, CellValue::Int64(1)),
                cell(2, CellValue::Text("pro".to_owned())),
            ],
        },
    };
    let truncate = CanonicalOperation {
        table_id: issues().table_id,
        primary_key: Vec::new(),
        kind: CanonicalOperationKind::TruncateAuthorizedTable {
            reason: "TRUNCATE".to_owned(),
        },
    };
    for tenant_value in [TENANT_A, TENANT_B] {
        assert!(routed(&policy, tenant_value, &shared));
        assert!(routed(&policy, tenant_value, &truncate));
    }
}

#[test]
fn a_row_changing_tenant_is_a_rebuild_boundary_for_both_tenants_and_no_other() {
    let policy = policy();
    let moved = update_issue(TENANT_A, TENANT_B, 1);
    for tenant_value in [TENANT_A, TENANT_B] {
        let routed = policy
            .scope_for(&tenant(tenant_value))
            .unwrap()
            .route_operation(&moved);
        assert_eq!(
            routed.unwrap_err(),
            CanonicalAuthorizationError::PartitionMoved,
            "tenant {tenant_value}"
        );
    }
    assert!(!routed(&policy, TENANT_C, &moved));
    // A whole transaction holding the move is refused, not delivered in part.
    let scope = policy.scope_for(&tenant(TENANT_A)).unwrap();
    assert_eq!(
        scope
            .route_operations(&[insert_issue(TENANT_A, 2), moved.clone()])
            .unwrap_err(),
        CanonicalAuthorizationError::PartitionMoved
    );
    assert_eq!(
        policy.partition_touch(&moved).unwrap(),
        Some(PartitionTouch::Moved {
            from: PartitionValue::Int(TENANT_A),
            to: PartitionValue::Int(TENANT_B),
        })
    );
    assert_eq!(
        policy
            .partition_touch(&update_issue(TENANT_A, TENANT_A, 1))
            .unwrap(),
        Some(PartitionTouch::Row(PartitionValue::Int(TENANT_A)))
    );
}

#[test]
fn a_key_that_does_not_lead_with_the_partition_column_fails_closed() {
    let policy = policy();
    let scope = policy.scope_for(&tenant(TENANT_A)).unwrap();
    let mut operation = delete_issue(TENANT_A, 1);
    operation.primary_key = key(&[(2, TENANT_A), (1, TENANT_A)]);
    assert_eq!(
        scope.route_operation(&operation).unwrap_err(),
        CanonicalAuthorizationError::InvalidPartitionKey
    );
    operation.primary_key = vec![0xff];
    assert_eq!(
        scope.route_operation(&operation).unwrap_err(),
        CanonicalAuthorizationError::InvalidPartitionKey
    );
}

#[test]
fn the_scope_is_the_grants_value_and_its_hash_covers_it() {
    let policy = policy();
    let a = policy.scope_for(&tenant(TENANT_A)).unwrap();
    let a_again = policy.scope_for(&tenant(TENANT_A)).unwrap();
    let b = policy.scope_for(&tenant(TENANT_B)).unwrap();
    assert_eq!(a.partition_value(), Some(&PartitionValue::Int(TENANT_A)));
    assert_eq!(b.partition_value(), Some(&PartitionValue::Int(TENANT_B)));
    assert_eq!(a.scope_hash(), a_again.scope_hash());
    assert_ne!(a.scope_hash(), b.scope_hash());
    assert_eq!(a.policy_hash(), b.policy_hash());
    assert_eq!(
        a.scope_hash(),
        policy
            .partition_scope(PartitionValue::Int(TENANT_A))
            .unwrap()
            .scope_hash()
    );
    // Another generation, another hash for the same tenant.
    let next =
        AuthorizationPolicy::compile_partitioned_detached(definition(4), partition(), &tables())
            .unwrap();
    assert_ne!(
        next.scope_for(&tenant(TENANT_A)).unwrap().scope_hash(),
        a.scope_hash()
    );
    // The source-wide policy over the same tables hashes apart from every
    // partitioned one.
    let source_wide = AuthorizationPolicy::compile(definition(3), &tables()).unwrap();
    assert_ne!(source_wide.hash(), policy.hash());
    assert_ne!(source_wide.scope().scope_hash(), a.scope_hash());
    assert_eq!(
        a.partition_filter(&issues().table_id),
        Some((ColumnId::new(1), &PartitionValue::Int(TENANT_A)))
    );
    assert_eq!(a.partition_filter(&plans().table_id), None);
}

#[test]
fn a_grant_without_the_attribute_or_of_another_type_has_no_scope() {
    let policy = policy();
    assert_eq!(
        policy.scope_for(&principal(&[])).unwrap_err(),
        ScopeError::MissingPartitionAttribute
    );
    assert_eq!(
        policy
            .scope_for(&principal(&[("tenant_id", PrincipalValue::Int8(TENANT_A))]))
            .unwrap_err(),
        ScopeError::MissingPartitionAttribute
    );
    assert_eq!(
        policy
            .scope_for(&principal(&[("tenant", PrincipalValue::Int4(7))]))
            .unwrap_err(),
        ScopeError::PartitionAttributeType
    );
    // Without a partition value only the shared tables are in scope.
    let shared = policy.scope();
    assert!(shared.permits_table(&plans().table_id));
    assert!(!shared.permits_table(&issues().table_id));
    assert_eq!(shared.partition_value(), None);
}

#[test]
fn a_write_names_a_row_of_the_grants_tenant() {
    let policy = policy();
    let a = policy.scope_for(&tenant(TENANT_A)).unwrap();
    assert!(a
        .admits_row_key(&issues().table_id, &issue_key(TENANT_A, 1))
        .unwrap());
    assert!(!a
        .admits_row_key(&issues().table_id, &issue_key(TENANT_B, 1))
        .unwrap());
    assert!(a
        .admits_row_key(&plans().table_id, &key(&[(1, 1)]))
        .unwrap());
    let unknown = TableId::new("table:other:g1").unwrap();
    assert!(!a.admits_row_key(&unknown, &key(&[(1, 1)])).unwrap());
}

#[test]
fn the_partition_column_leads_a_supported_key_of_one_type() {
    let compile = |tables: Vec<RegisteredTable>, partition: PartitionDefinition| {
        let definition = PolicyDefinition {
            generation: 1,
            tables: tables
                .iter()
                .map(|table| TablePermitDefinition {
                    table_id: table.table_id.clone(),
                    permitted_columns: table.column_ids.clone(),
                })
                .collect(),
        };
        AuthorizationPolicy::compile_partitioned_detached(definition, partition, &tables)
    };
    let by_column = |table: &RegisteredTable, column: u32| PartitionDefinition {
        attribute: "tenant".to_owned(),
        tables: BTreeMap::from([(table.table_id.clone(), ColumnId::new(column))]),
    };

    // The key's second column.
    assert!(matches!(
        compile(vec![issues()], by_column(&issues(), 2)),
        Err(PolicyError::PartitionColumnNotLeadingKey)
    ));
    // A column outside the key.
    assert!(matches!(
        compile(vec![issues()], by_column(&issues(), 3)),
        Err(PolicyError::PartitionColumnNotLeadingKey)
    ));
    let text_keyed = registered(
        "notes",
        vec![
            column(1, "tenant", "text", Some(1)),
            column(2, "id", "int8", Some(2)),
        ],
    );
    assert!(matches!(
        compile(vec![text_keyed.clone()], by_column(&text_keyed, 1)),
        Err(PolicyError::UnsupportedPartitionType)
    ));
    let int4_keyed = registered(
        "labels",
        vec![
            column(1, "tenant_id", "int4", Some(1)),
            column(2, "id", "int8", Some(2)),
        ],
    );
    let mixed = PartitionDefinition {
        attribute: "tenant".to_owned(),
        tables: BTreeMap::from([
            (issues().table_id, ColumnId::new(1)),
            (int4_keyed.table_id.clone(), ColumnId::new(1)),
        ]),
    };
    assert!(matches!(
        compile(vec![issues(), int4_keyed], mixed),
        Err(PolicyError::PartitionTypesDiffer)
    ));
    assert!(matches!(
        compile(
            vec![plans()],
            PartitionDefinition {
                attribute: "tenant".to_owned(),
                tables: BTreeMap::new(),
            }
        ),
        Err(PolicyError::NoPartitionedTable)
    ));
    assert!(matches!(
        compile(vec![plans()], by_column(&issues(), 1)),
        Err(PolicyError::PartitionedTableNotPermitted)
    ));
    assert!(compile(vec![issues(), plans()], by_column(&issues(), 1)).is_ok());
}

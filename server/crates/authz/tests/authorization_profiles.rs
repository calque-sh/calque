use authz::{
    AuthorizationPolicy, CanonicalAuthorizationError, PolicyDefinition, PolicyError,
    TablePermitDefinition,
};
use canonical_log::{CanonicalOperation, CanonicalOperationKind, CellValue, ColumnValue};
use source_control::{
    ColumnInspection, QualifiedTable, RegisteredTable, TableInspection, UnlistedColumn,
};
use sync_types::{ColumnId, TableId};

fn value(column_id: u32, value: CellValue) -> ColumnValue {
    ColumnValue {
        column_id: ColumnId::new(column_id),
        value,
    }
}

fn operation(table_id: TableId, kind: CanonicalOperationKind) -> CanonicalOperation {
    CanonicalOperation {
        table_id,
        primary_key: vec![1],
        kind,
    }
}

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
        column_ids: (1..=6).map(ColumnId::new).collect(),
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
                column(1, "tenant\"id", "uuid", Some(1)),
                column(2, "region_id", "int4", Some(2)),
                column(3, "shard_id", "int2", Some(3)),
                column(4, "bucket_id", "int8", Some(4)),
                column(5, "id", "uuid", Some(5)),
                column(6, "title", "text", None),
            ],
        },
    }
}

fn all_columns() -> Vec<ColumnId> {
    (1..=6).map(ColumnId::new).collect()
}

fn source_wide(columns: Vec<ColumnId>) -> PolicyDefinition {
    PolicyDefinition {
        generation: 7,
        tables: vec![TablePermitDefinition {
            table_id: table().table_id,
            permitted_columns: columns,
        }],
    }
}

#[test]
fn policy_and_scope_hashes_are_order_independent_and_domain_separated() {
    let first = AuthorizationPolicy::compile(source_wide(all_columns()), &[table()]).unwrap();
    let second = AuthorizationPolicy::compile(
        source_wide(all_columns().into_iter().rev().collect()),
        &[table()],
    )
    .unwrap();
    assert_eq!(first.hash(), second.hash());
    let scope = first.scope();
    assert_eq!(scope.scope_hash(), second.scope().scope_hash());
    assert_ne!(scope.scope_hash().as_bytes(), first.hash().as_bytes());

    let mut next_generation = source_wide(all_columns());
    next_generation.generation += 1;
    let next = AuthorizationPolicy::compile(next_generation, &[table()]).unwrap();
    assert_ne!(first.hash(), next.hash());
    assert_ne!(scope.scope_hash(), next.scope().scope_hash());
}

/// A policy compiles for exactly what enrollment admits: a primary key, and
/// replica identity DEFAULT (the primary key) or FULL. NOTHING, USING INDEX
/// and a table without a primary key fail closed.
#[test]
fn replica_identity_and_primary_key_follow_the_enrollment_rule() {
    for identity in ['d', 'f', 'n', 'i'] {
        for has_primary_key in [true, false] {
            let mut table = table();
            table.inspection.replica_identity = identity;
            if !has_primary_key {
                for column in &mut table.inspection.columns {
                    column.primary_key_ordinal = None;
                }
            }
            let compiled = AuthorizationPolicy::compile(source_wide(all_columns()), &[table]);
            let case = format!("replica identity {identity:?}, primary key {has_primary_key}");
            if has_primary_key && matches!(identity, 'd' | 'f') {
                assert!(compiled.is_ok(), "{case}: {:?}", compiled.err());
            } else {
                assert!(
                    matches!(compiled, Err(PolicyError::MissingIdentity)),
                    "{case}: {:?}",
                    compiled.err()
                );
            }
        }
    }
}

#[test]
fn row_filters_are_refused_and_column_lists_leave_unlisted_columns_unnameable() {
    let mut filtered = table();
    filtered.inspection.has_publication_row_filter = true;
    assert!(matches!(
        AuthorizationPolicy::compile(source_wide(all_columns()), &[filtered]),
        Err(PolicyError::PublicationRowFilter)
    ));

    // A column the publication's column list leaves out is never registered,
    // so a policy compiles over the listed columns and can name no other.
    let mut projected = table();
    projected.inspection.has_publication_column_list = true;
    projected.inspection.unlisted_columns.push(UnlistedColumn {
        name: "secret".to_owned(),
        primary_key: false,
        required_on_insert: false,
    });
    assert!(AuthorizationPolicy::compile(source_wide(all_columns()), &[projected.clone()]).is_ok());
    let unlisted = ColumnId::new(u32::try_from(all_columns().len()).unwrap() + 1);
    let mut beyond = all_columns();
    beyond.push(unlisted);
    assert!(matches!(
        AuthorizationPolicy::compile(source_wide(beyond), &[projected]),
        Err(PolicyError::UnknownColumn)
    ));
}

#[test]
fn synchronized_column_set_is_fixed_validated_and_part_of_policy_identity() {
    let without_title: Vec<_> = (1..=5).map(ColumnId::new).collect();
    let narrow =
        AuthorizationPolicy::compile(source_wide(without_title.clone()), &[table()]).unwrap();
    let broad = AuthorizationPolicy::compile(source_wide(all_columns()), &[table()]).unwrap();
    assert_ne!(narrow.hash(), broad.hash());
    let scope = narrow.scope();
    assert!(scope.permits_column(&table().table_id, ColumnId::new(5)));
    assert!(!scope.permits_column(&table().table_id, ColumnId::new(6)));

    let mut missing_key = without_title;
    missing_key.retain(|column| *column != ColumnId::new(5));
    assert!(matches!(
        AuthorizationPolicy::compile(source_wide(missing_key), &[table()]),
        Err(PolicyError::PrimaryKeyColumnNotPermitted)
    ));
    assert!(matches!(
        AuthorizationPolicy::compile(
            source_wide(vec![
                ColumnId::new(1),
                ColumnId::new(1),
                ColumnId::new(2),
                ColumnId::new(3),
                ColumnId::new(4),
                ColumnId::new(5),
            ]),
            &[table()],
        ),
        Err(PolicyError::DuplicatePermittedColumn)
    ));
}

#[test]
fn shared_router_projects_source_wide_rows_and_omits_hidden_only_updates() {
    let permitted: Vec<_> = (1..=5).map(ColumnId::new).collect();
    let policy = AuthorizationPolicy::compile(source_wide(permitted), &[table()]).unwrap();
    let scope = policy.scope();
    let insert = operation(
        table().table_id,
        CanonicalOperationKind::InsertFullRow {
            columns: vec![
                value(1, CellValue::Uuid16([1; 16])),
                value(2, CellValue::Int64(42)),
                value(3, CellValue::Int64(3)),
                value(4, CellValue::Int64(9)),
                value(5, CellValue::Uuid16([8; 16])),
                value(6, CellValue::Text("secret".to_owned())),
            ],
        },
    );
    let routed = scope.route_operation(&insert).unwrap().unwrap();
    let CanonicalOperationKind::InsertFullRow { columns } = routed.kind else {
        panic!("expected insert");
    };
    assert_eq!(columns.len(), 5);
    assert!(columns.iter().all(|column| column.column_id.get() <= 5));

    let hidden_update = operation(
        table().table_id,
        CanonicalOperationKind::UpdatePatch {
            changed_columns: vec![value(6, CellValue::Text("hidden".to_owned()))],
        },
    );
    assert!(scope.route_operation(&hidden_update).unwrap().is_none());
}

/// Both entry points, borrowed and owned, project one operation the same way.
fn routed(
    scope: &authz::AuthorizedScope,
    operation: CanonicalOperation,
) -> Result<Option<CanonicalOperation>, CanonicalAuthorizationError> {
    let borrowed = scope.route_operation(&operation);
    let owned = scope.route_owned_operations(vec![operation]);
    match (&borrowed, owned) {
        (Ok(borrowed), Ok(owned)) => assert_eq!(
            borrowed.iter().collect::<Vec<_>>(),
            owned.iter().collect::<Vec<_>>()
        ),
        (Err(borrowed), Err(owned)) => assert_eq!(borrowed.to_string(), owned.to_string()),
        (borrowed, owned) => panic!("entry points disagree: {borrowed:?} and {owned:?}"),
    }
    borrowed
}

#[test]
fn full_rows_keep_their_values_in_permit_order_and_refuse_duplicate_or_missing_columns() {
    let permitted: Vec<_> = (1..=5).map(ColumnId::new).collect();
    let scope = AuthorizationPolicy::compile(source_wide(permitted), &[table()])
        .unwrap()
        .scope();
    let cell = |id: u32| value(id, CellValue::Text(format!("value {id}")));
    let insert = |ids: &[u32]| {
        operation(
            table().table_id,
            CanonicalOperationKind::InsertFullRow {
                columns: ids.iter().map(|&id| cell(id)).collect(),
            },
        )
    };
    let projected = |ids: &[u32]| {
        let routed = routed(&scope, insert(ids)).unwrap().unwrap();
        let CanonicalOperationKind::InsertFullRow { columns } = routed.kind else {
            panic!("expected insert");
        };
        columns
    };
    let expected = (1..=5).map(cell).collect::<Vec<_>>();
    // Exactly the permitted columns, in order; out of order; and with a
    // withheld column first, last or between.
    for ids in [
        &[1, 2, 3, 4, 5][..],
        &[5, 3, 1, 2, 4],
        &[1, 2, 3, 4, 5, 6],
        &[6, 1, 2, 3, 4, 5],
        &[1, 2, 6, 3, 4, 5],
    ] {
        assert_eq!(projected(ids), expected, "row {ids:?}");
    }
    for ids in [
        &[1, 2, 3, 4, 5, 5][..],
        &[1, 2, 3, 4, 4],
        &[1, 1, 2, 3, 4, 5],
    ] {
        assert!(
            matches!(
                routed(&scope, insert(ids)),
                Err(CanonicalAuthorizationError::DuplicateRowColumn)
            ),
            "row {ids:?}"
        );
    }
    for ids in [&[1, 2, 3, 4][..], &[1, 2, 3, 4, 6], &[2, 3, 4, 5, 6]] {
        assert!(
            matches!(
                routed(&scope, insert(ids)),
                Err(CanonicalAuthorizationError::MissingPermittedColumn)
            ),
            "row {ids:?}"
        );
    }

    let update = |ids: &[u32]| {
        operation(
            table().table_id,
            CanonicalOperationKind::UpdatePatch {
                changed_columns: ids.iter().map(|&id| cell(id)).collect(),
            },
        )
    };
    let routed_update = routed(&scope, update(&[6, 4, 2])).unwrap().unwrap();
    let CanonicalOperationKind::UpdatePatch { changed_columns } = routed_update.kind else {
        panic!("expected update");
    };
    assert_eq!(changed_columns, vec![cell(4), cell(2)]);
    assert!(matches!(
        routed(&scope, update(&[2, 6, 2])),
        Err(CanonicalAuthorizationError::DuplicateChangedColumn)
    ));
}

#[test]
fn operations_outside_the_scope_are_omitted_and_truncate_is_delivered() {
    let scope = AuthorizationPolicy::compile(source_wide(all_columns()), &[table()])
        .unwrap()
        .scope();
    let elsewhere = operation(
        TableId::new("table:other:g1").unwrap(),
        CanonicalOperationKind::DeleteByKey,
    );
    assert!(scope.route_operation(&elsewhere).unwrap().is_none());

    let truncate = operation(
        table().table_id,
        CanonicalOperationKind::TruncateAuthorizedTable {
            reason: "truncate".to_owned(),
        },
    );
    assert!(scope.route_operation(&truncate).unwrap().is_some());
}

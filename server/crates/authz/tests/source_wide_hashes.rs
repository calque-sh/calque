//! Golden bytes for the source-wide policy and scope hashes.
//!
//! Both hashes are carried in dataset grants and published artifact identities,
//! so any change to them is an authorization change. The hex values must not
//! move.

use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
use sha2::{Digest, Sha256};
use source_control::{ColumnInspection, QualifiedTable, RegisteredTable, TableInspection};
use sync_types::{ColumnId, TableId};

struct Shape {
    generation: u64,
    tables: Vec<RegisteredTable>,
    permitted: Vec<Vec<ColumnId>>,
}

fn registered(
    table_id: &str,
    name: &str,
    columns: &[(&str, &str, Option<u32>)],
) -> RegisteredTable {
    RegisteredTable {
        table_id: TableId::new(table_id).unwrap(),
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
            columns: columns
                .iter()
                .enumerate()
                .map(
                    |(index, (column, type_name, primary_key_ordinal))| ColumnInspection {
                        attribute_number: i16::try_from(index + 1).unwrap(),
                        name: (*column).to_owned(),
                        type_oid: 1_000 + u32::try_from(index).unwrap(),
                        type_modifier: -1,
                        type_schema: "pg_catalog".to_owned(),
                        type_name: (*type_name).to_owned(),
                        nullable: primary_key_ordinal.is_none(),
                        primary_key_ordinal: *primary_key_ordinal,
                        generated: false,
                        nondeterministic_collation: false,
                    },
                )
                .collect(),
        },
    }
}

/// A registry table id in the production form, `table:<sha256>:g<generation>`.
fn production_table_id(name: &str) -> String {
    format!("table:{}:g1", hex::encode(Sha256::digest(name.as_bytes())))
}

/// The example application's seven tables, every column permitted, at
/// generation 1.
fn incidents() -> Shape {
    let tables: Vec<_> = [
        (
            "comments",
            &[
                ("id", "uuid", Some(1)),
                ("issue_id", "uuid", None),
                ("author_id", "uuid", None),
                ("body", "text", None),
                ("created_at", "timestamptz", None),
                ("bench_index", "int8", None),
                ("is_sentinel", "bool", None),
            ][..],
        ),
        (
            "events",
            &[
                ("id", "int8", Some(1)),
                ("organization_id", "uuid", None),
                ("project_id", "uuid", None),
                ("issue_id", "uuid", None),
                ("level", "text", None),
                ("message", "text", None),
                ("occurred_at", "timestamptz", None),
                ("payload", "jsonb", None),
                ("bench_index", "int8", None),
                ("is_sentinel", "bool", None),
            ][..],
        ),
        (
            "issue_tags",
            &[
                ("issue_id", "uuid", Some(1)),
                ("tag", "text", Some(2)),
                ("bench_index", "int8", None),
                ("is_sentinel", "bool", None),
            ][..],
        ),
        (
            "issues",
            &[
                ("id", "uuid", Some(1)),
                ("organization_id", "uuid", None),
                ("project_id", "uuid", None),
                ("assignee_id", "uuid", None),
                ("status", "text", None),
                ("priority", "int4", None),
                ("title", "text", None),
                ("event_count", "int4", None),
                ("updated_at", "timestamptz", None),
                ("bench_index", "int8", None),
                ("is_sentinel", "bool", None),
            ][..],
        ),
        (
            "memberships",
            &[
                ("organization_id", "uuid", Some(1)),
                ("user_id", "uuid", Some(2)),
                ("role", "text", None),
                ("bench_index", "int8", None),
                ("is_sentinel", "bool", None),
            ][..],
        ),
        (
            "projects",
            &[
                ("id", "uuid", Some(1)),
                ("organization_id", "uuid", None),
                ("slug", "text", None),
                ("name", "text", None),
                ("bench_index", "int8", None),
                ("is_sentinel", "bool", None),
            ][..],
        ),
        (
            "users",
            &[
                ("id", "uuid", Some(1)),
                ("display_name", "text", None),
                ("email", "text", None),
                ("active", "bool", None),
                ("bench_index", "int8", None),
                ("is_sentinel", "bool", None),
            ][..],
        ),
    ]
    .into_iter()
    .map(|(name, columns)| registered(&production_table_id(name), name, columns))
    .collect();
    let permitted = tables
        .iter()
        .map(|table| table.column_ids.clone())
        .collect();
    Shape {
        generation: 1,
        tables,
        permitted,
    }
}

/// One table with a composite key and a column held back, at generation 7.
fn narrow() -> Shape {
    let table = registered(
        "table:tasks:g3",
        "tasks",
        &[
            ("tenant_id", "uuid", Some(1)),
            ("id", "int8", Some(2)),
            ("title", "text", None),
            ("secret", "text", None),
            ("updated_at", "timestamptz", None),
        ],
    );
    Shape {
        generation: 7,
        tables: vec![table],
        permitted: vec![vec![
            ColumnId::new(5),
            ColumnId::new(1),
            ColumnId::new(3),
            ColumnId::new(2),
        ]],
    }
}

fn compile(shape: &Shape) -> AuthorizationPolicy {
    // Tables are listed in reverse so the hash depends on the encoder's
    // ordering, not on the definition's.
    let tables = shape
        .tables
        .iter()
        .zip(&shape.permitted)
        .rev()
        .map(|(table, permitted)| TablePermitDefinition {
            table_id: table.table_id.clone(),
            permitted_columns: permitted.clone(),
        })
        .collect();
    AuthorizationPolicy::compile(
        PolicyDefinition {
            generation: shape.generation,
            tables,
        },
        &shape.tables,
    )
    .unwrap()
}

/// The documented encoding, written out byte by byte.
fn expected_hashes(shape: &Shape) -> ([u8; 32], [u8; 32]) {
    let mut permits: Vec<(&TableId, Vec<u32>)> = shape
        .tables
        .iter()
        .zip(&shape.permitted)
        .map(|(table, permitted)| {
            let mut columns: Vec<u32> = permitted.iter().map(|column| column.get()).collect();
            columns.sort_unstable();
            (&table.table_id, columns)
        })
        .collect();
    permits.sort_by(|left, right| left.0.cmp(right.0));

    let mut policy = Vec::new();
    policy.extend_from_slice(b"calque.authorization.policy.v1\0");
    policy.extend_from_slice(&shape.generation.to_be_bytes());
    policy.push(1);
    policy.extend_from_slice(&(permits.len() as u64).to_be_bytes());
    for (table_id, columns) in &permits {
        policy.extend_from_slice(&(table_id.as_str().len() as u64).to_be_bytes());
        policy.extend_from_slice(table_id.as_str().as_bytes());
        policy.extend_from_slice(&(columns.len() as u64).to_be_bytes());
        for column in columns {
            policy.extend_from_slice(&column.to_be_bytes());
        }
        policy.extend_from_slice(&0_u64.to_be_bytes());
    }
    let policy_hash: [u8; 32] = Sha256::digest(&policy).into();

    let mut scope = Vec::new();
    scope.extend_from_slice(b"calque.authorization.scope.v1\0");
    scope.extend_from_slice(&policy_hash);
    for (table_id, _) in &permits {
        scope.extend_from_slice(&(table_id.as_str().len() as u64).to_be_bytes());
        scope.extend_from_slice(table_id.as_str().as_bytes());
        scope.extend_from_slice(&0_u64.to_be_bytes());
    }
    (policy_hash, Sha256::digest(&scope).into())
}

#[test]
fn incidents_source_wide_hashes_are_pinned() {
    let policy = compile(&incidents());
    let scope = policy.scope();
    assert_eq!(
        policy.hash().to_string(),
        "sha256:0de99dd45d82b9c433d22f39cef514b4cf04b5714aa878c023b0c3be7795ea1b"
    );
    assert_eq!(
        scope.scope_hash().to_string(),
        "sha256:58dad7192f02616bdecb4ee7c7323776e2c9c036965cbc0a2032381c2c122254"
    );
}

#[test]
fn narrow_source_wide_hashes_are_pinned() {
    let policy = compile(&narrow());
    let scope = policy.scope();
    assert_eq!(
        policy.hash().to_string(),
        "sha256:7022d172887dc0ef0d39f36d970a2a6bdc55f5ec700ae3ba6febcf1f0c6c9424"
    );
    assert_eq!(
        scope.scope_hash().to_string(),
        "sha256:fae3238a97fed4278e7014d073ce23b2c4415cc687c4df1b78af75653adaaf65"
    );
}

#[test]
fn source_wide_hashes_follow_the_documented_encoding_with_profile_tag_one() {
    for shape in [incidents(), narrow()] {
        let policy = compile(&shape);
        let scope = policy.scope();
        let (policy_hash, scope_hash) = expected_hashes(&shape);
        assert_eq!(policy.hash().as_bytes(), &policy_hash);
        assert_eq!(scope.scope_hash().as_bytes(), &scope_hash);
    }
}

use std::{collections::BTreeSet, env};

use postgres::{Client, NoTls};
use source_control::{
    inspect_source, validate_enrollment, EnrollmentRequirements, QualifiedTable, SchemaRegistry,
    SchemaRegistryError, UnlistedColumn,
};

#[test]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
fn inspects_a_live_logical_replication_source() {
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let mut client = Client::connect(&database_url, NoTls).expect("connect to test PostgreSQL");
    client
        .batch_execute(
            "DROP PUBLICATION IF EXISTS calque_enrollment_test;
             DROP TABLE IF EXISTS public.calque_enrollment_tasks;
             CREATE TABLE public.calque_enrollment_tasks (
               id uuid PRIMARY KEY,
               title text NOT NULL,
               updated_at timestamptz NOT NULL
             );
             CREATE PUBLICATION calque_enrollment_test
               FOR TABLE public.calque_enrollment_tasks;",
        )
        .expect("create enrollment fixture");

    let table = QualifiedTable {
        schema: "public".to_owned(),
        table: "calque_enrollment_tasks".to_owned(),
    };
    let tables = BTreeSet::from([table]);
    let snapshot = inspect_source(&mut client, "calque_enrollment_test", &tables)
        .expect("inspect logical source");
    let requirements = EnrollmentRequirements {
        expected_identity: snapshot.identity.clone(),
        publication_name: "calque_enrollment_test".to_owned(),
        tables,
        pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
    };
    let report = validate_enrollment(&snapshot, &requirements);
    let registry = SchemaRegistry::initialize(&snapshot, &requirements).unwrap();

    assert!(report.is_ready(), "{:#?}", report.diagnostics);
    client
        .batch_execute(
            "DROP PUBLICATION calque_enrollment_test;
             CREATE PUBLICATION calque_enrollment_test
               FOR TABLE public.calque_enrollment_tasks (id, title)
               WHERE (id IS NOT NULL);",
        )
        .expect("replace publication with a row filter and a column list");
    let filtered = inspect_source(
        &mut client,
        "calque_enrollment_test",
        &requirements_tables(),
    )
    .expect("inspect filtered publication");
    let filtered_report = validate_enrollment(
        &filtered,
        &EnrollmentRequirements {
            expected_identity: filtered.identity.clone(),
            publication_name: "calque_enrollment_test".to_owned(),
            tables: requirements_tables(),
            pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
        },
    );
    let diagnostic_codes: BTreeSet<_> = filtered_report
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code.as_str())
        .collect();
    assert_eq!(
        diagnostic_codes,
        BTreeSet::from(["PUBLICATION_ROW_FILTER_UNSUPPORTED"])
    );
    let projected = &filtered.tables[0];
    assert_eq!(
        projected
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "title"]
    );
    assert_eq!(
        projected.unlisted_columns,
        [UnlistedColumn {
            name: "updated_at".to_owned(),
            primary_key: false,
            required_on_insert: true,
        }]
    );
    assert!(projected.insert_needs_unlisted_column());
    assert!(matches!(
        registry.verify_live(&mut client),
        Err(SchemaRegistryError::EnrollmentNotReady(_))
    ));
    client
        .batch_execute(
            "DROP PUBLICATION calque_enrollment_test;
             CREATE PUBLICATION calque_enrollment_test
               FOR TABLE public.calque_enrollment_tasks;
             ALTER TABLE public.calque_enrollment_tasks
               ADD COLUMN post_cut_ddl bigint;",
        )
        .expect("change the registered table layout");
    assert!(matches!(
        registry.verify_live(&mut client),
        Err(SchemaRegistryError::LiveDrift)
    ));
    client
        .batch_execute(
            "DROP PUBLICATION calque_enrollment_test;
             DROP TABLE public.calque_enrollment_tasks;",
        )
        .expect("remove enrollment fixture");
}

/// PostgreSQL reports a key column's collation, and enrollment admits a text
/// key only under a deterministic one.
#[test]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
fn enrolls_a_text_key_only_under_a_deterministic_collation() {
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let mut client = Client::connect(&database_url, NoTls).expect("connect to test PostgreSQL");
    client
        .batch_execute(
            "DROP PUBLICATION IF EXISTS calque_text_key_test;
             DROP TABLE IF EXISTS public.calque_text_keys;
             DROP COLLATION IF EXISTS public.calque_case_insensitive;
             CREATE COLLATION public.calque_case_insensitive
               (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
             CREATE TABLE public.calque_text_keys (
               slug text COLLATE \"en-x-icu\",
               version varchar(40),
               label text COLLATE public.calque_case_insensitive NOT NULL,
               PRIMARY KEY (slug, version)
             );
             CREATE PUBLICATION calque_text_key_test FOR TABLE public.calque_text_keys;",
        )
        .expect("create text-key fixture");
    let tables = BTreeSet::from([QualifiedTable {
        schema: "public".to_owned(),
        table: "calque_text_keys".to_owned(),
    }]);
    let codes = |client: &mut Client| {
        let snapshot = inspect_source(client, "calque_text_key_test", &tables)
            .expect("inspect text-key source");
        let flags: Vec<_> = snapshot.tables[0]
            .columns
            .iter()
            .map(|column| (column.name.clone(), column.nondeterministic_collation))
            .collect();
        let report = validate_enrollment(
            &snapshot,
            &EnrollmentRequirements {
                expected_identity: snapshot.identity.clone(),
                publication_name: "calque_text_key_test".to_owned(),
                tables: tables.clone(),
                pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
            },
        );
        let codes: Vec<_> = report
            .diagnostics
            .iter()
            .map(|diagnostic| (diagnostic.code.clone(), diagnostic.column.clone()))
            .collect();
        (flags, codes)
    };

    let (flags, admitted) = codes(&mut client);
    assert_eq!(
        flags,
        [
            ("slug".to_owned(), false),
            ("version".to_owned(), false),
            ("label".to_owned(), true),
        ]
    );
    assert_eq!(admitted, []);

    client
        .batch_execute(
            "ALTER TABLE public.calque_text_keys
               DROP CONSTRAINT calque_text_keys_pkey,
               ADD PRIMARY KEY (slug, label);",
        )
        .expect("key the table by its case-insensitive column");
    let (_, refused) = codes(&mut client);
    assert_eq!(
        refused,
        [(
            "PRIMARY_KEY_COLLATION_NONDETERMINISTIC".to_owned(),
            Some("label".to_owned())
        )]
    );
    client
        .batch_execute(
            "DROP PUBLICATION calque_text_key_test;
             DROP TABLE public.calque_text_keys;
             DROP COLLATION public.calque_case_insensitive;",
        )
        .expect("remove text-key fixture");
}

fn requirements_tables() -> BTreeSet<QualifiedTable> {
    BTreeSet::from([QualifiedTable {
        schema: "public".to_owned(),
        table: "calque_enrollment_tasks".to_owned(),
    }])
}

/// Inspecting several tables at once gives each table its own relation,
/// publication features and columns: a table found in another schema under
/// the same name, a row filter on one table and a column list on another, a
/// dropped and a generated column, a primary key out of column order, a
/// replica identity on one table only, a table outside the publication and a
/// missing table.
#[test]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
fn inspects_several_tables_each_with_its_own_catalog_facts() {
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let mut client = Client::connect(&database_url, NoTls).expect("connect to test PostgreSQL");
    client
        .batch_execute(
            "DROP PUBLICATION IF EXISTS calque_inspect_test;
             DROP SCHEMA IF EXISTS calque_inspect_schema CASCADE;
             DROP TABLE IF EXISTS public.calque_inspect_a, public.calque_inspect_b,
               public.calque_inspect_c;
             CREATE SCHEMA calque_inspect_schema;
             CREATE TABLE public.calque_inspect_a (
               id uuid PRIMARY KEY,
               dropped text,
               title text NOT NULL,
               title_length int4 GENERATED ALWAYS AS (length(title)) STORED
             );
             ALTER TABLE public.calque_inspect_a DROP COLUMN dropped;
             CREATE DOMAIN calque_inspect_schema.stamp AS int8 NOT NULL DEFAULT 0;
             CREATE DOMAIN calque_inspect_schema.strict AS text NOT NULL;
             CREATE TABLE calque_inspect_schema.calque_inspect_b (
               region text,
               number int8,
               note varchar(20),
               stamp calque_inspect_schema.stamp,
               strict calque_inspect_schema.strict,
               PRIMARY KEY (number, region)
             );
             CREATE TABLE public.calque_inspect_b (other bool);
             CREATE TABLE public.calque_inspect_c (id int8 PRIMARY KEY);
             ALTER TABLE public.calque_inspect_c REPLICA IDENTITY FULL;
             CREATE PUBLICATION calque_inspect_test
               FOR TABLE public.calque_inspect_a WHERE (id IS NOT NULL),
                         calque_inspect_schema.calque_inspect_b (region, number);",
        )
        .expect("create inspection fixture");
    let table = |schema: &str, table: &str| QualifiedTable {
        schema: schema.to_owned(),
        table: table.to_owned(),
    };
    let tables = BTreeSet::from([
        table("public", "calque_inspect_a"),
        table("calque_inspect_schema", "calque_inspect_b"),
        table("public", "calque_inspect_c"),
        table("public", "calque_inspect_missing"),
    ]);
    let snapshot =
        inspect_source(&mut client, "calque_inspect_test", &tables).expect("inspect tables");
    let oid = |client: &mut Client, name: &str| -> u32 {
        client
            .query_one("SELECT $1::text::regclass::oid", &[&name])
            .unwrap()
            .get(0)
    };

    let found: Vec<_> = snapshot
        .tables
        .iter()
        .map(|inspection| {
            (
                inspection.table.display_name(),
                inspection.relation_oid,
                inspection.in_publication,
                inspection.relation_kind,
                inspection.persistence,
                inspection.replica_identity,
                inspection.is_partition,
                inspection.has_publication_row_filter,
                inspection.has_publication_column_list,
            )
        })
        .collect();
    assert_eq!(
        found,
        vec![
            (
                "calque_inspect_schema.calque_inspect_b".to_owned(),
                oid(&mut client, "calque_inspect_schema.calque_inspect_b"),
                true,
                'r',
                'p',
                'd',
                false,
                false,
                true,
            ),
            (
                "public.calque_inspect_a".to_owned(),
                oid(&mut client, "public.calque_inspect_a"),
                true,
                'r',
                'p',
                'd',
                false,
                true,
                false,
            ),
            (
                "public.calque_inspect_c".to_owned(),
                oid(&mut client, "public.calque_inspect_c"),
                false,
                'r',
                'p',
                'f',
                false,
                false,
                false,
            ),
            (
                "public.calque_inspect_missing".to_owned(),
                0,
                false,
                '\0',
                '\0',
                '\0',
                false,
                false,
                false,
            ),
        ]
    );
    let columns: Vec<Vec<_>> = snapshot
        .tables
        .iter()
        .map(|inspection| {
            inspection
                .columns
                .iter()
                .map(|column| {
                    (
                        column.attribute_number,
                        column.name.as_str(),
                        column.type_name.as_str(),
                        column.type_modifier,
                        column.nullable,
                        column.primary_key_ordinal,
                        column.generated,
                    )
                })
                .collect()
        })
        .collect();
    assert_eq!(
        columns,
        vec![
            vec![
                (1, "region", "text", -1, false, Some(2), false),
                (2, "number", "int8", -1, false, Some(1), false),
            ],
            vec![
                (1, "id", "uuid", -1, false, Some(1), false),
                (3, "title", "text", -1, false, None, false),
                (4, "title_length", "int4", -1, true, None, true),
            ],
            vec![(1, "id", "int8", -1, false, Some(1), false)],
            vec![],
        ]
    );
    // A domain's default fills a column an insert leaves out; a domain's
    // NOT NULL without one makes the column required.
    assert_eq!(
        snapshot.tables[0].unlisted_columns,
        [
            UnlistedColumn {
                name: "note".to_owned(),
                primary_key: false,
                required_on_insert: false,
            },
            UnlistedColumn {
                name: "stamp".to_owned(),
                primary_key: false,
                required_on_insert: false,
            },
            UnlistedColumn {
                name: "strict".to_owned(),
                primary_key: false,
                required_on_insert: true,
            },
        ]
    );
    assert!(snapshot.tables[1..]
        .iter()
        .all(|inspection| inspection.unlisted_columns.is_empty()));

    client
        .batch_execute(
            "DROP PUBLICATION calque_inspect_test;
             DROP SCHEMA calque_inspect_schema CASCADE;
             DROP TABLE public.calque_inspect_a, public.calque_inspect_b,
               public.calque_inspect_c;",
        )
        .expect("remove inspection fixture");
}

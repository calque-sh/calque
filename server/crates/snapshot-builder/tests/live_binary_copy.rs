use std::{collections::BTreeSet, env};

use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
use direct_columnar::{
    schema_hash, ArtifactIdentity, Encoder, EncoderLimits, Value, WorkspaceSchema,
};
use futures_util::TryStreamExt;
use postgres::{Client as BlockingClient, NoTls as BlockingNoTls};
use snapshot_builder::{
    decode_direct_columnar_row, direct_columnar_table_schema, CopyLoadLimits,
    DirectColumnarCopyError, DirectColumnarTableBinding, SnapshotBuildPlan, SnapshotTablePlan,
};
use source_control::{inspect_source, EnrollmentRequirements, QualifiedTable, SchemaRegistry};
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL and a disposable PostgreSQL database"]
async fn maintained_binary_copy_codecs_decode_into_direct_columnar() {
    let database_url = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
    let setup_url = database_url.clone();
    let (setup, registry) = tokio::task::spawn_blocking(move || setup_fixture(&setup_url))
        .await
        .expect("join PostgreSQL fixture setup");
    let scope = source_wide_scope(&registry);
    let plan = SnapshotBuildPlan::from_registry(&registry, &scope).expect("compile snapshot plan");
    let table = &plan.tables()[0];

    let (client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("connect async COPY client");
    let connection_task = tokio::spawn(async move {
        connection.await.expect("drive PostgreSQL connection");
    });
    client
        .batch_execute("SET TIME ZONE 'UTC'")
        .await
        .expect("set deterministic session output");

    let value_limit_error = rejected_copy(
        &client,
        table,
        CopyLoadLimits {
            max_value_bytes: 8,
            ..CopyLoadLimits::default()
        },
    )
    .await;
    assert!(matches!(
        value_limit_error,
        DirectColumnarCopyError::ValueByteLimitExceeded {
            actual: 16,
            maximum: 8,
            ..
        }
    ));

    let row_limit_error = rejected_copy(
        &client,
        table,
        CopyLoadLimits {
            max_row_bytes: 16,
            ..CopyLoadLimits::default()
        },
    )
    .await;
    assert!(matches!(
        row_limit_error,
        DirectColumnarCopyError::RowByteLimitExceeded {
            actual: 17,
            maximum: 16,
        }
    ));

    let rows = copied_rows(&client, table, CopyLoadLimits::default())
        .await
        .expect("decode binary COPY rows into direct-columnar values");
    assert_eq!(rows.len(), 2);
    let json = br#"{"a":2,"z":1}"#.to_vec();
    assert_eq!(
        rows[0],
        vec![
            Value::Uuid(1_u128.to_be_bytes()),
            Value::Bool(true),
            Value::Int64(-32768),
            Value::Int64(2_147_483_647),
            Value::Int64(i64::MAX),
            Value::F64(1.5),
            Value::F64(-42.25),
            Value::DecimalText("12345678901234567890.1234".to_owned()),
            Value::Text("héllo".to_owned()),
            Value::Text("varchar".to_owned()),
            Value::Text("x   ".to_owned()),
            Value::Bytes(vec![0, 1, 255]),
            Value::Uuid([0xff; 16]),
            Value::Json(json.clone()),
            Value::Json(json),
            Value::DateText("1969-12-31".to_owned()),
            Value::TimeText("23:59:59.123456".to_owned()),
            Value::TimestampMicros(-876_544),
            Value::TimestampMicros(1_711_848_600_000_000),
        ]
    );
    assert_eq!(rows[1][0], Value::Uuid(2_u128.to_be_bytes()));
    assert!(rows[1][1..].iter().all(|value| *value == Value::Null));

    let direct_binding = DirectColumnarTableBinding {
        collection: "binary_copy_fixture".to_owned(),
        dictionary_columns: BTreeSet::from(["text_value".to_owned()]),
    };
    let direct_schema = WorkspaceSchema {
        tables: vec![direct_columnar_table_schema(table, &direct_binding)
            .expect("compile direct-columnar schema")],
    };
    let direct_identity = ArtifactIdentity {
        source_epoch: "epoch-v1".to_owned(),
        manifest_version: "manifest-v1".to_owned(),
        collection_set_hash: format!("sha256:{}", "0".repeat(64)),
        schema_hash: schema_hash(&direct_schema).expect("hash direct-columnar schema"),
        authorization_scope_version: "scope-v1".to_owned(),
        authorization_scope_hash:
            "sha256:f9154db512dfe802aa41f358909f833e798c077f0bfa6a2af71c91ab2797cf36".to_owned(),
        cursor: "cursor-v1".to_owned(),
    };
    let mut direct_encoder = Encoder::new(direct_identity, direct_schema, EncoderLimits::default())
        .expect("create direct-columnar encoder");
    for values in &rows {
        direct_encoder
            .push_row(&direct_binding.collection, values)
            .expect("spool direct-columnar row");
    }
    let mut direct_bytes = Vec::new();
    let direct_report = direct_encoder
        .finish(&mut direct_bytes)
        .expect("finish direct-columnar artifact");
    assert_eq!(direct_report.rows, 2);
    assert_eq!(direct_report.bytes as usize, direct_bytes.len());
    assert_eq!(&direct_bytes[..8], b"CALQART1");

    assert_temporal_copy_rejected(
        &client,
        table,
        "UPDATE public.calque_binary_copy_fixture
            SET date_value = DATE '0001-01-01 BC'
          WHERE id = '00000000-0000-0000-0000-000000000001'",
        "date_value",
    )
    .await;
    assert_temporal_copy_rejected(
        &client,
        table,
        "UPDATE public.calque_binary_copy_fixture
            SET date_value = DATE '2000-01-01',
                timestamp_value = TIMESTAMP '10000-01-01 00:00:00'
          WHERE id = '00000000-0000-0000-0000-000000000001'",
        "timestamp_value",
    )
    .await;
    assert_temporal_copy_rejected(
        &client,
        table,
        "UPDATE public.calque_binary_copy_fixture
            SET timestamp_value = TIMESTAMP '2000-01-01 00:00:00',
                timestamptz_value = TIMESTAMPTZ '10000-01-01 00:00:00+00'
          WHERE id = '00000000-0000-0000-0000-000000000001'",
        "timestamptz_value",
    )
    .await;

    drop(client);
    connection_task
        .await
        .expect("join PostgreSQL connection task");
    tokio::task::spawn_blocking(move || cleanup_fixture(setup))
        .await
        .expect("join PostgreSQL fixture cleanup");
}

fn source_wide_scope(registry: &SchemaRegistry) -> authz::AuthorizedScope {
    let policy = AuthorizationPolicy::compile_for_registry(
        PolicyDefinition {
            generation: 0,
            tables: registry
                .tables()
                .iter()
                .map(|table| TablePermitDefinition {
                    table_id: table.table_id.clone(),
                    permitted_columns: table.column_ids.clone(),
                })
                .collect(),
        },
        registry,
    )
    .expect("compile source-wide authorization policy");
    policy.scope()
}

/// Every row of `table`'s binary COPY, decoded into direct-columnar values,
/// or the first decoding refusal.
async fn copied_rows(
    client: &tokio_postgres::Client,
    table: &SnapshotTablePlan,
    limits: CopyLoadLimits,
) -> Result<Vec<Vec<Value>>, DirectColumnarCopyError> {
    let stream = client
        .copy_out(table.copy_sql())
        .await
        .expect("start binary COPY");
    let rows = tokio_postgres::binary_copy::BinaryCopyOutStream::new(stream, &table.copy_types());
    futures_util::pin_mut!(rows);
    let mut decoded = Vec::new();
    while let Some(row) = rows.try_next().await.expect("read binary COPY row") {
        decoded.push(decode_direct_columnar_row(table, &row, limits)?.0);
    }
    Ok(decoded)
}

async fn rejected_copy(
    client: &tokio_postgres::Client,
    table: &SnapshotTablePlan,
    limits: CopyLoadLimits,
) -> DirectColumnarCopyError {
    copied_rows(client, table, limits)
        .await
        .expect_err("COPY fixture must violate its configured bound")
}

async fn assert_temporal_copy_rejected(
    client: &tokio_postgres::Client,
    table: &SnapshotTablePlan,
    update: &str,
    expected_column: &str,
) {
    client
        .batch_execute(update)
        .await
        .expect("install out-of-range temporal fixture");
    let error = rejected_copy(client, table, CopyLoadLimits::default()).await;
    assert!(matches!(
        error,
        DirectColumnarCopyError::InvalidValue { column, reason }
            if column == expected_column
                && reason == "temporal value is outside codec-v1 years 1..=9999"
    ));
}

fn setup_fixture(database_url: &str) -> (BlockingClient, SchemaRegistry) {
    let mut setup = BlockingClient::connect(database_url, BlockingNoTls)
        .expect("connect to disposable PostgreSQL");
    setup
        .batch_execute(
            "SET TIME ZONE 'UTC';
             DROP PUBLICATION IF EXISTS calque_binary_copy_test;
             DROP TABLE IF EXISTS public.calque_binary_copy_fixture;
             CREATE TABLE public.calque_binary_copy_fixture (
               id uuid PRIMARY KEY,
               enabled bool,
               small int2,
               regular int4,
               large int8,
               single_value float4,
               double_value float8,
               exact_value numeric,
               text_value text,
               varchar_value varchar(32),
               char_value char(4),
               bytes_value bytea,
               other_uuid uuid,
               json_value json,
               jsonb_value jsonb,
               date_value date,
               time_value time,
               timestamp_value timestamp,
               timestamptz_value timestamptz
             );
             INSERT INTO public.calque_binary_copy_fixture VALUES (
               '00000000-0000-0000-0000-000000000001',
               true, -32768, 2147483647, 9223372036854775807,
               1.5, -42.25, 12345678901234567890.123400,
               'héllo', 'varchar', 'x', decode('0001ff', 'hex'),
               'ffffffff-ffff-ffff-ffff-ffffffffffff',
               '{\"z\":1,\"a\":2}', '{\"z\":1,\"a\":2}',
               '1969-12-31', '23:59:59.123456',
               '1969-12-31 23:59:59.123456',
               '2024-03-31 01:30:00+00'
             ), (
               '00000000-0000-0000-0000-000000000002',
               NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
               NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
             );
             CREATE PUBLICATION calque_binary_copy_test
               FOR TABLE public.calque_binary_copy_fixture;",
        )
        .expect("create binary COPY fixture");
    let qualified = QualifiedTable {
        schema: "public".to_owned(),
        table: "calque_binary_copy_fixture".to_owned(),
    };
    let tables = BTreeSet::from([qualified]);
    let snapshot = inspect_source(&mut setup, "calque_binary_copy_test", &tables)
        .expect("inspect COPY fixture");
    let registry = SchemaRegistry::initialize(
        &snapshot,
        &EnrollmentRequirements {
            expected_identity: snapshot.identity.clone(),
            publication_name: "calque_binary_copy_test".to_owned(),
            tables,
            pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
        },
    )
    .expect("compile schema registry");
    (setup, registry)
}

fn cleanup_fixture(mut setup: BlockingClient) {
    setup
        .batch_execute(
            "DROP PUBLICATION calque_binary_copy_test;
             DROP TABLE public.calque_binary_copy_fixture;",
        )
        .expect("remove binary COPY fixture");
}

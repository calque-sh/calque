//! The definition of the app-owned publication. Enrollment creates it, and
//! every start, reconnect and destroy checks it through
//! [`inspect_owned_publication`], so the checks cannot disagree. A running
//! session checks it too, every [`PUBLICATION_CHECK_INTERVAL`].

use std::{collections::BTreeSet, time::Duration};

use postgres::GenericClient;
use source_control::{QualifiedTable, RegisteredColumn};

/// How often a running session reads the publication's fingerprint, and the
/// values of the enums its columns use. pgoutput applies a publication change
/// from the change's own commit on, so the changes it leaves out until the
/// check sees it are lost to the working set; the check stops the session,
/// and the state is prepared anew. Renaming an enum's value changes the text
/// of every row holding it without a row change, which the enum check stops
/// on (see `enum_labels`). Dropping, renaming or retyping a column changes
/// every row without a row change too, which the column check stops on (see
/// [`changed_column`]).
pub(crate) const PUBLICATION_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// What the app-owned publication is, against the definition Calque creates
/// for its enrolled tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedPublication {
    Missing,
    /// It exists without that definition.
    Drifted,
    /// It has that definition. `quoted_name` is its name as an SQL identifier.
    /// `fingerprint` names the catalog rows that hold the definition: any
    /// `ALTER PUBLICATION` writes a new one, and so does dropping and creating
    /// the publication again, even when the definition is put back as it was.
    Matches {
        quoted_name: String,
        fingerprint: String,
    },
}

/// Inspects `publication` against the definition Calque serves `tables`
/// from: exactly those tables, listed or through `FOR TABLES IN SCHEMA`,
/// never `FOR ALL TABLES`, publishing insert, update, delete and truncate,
/// without `publish_via_partition_root`, and with no row filter on any table.
/// A row filter makes pgoutput leave out changes to rows the snapshot holds.
/// A column list is allowed: enrollment reads only the columns it names, and
/// a change to it writes a new fingerprint.
pub fn inspect_owned_publication(
    client: &mut impl GenericClient,
    publication: &str,
    tables: &BTreeSet<QualifiedTable>,
) -> Result<OwnedPublication, postgres::Error> {
    let Some(row) = client.query_opt(
        "SELECT publication.pubinsert, publication.pubupdate, publication.pubdelete,
                publication.pubtruncate, publication.pubviaroot, publication.puballtables,
                EXISTS (
                  SELECT 1 FROM pg_catalog.pg_publication_rel AS publication_relation
                   WHERE publication_relation.prpubid = publication.oid
                     AND publication_relation.prqual IS NOT NULL
                ),
                quote_ident(publication.pubname::text),
                concat_ws(';',
                  publication.oid::text || ':' || publication.xmin::text,
                  coalesce((
                    SELECT string_agg(relation.oid::text || ':' || relation.xmin::text, ','
                                      ORDER BY relation.oid)
                      FROM pg_catalog.pg_publication_rel AS relation
                     WHERE relation.prpubid = publication.oid), ''),
                  coalesce((
                    SELECT string_agg(namespace.oid::text || ':' || namespace.xmin::text, ','
                                      ORDER BY namespace.oid)
                      FROM pg_catalog.pg_publication_namespace AS namespace
                     WHERE namespace.pnpubid = publication.oid), ''))
           FROM pg_catalog.pg_publication AS publication
          WHERE publication.pubname = $1",
        &[&publication],
    )?
    else {
        return Ok(OwnedPublication::Missing);
    };
    let settings_hold = row.get::<_, bool>(0)
        && row.get::<_, bool>(1)
        && row.get::<_, bool>(2)
        && row.get::<_, bool>(3)
        && !row.get::<_, bool>(4)
        && !row.get::<_, bool>(5)
        && !row.get::<_, bool>(6);
    let published = client
        .query(
            "SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables
              WHERE pubname = $1",
            &[&publication],
        )?
        .into_iter()
        .map(|row| QualifiedTable {
            schema: row.get(0),
            table: row.get(1),
        })
        .collect::<BTreeSet<_>>();
    if !settings_hold || published != *tables {
        return Ok(OwnedPublication::Drifted);
    }
    Ok(OwnedPublication::Matches {
        quoted_name: row.get(7),
        fingerprint: row.get(8),
    })
}

/// The first of `columns` the catalog no longer holds as registered: dropped,
/// renamed, or of another type or type modifier. PostgreSQL applies each to
/// every row without a row change, so the working set may no longer match
/// its rows.
pub(crate) fn changed_column<'a>(
    client: &mut impl GenericClient,
    columns: &'a [RegisteredColumn],
) -> Result<Option<&'a RegisteredColumn>, postgres::Error> {
    let relations: Vec<u32> = columns.iter().map(|column| column.relation_oid).collect();
    let attributes: Vec<i16> = columns
        .iter()
        .map(|column| column.attribute_number)
        .collect();
    let names: Vec<&str> = columns.iter().map(|column| column.name.as_str()).collect();
    let types: Vec<u32> = columns.iter().map(|column| column.type_oid).collect();
    let modifiers: Vec<i32> = columns.iter().map(|column| column.type_modifier).collect();
    let changed = client.query_opt(
        "SELECT registered.ordinal
           FROM unnest($1::oid[], $2::int2[], $3::text[], $4::oid[], $5::int4[])
                WITH ORDINALITY AS registered(relation, attnum, name, type_oid, type_modifier,
                                              ordinal)
           LEFT JOIN pg_catalog.pg_attribute AS attribute
             ON attribute.attrelid = registered.relation
            AND attribute.attnum = registered.attnum
            AND NOT attribute.attisdropped
            AND attribute.attname = registered.name
            AND attribute.atttypid = registered.type_oid
            AND attribute.atttypmod = registered.type_modifier
          WHERE attribute.attrelid IS NULL
          ORDER BY registered.ordinal
          LIMIT 1",
        &[&relations, &attributes, &names, &types, &modifiers],
    )?;
    Ok(changed.and_then(|row| {
        usize::try_from(row.get::<_, i64>(0) - 1)
            .ok()
            .and_then(|index| columns.get(index))
    }))
}

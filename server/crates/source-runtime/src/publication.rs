//! The definition of the app-owned publication. Enrollment creates it, and
//! every start, reconnect and destroy checks it through
//! [`inspect_owned_publication`], so the checks cannot disagree.

use std::collections::BTreeSet;

use postgres::GenericClient;
use source_control::QualifiedTable;

/// What the app-owned publication is, against the definition Calque creates
/// for its enrolled tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedPublication {
    Missing,
    /// It exists without that definition.
    Drifted,
    /// It has that definition. `quoted_name` is its name as an SQL identifier.
    Matches {
        quoted_name: String,
    },
}

/// Inspects `publication` against the definition Calque creates for `tables`:
/// `FOR TABLE` exactly those tables, never `FOR ALL TABLES`, publishing
/// insert, update, delete and truncate, without `publish_via_partition_root`,
/// and with no row filter or column list on any table. A row filter makes
/// pgoutput leave out changes to rows the snapshot holds, and a column list
/// leaves out columns it holds.
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
                     AND (publication_relation.prqual IS NOT NULL
                          OR publication_relation.prattrs IS NOT NULL)
                ),
                quote_ident(publication.pubname::text)
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
    })
}

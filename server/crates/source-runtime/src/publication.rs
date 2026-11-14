//! The definition of the app-owned publication. Enrollment creates it, and
//! every start, reconnect and destroy checks it through
//! [`inspect_owned_publication`], so the checks cannot disagree. A running
//! session checks it too, every [`PUBLICATION_CHECK_INTERVAL`].

use std::{collections::BTreeSet, time::Duration};

use postgres::GenericClient;
use source_control::QualifiedTable;

/// How often a running session reads the publication's fingerprint. pgoutput
/// applies a publication change from the change's own commit on, so the
/// changes it leaves out until the check sees it are lost to the working set;
/// the check stops the session, and the state is prepared anew. Renaming an
/// enum's value changes the text of every row holding it without a row
/// change, so the fingerprint covers the enums the published columns use.
pub(crate) const PUBLICATION_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// What the app-owned publication is, against the definition Calque creates
/// for its enrolled tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedPublication {
    Missing,
    /// It exists without that definition.
    Drifted,
    /// It has that definition. `quoted_name` is its name as an SQL identifier.
    /// `fingerprint` names the catalog rows that hold the definition and the
    /// values of every enum a published column uses, directly or through a
    /// domain: any `ALTER PUBLICATION` writes a new one, and so does
    /// dropping and creating the publication again, even when the definition
    /// is put back as it was, and so does adding or renaming an enum value.
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
                     WHERE namespace.pnpubid = publication.oid), ''),
                  (SELECT 'enums=' || string_agg(enum_value.oid::text || ':'
                                                 || enum_value.xmin::text, ','
                                                 ORDER BY enum_value.oid)
                     FROM pg_catalog.pg_enum AS enum_value
                    WHERE enum_value.enumtypid IN (
                      WITH RECURSIVE chain(oid) AS (
                        SELECT attribute.atttypid
                          FROM pg_catalog.pg_publication_tables AS published
                          JOIN pg_catalog.pg_namespace AS namespace
                            ON namespace.nspname = published.schemaname
                          JOIN pg_catalog.pg_class AS class
                            ON class.relnamespace = namespace.oid
                           AND class.relname = published.tablename
                          JOIN pg_catalog.pg_attribute AS attribute
                            ON attribute.attrelid = class.oid
                         WHERE published.pubname = publication.pubname
                           AND attribute.attnum > 0
                           AND NOT attribute.attisdropped
                           AND attribute.attname = ANY(published.attnames)
                        UNION
                        SELECT domain.typbasetype
                          FROM chain
                          JOIN pg_catalog.pg_type AS domain ON domain.oid = chain.oid
                         WHERE domain.typtype = 'd')
                      SELECT oid FROM chain)))
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

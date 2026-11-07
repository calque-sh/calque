use serde::{Deserialize, Serialize};

/// Version-1 PostgreSQL built-in types with canonical cross-layer codecs.
/// Semantic metadata only: PostgreSQL wire decoding lives in the data-plane
/// crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PostgresTypeCodec {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Text,
    Varchar,
    Bpchar,
    Bytea,
    Uuid,
    Json,
    Jsonb,
    Date,
    Time,
    Timestamp,
    Timestamptz,
}

impl PostgresTypeCodec {
    /// Every codec, in declaration order.
    pub const ALL: [Self; 18] = [
        Self::Bool,
        Self::Int2,
        Self::Int4,
        Self::Int8,
        Self::Float4,
        Self::Float8,
        Self::Numeric,
        Self::Text,
        Self::Varchar,
        Self::Bpchar,
        Self::Bytea,
        Self::Uuid,
        Self::Json,
        Self::Jsonb,
        Self::Date,
        Self::Time,
        Self::Timestamp,
        Self::Timestamptz,
    ];

    pub fn from_catalog(schema: &str, name: &str) -> Option<Self> {
        if schema != "pg_catalog" {
            return None;
        }
        match name {
            "bool" => Some(Self::Bool),
            "int2" => Some(Self::Int2),
            "int4" => Some(Self::Int4),
            "int8" => Some(Self::Int8),
            "float4" => Some(Self::Float4),
            "float8" => Some(Self::Float8),
            "numeric" => Some(Self::Numeric),
            "text" => Some(Self::Text),
            "varchar" => Some(Self::Varchar),
            "bpchar" => Some(Self::Bpchar),
            "bytea" => Some(Self::Bytea),
            "uuid" => Some(Self::Uuid),
            "json" => Some(Self::Json),
            "jsonb" => Some(Self::Jsonb),
            "date" => Some(Self::Date),
            "time" => Some(Self::Time),
            "timestamp" => Some(Self::Timestamp),
            "timestamptz" => Some(Self::Timestamptz),
            _ => None,
        }
    }

    /// Whether a primary-key column of this type has one equality across
    /// PostgreSQL and JavaScript.
    ///
    /// `text` and `varchar` qualify only under a deterministic collation, where
    /// PostgreSQL equality is byte equality; enrollment refuses the others with
    /// `PRIMARY_KEY_COLLATION_NONDETERMINISTIC`. `bpchar` stays out:
    /// its equality ignores trailing spaces.
    pub const fn is_primary_key_safe(self) -> bool {
        matches!(
            self,
            Self::Bool
                | Self::Int2
                | Self::Int4
                | Self::Int8
                | Self::Text
                | Self::Varchar
                | Self::Bytea
                | Self::Uuid
        )
    }

    pub const fn catalog_name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int2 => "int2",
            Self::Int4 => "int4",
            Self::Int8 => "int8",
            Self::Float4 => "float4",
            Self::Float8 => "float8",
            Self::Numeric => "numeric",
            Self::Text => "text",
            Self::Varchar => "varchar",
            Self::Bpchar => "bpchar",
            Self::Bytea => "bytea",
            Self::Uuid => "uuid",
            Self::Json => "json",
            Self::Jsonb => "jsonb",
            Self::Date => "date",
            Self::Time => "time",
            Self::Timestamp => "timestamp",
            Self::Timestamptz => "timestamptz",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_catalog_types_and_key_subset_are_explicit() {
        let supported = [
            "bool",
            "int2",
            "int4",
            "int8",
            "float4",
            "float8",
            "numeric",
            "text",
            "varchar",
            "bpchar",
            "bytea",
            "uuid",
            "json",
            "jsonb",
            "date",
            "time",
            "timestamp",
            "timestamptz",
        ];
        assert!(supported
            .iter()
            .all(|name| PostgresTypeCodec::from_catalog("pg_catalog", name).is_some()));
        assert_eq!(
            PostgresTypeCodec::ALL.map(PostgresTypeCodec::catalog_name),
            supported
        );
        assert!(PostgresTypeCodec::from_catalog("extension", "text").is_none());
        assert!(PostgresTypeCodec::from_catalog("pg_catalog", "interval").is_none());
        let admitted = |rule: fn(PostgresTypeCodec) -> bool| -> Vec<&str> {
            supported
                .iter()
                .filter_map(|name| PostgresTypeCodec::from_catalog("pg_catalog", name))
                .filter(|codec| rule(*codec))
                .map(PostgresTypeCodec::catalog_name)
                .collect()
        };
        assert_eq!(
            admitted(PostgresTypeCodec::is_primary_key_safe),
            ["bool", "int2", "int4", "int8", "text", "varchar", "bytea", "uuid"]
        );
    }
}

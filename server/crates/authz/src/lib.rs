//! Server-owned authorization policy compilation and scope derivation: one
//! source-wide scope, or one scope per partition value.
//!
//! This crate accepts deployment policy plus checked `source-control` schema
//! facts. It contains no JWT/session verification, HTTP request parsing,
//! PostgreSQL access, or snapshot execution.

mod partition;

pub use partition::{PartitionTouch, PartitionValue};

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use canonical_log::{
    CanonicalOperation, CanonicalOperationKind, CanonicalTransaction, CellValue, ColumnValue,
};
use sha2::{Digest, Sha256};
use source_control::{PostgresTypeCodec, RegisteredTable, SchemaRegistry, SchemaRegistryBinding};
use sync_types::{ColumnId, TableId};
use thiserror::Error;

const MAX_PRINCIPAL_ATTRIBUTES: usize = 64;
const MAX_PRINCIPAL_VALUE_BYTES: usize = 4 * 1024;
const POLICY_HASH_DOMAIN: &[u8] = b"calque.authorization.policy.v1\0";
const SCOPE_HASH_DOMAIN: &[u8] = b"calque.authorization.scope.v1\0";
/// The policy preimage's profile byte.
const SOURCE_WIDE_PROFILE_TAG: u8 = 1;
const PARTITIONED_PROFILE_TAG: u8 = 2;
/// A zero-length frame, written once per table in each preimage so that the
/// pinned policy and scope hashes keep their values.
const EMPTY_FRAME: [u8; 8] = 0_u64.to_be_bytes();

/// Deployment-edge adapter from verified application credentials/session state
/// to an application-neutral principal. Client-provided scope capabilities are
/// never accepted by this interface.
pub trait PrincipalResolver<RequestContext> {
    type Error;

    fn resolve(&self, context: &RequestContext) -> Result<Principal, Self::Error>;
}

/// An identity authenticated by a deployment-edge [`PrincipalResolver`].
///
/// This type is not deserializable. HTTP/JWT/session adapters validate their
/// own input before constructing it.
#[derive(Clone, PartialEq, Eq)]
pub struct Principal {
    subject: String,
    attributes: BTreeMap<String, PrincipalValue>,
}

impl Principal {
    pub fn new(
        subject: impl Into<String>,
        attributes: BTreeMap<String, PrincipalValue>,
    ) -> Result<Self, PrincipalError> {
        let subject = subject.into();
        validate_name(&subject).map_err(|()| PrincipalError::InvalidSubject)?;
        if attributes.len() > MAX_PRINCIPAL_ATTRIBUTES {
            return Err(PrincipalError::TooManyAttributes);
        }
        for (name, value) in &attributes {
            validate_name(name).map_err(|()| PrincipalError::InvalidAttributeName)?;
            if value.encoded_len() > MAX_PRINCIPAL_VALUE_BYTES {
                return Err(PrincipalError::AttributeValueTooLarge);
            }
        }
        Ok(Self {
            subject,
            attributes,
        })
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn attribute(&self, name: &str) -> Option<&PrincipalValue> {
        self.attributes.get(name)
    }
}

impl fmt::Debug for Principal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Principal")
            .field("subject", &self.subject)
            .field("attribute_names", &self.attributes.keys())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PrincipalError {
    #[error("principal subject is empty, oversized, or contains control characters")]
    InvalidSubject,
    #[error("principal has too many attributes")]
    TooManyAttributes,
    #[error("principal attribute name is empty, oversized, or contains control characters")]
    InvalidAttributeName,
    #[error("principal attribute value exceeds its bound")]
    AttributeValueTooLarge,
}

/// A verified principal attribute, typed by its exact PostgreSQL codec.
#[derive(Clone, PartialEq, Eq)]
pub enum PrincipalValue {
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Bytes(Vec<u8>),
    Uuid([u8; 16]),
}

impl PrincipalValue {
    pub const fn codec(&self) -> PostgresTypeCodec {
        match self {
            Self::Bool(_) => PostgresTypeCodec::Bool,
            Self::Int2(_) => PostgresTypeCodec::Int2,
            Self::Int4(_) => PostgresTypeCodec::Int4,
            Self::Int8(_) => PostgresTypeCodec::Int8,
            Self::Bytes(_) => PostgresTypeCodec::Bytea,
            Self::Uuid(_) => PostgresTypeCodec::Uuid,
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Self::Bool(_) => 1,
            Self::Int2(_) => 2,
            Self::Int4(_) => 4,
            Self::Int8(_) => 8,
            Self::Bytes(value) => value.len(),
            Self::Uuid(_) => 16,
        }
    }
}

impl fmt::Debug for PrincipalValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "<redacted:{}>", self.codec().catalog_name())
    }
}

/// The server-side policy: the tables and columns every verified caller
/// receives, at one authorization generation.
#[derive(Debug, Clone)]
pub struct PolicyDefinition {
    pub generation: u64,
    pub tables: Vec<TablePermitDefinition>,
}

#[derive(Debug, Clone)]
pub struct TablePermitDefinition {
    pub table_id: TableId,
    pub permitted_columns: Vec<ColumnId>,
}

/// How a partitioned policy splits the source: the principal attribute that
/// carries a caller's partition value, and each partitioned table's partition
/// column, which must lead its primary key. Every other permitted table is
/// shared, delivered whole to every partition.
#[derive(Debug, Clone)]
pub struct PartitionDefinition {
    pub attribute: String,
    pub tables: BTreeMap<TableId, ColumnId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PolicyPartition {
    attribute: String,
    codec: PostgresTypeCodec,
}

#[derive(Clone)]
pub struct AuthorizationPolicy {
    generation: u64,
    hash: PolicyHash,
    tables: Vec<TablePermit>,
    partition: Option<PolicyPartition>,
    registry_binding: Option<SchemaRegistryBinding>,
}

impl AuthorizationPolicy {
    /// Compiles eligibility against detached table facts without binding the
    /// result to an activated schema registry.
    ///
    /// This exists for policy tooling and focused validation tests. Production
    /// snapshot, CDC, bootstrap, and change paths reject scopes derived from
    /// this constructor; use [`Self::compile_for_registry`] there.
    pub fn compile(
        definition: PolicyDefinition,
        registered_tables: &[RegisteredTable],
    ) -> Result<Self, PolicyError> {
        Self::compile_with_registry_binding(definition, registered_tables, None)
    }

    /// Compiles policy against one exact activated-registry candidate and
    /// carries that immutable binding into every derived scope.
    pub fn compile_for_registry(
        definition: PolicyDefinition,
        registry: &SchemaRegistry,
    ) -> Result<Self, PolicyError> {
        Self::compile_with_registry_binding(definition, registry.tables(), Some(registry.binding()))
    }

    /// [`Self::compile_for_registry`] for a partitioned policy. Each
    /// partitioned table's column must lead the primary key the registry
    /// read from PostgreSQL, and every partition column must be of one type a
    /// grant attribute can carry.
    pub fn compile_partitioned_for_registry(
        definition: PolicyDefinition,
        partition: PartitionDefinition,
        registry: &SchemaRegistry,
    ) -> Result<Self, PolicyError> {
        Self::compile_partitioned(
            definition,
            partition,
            registry.tables(),
            Some(registry.binding()),
        )
    }

    /// [`Self::compile`] for a partitioned policy, for focused tests.
    pub fn compile_partitioned_detached(
        definition: PolicyDefinition,
        partition: PartitionDefinition,
        registered_tables: &[RegisteredTable],
    ) -> Result<Self, PolicyError> {
        Self::compile_partitioned(definition, partition, registered_tables, None)
    }

    fn compile_partitioned(
        definition: PolicyDefinition,
        partition: PartitionDefinition,
        registered_tables: &[RegisteredTable],
        registry_binding: Option<SchemaRegistryBinding>,
    ) -> Result<Self, PolicyError> {
        validate_name(&partition.attribute).map_err(|()| PolicyError::InvalidPartitionAttribute)?;
        if partition.tables.is_empty() {
            return Err(PolicyError::NoPartitionedTable);
        }
        let mut policy =
            Self::compile_with_registry_binding(definition, registered_tables, registry_binding)?;
        let schema = schema_index(registered_tables)?;
        let mut codec = None;
        for (table_id, column_id) in &partition.tables {
            let permit = policy
                .tables
                .iter_mut()
                .find(|permit| &permit.table_id == table_id)
                .ok_or(PolicyError::PartitionedTableNotPermitted)?;
            let table = schema.get(table_id).ok_or(PolicyError::UnknownTable)?;
            let column = table
                .column_ids
                .iter()
                .zip(&table.inspection.columns)
                .find(|(id, _)| *id == column_id)
                .map(|(_, column)| column)
                .ok_or(PolicyError::UnknownColumn)?;
            if column.primary_key_ordinal != Some(1) {
                return Err(PolicyError::PartitionColumnNotLeadingKey);
            }
            let column_codec =
                PostgresTypeCodec::from_catalog(&column.type_schema, &column.type_name)
                    .filter(|codec| PartitionValue::supports(*codec))
                    .ok_or(PolicyError::UnsupportedPartitionType)?;
            if codec.is_some_and(|codec| codec != column_codec) {
                return Err(PolicyError::PartitionTypesDiffer);
            }
            codec = Some(column_codec);
            permit.partition_column = Some(*column_id);
        }
        let partition = PolicyPartition {
            attribute: partition.attribute,
            codec: codec.ok_or(PolicyError::NoPartitionedTable)?,
        };
        policy.hash = hash_policy(policy.generation, &policy.tables, Some(&partition));
        policy.partition = Some(partition);
        Ok(policy)
    }

    fn compile_with_registry_binding(
        definition: PolicyDefinition,
        registered_tables: &[RegisteredTable],
        registry_binding: Option<SchemaRegistryBinding>,
    ) -> Result<Self, PolicyError> {
        let schema = schema_index(registered_tables)?;
        let mut tables = definition
            .tables
            .into_iter()
            .map(|definition| {
                compile_table(definition.table_id, definition.permitted_columns, &schema)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if tables.is_empty() {
            return Err(PolicyError::EmptyPolicy);
        }
        tables.sort_by(|left, right| left.table_id.cmp(&right.table_id));
        if tables
            .windows(2)
            .any(|pair| pair[0].table_id == pair[1].table_id)
        {
            return Err(PolicyError::DuplicateTable);
        }
        let hash = hash_policy(definition.generation, &tables, None);
        Ok(Self {
            generation: definition.generation,
            hash,
            tables,
            partition: None,
            registry_binding,
        })
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn hash(&self) -> &PolicyHash {
        &self.hash
    }

    /// Exact registry identity used for production policy compilation.
    pub fn registry_binding(&self) -> Option<&SchemaRegistryBinding> {
        self.registry_binding.as_ref()
    }

    /// The scope every verified caller of a source-wide policy receives: a
    /// function of the policy alone, so nothing a caller presents reaches it.
    /// Under a partitioned policy it holds the shared tables and nothing
    /// else; a caller's scope there is [`Self::scope_for`].
    pub fn scope(&self) -> AuthorizedScope {
        let permits = self
            .tables
            .iter()
            .filter(|permit| permit.partition_column.is_none())
            .map(|permit| (permit.table_id.clone(), permit.clone()))
            .collect::<BTreeMap<_, _>>();
        self.scope_of(permits, None)
    }

    /// The scope one verified principal receives. Under a partitioned policy
    /// it is the partition the principal's attribute names; a principal
    /// without the attribute, or with one of another type, has none.
    pub fn scope_for(&self, principal: &Principal) -> Result<AuthorizedScope, ScopeError> {
        let Some(partition) = &self.partition else {
            return Ok(self.scope());
        };
        let attribute = principal
            .attribute(&partition.attribute)
            .ok_or(ScopeError::MissingPartitionAttribute)?;
        let value = PartitionValue::from_principal(attribute, partition.codec)
            .ok_or(ScopeError::PartitionAttributeType)?;
        self.partition_scope(value)
    }

    /// The scope of one partition, for the server's own work on it: its
    /// build, and the application's requests to warm it.
    pub fn partition_scope(&self, value: PartitionValue) -> Result<AuthorizedScope, ScopeError> {
        if self.partition.is_none() {
            return Err(ScopeError::NotPartitioned);
        }
        let permits = self
            .tables
            .iter()
            .map(|permit| (permit.table_id.clone(), permit.clone()))
            .collect::<BTreeMap<_, _>>();
        Ok(self.scope_of(permits, Some(value)))
    }

    fn scope_of(
        &self,
        permits: BTreeMap<TableId, TablePermit>,
        partition: Option<PartitionValue>,
    ) -> AuthorizedScope {
        let scope_hash = hash_scope(&self.hash, permits.values(), partition.as_ref());
        AuthorizedScope {
            generation: self.generation,
            policy_hash: self.hash,
            scope_hash,
            permits: Arc::new(permits),
            partition,
            registry_binding: self.registry_binding.clone(),
        }
    }

    /// The principal attribute that carries a partition value, and the type
    /// of every partition column, when the policy is partitioned.
    pub fn partition(&self) -> Option<(&str, PostgresTypeCodec)> {
        self.partition
            .as_ref()
            .map(|partition| (partition.attribute.as_str(), partition.codec))
    }

    /// The partition one canonical operation touches, or `None` for a shared
    /// table, a table outside the policy, or a source-wide policy.
    pub fn partition_touch(
        &self,
        operation: &CanonicalOperation,
    ) -> Result<Option<PartitionTouch>, CanonicalAuthorizationError> {
        if self.partition.is_none() {
            return Ok(None);
        }
        let Some(column) = self
            .tables
            .iter()
            .find(|permit| permit.table_id == operation.table_id)
            .and_then(|permit| permit.partition_column)
        else {
            return Ok(None);
        };
        partition::partition_touch(operation, column).map(Some)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ScopeError {
    #[error("the grant carries no value for the partition attribute")]
    MissingPartitionAttribute,
    #[error("the grant's partition attribute is not of the partition column's type")]
    PartitionAttributeType,
    #[error("the policy is not partitioned")]
    NotPartitioned,
}

impl fmt::Debug for AuthorizationPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationPolicy")
            .field("generation", &self.generation)
            .field("hash", &self.hash)
            .field("tables", &self.tables.len())
            .field("partitioned", &self.partition.is_some())
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("authorization policy must permit at least one table")]
    EmptyPolicy,
    #[error("authorization policy repeats a table")]
    DuplicateTable,
    #[error("authorization schema facts repeat a stable table ID")]
    DuplicateSchemaTable,
    #[error("authorization policy references an unknown table")]
    UnknownTable,
    #[error("publication row filters are not supported; authorization is source-wide")]
    PublicationRowFilter,
    #[error("publication column lists are not supported; authorization is source-wide")]
    PublicationColumnList,
    #[error("registered table/column mapping is incomplete")]
    InvalidSchemaMapping,
    #[error("authorization policy must permit at least one column")]
    EmptyColumnSet,
    #[error("authorization policy repeats a permitted column")]
    DuplicatePermittedColumn,
    #[error("authorization policy references an unknown column")]
    UnknownColumn,
    #[error("the synchronized column set must include every primary-key column")]
    PrimaryKeyColumnNotPermitted,
    #[error("table has no safely inspectable primary/replica identity")]
    MissingIdentity,
    #[error("partition attribute name is empty, oversized, or contains control characters")]
    InvalidPartitionAttribute,
    #[error("a partitioned policy must partition at least one table")]
    NoPartitionedTable,
    #[error("a partitioned table is not permitted by the policy")]
    PartitionedTableNotPermitted,
    #[error("a partition column must lead its table's primary key")]
    PartitionColumnNotLeadingKey,
    #[error("a partition column must be int2, int4, int8 or uuid")]
    UnsupportedPartitionType,
    #[error("every partition column must be of one type")]
    PartitionTypesDiffer,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CanonicalAuthorizationError {
    #[error("canonical row repeats a column")]
    DuplicateRowColumn,
    #[error("canonical update patch repeats a column")]
    DuplicateChangedColumn,
    #[error("canonical insert omits a permitted column")]
    MissingPermittedColumn,
    #[error("canonical key of a partitioned table does not lead with its partition value")]
    InvalidPartitionKey,
    /// An update moved a row into or out of the scope's partition. Neither
    /// partition's state can take it as a patch, so both rebuild from a base
    /// cut after it.
    #[error("a row moved between partitions; the partition must rebuild from a fresh base")]
    PartitionMoved,
}

/// Stable authorization receipt carried through snapshots, artifacts, plans,
/// and revalidation without exposing principal values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AuthorizationIdentity {
    generation: u64,
    policy_hash: PolicyHash,
    scope_hash: AuthorizationScopeHash,
}

impl AuthorizationIdentity {
    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub const fn policy_hash(self) -> PolicyHash {
        self.policy_hash
    }

    pub const fn scope_hash(self) -> AuthorizationScopeHash {
        self.scope_hash
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolicyHash([u8; 32]);

impl PolicyHash {
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for PolicyHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "sha256:{}", hex::encode(self.0))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AuthorizationScopeHash([u8; 32]);

impl AuthorizationScopeHash {
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for AuthorizationScopeHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "sha256:{}", hex::encode(self.0))
    }
}

/// Validated capability passed to snapshot, CDC, artifact, bootstrap, and
/// mutation layers. It has no public constructor and is not deserializable.
#[derive(Clone)]
pub struct AuthorizedScope {
    generation: u64,
    policy_hash: PolicyHash,
    scope_hash: AuthorizationScopeHash,
    // Shared because every change build clones its plan's scope.
    permits: Arc<BTreeMap<TableId, TablePermit>>,
    /// The partition value of a partitioned scope. Its partitioned tables
    /// are the permits that name a partition column.
    partition: Option<PartitionValue>,
    registry_binding: Option<SchemaRegistryBinding>,
}

impl AuthorizedScope {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn policy_hash(&self) -> &PolicyHash {
        &self.policy_hash
    }

    pub const fn scope_hash(&self) -> &AuthorizationScopeHash {
        &self.scope_hash
    }

    pub const fn identity(&self) -> AuthorizationIdentity {
        AuthorizationIdentity {
            generation: self.generation,
            policy_hash: self.policy_hash,
            scope_hash: self.scope_hash,
        }
    }

    /// Exact registry identity used for policy compilation, when the caller
    /// chose the production registry-bound constructor.
    pub fn registry_binding(&self) -> Option<&SchemaRegistryBinding> {
        self.registry_binding.as_ref()
    }

    pub fn permits_table(&self, table_id: &TableId) -> bool {
        self.permits.contains_key(table_id)
    }

    pub fn permitted_tables(&self) -> impl ExactSizeIterator<Item = &TableId> {
        self.permits.keys()
    }

    pub fn permits_column(&self, table_id: &TableId, column_id: ColumnId) -> bool {
        self.permits
            .get(table_id)
            .is_some_and(|permit| permit.permits_column(column_id))
    }

    pub fn table_permit(&self, table_id: &TableId) -> Option<&TablePermit> {
        self.permits.get(table_id)
    }

    /// The partition value this scope is bound to, when it is partitioned.
    pub fn partition_value(&self) -> Option<&PartitionValue> {
        self.partition.as_ref()
    }

    /// The column and value a snapshot of `table_id` is filtered by: present
    /// for a partitioned table of a partitioned scope, absent for a shared
    /// one.
    pub fn partition_filter(&self, table_id: &TableId) -> Option<(ColumnId, &PartitionValue)> {
        let column = self.permits.get(table_id)?.partition_column?;
        Some((column, self.partition.as_ref()?))
    }

    /// Whether a row of `table_id` with this canonical key may be written
    /// under this scope: the table is permitted, and a partitioned table's
    /// key leads with this scope's partition value.
    pub fn admits_row_key(
        &self,
        table_id: &TableId,
        primary_key: &[u8],
    ) -> Result<bool, CanonicalAuthorizationError> {
        let Some(permit) = self.permits.get(table_id) else {
            return Ok(false);
        };
        match (permit.partition_column, &self.partition) {
            (None, _) => Ok(true),
            (Some(column), Some(value)) => partition::key_in_partition(primary_key, column, value),
            (Some(_), None) => Ok(false),
        }
    }

    /// Routes and projects one complete canonical transaction for this scope.
    /// The transaction cursor is not part of this value; callers still advance
    /// their global cursor even when every operation is omitted.
    pub fn route_transaction(
        &self,
        transaction: &CanonicalTransaction,
    ) -> Result<CanonicalTransaction, CanonicalAuthorizationError> {
        Ok(CanonicalTransaction {
            key: transaction.key.clone(),
            transaction_id: transaction.transaction_id.clone(),
            begin_lsn: transaction.begin_lsn.clone(),
            commit_lsn: transaction.commit_lsn.clone(),
            commit_time_unix_micros: transaction.commit_time_unix_micros,
            operations: self.route_operations(&transaction.operations)?,
            mutation_ids: transaction.mutation_ids.clone(),
        })
    }

    /// Routes and projects every operation of one complete transaction, in
    /// order, refusing the whole transaction if any operation is refused.
    pub fn route_operations(
        &self,
        operations: &[CanonicalOperation],
    ) -> Result<Vec<CanonicalOperation>, CanonicalAuthorizationError> {
        let mut routed = Vec::with_capacity(operations.len());
        for operation in operations {
            if let Some(operation) = self.route_operation(operation)? {
                routed.push(operation);
            }
        }
        Ok(routed)
    }

    /// [`Self::route_operations`] for operations the caller owns, which are
    /// projected by moving their values rather than copying them.
    pub fn route_owned_operations(
        &self,
        operations: Vec<CanonicalOperation>,
    ) -> Result<Vec<CanonicalOperation>, CanonicalAuthorizationError> {
        let mut routed = Vec::with_capacity(operations.len());
        for operation in operations {
            if let Some(operation) = self.route_owned_operation(operation)? {
                routed.push(operation);
            }
        }
        Ok(routed)
    }

    /// Projects one operation onto this scope, or returns `None` when its table
    /// is outside the scope, its row belongs to another partition, or an
    /// update changes only withheld columns. A row moving into or out of this
    /// scope's partition is [`CanonicalAuthorizationError::PartitionMoved`].
    /// Snapshot roll-forward and change delivery share this implementation.
    pub fn route_operation(
        &self,
        operation: &CanonicalOperation,
    ) -> Result<Option<CanonicalOperation>, CanonicalAuthorizationError> {
        if !self.permits.contains_key(&operation.table_id) {
            return Ok(None);
        }
        self.route_owned_operation(operation.clone())
    }

    /// [`Self::route_operation`] for an operation the caller owns.
    pub fn route_owned_operation(
        &self,
        operation: CanonicalOperation,
    ) -> Result<Option<CanonicalOperation>, CanonicalAuthorizationError> {
        let Some(permit) = self.permits.get(&operation.table_id) else {
            return Ok(None);
        };
        if let Some(column) = permit.partition_column {
            let value = self
                .partition
                .as_ref()
                .ok_or(CanonicalAuthorizationError::InvalidPartitionKey)?;
            if !partition::routes_to(&operation, column, value)? {
                return Ok(None);
            }
        }
        project_operation(operation, permit)
    }
}

impl fmt::Debug for AuthorizedScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizedScope")
            .field("generation", &self.generation)
            .field("policy_hash", &self.policy_hash)
            .field("scope_hash", &self.scope_hash)
            .field("permitted_tables", &self.permits.keys())
            .field("partition", &self.partition)
            .finish()
    }
}

#[derive(Clone)]
pub struct TablePermit {
    table_id: TableId,
    permitted_columns: Vec<ColumnId>,
    /// The column that leads the key and names a row's partition, for a
    /// partitioned table.
    partition_column: Option<ColumnId>,
}

impl TablePermit {
    pub fn permitted_columns(&self) -> impl ExactSizeIterator<Item = ColumnId> + '_ {
        self.permitted_columns.iter().copied()
    }

    pub fn partition_column(&self) -> Option<ColumnId> {
        self.partition_column
    }

    pub fn permits_column(&self, column_id: ColumnId) -> bool {
        self.permitted_columns.binary_search(&column_id).is_ok()
    }
}

impl fmt::Debug for TablePermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TablePermit")
            .field("table_id", &self.table_id)
            .field("permitted_columns", &self.permitted_columns)
            .field("partition_column", &self.partition_column)
            .finish()
    }
}

fn project_operation(
    operation: CanonicalOperation,
    permit: &TablePermit,
) -> Result<Option<CanonicalOperation>, CanonicalAuthorizationError> {
    let CanonicalOperation {
        table_id,
        primary_key,
        kind,
    } = operation;
    let kind = match kind {
        CanonicalOperationKind::InsertFullRow { columns } => {
            CanonicalOperationKind::InsertFullRow {
                columns: project_row(columns, &permit.permitted_columns)?,
            }
        }
        CanonicalOperationKind::UpdatePatch {
            mut changed_columns,
        } => {
            let _ = index_columns(
                &changed_columns,
                CanonicalAuthorizationError::DuplicateChangedColumn,
            )?;
            changed_columns.retain(|column| permit.permits_column(column.column_id));
            if changed_columns.is_empty() {
                return Ok(None);
            }
            CanonicalOperationKind::UpdatePatch { changed_columns }
        }
        CanonicalOperationKind::DeleteByKey => CanonicalOperationKind::DeleteByKey,
        CanonicalOperationKind::TruncateAuthorizedTable { reason } => {
            CanonicalOperationKind::TruncateAuthorizedTable { reason }
        }
    };
    Ok(Some(CanonicalOperation {
        table_id,
        primary_key,
        kind,
    }))
}

/// A full row's permitted columns, in permit order. `permitted` is strictly
/// ascending, as a permit's columns always are, so a row whose columns are
/// exactly those, in that order, has no duplicate and misses none, and is
/// returned as it came.
fn project_row(
    mut columns: Vec<ColumnValue>,
    permitted: &[ColumnId],
) -> Result<Vec<ColumnValue>, CanonicalAuthorizationError> {
    if columns.len() == permitted.len()
        && columns
            .iter()
            .zip(permitted)
            .all(|(column, permitted)| column.column_id == *permitted)
    {
        return Ok(columns);
    }
    let positions = columns
        .iter()
        .enumerate()
        .map(|(position, column)| (column.column_id, position))
        .collect::<Vec<_>>();
    let positions = index_positions(positions, CanonicalAuthorizationError::DuplicateRowColumn)?;
    permitted
        .iter()
        .map(|permitted| {
            positions
                .get(permitted)
                .map(|&position| ColumnValue {
                    column_id: *permitted,
                    // Each position is taken at most once: positions and
                    // permitted columns are both unique.
                    value: std::mem::replace(&mut columns[position].value, CellValue::Null),
                })
                .ok_or(CanonicalAuthorizationError::MissingPermittedColumn)
        })
        .collect()
}

fn index_columns(
    columns: &[ColumnValue],
    duplicate: CanonicalAuthorizationError,
) -> Result<BTreeMap<ColumnId, &CellValue>, CanonicalAuthorizationError> {
    let mut result = BTreeMap::new();
    for column in columns {
        if result.insert(column.column_id, &column.value).is_some() {
            return Err(duplicate);
        }
    }
    Ok(result)
}

fn index_positions(
    positions: Vec<(ColumnId, usize)>,
    duplicate: CanonicalAuthorizationError,
) -> Result<BTreeMap<ColumnId, usize>, CanonicalAuthorizationError> {
    let mut result = BTreeMap::new();
    for (column_id, position) in positions {
        if result.insert(column_id, position).is_some() {
            return Err(duplicate);
        }
    }
    Ok(result)
}

fn schema_index(
    registered_tables: &[RegisteredTable],
) -> Result<BTreeMap<TableId, &RegisteredTable>, PolicyError> {
    let mut schema = BTreeMap::new();
    for table in registered_tables {
        if schema.insert(table.table_id.clone(), table).is_some() {
            return Err(PolicyError::DuplicateSchemaTable);
        }
    }
    Ok(schema)
}

fn compile_table(
    table_id: TableId,
    permitted_column_ids: Vec<ColumnId>,
    schema: &BTreeMap<TableId, &RegisteredTable>,
) -> Result<TablePermit, PolicyError> {
    let table = schema.get(&table_id).ok_or(PolicyError::UnknownTable)?;
    if table.inspection.has_publication_row_filter {
        return Err(PolicyError::PublicationRowFilter);
    }
    if table.inspection.has_publication_column_list {
        return Err(PolicyError::PublicationColumnList);
    }
    if table.column_ids.len() != table.inspection.columns.len() || table.column_ids.is_empty() {
        return Err(PolicyError::InvalidSchemaMapping);
    }
    let mut registered_ids = BTreeSet::new();
    for column_id in &table.column_ids {
        if !registered_ids.insert(*column_id) {
            return Err(PolicyError::InvalidSchemaMapping);
        }
    }
    let mut primary_key = BTreeSet::new();
    for (column_id, column) in table.column_ids.iter().zip(&table.inspection.columns) {
        if column.primary_key_ordinal.is_some() {
            primary_key.insert(*column_id);
        }
    }
    if primary_key.is_empty() {
        return Err(PolicyError::MissingIdentity);
    }
    // Replica identity DEFAULT (the primary key) or FULL. NOTHING and USING
    // INDEX leave updates and deletes without an identity this crate checked.
    if !matches!(table.inspection.replica_identity, 'd' | 'f') {
        return Err(PolicyError::MissingIdentity);
    }

    if permitted_column_ids.is_empty() {
        return Err(PolicyError::EmptyColumnSet);
    }
    let permitted_ids: BTreeSet<_> = permitted_column_ids.iter().copied().collect();
    if permitted_ids.len() != permitted_column_ids.len() {
        return Err(PolicyError::DuplicatePermittedColumn);
    }
    if permitted_ids.iter().any(|id| !registered_ids.contains(id)) {
        return Err(PolicyError::UnknownColumn);
    }
    if !primary_key.is_subset(&permitted_ids) {
        return Err(PolicyError::PrimaryKeyColumnNotPermitted);
    }
    Ok(TablePermit {
        table_id,
        permitted_columns: permitted_ids.into_iter().collect(),
        partition_column: None,
    })
}

/// A source-wide policy's preimage is the profile tag and, per table, its id,
/// its columns and an empty frame. A partitioned one has its own tag, the
/// attribute and the partition type after it, and its partition column in
/// each partitioned table's frame.
fn hash_policy(
    generation: u64,
    tables: &[TablePermit],
    partition: Option<&PolicyPartition>,
) -> PolicyHash {
    let mut hasher = Sha256::new();
    hasher.update(POLICY_HASH_DOMAIN);
    hasher.update(generation.to_be_bytes());
    match partition {
        None => hasher.update([SOURCE_WIDE_PROFILE_TAG]),
        Some(partition) => {
            hasher.update([PARTITIONED_PROFILE_TAG]);
            update_bytes(&mut hasher, partition.attribute.as_bytes());
            update_bytes(&mut hasher, partition.codec.catalog_name().as_bytes());
        }
    }
    update_len(&mut hasher, tables.len());
    for table in tables {
        update_bytes(&mut hasher, table.table_id.as_str().as_bytes());
        update_len(&mut hasher, table.permitted_columns.len());
        for column in &table.permitted_columns {
            hasher.update(column.get().to_be_bytes());
        }
        match table.partition_column {
            None => hasher.update(EMPTY_FRAME),
            Some(column) => update_bytes(&mut hasher, &column.get().to_be_bytes()),
        }
    }
    PolicyHash(hasher.finalize().into())
}

/// The policy hash and, per table, its id and a frame: empty for a table
/// delivered whole, the partition value for a partitioned table.
fn hash_scope<'a>(
    policy_hash: &PolicyHash,
    tables: impl Iterator<Item = &'a TablePermit>,
    partition: Option<&PartitionValue>,
) -> AuthorizationScopeHash {
    let mut hasher = Sha256::new();
    hasher.update(SCOPE_HASH_DOMAIN);
    hasher.update(policy_hash.0);
    for table in tables {
        update_bytes(&mut hasher, table.table_id.as_str().as_bytes());
        match (table.partition_column, partition) {
            (Some(_), Some(value)) => update_bytes(&mut hasher, &value.hash_frame()),
            _ => hasher.update(EMPTY_FRAME),
        }
    }
    AuthorizationScopeHash(hasher.finalize().into())
}

fn update_len(hasher: &mut Sha256, len: usize) {
    hasher.update(u64::try_from(len).unwrap_or(u64::MAX).to_be_bytes());
}

fn update_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    update_len(hasher, bytes.len());
    hasher.update(bytes);
}

fn validate_name(value: &str) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > 512
        || value
            .chars()
            .any(|character| character.is_control() || character == '\0')
    {
        Err(())
    } else {
        Ok(())
    }
}

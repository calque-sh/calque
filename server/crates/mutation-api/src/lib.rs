//! Idempotent registered mutation-command execution for PostgreSQL.
//!
//! Applications own authentication and command implementations. This crate
//! owns bounded wire validation, exact declared-footprint comparison,
//! transactionally durable idempotency receipts, and the logical marker that
//! lets canonical CDC acknowledge an optimistic mutation.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use authz::AuthorizedScope;
use postgres::{Client, Transaction};
use prost::Message;
use sha2::{Digest, Sha256};
use source_control::ActivatedSource;
use subtle::ConstantTimeEq;
use sync_protocol::{
    mutation::{
        AffectedRow, Conflict, Cursor, ExpectedVersion, MutationBatchRequest,
        MutationBatchResponse, MutationEnvelope, MutationFootprint, MutationLogicalMarker,
        MutationOutcome, MutationOutcomeCode,
    },
    validate_mutation_batch_request, MAX_CLIENT_INSTANCE_ID_BYTES, MAX_COMMAND_NAME_BYTES,
    MAX_CONFLICT_KIND_BYTES, MAX_CONFLICT_PAYLOAD_BYTES, MAX_DEVICE_ID_BYTES,
    MAX_EXPECTED_VERSIONS, MAX_MUTATION_BATCH_INPUT_BYTES, MAX_MUTATION_BATCH_SIZE,
    MAX_MUTATION_CANONICAL_INPUT_BYTES, MAX_MUTATION_FOOTPRINT_ROWS, MAX_MUTATION_FOOTPRINT_TABLES,
    MAX_MUTATION_SCOPE_TOKEN_BYTES, MAX_PENDING_MUTATION_IDS, PRIMARY_KEY_MAX_BYTES,
};
use sync_types::{
    CursorCodec, CursorPosition, MutationId, OpaqueCursor, SourceEpoch, SourceId, TableId,
};
use thiserror::Error;
use uuid::Uuid;

const MAX_FUTURE_SKEW_MICROS: u64 = 5 * 60 * 1_000_000;
const LOGICAL_MARKER_PREFIX: &str = "sync.mutation.v1";

const INSTALL_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS calque_internal;
CREATE TABLE IF NOT EXISTS calque_internal.mutation_receipts (
  mutation_id text PRIMARY KEY,
  source_id text NOT NULL,
  source_epoch text NOT NULL,
  authorization_generation bigint NOT NULL CHECK (authorization_generation >= 0),
  authorization_policy_hash bytea NOT NULL CHECK (octet_length(authorization_policy_hash) = 32),
  authorization_scope_hash bytea NOT NULL CHECK (octet_length(authorization_scope_hash) = 32),
  command_name text NOT NULL,
  command_version bigint NOT NULL CHECK (command_version >= 1),
  canonical_input_sha256 bytea NOT NULL CHECK (octet_length(canonical_input_sha256) = 32),
  server_transaction_id bytea NOT NULL CHECK (octet_length(server_transaction_id) = 16),
  device_id text NOT NULL,
  client_instance_id text,
  observed_canonical_cursor bytea,
  committed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
"#;

/// One operation a command capability may name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommandOperation {
    Insert,
    Update,
    Delete,
}

impl CommandOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// Which registered commands one presented authorization may execute, and with
/// which operations.
///
/// Built by the deployment from its configuration and narrowed by the presented
/// credential; nothing in a request contributes to it. There is no "everything"
/// value: a deployment that means every command it mounts enumerates them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandCapability {
    by_command: BTreeMap<String, BTreeSet<CommandOperation>>,
}

impl CommandCapability {
    /// A capability over exactly these commands and, for each, exactly these
    /// operations. A command whose operation set is empty is dropped: it
    /// authorizes nothing, and keeping it would make `is_empty` lie.
    pub fn new(commands: impl IntoIterator<Item = (String, BTreeSet<CommandOperation>)>) -> Self {
        Self {
            by_command: commands
                .into_iter()
                .filter(|(_, operations)| !operations.is_empty())
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.by_command.is_empty()
    }

    pub fn permits(&self, command_name: &str, operation: CommandOperation) -> bool {
        self.by_command
            .get(command_name)
            .is_some_and(|operations| operations.contains(&operation))
    }

    pub fn command_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.by_command.keys().map(String::as_str)
    }
}

#[derive(Clone)]
pub struct MutationAuthorization {
    source_id: SourceId,
    activated_source: ActivatedSource,
    scope_token: Vec<u8>,
    scope: AuthorizedScope,
    capability: CommandCapability,
}

impl MutationAuthorization {
    /// Builds the authorization one batch executes under.
    ///
    /// An empty `capability` is refused, so [`execute_batch`], which takes this
    /// type, cannot run for a principal that holds no write capability.
    pub fn new(
        source_id: SourceId,
        activated_source: &ActivatedSource,
        scope_token: Vec<u8>,
        scope: AuthorizedScope,
        capability: CommandCapability,
    ) -> Result<Self, MutationBatchError> {
        if scope_token.is_empty()
            || scope_token.len() > MAX_MUTATION_SCOPE_TOKEN_BYTES
            || scope.permitted_tables().len() == 0
            || i64::try_from(scope.generation()).is_err()
            || scope.registry_binding() != Some(activated_source.registry_binding())
            || capability.is_empty()
        {
            return Err(MutationBatchError::InvalidAuthorization);
        }
        Ok(Self {
            source_id,
            activated_source: activated_source.clone(),
            scope_token,
            scope,
            capability,
        })
    }

    fn verify_scope_token(&self, candidate: &[u8]) -> bool {
        candidate.len() == self.scope_token.len()
            && bool::from(candidate.ct_eq(self.scope_token.as_slice()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AffectedKey {
    pub table_id: TableId,
    pub primary_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommandFootprint {
    pub rows: BTreeSet<AffectedKey>,
    pub table_ids: BTreeSet<TableId>,
}

#[derive(Debug, Clone)]
pub struct CommandContext<'a> {
    pub mutation_id: &'a MutationId,
    pub base_cursor: &'a CursorPosition,
    pub expected_versions: &'a [ExpectedVersion],
    /// Server-derived table and column capability.
    pub authorized_scope: &'a AuthorizedScope,
}

/// Mutation IDs proven by durable receipts to belong to one scope and device.
#[derive(Debug, Clone, Default)]
pub struct ResolvedMutationAcknowledgements {
    ids: BTreeSet<MutationId>,
    source_id: Option<SourceId>,
    source_epoch: Option<SourceEpoch>,
    authorization_generation: Option<u64>,
    policy_hash: Option<[u8; 32]>,
    scope_hash: Option<[u8; 32]>,
    device_id: Option<String>,
    requested_ids: BTreeSet<MutationId>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone)]
pub struct MutationReceiptBinding {
    pub mutation_id: MutationId,
    pub source_id: SourceId,
    pub source_epoch: SourceEpoch,
    pub authorization_generation: u64,
    pub authorization_policy_hash: [u8; 32],
    pub authorization_scope_hash: [u8; 32],
    pub device_id: String,
}

impl ResolvedMutationAcknowledgements {
    pub fn contains(&self, id: &MutationId) -> bool {
        self.ids.contains(id)
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    pub fn matches_request(
        &self,
        source_id: &SourceId,
        source_epoch: &SourceEpoch,
        scope: &AuthorizedScope,
        device_id: &str,
        requested_ids: &BTreeSet<MutationId>,
    ) -> bool {
        if requested_ids.is_empty() && self.ids.is_empty() && self.source_id.is_none() {
            return true;
        }
        self.source_id.as_ref() == Some(source_id)
            && self.source_epoch.as_ref() == Some(source_epoch)
            && self.authorization_generation == Some(scope.generation())
            && self.policy_hash == Some(*scope.policy_hash().as_bytes())
            && self.scope_hash == Some(*scope.scope_hash().as_bytes())
            && self.device_id.as_deref() == Some(device_id)
            && &self.requested_ids == requested_ids
            && self.ids.is_subset(requested_ids)
    }

    /// Constructs a proof from rows already read through a durable receipt
    /// storage adapter. Rows outside the exact request/scope/device are omitted.
    #[cfg(any(test, feature = "test-support"))]
    pub fn from_test_receipt_bindings(
        source_id: SourceId,
        source_epoch: SourceEpoch,
        scope: &AuthorizedScope,
        device_id: &str,
        requested_ids: &BTreeSet<MutationId>,
        bindings: impl IntoIterator<Item = MutationReceiptBinding>,
    ) -> Result<Self, MutationBatchError> {
        if !valid_client_identifier(device_id, MAX_DEVICE_ID_BYTES)
            || requested_ids.len() > MAX_PENDING_MUTATION_IDS
        {
            return Err(MutationBatchError::InvalidRequest);
        }
        let ids = bindings
            .into_iter()
            .filter(|binding| {
                binding.authorization_generation == scope.generation()
                    && binding.authorization_policy_hash == *scope.policy_hash().as_bytes()
                    && binding.authorization_scope_hash == *scope.scope_hash().as_bytes()
                    && binding.source_id == source_id
                    && binding.source_epoch == source_epoch
                    && binding.device_id == device_id
                    && requested_ids.contains(&binding.mutation_id)
            })
            .map(|binding| binding.mutation_id)
            .collect();
        Ok(Self {
            ids,
            source_id: Some(source_id),
            source_epoch: Some(source_epoch),
            authorization_generation: Some(scope.generation()),
            policy_hash: Some(*scope.policy_hash().as_bytes()),
            scope_hash: Some(*scope.scope_hash().as_bytes()),
            device_id: Some(device_id.to_owned()),
            requested_ids: requested_ids.clone(),
        })
    }
}

/// Resolves only requested IDs whose committed receipt matches the exact
/// authorization scope and stable device. Unknown receipts, and receipts bound
/// to another scope or device, are omitted.
pub fn resolve_acknowledgements(
    client: &mut Client,
    source_id: &SourceId,
    source_epoch: &SourceEpoch,
    scope: &AuthorizedScope,
    device_id: &str,
    requested_ids: &BTreeSet<MutationId>,
) -> Result<ResolvedMutationAcknowledgements, MutationBatchError> {
    if !valid_client_identifier(device_id, MAX_DEVICE_ID_BYTES)
        || requested_ids.len() > MAX_PENDING_MUTATION_IDS
    {
        return Err(MutationBatchError::InvalidRequest);
    }
    if requested_ids.is_empty() {
        return Ok(ResolvedMutationAcknowledgements::default());
    }
    let requested = requested_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let authorization_generation =
        i64::try_from(scope.generation()).map_err(|_| MutationBatchError::InvalidAuthorization)?;
    let rows = client
        .query(
            "SELECT mutation_id FROM calque_internal.mutation_receipts \
             WHERE source_id = $1 AND source_epoch = $2 \
             AND authorization_generation = $3 \
             AND authorization_policy_hash = $4 \
             AND authorization_scope_hash = $5 AND device_id = $6 \
             AND mutation_id = ANY($7)",
            &[
                &source_id.as_str(),
                &source_epoch.as_str(),
                &authorization_generation,
                &scope.policy_hash().as_bytes().as_slice(),
                &scope.scope_hash().as_bytes().as_slice(),
                &device_id,
                &requested,
            ],
        )
        .map_err(MutationBatchError::AcknowledgementResolution)?;
    let ids = rows
        .into_iter()
        .map(|row| MutationId::new(row.get::<_, String>(0)))
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|_| MutationBatchError::InvalidRequest)?;
    Ok(ResolvedMutationAcknowledgements {
        ids,
        source_id: Some(source_id.clone()),
        source_epoch: Some(source_epoch.clone()),
        authorization_generation: Some(scope.generation()),
        policy_hash: Some(*scope.policy_hash().as_bytes()),
        scope_hash: Some(*scope.scope_hash().as_bytes()),
        device_id: Some(device_id.to_owned()),
        requested_ids: requested_ids.clone(),
    })
}

/// Records that the canonical log durably contains the transaction marker for
/// these receipts. Call this after canonical append and before acknowledging
/// the corresponding WAL position. Replays at the same cursor are idempotent;
/// assigning a receipt to a different cursor fails closed.
pub fn mark_canonical_receipts(
    client: &mut Client,
    cursor_codec: &CursorCodec,
    source_id: &SourceId,
    source_epoch: &SourceEpoch,
    server_transaction_id: &[u8; 16],
    mutation_ids: &BTreeSet<MutationId>,
    observed_cursor: &OpaqueCursor,
) -> Result<usize, MutationBatchError> {
    if mutation_ids.len() > MAX_PENDING_MUTATION_IDS {
        return Err(MutationBatchError::InvalidRequest);
    }
    if mutation_ids.is_empty() {
        return Ok(0);
    }
    cursor_codec
        .decode_for_epoch(observed_cursor, source_epoch)
        .map_err(|_| MutationBatchError::InvalidRequest)?;
    let ids = mutation_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let cursor = observed_cursor.as_str().as_bytes();
    let mut transaction = client
        .transaction()
        .map_err(MutationBatchError::CanonicalReceiptState)?;
    transaction
        .batch_execute("SET LOCAL synchronous_commit = on")
        .map_err(MutationBatchError::CanonicalReceiptState)?;
    let rows = transaction
        .query(
            "SELECT mutation_id, server_transaction_id, observed_canonical_cursor \
             FROM calque_internal.mutation_receipts \
             WHERE source_id = $1 AND source_epoch = $2 \
             AND authorization_generation IS NOT NULL \
             AND authorization_generation >= 0 \
             AND octet_length(authorization_policy_hash) = 32 \
             AND octet_length(authorization_scope_hash) = 32 \
             AND device_id IS NOT NULL \
             AND mutation_id = ANY($3) FOR UPDATE",
            &[&source_id.as_str(), &source_epoch.as_str(), &ids],
        )
        .map_err(MutationBatchError::CanonicalReceiptState)?;
    if rows.len() != mutation_ids.len() {
        return Err(MutationBatchError::CanonicalReceiptBindingMismatch);
    }
    for row in &rows {
        let stored_transaction_id = row.get::<_, Vec<u8>>(1);
        if stored_transaction_id.len() != server_transaction_id.len()
            || !bool::from(stored_transaction_id.ct_eq(server_transaction_id.as_slice()))
        {
            return Err(MutationBatchError::CanonicalReceiptBindingMismatch);
        }
        let existing = row.get::<_, Option<Vec<u8>>>(2);
        if existing.as_deref().is_some_and(|value| value != cursor) {
            return Err(MutationBatchError::CanonicalReceiptCursorMismatch);
        }
    }
    let updated = transaction
        .execute(
            "UPDATE calque_internal.mutation_receipts \
             SET observed_canonical_cursor = $5 \
             WHERE source_id = $1 AND source_epoch = $2 \
             AND mutation_id = ANY($3) AND server_transaction_id = $4 \
             AND authorization_generation IS NOT NULL \
             AND authorization_generation >= 0 \
             AND octet_length(authorization_policy_hash) = 32 \
             AND octet_length(authorization_scope_hash) = 32 \
             AND device_id IS NOT NULL \
             AND observed_canonical_cursor IS NULL",
            &[
                &source_id.as_str(),
                &source_epoch.as_str(),
                &ids,
                &server_transaction_id.as_slice(),
                &cursor,
            ],
        )
        .map_err(MutationBatchError::CanonicalReceiptState)?;
    transaction
        .commit()
        .map_err(MutationBatchError::CanonicalReceiptState)?;
    usize::try_from(updated).map_err(|_| MutationBatchError::InvalidRequest)
}

pub trait MutationCommand: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn version(&self) -> u32;
    fn footprint(&self, canonical_input: &[u8]) -> Result<CommandFootprint, CommandFailure>;
    /// The operation this canonical input performs, checked against the
    /// command capability. There is no default: a command that cannot tell
    /// returns [`CommandFailure::Validation`].
    fn operation(&self, canonical_input: &[u8]) -> Result<CommandOperation, CommandFailure>;
    fn execute(
        &self,
        transaction: &mut Transaction<'_>,
        canonical_input: &[u8],
        context: CommandContext<'_>,
    ) -> Result<(), CommandFailure>;
}

#[derive(Default)]
pub struct CommandRegistry {
    commands: BTreeMap<(String, u32), Arc<dyn MutationCommand>>,
}

impl CommandRegistry {
    pub fn register<C>(&mut self, command: C) -> Result<(), MutationBatchError>
    where
        C: MutationCommand,
    {
        if !valid_command_name(command.name()) || command.version() == 0 {
            return Err(MutationBatchError::InvalidCommandRegistration);
        }
        let key = (command.name().to_owned(), command.version());
        if self.commands.insert(key, Arc::new(command)).is_some() {
            return Err(MutationBatchError::DuplicateCommandRegistration);
        }
        Ok(())
    }

    fn get(&self, name: &str, version: u32) -> Option<&Arc<dyn MutationCommand>> {
        self.commands.get(&(name.to_owned(), version))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommandFailure {
    #[error("command input is invalid")]
    Validation,
    #[error("command is not authorized")]
    Authorization,
    #[error("command conflicts with canonical state")]
    Conflict { kind: String, payload: Vec<u8> },
    #[error("command execution is temporarily unavailable")]
    Retryable,
}

#[derive(Debug, Error)]
pub enum MutationBatchError {
    #[error("mutation protocol version is unsupported")]
    UnsupportedProtocol,
    #[error("mutation request is invalid")]
    InvalidRequest,
    #[error("mutation authorization is invalid")]
    InvalidAuthorization,
    #[error("mutation command registration is invalid")]
    InvalidCommandRegistration,
    #[error("mutation command is already registered")]
    DuplicateCommandRegistration,
    #[error("PostgreSQL metadata installation failed: {0}")]
    MetadataInstall(#[source] postgres::Error),
    #[error("PostgreSQL mutation acknowledgement resolution failed: {0}")]
    AcknowledgementResolution(#[source] postgres::Error),
    #[error("PostgreSQL canonical mutation receipt update failed: {0}")]
    CanonicalReceiptState(#[source] postgres::Error),
    #[error("canonical mutation marker does not match every durable receipt")]
    CanonicalReceiptBindingMismatch,
    #[error("mutation receipt was already assigned to a different canonical cursor")]
    CanonicalReceiptCursorMismatch,
}

/// Transaction-scoped advisory lock guarding the `calque_internal` install.
///
/// PostgreSQL's `CREATE ... IF NOT EXISTS` is check-then-create: two sessions
/// can both find the object absent, and one then fails with a unique violation.
/// This key differs from `canonical-log`'s because the two installs share no
/// object; any other installer that creates something in `calque_internal`
/// must take this key.
const INTERNAL_SCHEMA_BOOTSTRAP_LOCK_KEY: i64 = 7_249_927_482_239_849_733;

/// Installs `calque_internal` and the mutation receipt table.
///
/// A deployment must call this before serving any mutation: no query in this
/// crate creates the receipt table. Safe to call repeatedly and concurrently.
/// On an existing receipt table it takes no table lock: nothing deletes
/// receipts, so its autovacuum grows with every mutation, and a startup
/// `ALTER TABLE` would queue behind it.
pub fn install_metadata(client: &mut Client) -> Result<(), MutationBatchError> {
    let mut transaction = client
        .transaction()
        .map_err(MutationBatchError::MetadataInstall)?;
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock($1)",
            &[&INTERNAL_SCHEMA_BOOTSTRAP_LOCK_KEY],
        )
        .map_err(MutationBatchError::MetadataInstall)?;
    // PostgreSQL reports each object `IF NOT EXISTS` skips as a NOTICE,
    // which the driver logs at INFO on every start.
    transaction
        .batch_execute("SET LOCAL client_min_messages = warning")
        .map_err(MutationBatchError::MetadataInstall)?;
    transaction
        .batch_execute(INSTALL_SQL)
        .map_err(MutationBatchError::MetadataInstall)?;
    transaction
        .commit()
        .map_err(MutationBatchError::MetadataInstall)
}

pub fn execute_batch(
    client: &mut Client,
    registry: &CommandRegistry,
    authorization: &MutationAuthorization,
    request: MutationBatchRequest,
    server_time_unix_micros: u64,
) -> Result<MutationBatchResponse, MutationBatchError> {
    let parsed = ParsedBatch::parse(request, authorization, server_time_unix_micros)?;
    let outcomes = parsed
        .mutations
        .into_iter()
        .map(|mutation| {
            execute_one(
                client,
                registry,
                authorization,
                &parsed.client_instance_id,
                &parsed.device_id,
                mutation,
            )
        })
        .collect();
    Ok(MutationBatchResponse {
        protocol_version: 1,
        outcomes,
        server_time_unix_micros,
    })
}

struct ParsedBatch {
    client_instance_id: String,
    device_id: String,
    mutations: Vec<ParsedMutation>,
}

struct ParsedMutation {
    id: MutationId,
    command_name: String,
    command_version: u32,
    canonical_input: Vec<u8>,
    input_sha256: [u8; 32],
    base_cursor: CursorPosition,
    expected_versions: Vec<ExpectedVersion>,
    declared_footprint: CommandFootprint,
}

impl ParsedBatch {
    fn parse(
        request: MutationBatchRequest,
        authorization: &MutationAuthorization,
        server_time_unix_micros: u64,
    ) -> Result<Self, MutationBatchError> {
        validate_mutation_batch_request(&request)
            .map_err(|_| MutationBatchError::InvalidRequest)?;
        if server_time_unix_micros == 0
            || !valid_client_identifier(&request.client_instance_id, MAX_CLIENT_INSTANCE_ID_BYTES)
            || !valid_client_identifier(&request.device_id, MAX_DEVICE_ID_BYTES)
            || !authorization.verify_scope_token(&request.authorization_scope_token)
            || request.mutations.is_empty()
            || request.mutations.len() > MAX_MUTATION_BATCH_SIZE
        {
            return Err(MutationBatchError::InvalidRequest);
        }
        let mut total_input_bytes = 0usize;
        let mut seen = BTreeSet::new();
        let mut mutations = Vec::with_capacity(request.mutations.len());
        for wire in request.mutations {
            total_input_bytes = total_input_bytes
                .checked_add(wire.canonical_input.len())
                .ok_or(MutationBatchError::InvalidRequest)?;
            if total_input_bytes > MAX_MUTATION_BATCH_INPUT_BYTES {
                return Err(MutationBatchError::InvalidRequest);
            }
            let parsed = ParsedMutation::parse(wire, authorization, server_time_unix_micros)?;
            if !seen.insert(parsed.id.clone()) {
                return Err(MutationBatchError::InvalidRequest);
            }
            mutations.push(parsed);
        }
        Ok(Self {
            client_instance_id: request.client_instance_id,
            device_id: request.device_id,
            mutations,
        })
    }
}

impl ParsedMutation {
    fn parse(
        wire: MutationEnvelope,
        authorization: &MutationAuthorization,
        server_time_unix_micros: u64,
    ) -> Result<Self, MutationBatchError> {
        let id =
            MutationId::new(wire.mutation_id).map_err(|_| MutationBatchError::InvalidRequest)?;
        if !valid_command_name(&wire.command_name)
            || wire.command_version == 0
            || wire.canonical_input.is_empty()
            || wire.canonical_input.len() > MAX_MUTATION_CANONICAL_INPUT_BYTES
            || wire.created_at_unix_micros == 0
            || wire.created_at_unix_micros
                > server_time_unix_micros.saturating_add(MAX_FUTURE_SKEW_MICROS)
            || wire.expected_versions.len() > MAX_EXPECTED_VERSIONS
        {
            return Err(MutationBatchError::InvalidRequest);
        }
        let cursor = wire.base_cursor.ok_or(MutationBatchError::InvalidRequest)?;
        let opaque = OpaqueCursor::parse(
            String::from_utf8(cursor.token).map_err(|_| MutationBatchError::InvalidRequest)?,
        )
        .map_err(|_| MutationBatchError::InvalidRequest)?;
        let base_cursor = authorization
            .activated_source
            .cursor_codec()
            .decode_for_epoch(&opaque, authorization.activated_source.source_epoch())
            .map_err(|_| MutationBatchError::InvalidRequest)?;
        validate_expected_versions(&wire.expected_versions, authorization)?;
        let declared_footprint = parse_footprint(
            wire.declared_footprint
                .ok_or(MutationBatchError::InvalidRequest)?,
        )?;
        if !declared_footprint
            .table_ids
            .iter()
            .all(|table_id| authorization.scope.permits_table(table_id))
        {
            return Err(MutationBatchError::InvalidAuthorization);
        }
        let input_sha256 = Sha256::digest(&wire.canonical_input).into();
        Ok(Self {
            id,
            command_name: wire.command_name,
            command_version: wire.command_version,
            canonical_input: wire.canonical_input,
            input_sha256,
            base_cursor,
            expected_versions: wire.expected_versions,
            declared_footprint,
        })
    }
}

fn execute_one(
    client: &mut Client,
    registry: &CommandRegistry,
    authorization: &MutationAuthorization,
    client_instance_id: &str,
    device_id: &str,
    mutation: ParsedMutation,
) -> MutationOutcome {
    match lookup_existing_receipt(client, authorization, device_id, &mutation) {
        Ok(TransactionResult::AlreadyAccepted(server_transaction_id)) => {
            return already_accepted_outcome(&mutation.id, server_transaction_id);
        }
        Ok(TransactionResult::AlreadyCanonical(server_transaction_id, cursor)) => {
            return already_canonical_outcome(&mutation.id, server_transaction_id, cursor);
        }
        Ok(TransactionResult::BindingMismatch) => {
            return outcome(
                &mutation.id,
                MutationOutcomeCode::RejectedAuthorization,
                "mutation ID is unavailable in this authorization scope",
            );
        }
        Ok(TransactionResult::Absent | TransactionResult::Accepted(_)) => {}
        Err(ExecutionError::Postgres) => return retryable_outcome(&mutation.id),
        Err(ExecutionError::Command(_)) => return retryable_outcome(&mutation.id),
    }
    let Some(command) = registry.get(&mutation.command_name, mutation.command_version) else {
        return outcome(
            &mutation.id,
            MutationOutcomeCode::CommandVersionUnsupported,
            "mutation command version is unsupported",
        );
    };
    // The command capability, before the footprint and before any transaction:
    // a command or operation the presented authorization does not carry is
    // refused with nothing written.
    let operation = match command.operation(&mutation.canonical_input) {
        Ok(operation) => operation,
        Err(error) => return command_error_outcome(&mutation.id, error),
    };
    if !authorization
        .capability
        .permits(&mutation.command_name, operation)
    {
        return outcome(
            &mutation.id,
            MutationOutcomeCode::RejectedAuthorization,
            "mutation command is not authorized",
        );
    }
    let computed = match command.footprint(&mutation.canonical_input) {
        Ok(footprint) => footprint,
        Err(error) => return command_error_outcome(&mutation.id, error),
    };
    if computed != mutation.declared_footprint
        || !computed
            .table_ids
            .iter()
            .all(|table_id| authorization.scope.permits_table(table_id))
        || computed
            .rows
            .iter()
            .any(|row| !computed.table_ids.contains(&row.table_id))
    {
        return outcome(
            &mutation.id,
            MutationOutcomeCode::RejectedAuthorization,
            "mutation footprint is not authorized",
        );
    }

    match execute_transaction(
        client,
        authorization,
        command.as_ref(),
        client_instance_id,
        device_id,
        &mutation,
    ) {
        Ok(TransactionResult::Accepted(server_transaction_id)) => MutationOutcome {
            mutation_id: mutation.id.to_string(),
            code: MutationOutcomeCode::AcceptedAwaitingCanonicalEcho as i32,
            message: "mutation committed and is awaiting canonical echo".to_owned(),
            server_transaction_id,
            observed_canonical_cursor: None,
            conflict: None,
            retry_after_ms: 0,
        },
        Ok(TransactionResult::AlreadyAccepted(server_transaction_id)) => {
            already_accepted_outcome(&mutation.id, server_transaction_id)
        }
        Ok(TransactionResult::AlreadyCanonical(server_transaction_id, cursor)) => {
            already_canonical_outcome(&mutation.id, server_transaction_id, cursor)
        }
        Ok(TransactionResult::BindingMismatch) => outcome(
            &mutation.id,
            MutationOutcomeCode::RejectedAuthorization,
            "mutation ID is unavailable in this authorization scope",
        ),
        Err(ExecutionError::Command(error)) => command_error_outcome(&mutation.id, error),
        Err(ExecutionError::Postgres) => retryable_outcome(&mutation.id),
        Ok(TransactionResult::Absent) => retryable_outcome(&mutation.id),
    }
}

enum TransactionResult {
    Absent,
    Accepted(Vec<u8>),
    AlreadyAccepted(Vec<u8>),
    AlreadyCanonical(Vec<u8>, Vec<u8>),
    BindingMismatch,
}

enum ExecutionError {
    Command(CommandFailure),
    Postgres,
}

impl From<postgres::Error> for ExecutionError {
    fn from(_error: postgres::Error) -> Self {
        Self::Postgres
    }
}

fn lookup_existing_receipt(
    client: &mut Client,
    authorization: &MutationAuthorization,
    device_id: &str,
    mutation: &ParsedMutation,
) -> Result<TransactionResult, ExecutionError> {
    let Some(row) = client.query_opt(
        "SELECT source_id, source_epoch, authorization_generation, \
         authorization_policy_hash, authorization_scope_hash, device_id, \
         command_name, command_version, canonical_input_sha256, \
         server_transaction_id, observed_canonical_cursor \
         FROM calque_internal.mutation_receipts WHERE mutation_id = $1",
        &[&mutation.id.as_str()],
    )?
    else {
        return Ok(TransactionResult::Absent);
    };
    let source_id = row.get::<_, Option<String>>(0);
    let source_epoch = row.get::<_, Option<String>>(1);
    let authorization_generation = row.get::<_, Option<i64>>(2);
    let policy_hash = row.get::<_, Option<Vec<u8>>>(3);
    let scope_hash = row.get::<_, Option<Vec<u8>>>(4);
    let stored_device = row.get::<_, Option<String>>(5);
    let command_name = row.get::<_, String>(6);
    let command_version = row.get::<_, i64>(7);
    let input_sha256 = row.get::<_, Vec<u8>>(8);
    let expected_generation =
        i64::try_from(authorization.scope.generation()).map_err(|_| ExecutionError::Postgres)?;
    if source_id.as_deref() != Some(authorization.source_id.as_str())
        || source_epoch.as_deref() != Some(authorization.activated_source.source_epoch().as_str())
        || authorization_generation != Some(expected_generation)
        || !constant_time_optional_hash_eq(
            policy_hash.as_deref(),
            authorization.scope.policy_hash().as_bytes(),
        )
        || !constant_time_optional_hash_eq(
            scope_hash.as_deref(),
            authorization.scope.scope_hash().as_bytes(),
        )
        || stored_device.as_deref() != Some(device_id)
        || command_name != mutation.command_name
        || command_version != i64::from(mutation.command_version)
        || input_sha256.len() != mutation.input_sha256.len()
        || !bool::from(input_sha256.ct_eq(mutation.input_sha256.as_slice()))
    {
        return Ok(TransactionResult::BindingMismatch);
    }
    let server_transaction_id = row.get(9);
    Ok(match row.get::<_, Option<Vec<u8>>>(10) {
        Some(cursor) => TransactionResult::AlreadyCanonical(server_transaction_id, cursor),
        None => TransactionResult::AlreadyAccepted(server_transaction_id),
    })
}

fn execute_transaction(
    client: &mut Client,
    authorization: &MutationAuthorization,
    command: &dyn MutationCommand,
    client_instance_id: &str,
    device_id: &str,
    mutation: &ParsedMutation,
) -> Result<TransactionResult, ExecutionError> {
    let server_transaction_id = Uuid::new_v4().as_bytes().to_vec();
    let authorization_generation =
        i64::try_from(authorization.scope.generation()).map_err(|_| ExecutionError::Postgres)?;
    let mut transaction = client.transaction()?;
    transaction.batch_execute("SET LOCAL synchronous_commit = on")?;
    let inserted = transaction.query_opt(
        "INSERT INTO calque_internal.mutation_receipts \
         (mutation_id, source_id, source_epoch, authorization_generation, authorization_policy_hash, authorization_scope_hash, command_name, command_version, canonical_input_sha256, server_transaction_id, device_id, client_instance_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT (mutation_id) DO NOTHING \
         RETURNING server_transaction_id",
        &[
            &mutation.id.as_str(),
            &authorization.source_id.as_str(),
            &authorization.activated_source.source_epoch().as_str(),
            &authorization_generation,
            &authorization.scope.policy_hash().as_bytes().as_slice(),
            &authorization.scope.scope_hash().as_bytes().as_slice(),
            &mutation.command_name,
            &i64::from(mutation.command_version),
            &mutation.input_sha256.as_slice(),
            &server_transaction_id,
            &device_id,
            &client_instance_id,
        ],
    )?;
    if let Some(row) = inserted {
        command
            .execute(
                &mut transaction,
                &mutation.canonical_input,
                CommandContext {
                    mutation_id: &mutation.id,
                    base_cursor: &mutation.base_cursor,
                    expected_versions: &mutation.expected_versions,
                    authorized_scope: &authorization.scope,
                },
            )
            .map_err(ExecutionError::Command)?;
        // Commands are trusted application code but receive the transaction
        // directly. Reassert the durability policy after the callback so a
        // command-local setting cannot weaken receipt/business/marker commit.
        transaction.batch_execute("SET LOCAL synchronous_commit = on")?;
        let marker = MutationLogicalMarker {
            protocol_version: 1,
            mutation_ids: vec![mutation.id.to_string()],
            server_transaction_id: server_transaction_id.clone(),
        }
        .encode_to_vec();
        transaction.query_one(
            "SELECT pg_logical_emit_message(true, $1, $2::bytea)",
            &[&LOGICAL_MARKER_PREFIX, &marker],
        )?;
        transaction.commit()?;
        return Ok(TransactionResult::Accepted(row.get(0)));
    }

    let existing = transaction.query_opt(
        "SELECT server_transaction_id, observed_canonical_cursor \
         FROM calque_internal.mutation_receipts \
         WHERE mutation_id = $1 AND source_id = $2 AND source_epoch = $3 \
         AND authorization_generation = $4 AND authorization_policy_hash = $5 \
         AND authorization_scope_hash = $6 AND device_id = $7 \
         AND command_name = $8 AND command_version = $9 AND canonical_input_sha256 = $10",
        &[
            &mutation.id.as_str(),
            &authorization.source_id.as_str(),
            &authorization.activated_source.source_epoch().as_str(),
            &authorization_generation,
            &authorization.scope.policy_hash().as_bytes().as_slice(),
            &authorization.scope.scope_hash().as_bytes().as_slice(),
            &device_id,
            &mutation.command_name,
            &i64::from(mutation.command_version),
            &mutation.input_sha256.as_slice(),
        ],
    )?;
    transaction.commit()?;
    Ok(existing.map_or(TransactionResult::BindingMismatch, |row| {
        let server_transaction_id = row.get(0);
        match row.get::<_, Option<Vec<u8>>>(1) {
            Some(cursor) => TransactionResult::AlreadyCanonical(server_transaction_id, cursor),
            None => TransactionResult::AlreadyAccepted(server_transaction_id),
        }
    }))
}

fn validate_expected_versions(
    versions: &[ExpectedVersion],
    authorization: &MutationAuthorization,
) -> Result<(), MutationBatchError> {
    let mut seen = BTreeSet::new();
    for version in versions {
        let table = TableId::new(version.table_id.clone())
            .map_err(|_| MutationBatchError::InvalidRequest)?;
        if !authorization.scope.permits_table(&table)
            || version.primary_key.is_empty()
            || version.primary_key.len() > PRIMARY_KEY_MAX_BYTES
            || version.expected_version.is_empty()
            || !seen.insert((table, version.primary_key.clone()))
        {
            return Err(MutationBatchError::InvalidRequest);
        }
    }
    Ok(())
}

fn parse_footprint(wire: MutationFootprint) -> Result<CommandFootprint, MutationBatchError> {
    if wire.rows.len() > MAX_MUTATION_FOOTPRINT_ROWS
        || wire.table_ids.is_empty()
        || wire.table_ids.len() > MAX_MUTATION_FOOTPRINT_TABLES
    {
        return Err(MutationBatchError::InvalidRequest);
    }
    let mut table_ids = BTreeSet::new();
    for value in wire.table_ids {
        let table = TableId::new(value).map_err(|_| MutationBatchError::InvalidRequest)?;
        if !table_ids.insert(table) {
            return Err(MutationBatchError::InvalidRequest);
        }
    }
    let mut rows = BTreeSet::new();
    for AffectedRow {
        table_id,
        primary_key,
    } in wire.rows
    {
        let table_id = TableId::new(table_id).map_err(|_| MutationBatchError::InvalidRequest)?;
        if primary_key.is_empty()
            || primary_key.len() > PRIMARY_KEY_MAX_BYTES
            || !table_ids.contains(&table_id)
            || !rows.insert(AffectedKey {
                table_id,
                primary_key,
            })
        {
            return Err(MutationBatchError::InvalidRequest);
        }
    }
    Ok(CommandFootprint { rows, table_ids })
}

fn command_error_outcome(id: &MutationId, error: CommandFailure) -> MutationOutcome {
    match error {
        CommandFailure::Validation => outcome(
            id,
            MutationOutcomeCode::RejectedValidation,
            "mutation input is invalid",
        ),
        CommandFailure::Authorization => outcome(
            id,
            MutationOutcomeCode::RejectedAuthorization,
            "mutation is not authorized",
        ),
        CommandFailure::Conflict { kind, payload } => {
            if kind.is_empty()
                || kind.len() > MAX_CONFLICT_KIND_BYTES
                || kind.chars().any(char::is_control)
                || payload.len() > MAX_CONFLICT_PAYLOAD_BYTES
            {
                let mut result = outcome(
                    id,
                    MutationOutcomeCode::RetryableFailure,
                    "mutation conflict details exceed the response budget",
                );
                result.retry_after_ms = 1_000;
                return result;
            }
            MutationOutcome {
                mutation_id: id.to_string(),
                code: MutationOutcomeCode::Conflict as i32,
                message: "mutation conflicts with canonical state".to_owned(),
                server_transaction_id: Vec::new(),
                observed_canonical_cursor: None,
                conflict: Some(Conflict {
                    kind,
                    rows: Vec::new(),
                    application_payload: payload,
                }),
                retry_after_ms: 0,
            }
        }
        CommandFailure::Retryable => {
            let mut result = outcome(
                id,
                MutationOutcomeCode::RetryableFailure,
                "mutation execution is temporarily unavailable",
            );
            result.retry_after_ms = 1_000;
            result
        }
    }
}

fn outcome(id: &MutationId, code: MutationOutcomeCode, message: &'static str) -> MutationOutcome {
    MutationOutcome {
        mutation_id: id.to_string(),
        code: code as i32,
        message: message.to_owned(),
        server_transaction_id: Vec::new(),
        observed_canonical_cursor: None,
        conflict: None,
        retry_after_ms: 0,
    }
}

fn already_accepted_outcome(id: &MutationId, server_transaction_id: Vec<u8>) -> MutationOutcome {
    MutationOutcome {
        mutation_id: id.to_string(),
        code: MutationOutcomeCode::AlreadyAcceptedAwaitingCanonicalEcho as i32,
        message: "mutation was already committed and is awaiting canonical echo".to_owned(),
        server_transaction_id,
        observed_canonical_cursor: None,
        conflict: None,
        retry_after_ms: 0,
    }
}

fn already_canonical_outcome(
    id: &MutationId,
    server_transaction_id: Vec<u8>,
    cursor: Vec<u8>,
) -> MutationOutcome {
    MutationOutcome {
        mutation_id: id.to_string(),
        code: MutationOutcomeCode::AlreadyCanonical as i32,
        message: "mutation is already present in the canonical log".to_owned(),
        server_transaction_id,
        observed_canonical_cursor: Some(Cursor { token: cursor }),
        conflict: None,
        retry_after_ms: 0,
    }
}

fn retryable_outcome(id: &MutationId) -> MutationOutcome {
    let mut result = outcome(
        id,
        MutationOutcomeCode::RetryableFailure,
        "mutation execution is temporarily unavailable",
    );
    result.retry_after_ms = 1_000;
    result
}

fn valid_command_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_COMMAND_NAME_BYTES
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'/' | b'-')
        })
}

fn valid_client_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

fn constant_time_optional_hash_eq(candidate: Option<&[u8]>, expected: &[u8; 32]) -> bool {
    candidate.is_some_and(|candidate| {
        candidate.len() == expected.len() && bool::from(candidate.ct_eq(expected.as_slice()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
    use source_control::{
        ColumnInspection, EnrollmentRequirements, EnrollmentSnapshot, PgoutputLaunchProfile,
        PublicationSettings, QualifiedTable, RegisteredTable, SchemaRegistry, SourceIdentity,
        SourceSettings, TableInspection,
    };
    use sync_protocol::{
        change::{
            cell_value, CellValue as WireCellValue, ColumnValue as WireColumnValue, PrimaryKey,
        },
        encode_primary_key,
        mutation::{Cursor, MutationEnvelope},
        PRIMARY_KEY_CODEC_VERSION,
    };

    struct AddCommand {
        table_id: TableId,
    }

    /// The key of `calque_mutation_test`'s row `id`.
    fn row_key(id: i64) -> Vec<u8> {
        encode_primary_key(&PrimaryKey {
            codec_version: PRIMARY_KEY_CODEC_VERSION,
            parts: vec![WireColumnValue {
                column_id: 1,
                value: Some(WireCellValue {
                    kind: Some(cell_value::Kind::Int64Value(id)),
                }),
            }],
        })
        .unwrap()
    }

    /// The row every `tasks.add` changes.
    fn canonical_key() -> Vec<u8> {
        row_key(1)
    }

    fn source_id() -> SourceId {
        SourceId::new("source-mutation-test").unwrap()
    }

    fn registered_table() -> RegisteredTable {
        RegisteredTable {
            table_id: TableId::new("table/tasks").unwrap(),
            column_ids: vec![sync_types::ColumnId::new(1), sync_types::ColumnId::new(2)],
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
                columns: vec![
                    ColumnInspection {
                        attribute_number: 1,
                        name: "id".to_owned(),
                        type_oid: 20,
                        type_modifier: -1,
                        type_schema: "pg_catalog".to_owned(),
                        type_name: "int8".to_owned(),
                        nullable: false,
                        primary_key_ordinal: Some(1),
                        generated: false,
                        nondeterministic_collation: false,
                    },
                    ColumnInspection {
                        attribute_number: 2,
                        name: "value".to_owned(),
                        type_oid: 20,
                        type_modifier: -1,
                        type_schema: "pg_catalog".to_owned(),
                        type_name: "int8".to_owned(),
                        nullable: false,
                        primary_key_ordinal: None,
                        generated: false,
                        nondeterministic_collation: false,
                    },
                ],
            },
        }
    }

    fn authorized_scope(generation: u64, permitted_columns: Vec<u32>) -> AuthorizedScope {
        let policy = AuthorizationPolicy::compile(
            PolicyDefinition {
                generation,
                tables: vec![TablePermitDefinition {
                    table_id: TableId::new("table/tasks").unwrap(),
                    permitted_columns: permitted_columns
                        .into_iter()
                        .map(sync_types::ColumnId::new)
                        .collect(),
                }],
            },
            &[registered_table()],
        )
        .unwrap();
        policy.scope()
    }

    fn registry_for_source(system_identifier: &str, table_name: &str) -> SchemaRegistry {
        let identity = SourceIdentity {
            system_identifier: system_identifier.to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        };
        let mut inspection = registered_table().inspection;
        inspection.table.table = table_name.to_owned();
        let qualified_table = inspection.table.clone();
        let snapshot = EnrollmentSnapshot {
            identity: identity.clone(),
            settings: SourceSettings {
                server_version_num: 180_004,
                server_encoding: "UTF8".to_owned(),
                date_style: "ISO, MDY".to_owned(),
                time_zone: "UTC".to_owned(),
                bytea_output: "hex".to_owned(),
                wal_level: "logical".to_owned(),
                max_replication_slots: 10,
                max_wal_senders: 10,
                current_user: "sync".to_owned(),
                current_user_is_superuser: false,
                current_user_has_replication: true,
            },
            publication: PublicationSettings {
                name: "sync_publication".to_owned(),
                exists: true,
                publishes_insert: true,
                publishes_update: true,
                publishes_delete: true,
                publishes_truncate: true,
                publishes_via_partition_root: false,
            },
            tables: vec![inspection],
        };
        let requirements = EnrollmentRequirements {
            expected_identity: identity,
            publication_name: "sync_publication".to_owned(),
            tables: BTreeSet::from([qualified_table]),
            pgoutput_profile: PgoutputLaunchProfile::V1,
        };
        SchemaRegistry::initialize(&snapshot, &requirements).unwrap()
    }

    fn scope_for_registry(
        registry: &SchemaRegistry,
        generation: u64,
        permitted_columns: Vec<u32>,
    ) -> AuthorizedScope {
        let table = &registry.tables()[0];
        AuthorizationPolicy::compile_for_registry(
            PolicyDefinition {
                generation,
                tables: vec![TablePermitDefinition {
                    table_id: table.table_id.clone(),
                    permitted_columns: permitted_columns
                        .into_iter()
                        .map(sync_types::ColumnId::new)
                        .collect(),
                }],
            },
            registry,
        )
        .unwrap()
        .scope()
    }

    /// The capability every test's `tasks.add` batch executes under.
    fn tasks_add_capability() -> CommandCapability {
        CommandCapability::new([(
            "tasks.add".to_owned(),
            BTreeSet::from([CommandOperation::Update]),
        )])
    }

    fn activate_registry(
        registry: &SchemaRegistry,
        epoch: SourceEpoch,
        codec: CursorCodec,
    ) -> ActivatedSource {
        ActivatedSource::new(registry, epoch, codec, PgoutputLaunchProfile::V1)
    }

    impl MutationCommand for AddCommand {
        fn name(&self) -> &'static str {
            "tasks.add"
        }

        fn version(&self) -> u32 {
            1
        }

        fn operation(&self, canonical_input: &[u8]) -> Result<CommandOperation, CommandFailure> {
            if canonical_input.len() != 8 {
                return Err(CommandFailure::Validation);
            }
            Ok(CommandOperation::Update)
        }

        fn footprint(&self, canonical_input: &[u8]) -> Result<CommandFootprint, CommandFailure> {
            if canonical_input.len() != 8 {
                return Err(CommandFailure::Validation);
            }
            Ok(CommandFootprint {
                rows: BTreeSet::from([AffectedKey {
                    table_id: self.table_id.clone(),
                    primary_key: canonical_key(),
                }]),
                table_ids: BTreeSet::from([self.table_id.clone()]),
            })
        }

        fn execute(
            &self,
            transaction: &mut Transaction<'_>,
            canonical_input: &[u8],
            context: CommandContext<'_>,
        ) -> Result<(), CommandFailure> {
            if !context.authorized_scope.permits_table(&self.table_id) {
                return Err(CommandFailure::Authorization);
            }
            let synchronous_commit: String = transaction
                .query_one("SHOW synchronous_commit", &[])
                .map_err(|_| CommandFailure::Retryable)?
                .get(0);
            if synchronous_commit != "on" {
                return Err(CommandFailure::Retryable);
            }
            // Adversarially weaken the transaction-local setting. The mutation
            // runtime must restore it after this trusted callback returns.
            transaction
                .batch_execute("SET LOCAL synchronous_commit = off")
                .map_err(|_| CommandFailure::Retryable)?;
            let bytes: [u8; 8] = canonical_input
                .try_into()
                .map_err(|_| CommandFailure::Validation)?;
            let amount = i64::from_be_bytes(bytes);
            let changed = transaction
                .execute(
                    "UPDATE calque_mutation_test SET value = value + $1 WHERE id = 1",
                    &[&amount],
                )
                .map_err(|_| CommandFailure::Retryable)?;
            if changed != 1 {
                return Err(CommandFailure::Conflict {
                    kind: "missing-row".to_owned(),
                    payload: Vec::new(),
                });
            }
            Ok(())
        }
    }

    fn fixture(
        codec: &CursorCodec,
        epoch: &SourceEpoch,
        mutation_id: &str,
        table_id: &TableId,
    ) -> MutationBatchRequest {
        let cursor = codec.encode(&CursorPosition {
            source_epoch: epoch.clone(),
            sequence: 1,
        });
        MutationBatchRequest {
            protocol_version: 1,
            client_instance_id: "client-1".to_owned(),
            device_id: "device-1".to_owned(),
            authorization_scope_token: b"scope-token".to_vec(),
            mutations: vec![MutationEnvelope {
                mutation_id: mutation_id.to_owned(),
                command_name: "tasks.add".to_owned(),
                command_version: 1,
                canonical_input: 5_i64.to_be_bytes().to_vec(),
                base_cursor: Some(Cursor {
                    token: cursor.as_str().as_bytes().to_vec(),
                }),
                created_at_unix_micros: 1,
                expected_versions: Vec::new(),
                declared_footprint: Some(MutationFootprint {
                    rows: vec![AffectedRow {
                        table_id: table_id.to_string(),
                        primary_key: canonical_key(),
                    }],
                    table_ids: vec![table_id.to_string()],
                }),
            }],
        }
    }

    #[test]
    fn rejects_scope_token_and_declared_footprint_tampering_before_execution() {
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let registry = registry_for_source("7612345678901234567", "tasks");
        let activated_source = activate_registry(&registry, epoch.clone(), codec.clone());
        let scope = scope_for_registry(&registry, 7, vec![1, 2]);
        let table_id = registry.tables()[0].table_id.clone();
        let authorization = MutationAuthorization::new(
            source_id(),
            &activated_source,
            b"scope-token".to_vec(),
            scope,
            tasks_add_capability(),
        )
        .unwrap();
        let mut request = fixture(&codec, &epoch, "mutation-1", &table_id);
        request.authorization_scope_token = b"wrong-token".to_vec();
        assert!(matches!(
            ParsedBatch::parse(request, &authorization, 2),
            Err(MutationBatchError::InvalidRequest)
        ));

        let mut request = fixture(&codec, &epoch, "mutation-1", &table_id);
        request.mutations[0]
            .declared_footprint
            .as_mut()
            .unwrap()
            .table_ids = vec!["table/other".to_owned()];
        assert!(matches!(
            ParsedBatch::parse(request, &authorization, 2),
            Err(MutationBatchError::InvalidRequest)
        ));
    }

    #[test]
    fn accepts_zero_generation_and_rejects_values_outside_postgres_bigint() {
        let registry = registry_for_source("7612345678901234567", "tasks");
        let activated_source = activate_registry(
            &registry,
            SourceEpoch::new("epoch-1").unwrap(),
            CursorCodec::new([7; 32]).unwrap(),
        );
        let zero = MutationAuthorization::new(
            source_id(),
            &activated_source,
            b"scope-token".to_vec(),
            scope_for_registry(&registry, 0, vec![1, 2]),
            tasks_add_capability(),
        );
        assert!(zero.is_ok());

        let authorization = MutationAuthorization::new(
            source_id(),
            &activated_source,
            b"scope-token".to_vec(),
            scope_for_registry(&registry, u64::MAX, vec![1, 2]),
            tasks_add_capability(),
        );
        assert!(matches!(
            authorization,
            Err(MutationBatchError::InvalidAuthorization)
        ));
    }

    #[test]
    fn mutation_authorization_rejects_detached_wrong_registry_and_wrong_source_scopes() {
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let registry = registry_for_source("7612345678901234567", "tasks");
        let activated_source = activate_registry(&registry, epoch, codec);

        assert!(matches!(
            MutationAuthorization::new(
                source_id(),
                &activated_source,
                b"scope-token".to_vec(),
                authorized_scope(7, vec![1, 2]),
                tasks_add_capability(),
            ),
            Err(MutationBatchError::InvalidAuthorization)
        ));

        let other_registry = registry_for_source("7612345678901234567", "other_tasks");
        assert!(matches!(
            MutationAuthorization::new(
                source_id(),
                &activated_source,
                b"scope-token".to_vec(),
                scope_for_registry(&other_registry, 7, vec![1, 2]),
                tasks_add_capability(),
            ),
            Err(MutationBatchError::InvalidAuthorization)
        ));

        let other_source_registry = registry_for_source("7612345678901234568", "tasks");
        assert!(matches!(
            MutationAuthorization::new(
                source_id(),
                &activated_source,
                b"scope-token".to_vec(),
                scope_for_registry(&other_source_registry, 7, vec![1, 2]),
                tasks_add_capability(),
            ),
            Err(MutationBatchError::InvalidAuthorization)
        ));
    }

    /// No `MutationAuthorization` exists without a command capability, so
    /// `execute_batch` cannot be reached by a principal that holds none.
    #[test]
    fn an_authorization_without_a_command_capability_cannot_be_built() {
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let registry = registry_for_source("7612345678901234567", "tasks");
        let activated_source = activate_registry(&registry, epoch, codec);
        let scope = scope_for_registry(&registry, 7, vec![1, 2]);
        for empty in [
            CommandCapability::default(),
            // A named command with no operation authorizes nothing, so it is
            // dropped rather than counted.
            CommandCapability::new([("tasks.add".to_owned(), BTreeSet::new())]),
        ] {
            assert!(empty.is_empty());
            assert!(matches!(
                MutationAuthorization::new(
                    source_id(),
                    &activated_source,
                    b"scope-token".to_vec(),
                    scope.clone(),
                    empty,
                ),
                Err(MutationBatchError::InvalidAuthorization)
            ));
        }
    }

    /// The capability answers about one exact name and one exact operation.
    /// Nothing here is a prefix, a pattern, or a default.
    #[test]
    fn a_capability_permits_only_the_names_and_operations_it_carries() {
        let capability = CommandCapability::new([
            (
                "crud:public.tasks".to_owned(),
                BTreeSet::from([CommandOperation::Update]),
            ),
            (
                "crud:public.projects".to_owned(),
                BTreeSet::from([CommandOperation::Insert, CommandOperation::Delete]),
            ),
        ]);
        assert!(capability.permits("crud:public.tasks", CommandOperation::Update));
        assert!(!capability.permits("crud:public.tasks", CommandOperation::Insert));
        assert!(!capability.permits("crud:public.tasks", CommandOperation::Delete));
        assert!(capability.permits("crud:public.projects", CommandOperation::Insert));
        assert!(!capability.permits("crud:public.projects", CommandOperation::Update));
        assert!(!capability.permits("crud:public.tasks_archive", CommandOperation::Update));
        assert!(!capability.permits("crud:public.task", CommandOperation::Update));
        assert!(!capability.permits("", CommandOperation::Update));
        assert_eq!(
            capability.command_names().collect::<Vec<_>>(),
            vec!["crud:public.projects", "crud:public.tasks"]
        );
    }

    #[test]
    fn acknowledgement_proof_rejects_generation_policy_scope_and_table_mismatches() {
        let scope = authorized_scope(7, vec![1, 2]);
        let source = source_id();
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        let device = "device-1";
        let ids = (1..=4)
            .map(|index| MutationId::new(format!("mutation-{index}")).unwrap())
            .collect::<BTreeSet<_>>();
        let mut wrong_policy = *scope.policy_hash().as_bytes();
        wrong_policy[0] ^= 0xff;
        let mut wrong_scope = *scope.scope_hash().as_bytes();
        wrong_scope[0] ^= 0xff;
        let bindings = vec![
            MutationReceiptBinding {
                mutation_id: MutationId::new("mutation-1").unwrap(),
                source_id: source.clone(),
                source_epoch: epoch.clone(),
                authorization_generation: scope.generation(),
                authorization_policy_hash: *scope.policy_hash().as_bytes(),
                authorization_scope_hash: *scope.scope_hash().as_bytes(),
                device_id: device.to_owned(),
            },
            MutationReceiptBinding {
                mutation_id: MutationId::new("mutation-2").unwrap(),
                source_id: source.clone(),
                source_epoch: epoch.clone(),
                authorization_generation: scope.generation() + 1,
                authorization_policy_hash: *scope.policy_hash().as_bytes(),
                authorization_scope_hash: *scope.scope_hash().as_bytes(),
                device_id: device.to_owned(),
            },
            MutationReceiptBinding {
                mutation_id: MutationId::new("mutation-3").unwrap(),
                source_id: source.clone(),
                source_epoch: epoch.clone(),
                authorization_generation: scope.generation(),
                authorization_policy_hash: wrong_policy,
                authorization_scope_hash: *scope.scope_hash().as_bytes(),
                device_id: device.to_owned(),
            },
            MutationReceiptBinding {
                mutation_id: MutationId::new("mutation-4").unwrap(),
                source_id: source.clone(),
                source_epoch: epoch.clone(),
                authorization_generation: scope.generation(),
                authorization_policy_hash: *scope.policy_hash().as_bytes(),
                authorization_scope_hash: wrong_scope,
                device_id: device.to_owned(),
            },
        ];
        let resolved = ResolvedMutationAcknowledgements::from_test_receipt_bindings(
            source.clone(),
            epoch.clone(),
            &scope,
            device,
            &ids,
            bindings,
        )
        .unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(resolved.contains(&MutationId::new("mutation-1").unwrap()));
        assert!(resolved.matches_request(&source, &epoch, &scope, device, &ids));
        assert!(!resolved.matches_request(
            &source,
            &epoch,
            &authorized_scope(8, vec![1, 2]),
            device,
            &ids,
        ));
    }

    #[test]
    fn a_conflict_too_large_to_report_is_answered_as_retryable() {
        let id = MutationId::new("mutation-1").unwrap();
        let conflict = |kind: &str, payload: usize| {
            command_error_outcome(
                &id,
                CommandFailure::Conflict {
                    kind: kind.to_owned(),
                    payload: vec![0; payload],
                },
            )
        };
        let reported = conflict(
            &"k".repeat(MAX_CONFLICT_KIND_BYTES),
            MAX_CONFLICT_PAYLOAD_BYTES,
        );
        assert_eq!(reported.code, MutationOutcomeCode::Conflict as i32);
        assert_eq!(
            reported.conflict.unwrap().application_payload.len(),
            MAX_CONFLICT_PAYLOAD_BYTES
        );
        for (case, outcome) in [
            ("an empty kind", conflict("", 0)),
            (
                "a kind over its bound",
                conflict(&"k".repeat(MAX_CONFLICT_KIND_BYTES + 1), 0),
            ),
            (
                "a kind with a control character",
                conflict("missing\nrow", 0),
            ),
            (
                "a payload over its bound",
                conflict("missing-row", MAX_CONFLICT_PAYLOAD_BYTES + 1),
            ),
        ] {
            assert_eq!(
                outcome.code,
                MutationOutcomeCode::RetryableFailure as i32,
                "{case}"
            );
            assert_eq!(outcome.retry_after_ms, 1_000, "{case}");
            assert!(outcome.conflict.is_none(), "{case}");
            assert_eq!(
                outcome.message, "mutation conflict details exceed the response budget",
                "{case}"
            );
        }
    }

    #[test]
    #[ignore = "requires CALQUE_TEST_DATABASE_URL with wal_level=logical"]
    fn live_postgres_executes_once_and_replays_the_original_receipt() {
        let url = std::env::var("CALQUE_TEST_DATABASE_URL")
            .expect("CALQUE_TEST_DATABASE_URL must be set");
        let mut client = Client::connect(&url, postgres::NoTls).unwrap();
        install_metadata(&mut client).unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS calque_mutation_test; \
                 CREATE TABLE calque_mutation_test (id bigint PRIMARY KEY, value bigint NOT NULL); \
                 INSERT INTO calque_mutation_test VALUES (1, 0); \
                 SET synchronous_commit = off;",
            )
            .unwrap();
        let epoch = SourceEpoch::new("epoch-live").unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let schema_registry = registry_for_source("7612345678901234567", "tasks");
        let activated_source = activate_registry(&schema_registry, epoch.clone(), codec.clone());
        let scope = scope_for_registry(&schema_registry, 7, vec![1, 2]);
        let table_id = schema_registry.tables()[0].table_id.clone();
        let authorization = MutationAuthorization::new(
            source_id(),
            &activated_source,
            b"scope-token".to_vec(),
            scope.clone(),
            tasks_add_capability(),
        )
        .unwrap();
        let mut registry = CommandRegistry::default();
        registry
            .register(AddCommand {
                table_id: table_id.clone(),
            })
            .unwrap();
        let id = format!("mutation-{}", Uuid::new_v4());

        let first = execute_batch(
            &mut client,
            &registry,
            &authorization,
            fixture(&codec, &epoch, &id, &table_id),
            2,
        )
        .unwrap();
        let replay = execute_batch(
            &mut client,
            &registry,
            &authorization,
            fixture(&codec, &epoch, &id, &table_id),
            2,
        )
        .unwrap();
        assert_eq!(
            first.outcomes[0].code,
            MutationOutcomeCode::AcceptedAwaitingCanonicalEcho as i32
        );
        assert_eq!(
            replay.outcomes[0].code,
            MutationOutcomeCode::AlreadyAcceptedAwaitingCanonicalEcho as i32
        );
        assert_eq!(
            first.outcomes[0].server_transaction_id,
            replay.outcomes[0].server_transaction_id
        );
        let other_source_registry = registry_for_source("7612345678901234568", "tasks");
        let other_activated_source =
            activate_registry(&other_source_registry, epoch.clone(), codec.clone());
        let other_source_scope = scope_for_registry(&other_source_registry, 7, vec![1, 2]);
        let other_source_table_id = other_source_registry.tables()[0].table_id.clone();
        let other_source_authorization = MutationAuthorization::new(
            SourceId::new("source-other").unwrap(),
            &other_activated_source,
            b"scope-token".to_vec(),
            other_source_scope,
            tasks_add_capability(),
        )
        .unwrap();
        let cross_source = execute_batch(
            &mut client,
            &registry,
            &other_source_authorization,
            fixture(&codec, &epoch, &id, &other_source_table_id),
            2,
        )
        .unwrap();
        assert_eq!(
            cross_source.outcomes[0].code,
            MutationOutcomeCode::RejectedAuthorization as i32
        );
        let canonical_cursor = codec.encode(&CursorPosition {
            source_epoch: epoch.clone(),
            sequence: 2,
        });
        let canonical_ids = BTreeSet::from([MutationId::new(id.clone()).unwrap()]);
        let server_transaction_id: [u8; 16] = first.outcomes[0]
            .server_transaction_id
            .as_slice()
            .try_into()
            .unwrap();
        assert_eq!(
            mark_canonical_receipts(
                &mut client,
                &codec,
                &source_id(),
                &epoch,
                &server_transaction_id,
                &canonical_ids,
                &canonical_cursor,
            )
            .unwrap(),
            1
        );
        assert_eq!(
            mark_canonical_receipts(
                &mut client,
                &codec,
                &source_id(),
                &epoch,
                &server_transaction_id,
                &canonical_ids,
                &canonical_cursor,
            )
            .unwrap(),
            0
        );
        let different_cursor = codec.encode(&CursorPosition {
            source_epoch: epoch.clone(),
            sequence: 3,
        });
        assert!(matches!(
            mark_canonical_receipts(
                &mut client,
                &codec,
                &source_id(),
                &epoch,
                &server_transaction_id,
                &canonical_ids,
                &different_cursor,
            ),
            Err(MutationBatchError::CanonicalReceiptCursorMismatch)
        ));
        let canonical_replay = execute_batch(
            &mut client,
            &registry,
            &authorization,
            fixture(&codec, &epoch, &id, &table_id),
            2,
        )
        .unwrap();
        assert_eq!(
            canonical_replay.outcomes[0].code,
            MutationOutcomeCode::AlreadyCanonical as i32
        );
        assert_eq!(
            canonical_replay.outcomes[0]
                .observed_canonical_cursor
                .as_ref()
                .unwrap()
                .token,
            canonical_cursor.as_str().as_bytes()
        );
        let removed_registry = CommandRegistry::default();
        let canonical_without_registry = execute_batch(
            &mut client,
            &removed_registry,
            &authorization,
            fixture(&codec, &epoch, &id, &table_id),
            2,
        )
        .unwrap();
        assert_eq!(
            canonical_without_registry.outcomes[0].code,
            MutationOutcomeCode::AlreadyCanonical as i32
        );
        let value: i64 = client
            .query_one("SELECT value FROM calque_mutation_test WHERE id = 1", &[])
            .unwrap()
            .get(0);
        assert_eq!(value, 5);

        // Both authorizations below hold the same read scope and registry and
        // differ only in their command capability. Neither may change `value`
        // or leave a receipt.
        for capability in [
            // The right command, an operation the capability does not carry.
            CommandCapability::new([(
                "tasks.add".to_owned(),
                BTreeSet::from([CommandOperation::Insert, CommandOperation::Delete]),
            )]),
            // The right operation, on a command the capability does not name.
            CommandCapability::new([(
                "other.command".to_owned(),
                BTreeSet::from([CommandOperation::Update]),
            )]),
        ] {
            let uncapable = MutationAuthorization::new(
                source_id(),
                &activated_source,
                b"scope-token".to_vec(),
                scope.clone(),
                capability,
            )
            .unwrap();
            let refused_id = format!("mutation-{}", Uuid::new_v4());
            let refused = execute_batch(
                &mut client,
                &registry,
                &uncapable,
                fixture(&codec, &epoch, &refused_id, &table_id),
                2,
            )
            .unwrap();
            assert_eq!(
                refused.outcomes[0].code,
                MutationOutcomeCode::RejectedAuthorization as i32
            );
            let value: i64 = client
                .query_one("SELECT value FROM calque_mutation_test WHERE id = 1", &[])
                .unwrap()
                .get(0);
            assert_eq!(value, 5, "an unauthorized command changed a row");
            let receipts: i64 = client
                .query_one(
                    "SELECT count(*) FROM calque_internal.mutation_receipts \
                     WHERE mutation_id = $1",
                    &[&refused_id],
                )
                .unwrap()
                .get(0);
            assert_eq!(receipts, 0, "an unauthorized command left a receipt");
        }
        // A declared footprint that parses and names only permitted tables,
        // but a row the command does not touch. Parsing accepts it, and only
        // the comparison with the footprint the command computes refuses it.
        let undeclared_id = format!("mutation-{}", Uuid::new_v4());
        let mut undeclared = fixture(&codec, &epoch, &undeclared_id, &table_id);
        undeclared.mutations[0]
            .declared_footprint
            .as_mut()
            .unwrap()
            .rows[0]
            .primary_key = row_key(2);
        let refused = execute_batch(&mut client, &registry, &authorization, undeclared, 2).unwrap();
        assert_eq!(
            refused.outcomes[0].code,
            MutationOutcomeCode::RejectedAuthorization as i32
        );
        assert_eq!(
            refused.outcomes[0].message,
            "mutation footprint is not authorized"
        );
        let value: i64 = client
            .query_one("SELECT value FROM calque_mutation_test WHERE id = 1", &[])
            .unwrap()
            .get(0);
        assert_eq!(value, 5, "a mutation with a false footprint changed a row");
        let receipts: i64 = client
            .query_one(
                "SELECT count(*) FROM calque_internal.mutation_receipts \
                 WHERE mutation_id = $1",
                &[&undeclared_id],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            receipts, 0,
            "a mutation with a false footprint left a receipt"
        );

        // A command version the registry does not hold is refused before any
        // transaction.
        let unversioned_id = format!("mutation-{}", Uuid::new_v4());
        let mut unversioned = fixture(&codec, &epoch, &unversioned_id, &table_id);
        unversioned.mutations[0].command_version = 2;
        let refused =
            execute_batch(&mut client, &registry, &authorization, unversioned, 2).unwrap();
        assert_eq!(
            refused.outcomes[0].code,
            MutationOutcomeCode::CommandVersionUnsupported as i32
        );
        let receipts: i64 = client
            .query_one(
                "SELECT count(*) FROM calque_internal.mutation_receipts \
                 WHERE mutation_id = $1",
                &[&unversioned_id],
            )
            .unwrap()
            .get(0);
        assert_eq!(receipts, 0, "an unsupported version left a receipt");

        // A receipt that lands between the lookup and the insert, as a
        // concurrent duplicate's would: the insert conflicts, the command does
        // not run, and the stored receipt answers only under its own binding.
        let raced = ParsedBatch::parse(fixture(&codec, &epoch, &id, &table_id), &authorization, 2)
            .unwrap()
            .mutations
            .remove(0);
        let command = registry.get("tasks.add", 1).unwrap();
        match execute_transaction(
            &mut client,
            &authorization,
            command.as_ref(),
            "client-1",
            "device-1",
            &raced,
        ) {
            Ok(TransactionResult::AlreadyCanonical(stored_transaction_id, cursor)) => {
                assert_eq!(stored_transaction_id, server_transaction_id.to_vec());
                assert_eq!(cursor, canonical_cursor.as_str().as_bytes());
            }
            _ => panic!("a conflicting insert did not answer the stored receipt"),
        }
        assert!(
            matches!(
                execute_transaction(
                    &mut client,
                    &authorization,
                    command.as_ref(),
                    "client-1",
                    "device-other",
                    &raced,
                ),
                Ok(TransactionResult::BindingMismatch)
            ),
            "a conflicting insert from another device was answered"
        );
        let value: i64 = client
            .query_one("SELECT value FROM calque_mutation_test WHERE id = 1", &[])
            .unwrap()
            .get(0);
        assert_eq!(value, 5, "a conflicting insert ran its command");

        let requested = BTreeSet::from([MutationId::new(id).unwrap()]);
        let resolved = resolve_acknowledgements(
            &mut client,
            &source_id(),
            &epoch,
            &scope,
            "device-1",
            &requested,
        )
        .unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(resolve_acknowledgements(
            &mut client,
            &source_id(),
            &epoch,
            &scope_for_registry(&schema_registry, 8, vec![1, 2]),
            "device-1",
            &requested,
        )
        .unwrap()
        .is_empty());
        assert!(resolve_acknowledgements(
            &mut client,
            &source_id(),
            &epoch,
            &scope,
            "device-other",
            &requested,
        )
        .unwrap()
        .is_empty());
        assert!(resolve_acknowledgements(
            &mut client,
            &SourceId::new("source-other").unwrap(),
            &epoch,
            &scope,
            "device-1",
            &requested,
        )
        .unwrap()
        .is_empty());
    }
}

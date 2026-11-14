//! Server-scoped, bounded canonical-log to `sync.change.v1` batch construction.
//!
//! A deployment edge resolves an [`AuthorizedScope`] from a verified principal.
//! This crate then applies that same scope's row routing and column projection
//! before per-table snapshot starts, while cursor coverage remains global.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use authz::{AuthorizedScope, CanonicalAuthorizationError};
use bytes::Bytes;
use canonical_log::{
    CanonicalEntry, CanonicalLogRead, CanonicalOperation, CanonicalOperationKind, CanonicalRecord,
    CanonicalTransaction, CellValue, ColumnValue, LogBounds, LogError,
};
use mutation_api::ResolvedMutationAcknowledgements;
use prost::Message;
use source_control::{ActivatedSource, SchemaRegistryBinding};
use sync_protocol::{
    change::{
        self, change_stream_frame, operation, BatchEnd, BatchStart, CellValue as WireCellValue,
        ChangeStreamFrame, ColumnValue as WireColumnValue, Cursor, FullRow, Operation,
        RevocationBarrier, RowPatch, SourceGeneration, TransactionFragment, Truncate,
    },
    encode_sealed_change_frame, verify_revocation_barrier, BatchFramesDigest, FrameEncodeError,
    RevocationBarrierVerificationKey, SealedChangeFrame, SealedChangeFrameError,
    MAX_CHANGE_AUTHORIZATION_TOKEN_BYTES, MAX_CLIENT_INSTANCE_ID_BYTES, MAX_DEVICE_ID_BYTES,
    MAX_LONG_POLL_TIMEOUT_MS, MAX_PENDING_MUTATION_IDS, MAX_REQUEST_TABLES,
    MIN_LONG_POLL_TIMEOUT_MS, PROTOCOL_VERSION,
};
use sync_types::{
    CursorCodec, CursorError, CursorPosition, MutationId, OpaqueCursor, PlanId, SourceEpoch,
    SourceId, TableId,
};
use thiserror::Error;
use uuid::Uuid;

mod scope_encoding;

use scope_encoding::{EncodedTransaction, SharedFrame};
pub use scope_encoding::{ScopeEncoding, ScopeEncodingKey};

const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_BATCH_BYTES: usize = sync_protocol::MAX_CHANGE_BATCH_BYTES as usize;
const MIN_BATCH_BYTES: usize = sync_protocol::MIN_CHANGE_BATCH_BYTES as usize;
const MAX_TRANSACTIONS: usize = sync_protocol::MAX_CHANGE_TRANSACTIONS as usize;
const MAX_BATCH_OPERATIONS: u64 = 100_000;
// sync-types parses the cursors this crate puts on the wire, and the two crates
// do not depend on each other, so their two bounds meet here.
const _: () = assert!(sync_types::MAX_CURSOR_TOKEN_BYTES == sync_protocol::MAX_CURSOR_TOKEN_BYTES);
/// The most canonical records one build reads.
pub const MAX_SCAN_RECORDS: usize = 100_000;
/// The most stored bytes of canonical records one build reads.
pub const MAX_SCAN_STORED_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CANONICAL_RECORD_BYTES: u64 = 64 * 1024 * 1024;
const SCAN_PAGE_RECORDS: usize = 256;
const END_FRAME_RESERVE: usize = 4096;
const NESTED_LENGTH_GROWTH_RESERVE: usize = 8;
const PUBLICATION_GENERATION_V1: u64 = 1;

/// Control-plane binding between one logical source, its activated registry,
/// and one server-derived authorization scope.
///
/// Every field is private. Production callers can create this value only from
/// an [`ActivatedSource`], so change plans cannot independently assert source
/// epoch, cursor key, schema generation, or registry membership.
#[derive(Clone)]
pub struct ActivatedChangeScope {
    source_id: SourceId,
    source_epoch: SourceEpoch,
    cursor_codec: CursorCodec,
    publication_generation: u64,
    registry_binding: ChangeRegistryBinding,
    scope: AuthorizedScope,
}

#[derive(Debug, Clone)]
enum ChangeRegistryBinding {
    Activated(SchemaRegistryBinding),
    #[cfg(test)]
    Fixture {
        schema_generation: u64,
        tables: BTreeSet<TableId>,
    },
}

impl ChangeRegistryBinding {
    fn schema_generation(&self) -> u64 {
        match self {
            Self::Activated(binding) => binding.schema_generation(),
            #[cfg(test)]
            Self::Fixture {
                schema_generation, ..
            } => *schema_generation,
        }
    }

    fn contains_table(&self, table_id: &TableId) -> bool {
        match self {
            Self::Activated(binding) => binding
                .tables()
                .iter()
                .any(|(registered, _)| registered == table_id),
            #[cfg(test)]
            Self::Fixture { tables, .. } => tables.contains(table_id),
        }
    }

    fn table_count(&self) -> usize {
        match self {
            Self::Activated(binding) => binding.tables().len(),
            #[cfg(test)]
            Self::Fixture { tables, .. } => tables.len(),
        }
    }
}

impl ActivatedChangeScope {
    pub fn new(
        source_id: SourceId,
        activated_source: &ActivatedSource,
        scope: AuthorizedScope,
    ) -> Result<Self, ChangeAuthorizationError> {
        if scope.registry_binding() != Some(activated_source.registry_binding()) {
            return Err(ChangeAuthorizationError::ScopeRegistryBindingMismatch);
        }
        let registry_binding =
            ChangeRegistryBinding::Activated(activated_source.registry_binding().clone());
        if let Some(table_id) = scope
            .permitted_tables()
            .find(|table_id| !registry_binding.contains_table(table_id))
        {
            return Err(ChangeAuthorizationError::ScopeRegistryMismatch(
                table_id.clone(),
            ));
        }
        Ok(Self {
            source_id,
            source_epoch: activated_source.source_epoch().clone(),
            cursor_codec: activated_source.cursor_codec().clone(),
            publication_generation: PUBLICATION_GENERATION_V1,
            registry_binding,
            scope,
        })
    }
}

impl std::fmt::Debug for ActivatedChangeScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActivatedChangeScope")
            .field("source_id", &self.source_id)
            .field("source_epoch", &self.source_epoch)
            .field("publication_generation", &self.publication_generation)
            .field(
                "schema_generation",
                &self.registry_binding.schema_generation(),
            )
            .field("registry_tables", &self.registry_binding.table_count())
            .field("scope", &self.scope)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ChangeAuthorization {
    plan_id: PlanId,
    source_id: SourceId,
    source_epoch: SourceEpoch,
    publication_generation: u64,
    registry_binding: ChangeRegistryBinding,
    cursor_codec: CursorCodec,
    expires_at_unix_micros: i64,
    from_cursor_exclusive: OpaqueCursor,
    through_cursor_inclusive: OpaqueCursor,
    table_starts: BTreeMap<TableId, Option<OpaqueCursor>>,
    acknowledgeable_mutation_ids: ResolvedMutationAcknowledgements,
    scope: AuthorizedScope,
    revocation_barrier: Option<AuthorizedRevocationBarrier>,
}

/// One revocation notice as the application backend minted it, with the key
/// this deployment accepts its signatures under. Calque verifies notices; it
/// never mints them.
#[derive(Debug, Clone)]
pub struct MintedRevocationBarrier {
    pub barrier: RevocationBarrier,
    pub verification_key: RevocationBarrierVerificationKey,
}

/// A verified barrier, pinned to the canonical position it takes effect at.
///
/// Every field is private and the only constructor verifies, so a batch
/// builder cannot be handed a barrier that was never authenticated or that
/// belongs to another plan, scope, or source epoch.
#[derive(Debug, Clone)]
struct AuthorizedRevocationBarrier {
    barrier: RevocationBarrier,
    cursor: OpaqueCursor,
    position: CursorPosition,
}

#[derive(Debug, Clone)]
pub struct ScopedChangePlan {
    pub plan_id: PlanId,
    /// Unforgeable source/registry/scope binding derived from control-plane
    /// activated state, never from the change request.
    pub activated_scope: ActivatedChangeScope,
    pub expires_at_unix_micros: i64,
    pub from_cursor_exclusive: OpaqueCursor,
    pub through_cursor_inclusive: OpaqueCursor,
    pub table_starts: BTreeMap<TableId, Option<OpaqueCursor>>,
    /// Durable-receipt proof resolved for the request's scope and device.
    pub acknowledgeable_mutation_ids: ResolvedMutationAcknowledgements,
    /// The authorization-generation fence this plan's scope has been retired
    /// at, when one has been minted for it.
    pub revocation_barrier: Option<MintedRevocationBarrier>,
}

impl ChangeAuthorization {
    pub fn new(plan: ScopedChangePlan) -> Result<Self, ChangeAuthorizationError> {
        if plan.expires_at_unix_micros <= 0 {
            return Err(ChangeAuthorizationError::InvalidExpiry);
        }
        if plan.table_starts.is_empty() || plan.table_starts.len() > MAX_REQUEST_TABLES {
            return Err(ChangeAuthorizationError::InvalidTableSet);
        }
        if plan.acknowledgeable_mutation_ids.len() > MAX_PENDING_MUTATION_IDS {
            return Err(ChangeAuthorizationError::TooManyMutationAcknowledgements);
        }
        if let Some(table_id) = plan
            .table_starts
            .keys()
            .find(|table_id| !plan.activated_scope.scope.permits_table(table_id))
        {
            return Err(ChangeAuthorizationError::UnauthorizedTable(
                table_id.clone(),
            ));
        }
        if let Some(table_id) = plan.table_starts.keys().find(|table_id| {
            !plan
                .activated_scope
                .registry_binding
                .contains_table(table_id)
        }) {
            return Err(ChangeAuthorizationError::PlanRegistryMismatch(
                table_id.clone(),
            ));
        }
        let revocation_barrier = plan
            .revocation_barrier
            .map(|minted| {
                authorize_revocation_barrier(
                    minted,
                    &plan.activated_scope.scope,
                    &plan.activated_scope.cursor_codec,
                    &plan.activated_scope.source_epoch,
                )
            })
            .transpose()?;
        Ok(Self {
            plan_id: plan.plan_id,
            source_id: plan.activated_scope.source_id,
            source_epoch: plan.activated_scope.source_epoch,
            publication_generation: plan.activated_scope.publication_generation,
            registry_binding: plan.activated_scope.registry_binding,
            cursor_codec: plan.activated_scope.cursor_codec,
            expires_at_unix_micros: plan.expires_at_unix_micros,
            from_cursor_exclusive: plan.from_cursor_exclusive,
            through_cursor_inclusive: plan.through_cursor_inclusive,
            table_starts: plan.table_starts,
            acknowledgeable_mutation_ids: plan.acknowledgeable_mutation_ids,
            scope: plan.activated_scope.scope,
            revocation_barrier,
        })
    }
}

/// Verifies one minted barrier and binds it to this plan's own scope, epoch,
/// and cursor space.
///
/// The signature is checked first, because everything after it reads fields
/// the signature covers. A barrier whose cursor this source epoch's codec does
/// not accept has no position in this stream.
fn authorize_revocation_barrier(
    minted: MintedRevocationBarrier,
    scope: &AuthorizedScope,
    cursor_codec: &CursorCodec,
    source_epoch: &SourceEpoch,
) -> Result<AuthorizedRevocationBarrier, ChangeAuthorizationError> {
    let MintedRevocationBarrier {
        barrier,
        verification_key,
    } = minted;
    verify_revocation_barrier(&barrier, &verification_key)
        .map_err(|_| ChangeAuthorizationError::RevocationBarrierUnauthenticated)?;
    if barrier.revoked_authorization_generation != scope.generation()
        || barrier.revoked_authorization_scope_hash.as_slice()
            != scope.scope_hash().as_bytes().as_slice()
    {
        return Err(ChangeAuthorizationError::RevocationBarrierScopeMismatch);
    }
    if barrier.effective_authorization_generation <= barrier.revoked_authorization_generation {
        return Err(ChangeAuthorizationError::RevocationBarrierScopeMismatch);
    }
    let cursor = required_wire_cursor(barrier.barrier_cursor.clone())
        .map_err(|_| ChangeAuthorizationError::RevocationBarrierCursor)?;
    let position = cursor_codec
        .decode_for_epoch(&cursor, source_epoch)
        .map_err(|_| ChangeAuthorizationError::RevocationBarrierCursor)?;
    Ok(AuthorizedRevocationBarrier {
        barrier,
        cursor,
        position,
    })
}

#[derive(Debug, Error)]
pub enum ChangeAuthorizationError {
    #[error("change authorization expiry must be after the Unix epoch")]
    InvalidExpiry,
    #[error("change authorization table set must contain between 1 and 1024 tables")]
    InvalidTableSet,
    #[error("change authorization may acknowledge at most 256 mutations")]
    TooManyMutationAcknowledgements,
    #[error("change authorization includes table outside its server-resolved scope: {0}")]
    UnauthorizedTable(TableId),
    #[error("authorized scope includes table outside its activated schema registry: {0}")]
    ScopeRegistryMismatch(TableId),
    #[error("authorized scope was not compiled from the exact activated schema registry")]
    ScopeRegistryBindingMismatch,
    #[error("change plan includes table outside its activated schema registry: {0}")]
    PlanRegistryMismatch(TableId),
    #[error("revocation barrier was not signed by the configured authorization authority")]
    RevocationBarrierUnauthenticated,
    #[error("revocation barrier does not retire this plan's authorization generation and scope")]
    RevocationBarrierScopeMismatch,
    #[error("revocation barrier cursor is missing or outside this source epoch")]
    RevocationBarrierCursor,
}

impl From<ChangeAuthorizationError> for ChangeBatchError {
    fn from(error: ChangeAuthorizationError) -> Self {
        match error {
            ChangeAuthorizationError::ScopeRegistryMismatch(_)
            | ChangeAuthorizationError::ScopeRegistryBindingMismatch
            | ChangeAuthorizationError::PlanRegistryMismatch(_) => Self::SchemaGenerationMismatch,
            ChangeAuthorizationError::InvalidExpiry
            | ChangeAuthorizationError::InvalidTableSet
            | ChangeAuthorizationError::TooManyMutationAcknowledgements
            | ChangeAuthorizationError::UnauthorizedTable(_) => Self::InvalidPlanBinding,
            // A barrier that cannot be authenticated, or that does not belong
            // to this plan's scope, is reported rather than dropped: carrying
            // on without it would keep serving a scope the deployment meant to
            // retire.
            ChangeAuthorizationError::RevocationBarrierUnauthenticated
            | ChangeAuthorizationError::RevocationBarrierScopeMismatch
            | ChangeAuthorizationError::RevocationBarrierCursor => Self::AuthorizationExpired,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableSubscription {
    pub table_id: TableId,
    /// `None` means this resident table starts at the request's global cursor.
    pub snapshot_cursor: Option<OpaqueCursor>,
}

#[derive(Debug, Clone)]
pub struct ChangeBatchRequest {
    pub plan_id: PlanId,
    pub protocol_version: u32,
    pub from_cursor_exclusive: OpaqueCursor,
    /// `Some` is an exact bootstrap/expansion fence. `None` requests one live
    /// batch from the authenticated activation cursor toward the durable head.
    pub through_cursor_inclusive: Option<OpaqueCursor>,
    pub tables: Vec<TableSubscription>,
    pub max_batch_bytes: usize,
    pub max_transactions: usize,
    /// Zero requests an immediate response. For a live request, non-zero asks
    /// the owning backend to wait for a durable-head hint before building.
    pub long_poll_timeout_ms: u32,
    pub accept_transaction_fragmentation: bool,
    /// Whether the client accepts the `RevocationBarrier` body, which goes on
    /// the wire only to a client that asked for it. A client that did not is
    /// still fenced: standing on the barrier is refused rather than served.
    pub accept_revocation_barrier: bool,
    /// Asks for one open response instead of one batch, for at most
    /// `long_poll_timeout_ms`, so only a request with a non-zero wait may set
    /// it. A live request's carries a batch each time the head moves; a
    /// fenced request's carries its batches back to back and ends at the
    /// fence.
    pub stream: bool,
    device_id: String,
    pending_mutation_ids: BTreeSet<MutationId>,
}

pub struct IncomingChangeRequest {
    request: ChangeBatchRequest,
    authorization_scope_token: Vec<u8>,
    client_instance_id: String,
    device_id: String,
    pending_mutation_ids: BTreeSet<MutationId>,
}

impl IncomingChangeRequest {
    pub fn request(&self) -> &ChangeBatchRequest {
        &self.request
    }

    pub fn authorization_scope_token(&self) -> &[u8] {
        &self.authorization_scope_token
    }

    pub fn client_instance_id(&self) -> &str {
        &self.client_instance_id
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    pub fn pending_mutation_ids(&self) -> &BTreeSet<MutationId> {
        &self.pending_mutation_ids
    }
}

impl std::fmt::Debug for IncomingChangeRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IncomingChangeRequest")
            .field("request", &self.request)
            .field("authorization_scope_token", &"[REDACTED]")
            .field("client_instance_id", &self.client_instance_id)
            .field("device_id", &self.device_id)
            .field("pending_mutation_count", &self.pending_mutation_ids.len())
            .finish()
    }
}

pub fn parse_change_request(
    request: change::ChangeRequest,
) -> Result<IncomingChangeRequest, ChangeBatchError> {
    if request.protocol_version != PROTOCOL_VERSION {
        return Err(ChangeBatchError::UnsupportedProtocol);
    }
    if request.authorization_scope_token.is_empty()
        || request.authorization_scope_token.len() > MAX_CHANGE_AUTHORIZATION_TOKEN_BYTES
    {
        return Err(ChangeBatchError::InvalidAuthorizationToken);
    }
    if request.client_instance_id.is_empty()
        || request.client_instance_id.len() > MAX_CLIENT_INSTANCE_ID_BYTES
        || request.client_instance_id.chars().any(char::is_control)
    {
        return Err(ChangeBatchError::InvalidClientInstance);
    }
    if request.device_id.is_empty()
        || request.device_id.len() > MAX_DEVICE_ID_BYTES
        || request.device_id.chars().any(char::is_control)
        || request.pending_mutation_ids.len() > MAX_PENDING_MUTATION_IDS
    {
        return Err(ChangeBatchError::InvalidClientInstance);
    }
    let pending_mutation_count = request.pending_mutation_ids.len();
    let pending_mutation_ids = request
        .pending_mutation_ids
        .into_iter()
        .map(MutationId::new)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|_| ChangeBatchError::InvalidClientInstance)?;
    if pending_mutation_ids.len() != pending_mutation_count {
        return Err(ChangeBatchError::InvalidClientInstance);
    }
    if request.tables.is_empty() || request.tables.len() > MAX_REQUEST_TABLES {
        return Err(ChangeBatchError::InvalidTableSet);
    }
    if request.long_poll_timeout_ms != 0
        && !(MIN_LONG_POLL_TIMEOUT_MS..=MAX_LONG_POLL_TIMEOUT_MS)
            .contains(&request.long_poll_timeout_ms)
    {
        return Err(ChangeBatchError::InvalidLimits);
    }
    // A zero wait gives a stream no lifetime.
    if request.stream && request.long_poll_timeout_ms == 0 {
        return Err(ChangeBatchError::InvalidLimits);
    }

    let plan_id = PlanId::new(request.plan_id).map_err(|_| ChangeBatchError::InvalidPlanBinding)?;
    let from_cursor_exclusive = required_wire_cursor(request.from_cursor_exclusive)?;
    let through_cursor_inclusive = request
        .through_cursor_inclusive
        .map(wire_cursor)
        .transpose()?;
    let tables = request
        .tables
        .into_iter()
        .map(|subscription| {
            let table_id = TableId::new(subscription.table_id)
                .map_err(|_| ChangeBatchError::InvalidTableSet)?;
            let snapshot_cursor = subscription
                .shard_snapshot_cursor
                .map(wire_cursor)
                .transpose()?;
            Ok(TableSubscription {
                table_id,
                snapshot_cursor,
            })
        })
        .collect::<Result<Vec<_>, ChangeBatchError>>()?;
    // Checked here, before anything is reserved for the batch.
    let max_batch_bytes = usize::try_from(request.max_batch_bytes)
        .ok()
        .filter(|bytes| (MIN_BATCH_BYTES..=MAX_BATCH_BYTES).contains(bytes))
        .ok_or(ChangeBatchError::InvalidLimits)?;
    let max_transactions = usize::try_from(request.max_transactions)
        .ok()
        .filter(|count| (1..=MAX_TRANSACTIONS).contains(count))
        .ok_or(ChangeBatchError::InvalidLimits)?;

    Ok(IncomingChangeRequest {
        request: ChangeBatchRequest {
            plan_id,
            protocol_version: request.protocol_version,
            from_cursor_exclusive,
            through_cursor_inclusive,
            tables,
            max_batch_bytes,
            max_transactions,
            long_poll_timeout_ms: request.long_poll_timeout_ms,
            accept_transaction_fragmentation: request.accept_transaction_fragmentation,
            accept_revocation_barrier: request.accept_revocation_barrier,
            stream: request.stream,
            device_id: request.device_id.clone(),
            pending_mutation_ids: pending_mutation_ids.clone(),
        },
        authorization_scope_token: request.authorization_scope_token,
        client_instance_id: request.client_instance_id,
        device_id: request.device_id,
        pending_mutation_ids,
    })
}

/// One batch, as the buffers it is written as. A transaction frame the batch
/// shares with the other clients of its scope is the same refcounted bytes in
/// every batch that carries it; what is the client's own is its own buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeBatch {
    pub chunks: Vec<Bytes>,
    pub through_cursor_inclusive: OpaqueCursor,
    pub reached_requested_fence: bool,
    pub relevant_transactions: u64,
    pub relevant_operations: u64,
    /// True when this batch ended on the head it read, as its
    /// `BatchEnd.currently_up_to_date` says.
    pub currently_up_to_date: bool,
    /// True when this batch ended on the authorization-generation fence and
    /// carries the barrier frame that says so.
    pub revocation_barrier_emitted: bool,
    pub stats: ChangeBatchStats,
}

impl ChangeBatch {
    /// The batch's frames in one buffer.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.chunks.iter().map(Bytes::len).sum());
        for chunk in &self.chunks {
            bytes.extend_from_slice(chunk);
        }
        bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeBatchStats {
    pub scanned_records: u64,
    pub scanned_stored_bytes: u64,
    pub encoded_bytes: u64,
}

/// One canonical record a build reads, owned or held in a shared tail.
#[derive(Debug, Clone)]
#[allow(
    clippy::large_enum_variant,
    reason = "pages are read once and moved; boxing would allocate per record"
)]
pub enum PageRecord {
    Owned(CanonicalRecord),
    Shared(Arc<CanonicalRecord>),
}

impl PageRecord {
    fn record(&self) -> &CanonicalRecord {
        match self {
            Self::Owned(record) => record,
            Self::Shared(record) => record,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecordPage {
    pub records: Vec<PageRecord>,
    /// Checksummed provider bytes the records were read from.
    pub stored_bytes: u64,
}

/// The canonical records a build reads, under the contract of
/// [`CanonicalLogRead`]: contiguous complete records, within both page limits.
/// A canonical log is one, and so is a tail of it held in memory.
pub trait ChangeRecords {
    fn source_id(&self) -> &SourceId;

    fn source_epoch(&self) -> &SourceEpoch;

    fn read_page_after(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<RecordPage, LogError>;

    /// The bounds, and the page [`Self::read_page_after`] returns for the same
    /// arguments, as [`CanonicalLogRead::read_bounds_and_page_after_bounded`].
    fn read_bounds_and_page_after(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<(LogBounds, Result<RecordPage, LogError>), LogError>;
}

impl<L: CanonicalLogRead + ?Sized> ChangeRecords for L {
    fn source_id(&self) -> &SourceId {
        CanonicalLogRead::source_id(self)
    }

    fn source_epoch(&self) -> &SourceEpoch {
        CanonicalLogRead::source_epoch(self)
    }

    fn read_page_after(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<RecordPage, LogError> {
        self.read_page_after_bounded(
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )
        .map(owned_page)
    }

    fn read_bounds_and_page_after(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<(LogBounds, Result<RecordPage, LogError>), LogError> {
        let (bounds, page) = self.read_bounds_and_page_after_bounded(
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )?;
        Ok((bounds, page.map(owned_page)))
    }
}

fn owned_page(page: canonical_log::CanonicalPage) -> RecordPage {
    RecordPage {
        records: page.records.into_iter().map(PageRecord::Owned).collect(),
        stored_bytes: page.stored_bytes,
    }
}

pub struct ChangeBatchBuilder<'a, R: ChangeRecords + ?Sized> {
    records: &'a R,
    authorization: &'a ChangeAuthorization,
    shared: Option<&'a ScopeEncoding>,
}

impl<'a, R: ChangeRecords + ?Sized> ChangeBatchBuilder<'a, R> {
    pub fn new(
        records: &'a R,
        authorization: &'a ChangeAuthorization,
    ) -> Result<Self, ChangeBatchError> {
        if authorization
            .scope
            .permitted_tables()
            .any(|table_id| !authorization.registry_binding.contains_table(table_id))
            || authorization
                .table_starts
                .keys()
                .any(|table_id| !authorization.registry_binding.contains_table(table_id))
        {
            return Err(ChangeBatchError::SchemaGenerationMismatch);
        }
        if records.source_id() != &authorization.source_id {
            return Err(ChangeBatchError::SourceMismatch);
        }
        if records.source_epoch() != &authorization.source_epoch {
            return Err(ChangeBatchError::SourceEpochMismatch);
        }
        Ok(Self {
            records,
            authorization,
            shared: None,
        })
    }

    /// Sends each transaction as the scope's shared encoding of it, encoding
    /// it here only if no build of the scope has yet. A request whose tables
    /// start past its own cursor is encoded for itself.
    pub fn with_shared_encoding(
        mut self,
        encoding: &'a ScopeEncoding,
    ) -> Result<Self, ChangeBatchError> {
        if !encoding.key().matches(self.authorization) {
            return Err(ChangeBatchError::SharedEncodingMismatch);
        }
        self.shared = Some(encoding);
        Ok(self)
    }

    pub fn build_at(
        &self,
        request: &ChangeBatchRequest,
        now_unix_micros: i64,
    ) -> Result<ChangeBatch, ChangeBatchError> {
        self.build_at_with_stream_id(request, now_unix_micros, Uuid::new_v4().as_bytes().to_vec())
    }

    fn build_at_with_stream_id(
        &self,
        request: &ChangeBatchRequest,
        now_unix_micros: i64,
        stream_id: Vec<u8>,
    ) -> Result<ChangeBatch, ChangeBatchError> {
        let server_time_unix_micros =
            u64::try_from(now_unix_micros).map_err(|_| ChangeBatchError::InvalidServerTime)?;
        if now_unix_micros >= self.authorization.expires_at_unix_micros {
            return Err(ChangeBatchError::AuthorizationExpired);
        }
        self.validate_limits(request)?;
        if request.plan_id != self.authorization.plan_id {
            return Err(ChangeBatchError::InvalidPlanBinding);
        }
        if request
            .through_cursor_inclusive
            .as_ref()
            .is_some_and(|fence| fence != &self.authorization.through_cursor_inclusive)
        {
            return Err(ChangeBatchError::InvalidPlanBinding);
        }
        let acknowledgeable = &self.authorization.acknowledgeable_mutation_ids;
        if !acknowledgeable.matches_request(
            &self.authorization.source_id,
            &self.authorization.source_epoch,
            &self.authorization.scope,
            &request.device_id,
            &request.pending_mutation_ids,
        ) {
            return Err(ChangeBatchError::InvalidMutationAcknowledgementProof);
        }
        let live = request.through_cursor_inclusive.is_none();
        let authorized_from = self.decode(if live {
            &self.authorization.through_cursor_inclusive
        } else {
            &self.authorization.from_cursor_exclusive
        })?;
        let from = self.decode(&request.from_cursor_exclusive)?;
        let requested_fence = request
            .through_cursor_inclusive
            .as_ref()
            .map(|fence| self.decode(fence))
            .transpose()?;
        let barrier = self.authorization.revocation_barrier.as_ref();
        // The head, the floor and the first page come from one read, before
        // the request is checked against them, so a build that fits one page
        // costs one round trip. The page is bounded by what bounds the scan
        // below: the fence and the barrier where the request has them, and
        // otherwise the head the read itself stands on. A request the checks
        // refuse leaves its page unused.
        let first_through = match (&requested_fence, barrier) {
            (Some(fence), Some(barrier)) => Some(fence.sequence.min(barrier.position.sequence)),
            (Some(fence), None) => Some(fence.sequence),
            (None, Some(barrier)) => Some(barrier.position.sequence),
            (None, None) => None,
        };
        let (bounds, first_page) = self.records.read_bounds_and_page_after(
            from.sequence,
            first_through,
            MAX_SCAN_RECORDS.min(SCAN_PAGE_RECORDS),
            MAX_SCAN_STORED_BYTES.min(MAX_CANONICAL_RECORD_BYTES),
        )?;
        let head = bounds.head;
        let fence = requested_fence.unwrap_or_else(|| head.clone());
        if from.sequence < authorized_from.sequence
            || fence.sequence < from.sequence
            || fence.sequence > head.sequence
        {
            return Err(ChangeBatchError::FenceOutsideDurableRange);
        }
        let minimum = bounds.minimum_retained.sequence;
        if from.sequence.saturating_add(1) < minimum {
            return Err(ChangeBatchError::BeforeRetention { minimum });
        }
        // The barrier truncates this batch: everything at or before its
        // sequence was authorized by the generation it retires and is still
        // served, and nothing after it is scanned. A request that starts on the
        // barrier, where every caught-up client stands, gets a batch holding
        // only the barrier. One that starts after it asks to continue past a
        // fence this scope no longer holds, so it is refused as expired
        // authorization, which `change-http` answers with 401 and
        // `resnapshot_required`.
        if let Some(barrier) = barrier {
            // The application, not the log, mints the barrier cursor, so it
            // can name a position no record occupies. The log is dense between
            // its retention floor and its head, so a placeable barrier stands
            // on a record, or just below the floor where a client that has read
            // everything retained stands. An unplaceable barrier is refused:
            // ignoring it would keep serving the retired generation, and
            // truncating at a position the scan never reaches would stall every
            // later poll.
            if barrier.position.sequence.saturating_add(1) < minimum
                || barrier.position.sequence > head.sequence
            {
                return Err(ChangeBatchError::RevocationBarrierUnplaceable);
            }
            if from.sequence > barrier.position.sequence {
                return Err(ChangeBatchError::AuthorizationExpired);
            }
        }
        let scan_limit = barrier
            .map(|barrier| barrier.position.sequence.min(fence.sequence))
            .unwrap_or(fence.sequence);
        let subscriptions = if live {
            self.validate_live_tables(request, &from, &authorized_from, &fence)?
        } else {
            self.validate_tables(request, &from, &authorized_from, &fence)?
        };
        // A table that starts past the request's cursor filters records the
        // scope's shared encoding carries, so such a request is encoded for
        // itself.
        let shared = self
            .shared
            .filter(|_| subscriptions.values().all(|start| *start <= from.sequence));
        let table_names = subscriptions
            .keys()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let table_starts = request
            .tables
            .iter()
            .map(|subscription| change::TableSubscription {
                table_id: subscription.table_id.to_string(),
                shard_snapshot_cursor: subscription.snapshot_cursor.as_ref().map(cursor),
            })
            .collect();
        let generation = SourceGeneration {
            source_epoch: self.authorization.source_epoch.to_string(),
            publication_generation: self.authorization.publication_generation,
            schema_generation: self.authorization.registry_binding.schema_generation(),
            authorization_generation: self.authorization.scope.generation(),
            authorization_scope_hash: self.authorization.scope.scope_hash().as_bytes().to_vec(),
        };
        let mut frames = BatchFrames::default();
        frames.push(sealed_frame(
            frame(change_stream_frame::Body::BatchStart(BatchStart {
                generation: Some(generation),
                from_cursor_exclusive: Some(cursor(&request.from_cursor_exclusive)),
                requested_fence: request.through_cursor_inclusive.as_ref().map(cursor),
                table_ids: table_names,
                server_time_unix_micros,
                plan_id: request.plan_id.to_string(),
                table_starts,
                stream_id,
            })),
            MAX_FRAME_BYTES,
        )?);

        let mut scanned = from.sequence;
        let mut scanned_records = 0_usize;
        let mut scanned_bytes = 0_u64;
        let mut relevant_transactions = 0_u64;
        let mut relevant_operations = 0_u64;
        let mut acknowledged = Vec::<String>::new();
        let mut acknowledged_bytes = 0_usize;
        // The scan's first page is the page read with the bounds: it starts
        // where the scan starts, is bounded by `scan_limit` and has the whole
        // scan budget, so it is the page the first pass would read.
        let mut first_page = Some(first_page);
        'scan: while scanned < scan_limit && relevant_transactions < request.max_transactions as u64
        {
            if scanned_records == MAX_SCAN_RECORDS || scanned_bytes == MAX_SCAN_STORED_BYTES {
                break;
            }
            let remaining_scan_bytes = MAX_SCAN_STORED_BYTES - scanned_bytes;
            let remaining_scan_records = MAX_SCAN_RECORDS - scanned_records;
            let read = match first_page.take() {
                Some(read) => read,
                None => self.records.read_page_after(
                    scanned,
                    Some(scan_limit),
                    remaining_scan_records.min(SCAN_PAGE_RECORDS),
                    remaining_scan_bytes.min(MAX_CANONICAL_RECORD_BYTES),
                ),
            };
            let page = match read {
                Ok(page) => page,
                Err(LogError::ReadLimitTooSmall { .. }) if scanned_records > 0 => break,
                // Trimmed past the scan between two of its pages.
                Err(LogError::BeforeRetention { minimum, .. }) => {
                    return Err(ChangeBatchError::BeforeRetention { minimum })
                }
                Err(error) => return Err(error.into()),
            };
            if page.records.is_empty() {
                return Err(ChangeBatchError::CanonicalRangeIncomplete);
            }
            scanned_records = scanned_records
                .checked_add(page.records.len())
                .ok_or(ChangeBatchError::CountOverflow)?;
            scanned_bytes = scanned_bytes
                .checked_add(page.stored_bytes)
                .ok_or(ChangeBatchError::CountOverflow)?;
            for record in page.records {
                if relevant_transactions >= request.max_transactions as u64 {
                    break 'scan;
                }
                let record_sequence = record.record().position.sequence;
                let acknowledgements =
                    record_acknowledgements(record.record(), acknowledgeable, &acknowledged);
                let acknowledgement_bytes = acknowledgements
                    .iter()
                    .map(|id| acknowledgement_len(id))
                    .sum::<usize>();
                // The batch end carries every acknowledgement, so the room a
                // transaction has is what is left after them.
                let reserved = frames
                    .len()
                    .saturating_add(END_FRAME_RESERVE)
                    .saturating_add(acknowledged_bytes)
                    .saturating_add(acknowledgement_bytes);
                let transaction = match shared {
                    Some(encoding) => encoding.get_or_encode(record_sequence, || {
                        let canonical = record.record();
                        let CanonicalEntry::Transaction(transaction) = &canonical.entry else {
                            return Ok(None);
                        };
                        let operations = self
                            .authorization
                            .scope
                            .route_operations(&transaction.operations)?
                            .into_iter()
                            .filter(|operation| {
                                encoding.key().tables().contains(&operation.table_id)
                            })
                            .collect();
                        encoded_transaction(
                            transaction,
                            &canonical.position,
                            operations,
                            &self.authorization.cursor_codec,
                        )
                    })?,
                    None => self
                        .encode_for_request(record, &subscriptions)?
                        .map(Arc::new),
                };
                if let Some(transaction) = transaction {
                    if relevant_operations.saturating_add(transaction.operation_count)
                        > MAX_BATCH_OPERATIONS
                    {
                        break 'scan;
                    }
                    if transaction.encoded_bytes > request.max_batch_bytes.saturating_sub(reserved)
                    {
                        if scanned != from.sequence {
                            break 'scan;
                        }
                        return Err(ChangeBatchError::BatchLimitTooSmall);
                    }
                    if transaction.frames.len() > 1 && !request.accept_transaction_fragmentation {
                        return Err(ChangeBatchError::FragmentationRequired);
                    }
                    frames.push_transaction(&transaction, shared.is_some());
                    relevant_transactions += 1;
                    relevant_operations = relevant_operations
                        .checked_add(transaction.operation_count)
                        .ok_or(ChangeBatchError::CountOverflow)?;
                } else if reserved > request.max_batch_bytes {
                    if scanned != from.sequence {
                        break 'scan;
                    }
                    return Err(ChangeBatchError::BatchLimitTooSmall);
                }
                acknowledged.extend(acknowledgements);
                acknowledged_bytes += acknowledgement_bytes;
                scanned = record_sequence;
            }
        }
        // The barrier is emitted only once the scan actually stands on it. A
        // batch cut short by its byte or transaction budget has not reached
        // the fence yet, so it ends where it stopped and the next request
        // carries on to the barrier.
        let emitted_barrier = barrier.filter(|barrier| scanned == barrier.position.sequence);
        // A client that did not negotiate the barrier body gets no barrier and
        // nothing past it: standing on it is the coarse expired authorization,
        // which `change-http` answers with 401 and `resnapshot_required`.
        if emitted_barrier.is_some() && !request.accept_revocation_barrier {
            return Err(ChangeBatchError::RevocationBarrierNotNegotiated);
        }
        if let Some(barrier) = emitted_barrier {
            frames.push(sealed_frame(
                frame(change_stream_frame::Body::RevocationBarrier(
                    barrier.barrier.clone(),
                )),
                MAX_FRAME_BYTES,
            )?);
        }
        let through_position = CursorPosition {
            source_epoch: self.authorization.source_epoch.clone(),
            sequence: scanned,
        };
        // On a barrier batch the end stands on the barrier's own cursor bytes,
        // so a receiver that compares cursors only for equality can still tell
        // "before" from "after".
        let through_cursor = match emitted_barrier {
            Some(barrier) => barrier.cursor.clone(),
            None => self.authorization.cursor_codec.encode(&through_position),
        };
        let reached = !live && scanned == fence.sequence;
        let currently_up_to_date = scanned == head.sequence;
        let (chunks, encoded_len) = frames.end(BatchEnd {
            through_cursor_inclusive: Some(cursor(&through_cursor)),
            reached_requested_fence: reached,
            currently_up_to_date,
            relevant_transaction_count: relevant_transactions,
            relevant_operation_count: relevant_operations,
            server_time_unix_micros,
            acknowledged_mutation_ids: acknowledged,
            frame_count: 0,
            frames_sha256: Vec::new(),
        })?;
        if encoded_len > request.max_batch_bytes {
            return Err(ChangeBatchError::BatchLimitTooSmall);
        }
        Ok(ChangeBatch {
            chunks,
            through_cursor_inclusive: through_cursor,
            reached_requested_fence: reached,
            relevant_transactions,
            relevant_operations,
            currently_up_to_date,
            revocation_barrier_emitted: emitted_barrier.is_some(),
            stats: ChangeBatchStats {
                scanned_records: u64::try_from(scanned_records)
                    .map_err(|_| ChangeBatchError::CountOverflow)?,
                scanned_stored_bytes: scanned_bytes,
                encoded_bytes: u64::try_from(encoded_len)
                    .map_err(|_| ChangeBatchError::CountOverflow)?,
            },
        })
    }

    /// Encodes one record for this request alone, applying each table's own
    /// start.
    fn encode_for_request(
        &self,
        record: PageRecord,
        subscriptions: &BTreeMap<TableId, u64>,
    ) -> Result<Option<EncodedTransaction>, ChangeBatchError> {
        let scope = &self.authorization.scope;
        match record {
            PageRecord::Owned(CanonicalRecord {
                position,
                entry: CanonicalEntry::Transaction(mut transaction),
            }) => {
                // Route and project the complete transaction before applying
                // per-table snapshot starts. This keeps policy violations
                // fail-closed even for an operation that predates one table's
                // activation cursor.
                let operations =
                    scope.route_owned_operations(std::mem::take(&mut transaction.operations))?;
                self.encode_started(&transaction, &position, operations, subscriptions)
            }
            PageRecord::Shared(record) => {
                let CanonicalEntry::Transaction(transaction) = &record.entry else {
                    return Ok(None);
                };
                let operations = scope.route_operations(&transaction.operations)?;
                self.encode_started(transaction, &record.position, operations, subscriptions)
            }
            PageRecord::Owned(_) => Ok(None),
        }
    }

    fn encode_started(
        &self,
        transaction: &CanonicalTransaction,
        position: &CursorPosition,
        operations: Vec<CanonicalOperation>,
        subscriptions: &BTreeMap<TableId, u64>,
    ) -> Result<Option<EncodedTransaction>, ChangeBatchError> {
        let operations = operations
            .into_iter()
            .filter(|operation| {
                subscriptions
                    .get(&operation.table_id)
                    .is_some_and(|start| position.sequence > *start)
            })
            .collect();
        encoded_transaction(
            transaction,
            position,
            operations,
            &self.authorization.cursor_codec,
        )
    }

    fn decode(&self, cursor: &OpaqueCursor) -> Result<CursorPosition, ChangeBatchError> {
        Ok(self
            .authorization
            .cursor_codec
            .decode_for_epoch(cursor, &self.authorization.source_epoch)?)
    }

    fn validate_limits(&self, request: &ChangeBatchRequest) -> Result<(), ChangeBatchError> {
        if request.protocol_version != PROTOCOL_VERSION {
            return Err(ChangeBatchError::UnsupportedProtocol);
        }
        if !(MIN_BATCH_BYTES..=MAX_BATCH_BYTES).contains(&request.max_batch_bytes)
            || request.max_transactions == 0
            || request.max_transactions > MAX_TRANSACTIONS
        {
            return Err(ChangeBatchError::InvalidLimits);
        }
        Ok(())
    }

    fn validate_tables(
        &self,
        request: &ChangeBatchRequest,
        from: &CursorPosition,
        authorized_from: &CursorPosition,
        fence: &CursorPosition,
    ) -> Result<BTreeMap<TableId, u64>, ChangeBatchError> {
        if request.tables.is_empty()
            || request.tables.len() > MAX_REQUEST_TABLES
            || request.tables.len() != self.authorization.table_starts.len()
        {
            return Err(ChangeBatchError::InvalidTableSet);
        }
        let mut result = BTreeMap::new();
        for subscription in &request.tables {
            if !self
                .authorization
                .scope
                .permits_table(&subscription.table_id)
            {
                return Err(ChangeBatchError::UnauthorizedTable(
                    subscription.table_id.clone(),
                ));
            }
            let Some(expected_start) = self.authorization.table_starts.get(&subscription.table_id)
            else {
                return Err(ChangeBatchError::UnauthorizedTable(
                    subscription.table_id.clone(),
                ));
            };
            if &subscription.snapshot_cursor != expected_start {
                return Err(ChangeBatchError::InvalidPlanBinding);
            }
            let start = match &subscription.snapshot_cursor {
                Some(cursor) => self.decode(cursor)?.sequence,
                None => from.sequence,
            };
            if start < authorized_from.sequence || start > fence.sequence {
                return Err(ChangeBatchError::InvalidSnapshotCursor);
            }
            if result
                .insert(subscription.table_id.clone(), start)
                .is_some()
            {
                return Err(ChangeBatchError::InvalidTableSet);
            }
        }
        Ok(result)
    }

    fn validate_live_tables(
        &self,
        request: &ChangeBatchRequest,
        from: &CursorPosition,
        authorized_from: &CursorPosition,
        fence: &CursorPosition,
    ) -> Result<BTreeMap<TableId, u64>, ChangeBatchError> {
        if request.tables.is_empty()
            || request.tables.len() > MAX_REQUEST_TABLES
            || request.tables.len() != self.authorization.table_starts.len()
            || request
                .tables
                .iter()
                .any(|subscription| subscription.snapshot_cursor.is_some())
        {
            return Err(ChangeBatchError::InvalidTableSet);
        }
        if from.sequence < authorized_from.sequence || from.sequence > fence.sequence {
            return Err(ChangeBatchError::FenceOutsideDurableRange);
        }
        let mut result = BTreeMap::new();
        for subscription in &request.tables {
            if !self
                .authorization
                .scope
                .permits_table(&subscription.table_id)
                || !self
                    .authorization
                    .table_starts
                    .contains_key(&subscription.table_id)
            {
                return Err(ChangeBatchError::UnauthorizedTable(
                    subscription.table_id.clone(),
                ));
            }
            if result
                .insert(subscription.table_id.clone(), from.sequence)
                .is_some()
            {
                return Err(ChangeBatchError::InvalidTableSet);
            }
        }
        Ok(result)
    }
}

/// The frames of one transaction as `scope` sees it, or `None` when the scope
/// is sent none of its operations.
fn encoded_transaction(
    transaction: &CanonicalTransaction,
    position: &CursorPosition,
    operations: Vec<CanonicalOperation>,
    cursor_codec: &CursorCodec,
) -> Result<Option<EncodedTransaction>, ChangeBatchError> {
    if operations.is_empty() {
        return Ok(None);
    }
    let operation_count =
        u64::try_from(operations.len()).map_err(|_| ChangeBatchError::CountOverflow)?;
    if operation_count > MAX_BATCH_OPERATIONS {
        return Err(ChangeBatchError::TransactionOperationLimitExceeded);
    }
    let operations = operations
        .into_iter()
        .map(wire_operation)
        .collect::<Result<Vec<_>, _>>()?;
    let frames = transaction_frames(
        wire_transaction(transaction, cursor_codec.encode(position)),
        operations,
    )?;
    let encoded_bytes = frames.iter().map(|frame| frame.bytes.len()).sum();
    Ok(Some(EncodedTransaction {
        frames,
        operation_count,
        encoded_bytes,
    }))
}

/// The pending mutations of this request that `record` committed and the
/// batch has not yet acknowledged.
fn record_acknowledgements(
    record: &CanonicalRecord,
    acknowledgeable: &ResolvedMutationAcknowledgements,
    acknowledged: &[String],
) -> Vec<String> {
    if acknowledgeable.is_empty() {
        return Vec::new();
    }
    let CanonicalEntry::Transaction(transaction) = &record.entry else {
        return Vec::new();
    };
    let mut acknowledgements = Vec::<String>::new();
    for id in &transaction.mutation_ids {
        let known = |list: &[String]| list.iter().any(|known| known == id.as_str());
        if acknowledgeable.contains(id) && !known(acknowledged) && !known(&acknowledgements) {
            acknowledgements.push(id.to_string());
        }
    }
    acknowledgements
}

/// What one acknowledgement adds to `BatchEnd`: its key, length and bytes.
fn acknowledgement_len(id: &str) -> usize {
    1 + varint_len(id.len() as u64) + id.len()
}

/// A batch's frames as they are pushed, and the digest its end carries.
#[derive(Default)]
struct BatchFrames {
    chunks: Vec<Bytes>,
    /// Frames of this batch alone, not yet cut into a chunk.
    own: Vec<u8>,
    len: usize,
    digest: BatchFramesDigest,
}

impl BatchFrames {
    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, frame: SealedChangeFrame) {
        self.len += frame.bytes.len();
        self.digest.push(&frame.body_sha256);
        if self.own.is_empty() {
            self.own = frame.bytes;
        } else {
            self.own.extend_from_slice(&frame.bytes);
        }
    }

    /// Adds a transaction's frames, as the shared buffers themselves when
    /// `shared`, so every batch of the scope writes the same bytes.
    fn push_transaction(&mut self, transaction: &EncodedTransaction, shared: bool) {
        for frame in &transaction.frames {
            self.len += frame.bytes.len();
            self.digest.push(&frame.body_sha256);
            if shared {
                self.cut();
                self.chunks.push(frame.bytes.clone());
            } else {
                self.own.extend_from_slice(&frame.bytes);
            }
        }
    }

    fn cut(&mut self) {
        if !self.own.is_empty() {
            self.chunks.push(Bytes::from(std::mem::take(&mut self.own)));
        }
    }

    fn end(mut self, mut end: BatchEnd) -> Result<(Vec<Bytes>, usize), ChangeBatchError> {
        end.frame_count = self.digest.frames();
        end.frames_sha256 = std::mem::take(&mut self.digest).finish().to_vec();
        let end = sealed_frame(
            frame(change_stream_frame::Body::BatchEnd(end)),
            MAX_FRAME_BYTES,
        )?;
        self.len += end.bytes.len();
        self.own.extend_from_slice(&end.bytes);
        self.cut();
        Ok((self.chunks, self.len))
    }
}

fn wire_transaction(
    transaction: &CanonicalTransaction,
    transaction_cursor: OpaqueCursor,
) -> TransactionFragment {
    TransactionFragment {
        transaction_id: transaction.transaction_id.as_bytes().to_vec(),
        transaction_cursor: Some(cursor(&transaction_cursor)),
        fragment_index: 0,
        first_fragment: true,
        final_fragment: true,
        postgres_xid: u64::from(transaction.key.postgres_xid),
        begin_lsn: transaction.begin_lsn.clone(),
        commit_lsn: transaction.commit_lsn.clone(),
        commit_time_unix_micros: transaction.commit_time_unix_micros,
        operations: Vec::new(),
    }
}

fn wire_operation(operation: CanonicalOperation) -> Result<Operation, ChangeBatchError> {
    let (kind, payload) = match operation.kind {
        CanonicalOperationKind::InsertFullRow { columns } => (
            change::OperationKind::InsertFullRow,
            operation::Payload::Insert(FullRow {
                columns: wire_columns(columns)?,
            }),
        ),
        CanonicalOperationKind::UpdatePatch { changed_columns } => (
            change::OperationKind::UpdatePatch,
            operation::Payload::Update(RowPatch {
                changed_columns: wire_columns(changed_columns)?,
            }),
        ),
        CanonicalOperationKind::DeleteByKey => (
            change::OperationKind::DeleteByKey,
            operation::Payload::Delete(change::Delete {}),
        ),
        CanonicalOperationKind::TruncateAuthorizedTable { reason } => (
            change::OperationKind::TruncateAuthorizedTable,
            operation::Payload::Truncate(Truncate { reason }),
        ),
    };
    Ok(Operation {
        table_id: operation.table_id.into(),
        kind: kind as i32,
        primary_key: operation.primary_key,
        payload: Some(payload),
    })
}

/// Cuts one transaction into as few fragments as the frame size allows, each
/// a sealed frame carrying nothing of the batch it is sent in.
fn transaction_frames(
    transaction: TransactionFragment,
    operations: Vec<Operation>,
) -> Result<Vec<SharedFrame>, ChangeBatchError> {
    let operation_budget = fragment_operation_budget(&transaction)?;
    let mut encoded = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0_usize;
    for operation in operations {
        let operation_bytes = embedded_message_len(operation.encoded_len());
        if operation_bytes > operation_budget {
            return Err(ChangeBatchError::OperationFrameTooLarge);
        }
        if !current.is_empty() && current_bytes.saturating_add(operation_bytes) > operation_budget {
            encoded.push(encode_transaction_fragment(
                encoded.len(),
                false,
                &transaction,
                std::mem::take(&mut current),
            )?);
            current_bytes = 0;
        }
        current.push(operation);
        current_bytes = current_bytes
            .checked_add(operation_bytes)
            .ok_or(ChangeBatchError::CountOverflow)?;
    }
    if current.is_empty() {
        return Err(ChangeBatchError::CountOverflow);
    }
    encoded.push(encode_transaction_fragment(
        encoded.len(),
        true,
        &transaction,
        current,
    )?);
    Ok(encoded)
}

fn encode_transaction_fragment(
    index: usize,
    final_fragment: bool,
    transaction: &TransactionFragment,
    operations: Vec<Operation>,
) -> Result<SharedFrame, ChangeBatchError> {
    let sealed = sealed_frame(
        frame(change_stream_frame::Body::TransactionFragment(
            TransactionFragment {
                operations,
                fragment_index: u32::try_from(index)
                    .map_err(|_| ChangeBatchError::CountOverflow)?,
                first_fragment: index == 0,
                final_fragment,
                ..transaction.clone()
            },
        )),
        MAX_FRAME_BYTES,
    )?;
    Ok(SharedFrame {
        bytes: Bytes::from(sealed.bytes),
        body_sha256: sealed.body_sha256,
    })
}

fn fragment_operation_budget(transaction: &TransactionFragment) -> Result<usize, ChangeBatchError> {
    // Sized, not sealed: a digest is always 32 bytes, so a placeholder of
    // that width gives the sealed frame's length without hashing it.
    let baseline_payload = ChangeStreamFrame {
        body_sha256: vec![0; 32],
        ..frame(change_stream_frame::Body::TransactionFragment(
            TransactionFragment {
                operations: Vec::new(),
                fragment_index: u32::MAX,
                first_fragment: true,
                final_fragment: true,
                ..transaction.clone()
            },
        ))
    }
    .encoded_len();
    if baseline_payload > MAX_FRAME_BYTES {
        return Err(FrameEncodeError::PayloadTooLarge {
            declared: baseline_payload,
            maximum: MAX_FRAME_BYTES,
        }
        .into());
    }
    MAX_FRAME_BYTES
        .checked_sub(baseline_payload)
        .and_then(|available| available.checked_sub(NESTED_LENGTH_GROWTH_RESERVE))
        .ok_or(ChangeBatchError::OperationFrameTooLarge)
}

fn embedded_message_len(message_len: usize) -> usize {
    // TransactionFragment.operations is field 10, so its length-delimited key
    // occupies one byte. The nested-message length itself is a protobuf varint.
    1 + varint_len(message_len as u64) + message_len
}

fn varint_len(mut value: u64) -> usize {
    let mut bytes = 1;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

fn wire_columns(columns: Vec<ColumnValue>) -> Result<Vec<WireColumnValue>, ChangeBatchError> {
    columns
        .into_iter()
        .map(|column| {
            Ok(WireColumnValue {
                column_id: column.column_id.get(),
                value: Some(WireCellValue {
                    kind: Some(match column.value {
                        CellValue::Null => {
                            change::cell_value::Kind::NullValue(change::NullValue::NullValue as i32)
                        }
                        CellValue::Int64(value) => change::cell_value::Kind::Int64Value(value),
                        CellValue::Uint64(value) => change::cell_value::Kind::Uint64Value(value),
                        CellValue::Float64(value) if value.is_finite() => {
                            change::cell_value::Kind::Float64Value(value)
                        }
                        CellValue::Float64(_) => return Err(ChangeBatchError::NonFiniteFloat),
                        CellValue::Text(value) => change::cell_value::Kind::TextValue(value),
                        CellValue::Blob(value) => change::cell_value::Kind::BlobValue(value),
                        CellValue::Bool(value) => change::cell_value::Kind::BoolValue(value),
                        CellValue::DecimalText(value) => {
                            change::cell_value::Kind::DecimalText(value)
                        }
                        CellValue::TimestampUnixMicros(value) => {
                            change::cell_value::Kind::TimestampUnixMicros(value)
                        }
                        CellValue::Uuid16(value) => {
                            change::cell_value::Kind::Uuid16(value.to_vec())
                        }
                        CellValue::JsonText(value) => change::cell_value::Kind::JsonText(value),
                        CellValue::DateIso8601(value) => {
                            change::cell_value::Kind::DateIso8601(value)
                        }
                        CellValue::TimeIso8601(value) => {
                            change::cell_value::Kind::TimeIso8601(value)
                        }
                        CellValue::IntervalIso8601(value) => {
                            change::cell_value::Kind::IntervalIso8601(value)
                        }
                    }),
                }),
            })
        })
        .collect()
}

fn cursor(cursor: &OpaqueCursor) -> Cursor {
    Cursor {
        token: cursor.as_str().as_bytes().to_vec(),
    }
}

fn required_wire_cursor(cursor: Option<Cursor>) -> Result<OpaqueCursor, ChangeBatchError> {
    cursor
        .ok_or(ChangeBatchError::InvalidWireCursor)
        .and_then(wire_cursor)
}

fn wire_cursor(cursor: Cursor) -> Result<OpaqueCursor, ChangeBatchError> {
    let token = String::from_utf8(cursor.token).map_err(|_| ChangeBatchError::InvalidWireCursor)?;
    OpaqueCursor::parse(token).map_err(|_| ChangeBatchError::InvalidWireCursor)
}

fn frame(body: change_stream_frame::Body) -> ChangeStreamFrame {
    ChangeStreamFrame {
        protocol_version: PROTOCOL_VERSION,
        body: Some(body),
        body_sha256: Vec::new(),
    }
}

fn sealed_frame(
    frame: ChangeStreamFrame,
    max_payload: usize,
) -> Result<SealedChangeFrame, ChangeBatchError> {
    encode_sealed_change_frame(frame, max_payload).map_err(|error| match error {
        SealedChangeFrameError::Integrity(error) => error.into(),
        SealedChangeFrameError::Frame(error) => error.into(),
    })
}

#[derive(Debug, Error)]
pub enum ChangeBatchError {
    #[error("change source does not match the authorized canonical log")]
    SourceMismatch,
    #[error("change source epoch does not match the authorized canonical log")]
    SourceEpochMismatch,
    #[error("change authorization does not match the activated schema registry")]
    SchemaGenerationMismatch,
    #[error("unsupported change protocol version")]
    UnsupportedProtocol,
    #[error("change authorization token is missing or exceeds its limit")]
    InvalidAuthorizationToken,
    #[error("change request does not match its authenticated bootstrap plan")]
    InvalidPlanBinding,
    #[error("mutation acknowledgement proof does not match the source, scope, device, or request")]
    InvalidMutationAcknowledgementProof,
    #[error("change client instance ID is invalid")]
    InvalidClientInstance,
    #[error("change request cursor is missing or not an opaque UTF-8 token")]
    InvalidWireCursor,
    #[error("change authorization has expired")]
    AuthorizationExpired,
    #[error("revocation barrier cursor is outside the durable canonical range")]
    RevocationBarrierUnplaceable,
    #[error("change request did not negotiate the revocation barrier body")]
    RevocationBarrierNotNegotiated,
    #[error("change server time is before the Unix epoch")]
    InvalidServerTime,
    #[error("change batch limits are invalid")]
    InvalidLimits,
    #[error("change fence is outside the requested durable range")]
    FenceOutsideDurableRange,
    #[error("change cursor is before retention floor {minimum}")]
    BeforeRetention { minimum: u64 },
    #[error("canonical log could not prove the requested contiguous range")]
    CanonicalRangeIncomplete,
    #[error("change table set is empty or duplicated")]
    InvalidTableSet,
    #[error("change request includes unauthorized table {0}")]
    UnauthorizedTable(TableId),
    #[error("per-table snapshot cursor is outside the requested range")]
    InvalidSnapshotCursor,
    #[error("one complete transaction cannot fit the requested batch limit")]
    BatchLimitTooSmall,
    #[error("one canonical operation cannot fit the maximum change frame")]
    OperationFrameTooLarge,
    #[error("the requested batch requires transaction fragmentation")]
    FragmentationRequired,
    #[error("one canonical transaction exceeds the client operation limit")]
    TransactionOperationLimitExceeded,
    #[error("shared change encoding belongs to another source epoch, scope or table set")]
    SharedEncodingMismatch,
    #[error("this process holds as many open change streams as it may")]
    OpenStreamLimit,
    #[error("change count overflow")]
    CountOverflow,
    #[error("canonical float is non-finite")]
    NonFiniteFloat,
    /// A row moved into or out of the plan's partition after its base was
    /// cut. The plan cannot continue; the client takes a fresh base.
    #[error("a row moved between partitions after the plan's base")]
    PartitionMoved,
    #[error(transparent)]
    Authorization(CanonicalAuthorizationError),
    #[error(transparent)]
    Cursor(#[from] CursorError),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error(transparent)]
    Integrity(#[from] sync_protocol::ChangeFrameIntegrityError),
    #[error(transparent)]
    Frame(#[from] FrameEncodeError),
}

impl From<CanonicalAuthorizationError> for ChangeBatchError {
    fn from(error: CanonicalAuthorizationError) -> Self {
        match error {
            CanonicalAuthorizationError::PartitionMoved => Self::PartitionMoved,
            error => Self::Authorization(error),
        }
    }
}

impl ChangeBatchError {
    /// Only uncertain storage availability is transport-retryable. Canonical
    /// corruption, policy violations, and impossible internal states require
    /// operator action instead of an automatic retry loop.
    pub fn is_retryable_source_unavailable(&self) -> bool {
        matches!(
            self,
            Self::Log(LogError::Io(_) | LogError::PostgresUnavailable)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
    use canonical_log::{
        CanonicalBarrier, CanonicalOperation, CanonicalPage, FileCanonicalLog, LogBounds,
        TransactionKey,
    };
    use mutation_api::{install_metadata, resolve_acknowledgements, MutationReceiptBinding};
    use postgres::Client;
    use source_control::{
        ActivatedSource, ColumnInspection, EnrollmentRequirements, EnrollmentSnapshot,
        PgoutputLaunchProfile, PublicationSettings, QualifiedTable, RegisteredTable,
        SchemaRegistry, SourceIdentity, SourceSettings, TableInspection,
    };
    use sync_protocol::{
        change::{
            cell_value, change_stream_frame, CellValue as WireCellValue,
            ColumnValue as WireColumnValue, PrimaryKey,
        },
        change_frame_body_sha256, encode_frame_with_limit, encode_primary_key,
        verify_change_frame_body_sha256, FrameDecoder,
    };
    use sync_types::ColumnId;
    use tempfile::TempDir;

    fn source_id() -> SourceId {
        SourceId::new("source-main").unwrap()
    }

    fn epoch() -> SourceEpoch {
        SourceEpoch::new("epoch-1").unwrap()
    }

    fn table(name: &str) -> TableId {
        TableId::new(format!("public.{name}")).unwrap()
    }

    fn plan_id() -> PlanId {
        PlanId::new("plan-1").unwrap()
    }

    fn test_activated_scope(codec: &CursorCodec, scope: AuthorizedScope) -> ActivatedChangeScope {
        let tables = scope.permitted_tables().cloned().collect();
        ActivatedChangeScope {
            source_id: source_id(),
            source_epoch: epoch(),
            cursor_codec: codec.clone(),
            publication_generation: 2,
            registry_binding: ChangeRegistryBinding::Fixture {
                schema_generation: 3,
                tables,
            },
            scope,
        }
    }

    fn registered_table(table_id: TableId) -> RegisteredTable {
        RegisteredTable {
            table_id,
            column_ids: vec![sync_types::ColumnId::new(1)],
            inspection: TableInspection {
                table: QualifiedTable {
                    schema: "public".to_owned(),
                    table: "authorized".to_owned(),
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
                columns: vec![ColumnInspection {
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
                    carried_as: None,
                }],
            },
        }
    }

    fn activated_source_with_table() -> (ActivatedSource, SchemaRegistry, TableId) {
        let identity = SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        };
        let inspection = registered_table(table("placeholder")).inspection;
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
        let registry = SchemaRegistry::initialize(&snapshot, &requirements).unwrap();
        let table_id = registry.tables()[0].table_id.clone();
        let activated_source = ActivatedSource::new(
            &registry,
            epoch(),
            CursorCodec::new([9; 32]).unwrap(),
            PgoutputLaunchProfile::V1,
        );
        (activated_source, registry, table_id)
    }

    fn scope_for_registry(registry: &SchemaRegistry) -> AuthorizedScope {
        let policy = AuthorizationPolicy::compile_for_registry(
            PolicyDefinition {
                generation: 7,
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
        .unwrap();
        policy.scope()
    }

    fn scope_for(table_ids: impl IntoIterator<Item = TableId>) -> AuthorizedScope {
        let tables = table_ids
            .into_iter()
            .map(registered_table)
            .collect::<Vec<_>>();
        let policy = AuthorizationPolicy::compile(
            PolicyDefinition {
                generation: 7,
                tables: tables
                    .iter()
                    .map(|table| TablePermitDefinition {
                        table_id: table.table_id.clone(),
                        permitted_columns: table.column_ids.clone(),
                    })
                    .collect(),
            },
            &tables,
        )
        .unwrap();
        policy.scope()
    }

    /// The tasks table with `server_secret` withheld.
    fn projected_scope() -> AuthorizedScope {
        let registered = RegisteredTable {
            table_id: table("tasks"),
            column_ids: (1..=3).map(ColumnId::new).collect(),
            inspection: TableInspection {
                table: QualifiedTable {
                    schema: "public".to_owned(),
                    table: "tasks".to_owned(),
                },
                relation_oid: 16_385,
                in_publication: true,
                relation_kind: 'r',
                persistence: 'p',
                replica_identity: 'd',
                is_partition: false,
                has_publication_row_filter: false,
                has_publication_column_list: false,
                unlisted_columns: Vec::new(),
                columns: vec![
                    ColumnInspection {
                        attribute_number: 1,
                        name: "tenant_id".to_owned(),
                        type_oid: 2_950,
                        type_modifier: -1,
                        type_schema: "pg_catalog".to_owned(),
                        type_name: "uuid".to_owned(),
                        nullable: false,
                        primary_key_ordinal: Some(1),
                        generated: false,
                        nondeterministic_collation: false,
                        carried_as: None,
                    },
                    ColumnInspection {
                        attribute_number: 2,
                        name: "title".to_owned(),
                        type_oid: 25,
                        type_modifier: -1,
                        type_schema: "pg_catalog".to_owned(),
                        type_name: "text".to_owned(),
                        nullable: false,
                        primary_key_ordinal: None,
                        generated: false,
                        nondeterministic_collation: false,
                        carried_as: None,
                    },
                    ColumnInspection {
                        attribute_number: 3,
                        name: "server_secret".to_owned(),
                        type_oid: 25,
                        type_modifier: -1,
                        type_schema: "pg_catalog".to_owned(),
                        type_name: "text".to_owned(),
                        nullable: false,
                        primary_key_ordinal: None,
                        generated: false,
                        nondeterministic_collation: false,
                        carried_as: None,
                    },
                ],
            },
        };
        AuthorizationPolicy::compile(
            PolicyDefinition {
                generation: 11,
                tables: vec![TablePermitDefinition {
                    table_id: registered.table_id.clone(),
                    permitted_columns: vec![ColumnId::new(1), ColumnId::new(2)],
                }],
            },
            std::slice::from_ref(&registered),
        )
        .unwrap()
        .scope()
    }

    fn column(column_id: u32, value: CellValue) -> ColumnValue {
        ColumnValue {
            column_id: ColumnId::new(column_id),
            value,
        }
    }

    fn routed_transaction(sequence: u64, operation: CanonicalOperation) -> CanonicalTransaction {
        let mut transaction = transaction(sequence, &[], 0);
        transaction.operations = vec![operation];
        transaction
    }

    fn tenant_primary_key(tenant: u8) -> Vec<u8> {
        encode_primary_key(&PrimaryKey {
            codec_version: sync_protocol::PRIMARY_KEY_CODEC_VERSION,
            parts: vec![WireColumnValue {
                column_id: 1,
                value: Some(WireCellValue {
                    kind: Some(cell_value::Kind::Uuid16(vec![tenant; 16])),
                }),
            }],
        })
        .unwrap()
    }

    fn transaction(sequence: u64, tables: &[&str], reason_bytes: usize) -> CanonicalTransaction {
        CanonicalTransaction {
            key: TransactionKey {
                source_epoch: epoch(),
                end_lsn: format!("0/{sequence:X}"),
                postgres_xid: u32::try_from(sequence).unwrap(),
            },
            transaction_id: format!("tx-{sequence}"),
            begin_lsn: format!("0/{sequence:X}"),
            commit_lsn: format!("0/{:X}", sequence + 1),
            commit_time_unix_micros: sequence,
            operations: tables
                .iter()
                .map(|name| CanonicalOperation {
                    table_id: table(name),
                    primary_key: Vec::new(),
                    kind: CanonicalOperationKind::TruncateAuthorizedTable {
                        reason: "x".repeat(reason_bytes),
                    },
                })
                .collect(),
            mutation_ids: Vec::new(),
        }
    }

    fn barrier(sequence: u64) -> CanonicalBarrier {
        CanonicalBarrier {
            key: TransactionKey {
                source_epoch: epoch(),
                end_lsn: format!("0/{sequence:X}"),
                postgres_xid: u32::try_from(sequence).unwrap(),
            },
            build_id: format!("build-{sequence}"),
            nonce: format!("nonce-{sequence}"),
            message_lsn: format!("0/{sequence:X}"),
            commit_lsn: format!("0/{:X}", sequence + 1),
            commit_time_unix_micros: sequence,
        }
    }

    fn authorization(codec: &CursorCodec, fence: u64) -> ChangeAuthorization {
        authorization_with(
            codec,
            cursor_at(codec, 0),
            cursor_at(codec, fence),
            BTreeMap::from([
                (table("tasks"), Some(cursor_at(codec, 1))),
                (table("users"), Some(cursor_at(codec, 0))),
            ]),
        )
    }

    fn authorization_with(
        codec: &CursorCodec,
        from: OpaqueCursor,
        fence: OpaqueCursor,
        table_starts: BTreeMap<TableId, Option<OpaqueCursor>>,
    ) -> ChangeAuthorization {
        let scope = scope_for(table_starts.keys().cloned());
        ChangeAuthorization::new(ScopedChangePlan {
            plan_id: plan_id(),
            activated_scope: test_activated_scope(codec, scope),
            expires_at_unix_micros: 10_000,
            from_cursor_exclusive: from,
            through_cursor_inclusive: fence,
            table_starts,
            acknowledgeable_mutation_ids: ResolvedMutationAcknowledgements::default(),
            revocation_barrier: None,
        })
        .unwrap()
    }

    fn cursor_at(codec: &CursorCodec, sequence: u64) -> OpaqueCursor {
        codec.encode(&CursorPosition {
            source_epoch: epoch(),
            sequence,
        })
    }

    fn request(codec: &CursorCodec, fence: u64) -> ChangeBatchRequest {
        ChangeBatchRequest {
            plan_id: plan_id(),
            protocol_version: 1,
            from_cursor_exclusive: cursor_at(codec, 0),
            through_cursor_inclusive: Some(cursor_at(codec, fence)),
            tables: vec![
                TableSubscription {
                    table_id: table("tasks"),
                    snapshot_cursor: Some(cursor_at(codec, 1)),
                },
                TableSubscription {
                    table_id: table("users"),
                    snapshot_cursor: Some(cursor_at(codec, 0)),
                },
            ],
            max_batch_bytes: 1024 * 1024,
            max_transactions: 100,
            long_poll_timeout_ms: 0,
            accept_transaction_fragmentation: true,
            accept_revocation_barrier: true,
            stream: false,
            device_id: "device-1".to_owned(),
            pending_mutation_ids: BTreeSet::new(),
        }
    }

    fn live_request(codec: &CursorCodec, from: u64) -> ChangeBatchRequest {
        ChangeBatchRequest {
            plan_id: plan_id(),
            protocol_version: 1,
            from_cursor_exclusive: cursor_at(codec, from),
            through_cursor_inclusive: None,
            tables: vec![
                TableSubscription {
                    table_id: table("tasks"),
                    snapshot_cursor: None,
                },
                TableSubscription {
                    table_id: table("users"),
                    snapshot_cursor: None,
                },
            ],
            max_batch_bytes: 1024 * 1024,
            max_transactions: 100,
            long_poll_timeout_ms: MIN_LONG_POLL_TIMEOUT_MS,
            accept_transaction_fragmentation: true,
            accept_revocation_barrier: true,
            stream: false,
            device_id: "device-1".to_owned(),
            pending_mutation_ids: BTreeSet::new(),
        }
    }

    fn wire_request(codec: &CursorCodec) -> change::ChangeRequest {
        change::ChangeRequest {
            protocol_version: PROTOCOL_VERSION,
            from_cursor_exclusive: Some(cursor(&cursor_at(codec, 0))),
            through_cursor_inclusive: Some(cursor(&cursor_at(codec, 1))),
            tables: vec![change::TableSubscription {
                table_id: "public.tasks".to_owned(),
                shard_snapshot_cursor: Some(cursor(&cursor_at(codec, 0))),
            }],
            authorization_scope_token: b"scope-token-secret".to_vec(),
            max_batch_bytes: 1024 * 1024,
            max_transactions: 100,
            long_poll_timeout_ms: 0,
            client_instance_id: "client-1".to_owned(),
            device_id: "device-1".to_owned(),
            pending_mutation_ids: vec!["mutation-1".to_owned()],
            accept_transaction_fragmentation: true,
            accept_revocation_barrier: true,
            plan_id: plan_id().to_string(),
            stream: false,
        }
    }

    fn frames(bytes: &[u8]) -> Vec<ChangeStreamFrame> {
        let mut decoder = FrameDecoder::new(bytes);
        let mut frames = Vec::new();
        while let Some(frame) = decoder.read_message().unwrap() {
            verify_change_frame_body_sha256(&frame).unwrap();
            frames.push(frame);
        }
        frames
    }

    /// The fixed Ed25519 key pair these tests mint barriers with, standing in
    /// for the application backend; the server under test holds only the
    /// public half.
    const BARRIER_SEED: [u8; 32] = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31,
    ];
    const BARRIER_KEY_ID: &str = "revocation-key-1";

    fn barrier_key_pair() -> ring::signature::Ed25519KeyPair {
        ring::signature::Ed25519KeyPair::from_seed_unchecked(&BARRIER_SEED).unwrap()
    }

    fn barrier_verification_key() -> RevocationBarrierVerificationKey {
        use ring::signature::KeyPair;
        RevocationBarrierVerificationKey::new(
            BARRIER_KEY_ID,
            barrier_key_pair().public_key().as_ref().try_into().unwrap(),
        )
    }

    fn minted_barrier(
        codec: &CursorCodec,
        scope: &AuthorizedScope,
        sequence: u64,
    ) -> MintedRevocationBarrier {
        let mut barrier = RevocationBarrier {
            barrier_cursor: Some(cursor(&cursor_at(codec, sequence))),
            revoked_authorization_generation: scope.generation(),
            effective_authorization_generation: scope.generation() + 1,
            revoked_authorization_scope_hash: scope.scope_hash().as_bytes().to_vec(),
            barrier_id: "barrier-1".to_owned(),
            issued_at_unix_micros: 1_700_000_000_000_000,
            signing_key_id: BARRIER_KEY_ID.to_owned(),
            signature: Vec::new(),
        };
        let preimage = sync_protocol::revocation_barrier_signing_preimage(&barrier).unwrap();
        barrier.signature = barrier_key_pair().sign(&preimage).as_ref().to_vec();
        MintedRevocationBarrier {
            barrier,
            verification_key: barrier_verification_key(),
        }
    }

    fn authorization_with_barrier(
        codec: &CursorCodec,
        from: u64,
        fence: u64,
        barrier_at: u64,
    ) -> Result<ChangeAuthorization, ChangeAuthorizationError> {
        let table_starts = BTreeMap::from([
            (table("tasks"), Some(cursor_at(codec, 1))),
            (table("users"), Some(cursor_at(codec, 0))),
        ]);
        let scope = scope_for(table_starts.keys().cloned());
        let minted = minted_barrier(codec, &scope, barrier_at);
        ChangeAuthorization::new(ScopedChangePlan {
            plan_id: plan_id(),
            activated_scope: test_activated_scope(codec, scope),
            expires_at_unix_micros: 10_000,
            from_cursor_exclusive: cursor_at(codec, from),
            through_cursor_inclusive: cursor_at(codec, fence),
            table_starts,
            acknowledgeable_mutation_ids: ResolvedMutationAcknowledgements::default(),
            revocation_barrier: Some(minted),
        })
    }

    fn barrier_log(directory: &TempDir) -> FileCanonicalLog {
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1, &["tasks"], 1)).unwrap();
        log.append(transaction(2, &["users"], 1)).unwrap();
        log.append(transaction(3, &["tasks"], 1)).unwrap();
        log.append(transaction(4, &["users"], 1)).unwrap();
        log
    }

    #[test]
    fn a_batch_ends_on_the_barrier_and_delivers_nothing_after_it() {
        let directory = TempDir::new().unwrap();
        let log = barrier_log(&directory);
        let codec = CursorCodec::new([7; 32]).unwrap();
        let authorization = authorization_with_barrier(&codec, 0, 4, 3).unwrap();
        let batch = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .build_at(&request(&codec, 4), 9_999)
            .unwrap();

        assert!(batch.revocation_barrier_emitted);
        // The batch stops at the barrier, short of the fence it was asked for.
        assert!(!batch.reached_requested_fence);
        assert_eq!(
            codec
                .decode(&batch.through_cursor_inclusive)
                .unwrap()
                .sequence,
            3
        );
        let frames = frames(&batch.to_bytes());
        // start, the two authorized transactions at or before the barrier, the
        // barrier, and the end. Sequence 4 is not in it.
        let bodies = frames
            .iter()
            .map(|frame| match frame.body.as_ref() {
                Some(change_stream_frame::Body::BatchStart(_)) => "start",
                Some(change_stream_frame::Body::TransactionFragment(_)) => "fragment",
                Some(change_stream_frame::Body::RevocationBarrier(_)) => "barrier",
                Some(change_stream_frame::Body::BatchEnd(_)) => "end",
                _ => "other",
            })
            .collect::<Vec<_>>();
        assert_eq!(bodies, ["start", "fragment", "fragment", "barrier", "end"]);
        assert_eq!(batch.relevant_transactions, 2);

        // The barrier's cursor and the batch end's cursor are byte-identical.
        let Some(change_stream_frame::Body::RevocationBarrier(barrier)) = frames[3].body.as_ref()
        else {
            panic!("the fourth frame is the barrier");
        };
        let Some(change_stream_frame::Body::BatchEnd(end)) = frames[4].body.as_ref() else {
            panic!("the fifth frame is the batch end");
        };
        assert_eq!(
            barrier.barrier_cursor.as_ref().unwrap().token,
            end.through_cursor_inclusive.as_ref().unwrap().token
        );
        assert_eq!(
            barrier.effective_authorization_generation,
            barrier.revoked_authorization_generation + 1
        );
    }

    #[test]
    fn a_request_that_starts_after_the_barrier_is_refused_as_expired() {
        let directory = TempDir::new().unwrap();
        let log = barrier_log(&directory);
        let codec = CursorCodec::new([7; 32]).unwrap();
        for from in [3, 4] {
            let authorization = authorization_with_barrier(&codec, from, 4, 2).unwrap();
            let mut request = request(&codec, 4);
            request.from_cursor_exclusive = cursor_at(&codec, from);
            assert!(
                matches!(
                    ChangeBatchBuilder::new(&log, &authorization)
                        .unwrap()
                        .build_at(&request, 9_999),
                    Err(ChangeBatchError::AuthorizationExpired)
                ),
                "a request from {from} past a barrier at 2 was served"
            );
        }
    }

    /// A caught-up client already stands on the barrier's cursor. It gets a
    /// batch carrying only the barrier, not a refusal, which would retire the
    /// scope without the ordered, authenticated signal.
    #[test]
    fn a_request_standing_on_the_barrier_receives_only_the_barrier() {
        let directory = TempDir::new().unwrap();
        let log = barrier_log(&directory);
        let codec = CursorCodec::new([7; 32]).unwrap();
        for live in [false, true] {
            let authorization = authorization_with_barrier(&codec, 0, 2, 2).unwrap();
            let mut request = if live {
                live_request(&codec, 2)
            } else {
                request(&codec, 2)
            };
            request.from_cursor_exclusive = cursor_at(&codec, 2);
            let batch = ChangeBatchBuilder::new(&log, &authorization)
                .unwrap()
                .build_at(&request, 9_999)
                .unwrap();
            assert!(batch.revocation_barrier_emitted, "live={live}");
            assert_eq!(batch.relevant_transactions, 0);
            assert_eq!(batch.through_cursor_inclusive, cursor_at(&codec, 2));
            let bodies = frames(&batch.to_bytes())
                .iter()
                .map(|frame| match frame.body.as_ref() {
                    Some(change_stream_frame::Body::BatchStart(_)) => "start",
                    Some(change_stream_frame::Body::RevocationBarrier(_)) => "barrier",
                    Some(change_stream_frame::Body::BatchEnd(_)) => "end",
                    _ => "other",
                })
                .collect::<Vec<_>>();
            assert_eq!(bodies, ["start", "barrier", "end"], "live={live}");
        }
    }

    #[test]
    fn a_client_that_did_not_negotiate_the_barrier_is_refused_rather_than_served_past_it() {
        let directory = TempDir::new().unwrap();
        let log = barrier_log(&directory);
        let codec = CursorCodec::new([7; 32]).unwrap();
        let authorization = authorization_with_barrier(&codec, 0, 4, 3).unwrap();
        let mut request = request(&codec, 4);
        request.accept_revocation_barrier = false;

        // A client that did not negotiate the barrier body is never sent one,
        // and is never served past it either.
        assert!(matches!(
            ChangeBatchBuilder::new(&log, &authorization)
                .unwrap()
                .build_at(&request, 9_999),
            Err(ChangeBatchError::RevocationBarrierNotNegotiated)
        ));

        // Everything before the barrier is still delivered to that client: the
        // rows at or before it were authorized by the generation being
        // retired, and only the frame that stands on the barrier is withheld.
        let mut short = request.clone();
        short.max_transactions = 1;
        let batch = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .build_at(&short, 9_999)
            .unwrap();
        assert!(!batch.revocation_barrier_emitted);
        assert_eq!(batch.relevant_transactions, 1);
    }

    /// A deployment fences at the head it reads, and a log that has not yet
    /// recorded a transaction has its head just below its retention floor.
    /// A barrier there must still reach a client standing on it.
    #[test]
    fn a_barrier_on_the_head_of_an_empty_log_is_delivered() {
        let directory = TempDir::new().unwrap();
        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let table_starts = BTreeMap::from([
            (table("tasks"), Some(cursor_at(&codec, 0))),
            (table("users"), Some(cursor_at(&codec, 0))),
        ]);
        let scope = scope_for(table_starts.keys().cloned());
        let authorization = ChangeAuthorization::new(ScopedChangePlan {
            plan_id: plan_id(),
            activated_scope: test_activated_scope(&codec, scope.clone()),
            expires_at_unix_micros: 10_000,
            from_cursor_exclusive: cursor_at(&codec, 0),
            through_cursor_inclusive: cursor_at(&codec, 0),
            table_starts,
            acknowledgeable_mutation_ids: ResolvedMutationAcknowledgements::default(),
            revocation_barrier: Some(minted_barrier(&codec, &scope, 0)),
        })
        .unwrap();
        let batch = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .build_at(&live_request(&codec, 0), 9_999)
            .unwrap();
        assert!(batch.revocation_barrier_emitted);
        assert_eq!(batch.through_cursor_inclusive, cursor_at(&codec, 0));
    }

    #[test]
    fn a_barrier_cursor_the_canonical_log_cannot_place_is_refused() {
        let directory = TempDir::new().unwrap();
        let log = barrier_log(&directory);
        let codec = CursorCodec::new([7; 32]).unwrap();
        // The log holds sequences 1..=4 and is dense between its retention
        // floor and its head, so a barrier at 9 stands on no record. Left
        // unchecked it would truncate every scan at a position the scan can
        // never reach, and the revocation would never be delivered at all.
        let authorization = authorization_with_barrier(&codec, 0, 4, 9).unwrap();
        assert!(matches!(
            ChangeBatchBuilder::new(&log, &authorization)
                .unwrap()
                .build_at(&request(&codec, 4), 9_999),
            Err(ChangeBatchError::RevocationBarrierUnplaceable)
        ));
        // The same refusal on the live path, where the fence is the head
        // itself and nothing else would ever notice the gap.
        let live = live_request(&codec, 4);
        assert!(matches!(
            ChangeBatchBuilder::new(&log, &authorization)
                .unwrap()
                .build_at(&live, 9_999),
            Err(ChangeBatchError::RevocationBarrierUnplaceable)
        ));
    }

    #[test]
    fn a_batch_that_stops_before_the_barrier_carries_no_barrier() {
        let directory = TempDir::new().unwrap();
        let log = barrier_log(&directory);
        let codec = CursorCodec::new([7; 32]).unwrap();
        let authorization = authorization_with_barrier(&codec, 0, 4, 3).unwrap();
        let mut request = request(&codec, 4);
        request.max_transactions = 1;
        let batch = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .build_at(&request, 9_999)
            .unwrap();
        assert!(!batch.revocation_barrier_emitted);
        assert!(frames(&batch.to_bytes()).iter().all(|frame| !matches!(
            frame.body.as_ref(),
            Some(change_stream_frame::Body::RevocationBarrier(_))
        )));
        assert!(
            codec
                .decode(&batch.through_cursor_inclusive)
                .unwrap()
                .sequence
                < 3
        );
    }

    /// A log that records each read a build makes of it: what it read and
    /// the bound it read through.
    struct RecordedReads<'a> {
        log: &'a FileCanonicalLog,
        reads: std::sync::Mutex<Vec<(&'static str, Option<u64>)>>,
    }

    impl RecordedReads<'_> {
        fn taken(&self) -> Vec<(&'static str, Option<u64>)> {
            std::mem::take(&mut *self.reads.lock().unwrap())
        }
    }

    impl CanonicalLogRead for RecordedReads<'_> {
        fn source_id(&self) -> &SourceId {
            CanonicalLogRead::source_id(self.log)
        }

        fn source_epoch(&self) -> &SourceEpoch {
            CanonicalLogRead::source_epoch(self.log)
        }

        fn head(&self) -> Result<CursorPosition, LogError> {
            self.reads.lock().unwrap().push(("head", None));
            CanonicalLogRead::head(self.log)
        }

        fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
            self.reads.lock().unwrap().push(("floor", None));
            CanonicalLogRead::minimum_retained(self.log)
        }

        fn bounds(&self) -> Result<LogBounds, LogError> {
            self.reads.lock().unwrap().push(("bounds", None));
            CanonicalLogRead::bounds(self.log)
        }

        fn read_page_after_bounded(
            &self,
            from_sequence_exclusive: u64,
            through_sequence_inclusive: Option<u64>,
            max_records: usize,
            max_bytes: u64,
        ) -> Result<CanonicalPage, LogError> {
            self.reads
                .lock()
                .unwrap()
                .push(("page", through_sequence_inclusive));
            CanonicalLogRead::read_page_after_bounded(
                self.log,
                from_sequence_exclusive,
                through_sequence_inclusive,
                max_records,
                max_bytes,
            )
        }

        fn read_bounds_and_page_after_bounded(
            &self,
            from_sequence_exclusive: u64,
            through_sequence_inclusive: Option<u64>,
            max_records: usize,
            max_bytes: u64,
        ) -> Result<(LogBounds, Result<CanonicalPage, LogError>), LogError> {
            self.reads
                .lock()
                .unwrap()
                .push(("bounds and page", through_sequence_inclusive));
            CanonicalLogRead::read_bounds_and_page_after_bounded(
                self.log,
                from_sequence_exclusive,
                through_sequence_inclusive,
                max_records,
                max_bytes,
            )
        }
    }

    /// A log trimmed past a build between its first page and its next.
    struct TrimmedAfterFirstPage<'a>(&'a FileCanonicalLog);

    impl CanonicalLogRead for TrimmedAfterFirstPage<'_> {
        fn source_id(&self) -> &SourceId {
            CanonicalLogRead::source_id(self.0)
        }

        fn source_epoch(&self) -> &SourceEpoch {
            CanonicalLogRead::source_epoch(self.0)
        }

        fn head(&self) -> Result<CursorPosition, LogError> {
            CanonicalLogRead::head(self.0)
        }

        fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
            CanonicalLogRead::minimum_retained(self.0)
        }

        fn read_page_after_bounded(
            &self,
            from_sequence_exclusive: u64,
            _through_sequence_inclusive: Option<u64>,
            _max_records: usize,
            _max_bytes: u64,
        ) -> Result<CanonicalPage, LogError> {
            Err(LogError::BeforeRetention {
                requested: from_sequence_exclusive,
                minimum: from_sequence_exclusive + 2,
            })
        }

        fn read_bounds_and_page_after_bounded(
            &self,
            from_sequence_exclusive: u64,
            through_sequence_inclusive: Option<u64>,
            _max_records: usize,
            max_bytes: u64,
        ) -> Result<(LogBounds, Result<CanonicalPage, LogError>), LogError> {
            CanonicalLogRead::read_bounds_and_page_after_bounded(
                self.0,
                from_sequence_exclusive,
                through_sequence_inclusive,
                1,
                max_bytes,
            )
        }
    }

    #[test]
    fn a_log_trimmed_past_a_build_between_its_pages_answers_before_retention() {
        let directory = TempDir::new().unwrap();
        let log = barrier_log(&directory);
        let codec = CursorCodec::new([7; 32]).unwrap();
        assert!(matches!(
            ChangeBatchBuilder::new(&TrimmedAfterFirstPage(&log), &authorization(&codec, 4))
                .unwrap()
                .build_at(&request(&codec, 4), 9_999),
            Err(ChangeBatchError::BeforeRetention { minimum: 3 })
        ));
    }

    /// A build reads the head, the floor and its first page together, through
    /// its fence, its barrier, or the head that read stands on, so a build
    /// whose scan fits one page reads the log once and nothing past either
    /// bound reaches it.
    #[test]
    fn a_build_reads_its_bounds_with_its_first_page_and_nothing_past_its_fence_or_barrier() {
        let directory = TempDir::new().unwrap();
        let log = barrier_log(&directory);
        let reads = RecordedReads {
            log: &log,
            reads: std::sync::Mutex::new(Vec::new()),
        };
        let codec = CursorCodec::new([7; 32]).unwrap();

        // A fence short of the head: the users transaction at 2, and neither
        // record after it.
        let fenced = authorization(&codec, 2);
        let batch = ChangeBatchBuilder::new(&reads, &fenced)
            .unwrap()
            .build_at(&request(&codec, 2), 9_999)
            .unwrap();
        assert_eq!(batch.through_cursor_inclusive, cursor_at(&codec, 2));
        assert!(batch.reached_requested_fence);
        assert_eq!(batch.relevant_transactions, 1);
        assert_eq!(reads.taken(), [("bounds and page", Some(2))]);

        // A fence and a barrier below it: the barrier bounds the read.
        let barred = authorization_with_barrier(&codec, 0, 4, 3).unwrap();
        let batch = ChangeBatchBuilder::new(&reads, &barred)
            .unwrap()
            .build_at(&request(&codec, 4), 9_999)
            .unwrap();
        assert!(batch.revocation_barrier_emitted);
        assert_eq!(batch.through_cursor_inclusive, cursor_at(&codec, 3));
        assert_eq!(reads.taken(), [("bounds and page", Some(3))]);

        // A live build, which starts at its plan's fence, has no fence of its
        // own, so its barrier bounds it.
        let live_barred = authorization_with_barrier(&codec, 0, 1, 3).unwrap();
        let batch = ChangeBatchBuilder::new(&reads, &live_barred)
            .unwrap()
            .build_at(&live_request(&codec, 1), 9_999)
            .unwrap();
        assert!(batch.revocation_barrier_emitted);
        assert_eq!(batch.through_cursor_inclusive, cursor_at(&codec, 3));
        assert_eq!(reads.taken(), [("bounds and page", Some(3))]);

        // With neither, the read goes to the head it reads.
        let open = authorization(&codec, 1);
        let batch = ChangeBatchBuilder::new(&reads, &open)
            .unwrap()
            .build_at(&live_request(&codec, 1), 9_999)
            .unwrap();
        assert_eq!(batch.through_cursor_inclusive, cursor_at(&codec, 4));
        assert_eq!(reads.taken(), [("bounds and page", None)]);

        // A request the checks refuse is refused for itself, after the one
        // read, and reads nothing more.
        let mut past_head = request(&codec, 4);
        past_head.through_cursor_inclusive = Some(cursor_at(&codec, 9));
        let past_head_authorization = authorization_with(
            &codec,
            past_head.from_cursor_exclusive.clone(),
            past_head.through_cursor_inclusive.clone().unwrap(),
            open.table_starts.clone(),
        );
        assert!(matches!(
            ChangeBatchBuilder::new(&reads, &past_head_authorization)
                .unwrap()
                .build_at(&past_head, 9_999),
            Err(ChangeBatchError::FenceOutsideDurableRange)
        ));
        assert_eq!(reads.taken(), [("bounds and page", Some(9))]);
    }

    #[test]
    fn a_forged_misaddressed_or_non_advancing_barrier_never_reaches_a_batch() {
        let codec = CursorCodec::new([7; 32]).unwrap();
        let table_starts = BTreeMap::from([
            (table("tasks"), Some(cursor_at(&codec, 1))),
            (table("users"), Some(cursor_at(&codec, 0))),
        ]);
        let scope = scope_for(table_starts.keys().cloned());

        let build = |mutate: &dyn Fn(&mut MintedRevocationBarrier)| {
            let mut minted = minted_barrier(&codec, &scope, 2);
            mutate(&mut minted);
            ChangeAuthorization::new(ScopedChangePlan {
                plan_id: plan_id(),
                activated_scope: test_activated_scope(&codec, scope.clone()),
                expires_at_unix_micros: 10_000,
                from_cursor_exclusive: cursor_at(&codec, 0),
                through_cursor_inclusive: cursor_at(&codec, 4),
                table_starts: table_starts.clone(),
                acknowledgeable_mutation_ids: ResolvedMutationAcknowledgements::default(),
                revocation_barrier: Some(minted),
            })
            .err()
        };

        assert!(matches!(
            build(&|minted| minted.barrier.signature[0] ^= 0xff),
            Some(ChangeAuthorizationError::RevocationBarrierUnauthenticated)
        ));
        assert!(matches!(
            build(&|minted| {
                minted.verification_key =
                    RevocationBarrierVerificationKey::new("another-key", [0; 32]);
            }),
            Some(ChangeAuthorizationError::RevocationBarrierUnauthenticated)
        ));
        // Signed, but for another generation or scope, an
        // effective generation that does not advance, or a cursor under
        // another secret. Each has to be re-signed, or the signature check
        // would catch it first and prove nothing about the binding.
        let resign = |minted: &mut MintedRevocationBarrier| {
            let preimage =
                sync_protocol::revocation_barrier_signing_preimage(&minted.barrier).unwrap();
            minted.barrier.signature = barrier_key_pair().sign(&preimage).as_ref().to_vec();
        };
        assert!(matches!(
            build(&|minted| {
                minted.barrier.revoked_authorization_generation += 1;
                minted.barrier.effective_authorization_generation += 1;
                resign(minted);
            }),
            Some(ChangeAuthorizationError::RevocationBarrierScopeMismatch)
        ));
        assert!(matches!(
            build(&|minted| {
                minted.barrier.revoked_authorization_scope_hash = vec![9; 32];
                resign(minted);
            }),
            Some(ChangeAuthorizationError::RevocationBarrierScopeMismatch)
        ));
        for behind in [0, 1] {
            assert!(matches!(
                build(&|minted| {
                    minted.barrier.effective_authorization_generation =
                        minted.barrier.revoked_authorization_generation - behind;
                    resign(minted);
                }),
                Some(ChangeAuthorizationError::RevocationBarrierScopeMismatch)
            ));
        }
        let other_secret = CursorCodec::new([9; 32]).unwrap();
        assert!(matches!(
            build(&|minted| {
                minted.barrier.barrier_cursor =
                    Some(cursor(&other_secret.encode(&CursorPosition {
                        source_epoch: epoch(),
                        sequence: 2,
                    })));
                resign(minted);
            }),
            Some(ChangeAuthorizationError::RevocationBarrierCursor)
        ));
    }

    #[test]
    fn activated_scope_derives_registry_identity_and_rejects_mismatches_before_scanning() {
        let (activated_source, registry, registered_table_id) = activated_source_with_table();
        let scope = scope_for_registry(&registry);
        let binding = ActivatedChangeScope::new(source_id(), &activated_source, scope).unwrap();
        assert_eq!(binding.source_epoch, epoch());
        assert_eq!(binding.publication_generation, PUBLICATION_GENERATION_V1);
        assert_eq!(binding.registry_binding.schema_generation(), 1);
        assert!(binding
            .registry_binding
            .contains_table(&registered_table_id));

        assert!(matches!(
            ActivatedChangeScope::new(
                source_id(),
                &activated_source,
                scope_for([registered_table_id.clone()])
            ),
            Err(ChangeAuthorizationError::ScopeRegistryBindingMismatch)
        ));

        let mut inconsistent_binding = binding;
        inconsistent_binding.registry_binding = ChangeRegistryBinding::Fixture {
            schema_generation: 1,
            tables: BTreeSet::new(),
        };
        let cursor = activated_source.cursor_codec().encode(&CursorPosition {
            source_epoch: epoch(),
            sequence: 0,
        });
        assert!(matches!(
            ChangeAuthorization::new(ScopedChangePlan {
                plan_id: plan_id(),
                activated_scope: inconsistent_binding,
                expires_at_unix_micros: 10_000,
                from_cursor_exclusive: cursor.clone(),
                through_cursor_inclusive: cursor,
                table_starts: BTreeMap::from([(registered_table_id.clone(), None)]),
                acknowledgeable_mutation_ids: Default::default(),
                revocation_barrier: None,
            }),
            Err(ChangeAuthorizationError::PlanRegistryMismatch(table_id))
                if table_id == registered_table_id
        ));
    }

    #[test]
    fn filters_each_table_after_its_own_snapshot_cursor_and_covers_irrelevant_ranges() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1, &["tasks"], 1)).unwrap();
        log.append(transaction(2, &["users"], 1)).unwrap();
        log.append(transaction(3, &["tasks", "users"], 1)).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let authorization = authorization(&codec, 3);
        let batch = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .build_at(&request(&codec, 3), 9_999)
            .unwrap();

        assert!(batch.reached_requested_fence);
        assert_eq!(batch.relevant_transactions, 2);
        assert_eq!(batch.relevant_operations, 3);
        assert_eq!(batch.stats.scanned_records, 3);
        assert_eq!(batch.stats.encoded_bytes, batch.to_bytes().len() as u64);
        assert!(batch.stats.scanned_stored_bytes > 0);
        assert_eq!(
            codec
                .decode(&batch.through_cursor_inclusive)
                .unwrap()
                .sequence,
            3
        );
        let frames = frames(&batch.to_bytes());
        assert_eq!(frames.len(), 4);
        let operation_tables = frames
            .iter()
            .filter_map(|frame| match frame.body.as_ref() {
                Some(change_stream_frame::Body::TransactionFragment(fragment)) => Some(
                    fragment
                        .operations
                        .iter()
                        .map(|operation| operation.table_id.as_str())
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            operation_tables,
            vec![vec!["public.users"], vec!["public.tasks", "public.users"]]
        );
    }

    #[test]
    fn live_batches_follow_the_durable_head_without_snapshot_restarts() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1, &["tasks"], 1)).unwrap();
        log.append(transaction(2, &["users"], 1)).unwrap();
        log.append(transaction(3, &["tasks", "users"], 1)).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let authorization = authorization(&codec, 1);
        let builder = ChangeBatchBuilder::new(&log, &authorization).unwrap();

        let first = builder.build_at(&live_request(&codec, 1), 9_999).unwrap();
        assert!(!first.reached_requested_fence);
        assert_eq!(first.relevant_transactions, 2);
        assert_eq!(first.relevant_operations, 3);
        assert_eq!(
            codec
                .decode(&first.through_cursor_inclusive)
                .unwrap()
                .sequence,
            3
        );
        let first_frames = frames(&first.to_bytes());
        match first_frames.first().and_then(|frame| frame.body.as_ref()) {
            Some(change_stream_frame::Body::BatchStart(start)) => {
                assert!(start.requested_fence.is_none());
                assert!(start
                    .table_starts
                    .iter()
                    .all(|subscription| subscription.shard_snapshot_cursor.is_none()));
            }
            other => panic!("expected live batch start, got {other:?}"),
        }
        match first_frames.last().and_then(|frame| frame.body.as_ref()) {
            Some(change_stream_frame::Body::BatchEnd(end)) => {
                assert!(!end.reached_requested_fence);
                assert!(end.currently_up_to_date);
            }
            other => panic!("expected live batch end, got {other:?}"),
        }

        let current = builder.build_at(&live_request(&codec, 3), 9_999).unwrap();
        assert!(!current.reached_requested_fence);
        assert_eq!(current.relevant_transactions, 0);
        assert_eq!(current.relevant_operations, 0);
        assert_eq!(
            codec
                .decode(&current.through_cursor_inclusive)
                .unwrap()
                .sequence,
            3
        );
        match frames(&current.to_bytes())
            .last()
            .and_then(|frame| frame.body.as_ref())
        {
            Some(change_stream_frame::Body::BatchEnd(end)) => {
                assert!(end.currently_up_to_date);
            }
            other => panic!("expected live heartbeat end, got {other:?}"),
        }

        let mut invalid = live_request(&codec, 1);
        invalid.tables[0].snapshot_cursor = Some(cursor_at(&codec, 1));
        assert!(matches!(
            builder.build_at(&invalid, 9_999),
            Err(ChangeBatchError::InvalidTableSet)
        ));
    }

    #[test]
    fn a_projected_scope_routes_projects_and_advances_over_omitted_transactions() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        for (sequence, tenant, kind) in [
            (
                1,
                1,
                CanonicalOperationKind::InsertFullRow {
                    columns: vec![
                        column(1, CellValue::Uuid16([1; 16])),
                        column(2, CellValue::Text("visible".to_owned())),
                        column(3, CellValue::Text("hidden".to_owned())),
                    ],
                },
            ),
            (
                2,
                2,
                CanonicalOperationKind::InsertFullRow {
                    columns: vec![
                        column(1, CellValue::Uuid16([2; 16])),
                        column(2, CellValue::Text("other tenant".to_owned())),
                    ],
                },
            ),
            (
                3,
                1,
                CanonicalOperationKind::UpdatePatch {
                    changed_columns: vec![column(3, CellValue::Text("hidden-only".to_owned()))],
                },
            ),
            (4, 1, CanonicalOperationKind::DeleteByKey),
        ] {
            log.append(routed_transaction(
                sequence,
                CanonicalOperation {
                    table_id: table("tasks"),
                    primary_key: tenant_primary_key(tenant),
                    kind,
                },
            ))
            .unwrap();
        }
        let codec = CursorCodec::new([7; 32]).unwrap();
        let scope = projected_scope();
        let scope_hash = *scope.scope_hash().as_bytes();
        let authorization = ChangeAuthorization::new(ScopedChangePlan {
            plan_id: plan_id(),
            activated_scope: test_activated_scope(&codec, scope),
            expires_at_unix_micros: 10_000,
            from_cursor_exclusive: cursor_at(&codec, 0),
            through_cursor_inclusive: cursor_at(&codec, 4),
            table_starts: BTreeMap::from([(table("tasks"), Some(cursor_at(&codec, 0)))]),
            acknowledgeable_mutation_ids: Default::default(),
            revocation_barrier: None,
        })
        .unwrap();
        let request = ChangeBatchRequest {
            plan_id: plan_id(),
            protocol_version: 1,
            from_cursor_exclusive: cursor_at(&codec, 0),
            through_cursor_inclusive: Some(cursor_at(&codec, 4)),
            tables: vec![TableSubscription {
                table_id: table("tasks"),
                snapshot_cursor: Some(cursor_at(&codec, 0)),
            }],
            max_batch_bytes: 1024 * 1024,
            max_transactions: 100,
            long_poll_timeout_ms: 0,
            accept_transaction_fragmentation: true,
            accept_revocation_barrier: true,
            stream: false,
            device_id: "device-1".to_owned(),
            pending_mutation_ids: BTreeSet::new(),
        };
        let batch = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .build_at_with_stream_id(&request, 9_999, vec![5; 16])
            .unwrap();

        assert!(batch.reached_requested_fence);
        assert_eq!(batch.relevant_transactions, 3);
        assert_eq!(batch.relevant_operations, 3);
        assert_eq!(
            codec
                .decode(&batch.through_cursor_inclusive)
                .unwrap()
                .sequence,
            4
        );
        let frames = frames(&batch.to_bytes());
        let Some(change_stream_frame::Body::BatchStart(start)) =
            frames.first().unwrap().body.as_ref()
        else {
            panic!("batch must start with metadata");
        };
        let generation = start.generation.as_ref().unwrap();
        assert_eq!(generation.authorization_generation, 11);
        assert_eq!(generation.authorization_scope_hash, scope_hash);
        let transactions = frames
            .iter()
            .filter_map(|frame| match frame.body.as_ref() {
                Some(change_stream_frame::Body::TransactionFragment(fragment)) => Some(fragment),
                _ => None,
            })
            .collect::<Vec<_>>();
        // The hidden-only update is omitted; both inserts lose the withheld
        // column.
        assert_eq!(transactions.len(), 3);
        for transaction in &transactions[..2] {
            let Some(operation::Payload::Insert(insert)) =
                transaction.operations[0].payload.as_ref()
            else {
                panic!("the first two routed operations must be inserts");
            };
            assert_eq!(
                insert
                    .columns
                    .iter()
                    .map(|column| column.column_id)
                    .collect::<Vec<_>>(),
                [1, 2]
            );
        }
        assert_eq!(
            transactions[2].operations[0].kind,
            change::OperationKind::DeleteByKey as i32
        );
    }

    /// `tasks (tenant_id uuid, id int8)` partitioned by tenant, and a plan of
    /// `tenant` over it from the log's start to `fence`.
    fn partitioned_authorization(
        codec: &CursorCodec,
        tenant: u8,
        fence: u64,
    ) -> ChangeAuthorization {
        let inspection = |number: i16, name: &str, type_name: &str, key: u32| ColumnInspection {
            attribute_number: number,
            name: name.to_owned(),
            type_oid: 1_000 + u32::from(number.unsigned_abs()),
            type_modifier: -1,
            type_schema: "pg_catalog".to_owned(),
            type_name: type_name.to_owned(),
            nullable: false,
            primary_key_ordinal: Some(key),
            generated: false,
            nondeterministic_collation: false,
            carried_as: None,
        };
        let registered = RegisteredTable {
            table_id: table("tasks"),
            column_ids: vec![ColumnId::new(1), ColumnId::new(2)],
            inspection: TableInspection {
                table: QualifiedTable {
                    schema: "public".to_owned(),
                    table: "tasks".to_owned(),
                },
                relation_oid: 16_385,
                in_publication: true,
                relation_kind: 'r',
                persistence: 'p',
                replica_identity: 'd',
                is_partition: false,
                has_publication_row_filter: false,
                has_publication_column_list: false,
                unlisted_columns: Vec::new(),
                columns: vec![
                    inspection(1, "tenant_id", "uuid", 1),
                    inspection(2, "id", "int8", 2),
                ],
            },
        };
        let policy = AuthorizationPolicy::compile_partitioned_detached(
            PolicyDefinition {
                generation: 5,
                tables: vec![TablePermitDefinition {
                    table_id: registered.table_id.clone(),
                    permitted_columns: registered.column_ids.clone(),
                }],
            },
            authz::PartitionDefinition {
                attribute: "tenant".to_owned(),
                tables: BTreeMap::from([(registered.table_id.clone(), ColumnId::new(1))]),
            },
            std::slice::from_ref(&registered),
        )
        .unwrap();
        let scope = policy
            .partition_scope(authz::PartitionValue::Uuid([tenant; 16]))
            .unwrap();
        ChangeAuthorization::new(ScopedChangePlan {
            plan_id: plan_id(),
            activated_scope: test_activated_scope(codec, scope),
            expires_at_unix_micros: 10_000,
            from_cursor_exclusive: cursor_at(codec, 0),
            through_cursor_inclusive: cursor_at(codec, fence),
            table_starts: BTreeMap::from([(table("tasks"), Some(cursor_at(codec, 0)))]),
            acknowledgeable_mutation_ids: Default::default(),
            revocation_barrier: None,
        })
        .unwrap()
    }

    fn task_key(tenant: u8, id: i64) -> Vec<u8> {
        encode_primary_key(&PrimaryKey {
            codec_version: sync_protocol::PRIMARY_KEY_CODEC_VERSION,
            parts: vec![
                WireColumnValue {
                    column_id: 1,
                    value: Some(WireCellValue {
                        kind: Some(cell_value::Kind::Uuid16(vec![tenant; 16])),
                    }),
                },
                WireColumnValue {
                    column_id: 2,
                    value: Some(WireCellValue {
                        kind: Some(cell_value::Kind::Int64Value(id)),
                    }),
                },
            ],
        })
        .unwrap()
    }

    /// Tenants 1 and 2 hold a task of the same id; a third transaction moves
    /// tenant 1's task to tenant 2. Tenant 1's plan is served its own insert
    /// and not tenant 2's, and stops at the move, as tenant 2's does; tenant
    /// 3's plan reads past all three and is served nothing.
    #[test]
    fn a_partitioned_plan_gets_its_own_rows_and_stops_at_a_move() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        for (sequence, tenant) in [(1, 1_u8), (2, 2)] {
            log.append(routed_transaction(
                sequence,
                CanonicalOperation {
                    table_id: table("tasks"),
                    primary_key: task_key(tenant, 10),
                    kind: CanonicalOperationKind::InsertFullRow {
                        columns: vec![
                            column(1, CellValue::Uuid16([tenant; 16])),
                            column(2, CellValue::Int64(10)),
                        ],
                    },
                },
            ))
            .unwrap();
        }
        log.append(routed_transaction(
            3,
            CanonicalOperation {
                table_id: table("tasks"),
                primary_key: task_key(1, 10),
                kind: CanonicalOperationKind::UpdatePatch {
                    changed_columns: vec![
                        column(1, CellValue::Uuid16([2; 16])),
                        column(2, CellValue::Int64(11)),
                    ],
                },
            },
        ))
        .unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let build = |tenant: u8, fence: u64| {
            let authorization = partitioned_authorization(&codec, tenant, fence);
            let mut request = request(&codec, fence);
            request.tables = vec![TableSubscription {
                table_id: table("tasks"),
                snapshot_cursor: Some(cursor_at(&codec, 0)),
            }];
            ChangeBatchBuilder::new(&log, &authorization)
                .unwrap()
                .build_at(&request, 9_999)
        };

        let first = build(1, 2).unwrap();
        assert_eq!(first.relevant_transactions, 1);
        let operations = frames(&first.to_bytes())
            .into_iter()
            .filter_map(|frame| match frame.body {
                Some(change_stream_frame::Body::TransactionFragment(fragment)) => Some(fragment),
                _ => None,
            })
            .flat_map(|fragment| fragment.operations)
            .collect::<Vec<_>>();
        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].primary_key, task_key(1, 10));
        for tenant in [1, 2] {
            assert!(matches!(
                build(tenant, 3),
                Err(ChangeBatchError::PartitionMoved)
            ));
        }
        let other = build(3, 3).unwrap();
        assert_eq!(other.relevant_transactions, 0);
        assert!(other.reached_requested_fence);
    }

    /// Two tenants of one policy hold a task of the same id. The tenant is
    /// part of the key a scope's shared encoding is held under, so each
    /// tenant's builds share an encoding of their own however they are
    /// ordered, and each is sent only its own row; a move then refuses both
    /// through their encodings as it does without them.
    #[test]
    fn two_tenants_of_one_policy_never_share_an_encoding() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        for (sequence, tenant) in [(1, 1_u8), (2, 2)] {
            log.append(routed_transaction(
                sequence,
                CanonicalOperation {
                    table_id: table("tasks"),
                    primary_key: task_key(tenant, 10),
                    kind: CanonicalOperationKind::InsertFullRow {
                        columns: vec![
                            column(1, CellValue::Uuid16([tenant; 16])),
                            column(2, CellValue::Int64(10)),
                        ],
                    },
                },
            ))
            .unwrap();
        }
        let codec = CursorCodec::new([7; 32]).unwrap();
        let first = partitioned_authorization(&codec, 1, 2);
        let second = partitioned_authorization(&codec, 2, 2);
        assert_eq!(first.scope.policy_hash(), second.scope.policy_hash());
        assert_ne!(ScopeEncodingKey::of(&first), ScopeEncodingKey::of(&second));
        assert!(matches!(
            ChangeBatchBuilder::new(&log, &second)
                .unwrap()
                .with_shared_encoding(&ScopeEncoding::new(ScopeEncodingKey::of(&first))),
            Err(ChangeBatchError::SharedEncodingMismatch)
        ));

        // The change tail holds one encoding per key, as here.
        let mut encodings = std::collections::HashMap::new();
        let mut request = request(&codec, 2);
        request.tables = vec![TableSubscription {
            table_id: table("tasks"),
            snapshot_cursor: Some(cursor_at(&codec, 0)),
        }];
        for (tenant, authorization) in [(1, &first), (2, &second), (1, &first)] {
            let key = ScopeEncodingKey::of(authorization);
            let encoding = encodings
                .entry(key.clone())
                .or_insert_with(|| ScopeEncoding::new(key));
            let batch = ChangeBatchBuilder::new(&log, authorization)
                .unwrap()
                .with_shared_encoding(encoding)
                .unwrap()
                .build_at(&request, 9_999)
                .unwrap();
            let keys = frames(&batch.to_bytes())
                .into_iter()
                .filter_map(|frame| match frame.body {
                    Some(change_stream_frame::Body::TransactionFragment(fragment)) => {
                        Some(fragment)
                    }
                    _ => None,
                })
                .flat_map(|fragment| fragment.operations)
                .map(|operation| operation.primary_key)
                .collect::<Vec<_>>();
            assert_eq!(keys, [task_key(tenant, 10)], "tenant {tenant}");
        }
        assert_eq!(encodings.len(), 2);

        log.append(routed_transaction(
            3,
            CanonicalOperation {
                table_id: table("tasks"),
                primary_key: task_key(1, 10),
                kind: CanonicalOperationKind::UpdatePatch {
                    changed_columns: vec![column(1, CellValue::Uuid16([2; 16]))],
                },
            },
        ))
        .unwrap();
        for tenant in [1, 2] {
            let authorization = partitioned_authorization(&codec, tenant, 3);
            let encoding = ScopeEncoding::new(ScopeEncodingKey::of(&authorization));
            let mut request = request.clone();
            request.through_cursor_inclusive = Some(cursor_at(&codec, 3));
            assert!(matches!(
                ChangeBatchBuilder::new(&log, &authorization)
                    .unwrap()
                    .with_shared_encoding(&encoding)
                    .unwrap()
                    .build_at(&request, 9_999),
                Err(ChangeBatchError::PartitionMoved)
            ));
        }
    }

    #[test]
    fn policy_violations_fail_before_table_snapshot_filtering() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(routed_transaction(
            1,
            CanonicalOperation {
                table_id: table("tasks"),
                primary_key: tenant_primary_key(1),
                // The scope permits columns 1 and 2, and a full row that omits
                // one cannot be projected.
                kind: CanonicalOperationKind::InsertFullRow {
                    columns: vec![column(1, CellValue::Uuid16([1; 16]))],
                },
            },
        ))
        .unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let scope = projected_scope();
        let authorization = ChangeAuthorization::new(ScopedChangePlan {
            plan_id: plan_id(),
            activated_scope: test_activated_scope(&codec, scope),
            expires_at_unix_micros: 10_000,
            from_cursor_exclusive: cursor_at(&codec, 0),
            through_cursor_inclusive: cursor_at(&codec, 1),
            table_starts: BTreeMap::from([(table("tasks"), Some(cursor_at(&codec, 1)))]),
            acknowledgeable_mutation_ids: Default::default(),
            revocation_barrier: None,
        })
        .unwrap();
        let mut request = request(&codec, 1);
        request.tables = vec![TableSubscription {
            table_id: table("tasks"),
            snapshot_cursor: Some(cursor_at(&codec, 1)),
        }];

        assert!(matches!(
            ChangeBatchBuilder::new(&log, &authorization)
                .unwrap()
                .build_at(&request, 9_999),
            Err(ChangeBatchError::Authorization(
                CanonicalAuthorizationError::MissingPermittedColumn
            ))
        ));
    }

    #[test]
    fn rejects_unauthorized_tables_bad_fences_retention_and_tight_batches() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1, &["tasks"], 2_000)).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let scope = scope_for([table("tasks")]);
        assert!(matches!(
            ChangeAuthorization::new(ScopedChangePlan {
                plan_id: plan_id(),
                activated_scope: test_activated_scope(&codec, scope),
                expires_at_unix_micros: 10_000,
                from_cursor_exclusive: cursor_at(&codec, 0),
                through_cursor_inclusive: cursor_at(&codec, 1),
                table_starts: BTreeMap::from([(table("secrets"), None)]),
                acknowledgeable_mutation_ids: Default::default(),
                revocation_barrier: None,
            }),
            Err(ChangeAuthorizationError::UnauthorizedTable(table_id))
                if table_id == table("secrets")
        ));
        let authorization = authorization(&codec, 1);
        {
            let builder = ChangeBatchBuilder::new(&log, &authorization).unwrap();

            assert!(matches!(
                builder.build_at(&request(&codec, 1), -1),
                Err(ChangeBatchError::InvalidServerTime)
            ));
            assert!(matches!(
                builder.build_at(&request(&codec, 1), 10_000),
                Err(ChangeBatchError::AuthorizationExpired)
            ));

            let mut invalid = request(&codec, 1);
            invalid.tables[0].table_id = table("secrets");
            assert!(matches!(
                builder.build_at(&invalid, 9_999),
                Err(ChangeBatchError::UnauthorizedTable(_))
            ));

            let mut invalid = request(&codec, 1);
            invalid.through_cursor_inclusive = Some(cursor_at(&codec, 2));
            let invalid_authorization = authorization_with(
                &codec,
                invalid.from_cursor_exclusive.clone(),
                invalid.through_cursor_inclusive.clone().unwrap(),
                authorization.table_starts.clone(),
            );
            assert!(matches!(
                ChangeBatchBuilder::new(&log, &invalid_authorization)
                    .unwrap()
                    .build_at(&invalid, 9_999),
                Err(ChangeBatchError::FenceOutsideDurableRange)
            ));

            let mut tight = request(&codec, 1);
            tight.tables[0].snapshot_cursor = Some(cursor_at(&codec, 0));
            tight.max_batch_bytes = MIN_BATCH_BYTES;
            let tight_authorization = authorization_with(
                &codec,
                tight.from_cursor_exclusive.clone(),
                tight.through_cursor_inclusive.clone().unwrap(),
                BTreeMap::from([
                    (table("tasks"), Some(cursor_at(&codec, 0))),
                    (table("users"), Some(cursor_at(&codec, 0))),
                ]),
            );
            assert!(matches!(
                ChangeBatchBuilder::new(&log, &tight_authorization)
                    .unwrap()
                    .build_at(&tight, 9_999),
                Err(ChangeBatchError::BatchLimitTooSmall)
            ));
        }

        log.retain_from(2).unwrap();
        let builder = ChangeBatchBuilder::new(&log, &authorization).unwrap();
        assert!(matches!(
            builder.build_at(&request(&codec, 1), 9_999),
            Err(ChangeBatchError::BeforeRetention { minimum: 2 })
        ));
    }

    #[test]
    fn rejects_tampered_epoch_and_invalid_per_table_cursors() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1, &["tasks"], 1)).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let authorization = authorization(&codec, 1);
        let builder = ChangeBatchBuilder::new(&log, &authorization).unwrap();

        let valid = cursor_at(&codec, 0);
        let mut bytes = valid.as_str().as_bytes().to_vec();
        let last = bytes.last_mut().unwrap();
        *last = if *last == b'A' { b'B' } else { b'A' };
        let mut invalid = request(&codec, 1);
        invalid.from_cursor_exclusive =
            OpaqueCursor::parse(String::from_utf8(bytes).unwrap()).unwrap();
        let invalid_authorization = authorization_with(
            &codec,
            invalid.from_cursor_exclusive.clone(),
            invalid.through_cursor_inclusive.clone().unwrap(),
            authorization.table_starts.clone(),
        );
        assert!(matches!(
            ChangeBatchBuilder::new(&log, &invalid_authorization)
                .unwrap()
                .build_at(&invalid, 9_999),
            Err(ChangeBatchError::Cursor(CursorError::InvalidSignature))
        ));

        let mut invalid = request(&codec, 1);
        invalid.from_cursor_exclusive = codec.encode(&CursorPosition {
            source_epoch: SourceEpoch::new("epoch-2").unwrap(),
            sequence: 0,
        });
        let invalid_authorization = authorization_with(
            &codec,
            invalid.from_cursor_exclusive.clone(),
            invalid.through_cursor_inclusive.clone().unwrap(),
            authorization.table_starts.clone(),
        );
        assert!(matches!(
            ChangeBatchBuilder::new(&log, &invalid_authorization)
                .unwrap()
                .build_at(&invalid, 9_999),
            Err(ChangeBatchError::Cursor(CursorError::EpochMismatch { .. }))
        ));

        let mut invalid = request(&codec, 1);
        invalid.tables.push(invalid.tables[0].clone());
        assert!(matches!(
            builder.build_at(&invalid, 9_999),
            Err(ChangeBatchError::InvalidTableSet)
        ));

        let mut invalid = request(&codec, 1);
        invalid.tables[0].snapshot_cursor = Some(cursor_at(&codec, 2));
        let invalid_authorization = authorization_with(
            &codec,
            invalid.from_cursor_exclusive.clone(),
            invalid.through_cursor_inclusive.clone().unwrap(),
            BTreeMap::from([
                (table("tasks"), Some(cursor_at(&codec, 2))),
                (table("users"), Some(cursor_at(&codec, 0))),
            ]),
        );
        assert!(matches!(
            ChangeBatchBuilder::new(&log, &invalid_authorization)
                .unwrap()
                .build_at(&invalid, 9_999),
            Err(ChangeBatchError::InvalidSnapshotCursor)
        ));
    }

    #[test]
    fn parses_bounded_wire_requests_without_exposing_the_scope_token() {
        let codec = CursorCodec::new([7; 32]).unwrap();
        let parsed = parse_change_request(wire_request(&codec)).unwrap();
        assert_eq!(parsed.authorization_scope_token(), b"scope-token-secret");
        assert_eq!(parsed.client_instance_id(), "client-1");
        assert_eq!(parsed.device_id(), "device-1");
        assert!(parsed
            .pending_mutation_ids()
            .contains(&MutationId::new("mutation-1").unwrap()));
        assert_eq!(parsed.request().tables[0].table_id, table("tasks"));
        assert!(parsed.request().accept_transaction_fragmentation);
        assert!(!format!("{parsed:?}").contains("scope-token-secret"));

        let mut long_poll = wire_request(&codec);
        long_poll.long_poll_timeout_ms = MIN_LONG_POLL_TIMEOUT_MS;
        assert_eq!(
            parse_change_request(long_poll)
                .unwrap()
                .request()
                .long_poll_timeout_ms,
            MIN_LONG_POLL_TIMEOUT_MS
        );

        let mut invalid = wire_request(&codec);
        invalid.long_poll_timeout_ms = MIN_LONG_POLL_TIMEOUT_MS - 1;
        assert!(matches!(
            parse_change_request(invalid),
            Err(ChangeBatchError::InvalidLimits)
        ));

        for (max_batch_bytes, max_transactions) in [
            (MIN_BATCH_BYTES - 1, 1),
            (MAX_BATCH_BYTES + 1, 1),
            (300_000_000, 1),
            (MAX_BATCH_BYTES, 0),
            (MAX_BATCH_BYTES, MAX_TRANSACTIONS + 1),
        ] {
            let mut invalid = wire_request(&codec);
            invalid.max_batch_bytes = u64::try_from(max_batch_bytes).unwrap();
            invalid.max_transactions = u32::try_from(max_transactions).unwrap();
            assert!(matches!(
                parse_change_request(invalid),
                Err(ChangeBatchError::InvalidLimits)
            ));
        }
        let mut largest = wire_request(&codec);
        largest.max_batch_bytes = u64::try_from(MAX_BATCH_BYTES).unwrap();
        largest.max_transactions = u32::try_from(MAX_TRANSACTIONS).unwrap();
        assert_eq!(
            parse_change_request(largest)
                .unwrap()
                .request()
                .max_batch_bytes,
            MAX_BATCH_BYTES
        );

        let mut invalid = wire_request(&codec);
        invalid.authorization_scope_token.clear();
        assert!(matches!(
            parse_change_request(invalid),
            Err(ChangeBatchError::InvalidAuthorizationToken)
        ));

        let mut invalid = wire_request(&codec);
        invalid.client_instance_id = "bad\nclient".to_owned();
        assert!(matches!(
            parse_change_request(invalid),
            Err(ChangeBatchError::InvalidClientInstance)
        ));

        let mut invalid = wire_request(&codec);
        invalid.through_cursor_inclusive = None;
        assert!(parse_change_request(invalid)
            .unwrap()
            .request()
            .through_cursor_inclusive
            .is_none());

        let mut stream = wire_request(&codec);
        stream.through_cursor_inclusive = None;
        stream.long_poll_timeout_ms = MIN_LONG_POLL_TIMEOUT_MS;
        stream.stream = true;
        assert!(
            parse_change_request(stream.clone())
                .unwrap()
                .request()
                .stream
        );
        let mut fenced = stream.clone();
        fenced.through_cursor_inclusive = Some(cursor(&cursor_at(&codec, 1)));
        assert!(parse_change_request(fenced).unwrap().request().stream);
        stream.long_poll_timeout_ms = 0;
        assert!(matches!(
            parse_change_request(stream),
            Err(ChangeBatchError::InvalidLimits)
        ));
    }

    #[test]
    fn pages_only_at_transaction_boundaries_and_advances_over_irrelevant_records() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1, &["tasks"], 1)).unwrap();
        log.append(transaction(2, &["tasks"], 1)).unwrap();
        log.append_barrier(barrier(3)).unwrap();
        log.append(transaction(4, &["secrets"], 1)).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let mut paged = request(&codec, 4);
        paged.tables[0].snapshot_cursor = Some(cursor_at(&codec, 0));
        paged.max_transactions = 1;
        let authorization = authorization_with(
            &codec,
            paged.from_cursor_exclusive.clone(),
            paged.through_cursor_inclusive.clone().unwrap(),
            BTreeMap::from([
                (table("tasks"), Some(cursor_at(&codec, 0))),
                (table("users"), Some(cursor_at(&codec, 0))),
            ]),
        );
        let builder = ChangeBatchBuilder::new(&log, &authorization).unwrap();
        let first = builder.build_at(&paged, 9_999).unwrap();
        assert!(!first.reached_requested_fence);
        assert_eq!(first.relevant_transactions, 1);
        assert_eq!(
            codec
                .decode(&first.through_cursor_inclusive)
                .unwrap()
                .sequence,
            1
        );

        paged.from_cursor_exclusive = first.through_cursor_inclusive;
        paged.max_transactions = 100;
        let continuation = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .build_at(&paged, 9_999)
            .unwrap();
        assert!(continuation.reached_requested_fence);
        assert_eq!(continuation.relevant_transactions, 1);
        assert_eq!(continuation.relevant_operations, 1);
        assert_eq!(continuation.stats.scanned_records, 3);
        assert_eq!(
            codec
                .decode(&continuation.through_cursor_inclusive)
                .unwrap()
                .sequence,
            4
        );
    }

    #[test]
    fn fragments_one_large_transaction_without_splitting_batch_visibility() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(
            1,
            &["tasks", "tasks", "tasks", "tasks", "tasks"],
            1024 * 1024,
        ))
        .unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let mut request = request(&codec, 1);
        request.tables[0].snapshot_cursor = Some(cursor_at(&codec, 0));
        let authorization = authorization_with(
            &codec,
            request.from_cursor_exclusive.clone(),
            request.through_cursor_inclusive.clone().unwrap(),
            BTreeMap::from([
                (table("tasks"), Some(cursor_at(&codec, 0))),
                (table("users"), Some(cursor_at(&codec, 0))),
            ]),
        );
        request.max_batch_bytes = 12 * 1024 * 1024;
        request.accept_transaction_fragmentation = false;
        assert!(matches!(
            ChangeBatchBuilder::new(&log, &authorization)
                .unwrap()
                .build_at(&request, 9_999),
            Err(ChangeBatchError::FragmentationRequired)
        ));
        request.accept_transaction_fragmentation = true;
        let batch = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .build_at(&request, 9_999)
            .unwrap();
        let fragments = frames(&batch.to_bytes())
            .into_iter()
            .filter_map(|frame| match frame.body {
                Some(change_stream_frame::Body::TransactionFragment(fragment)) => Some(fragment),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(fragments.len() > 1);
        assert!(fragments[0].first_fragment);
        assert!(fragments.last().unwrap().final_fragment);
        assert_eq!(
            fragments
                .iter()
                .map(|fragment| fragment.operations.len())
                .sum::<usize>(),
            5
        );
        assert_eq!(batch.relevant_transactions, 1);
        assert_eq!(batch.relevant_operations, 5);
        assert!(batch.reached_requested_fence);
    }

    #[test]
    fn acknowledges_only_server_authorized_mutations_including_those_of_transactions_it_omits() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let mut canonical = transaction(1, &["users"], 4);
        canonical.mutation_ids = vec![
            MutationId::new("mutation-owned").unwrap(),
            MutationId::new("mutation-other-scope").unwrap(),
        ];
        log.append(canonical).unwrap();
        // Named again, it is still acknowledged once: the page refuses a
        // batch end that names an id twice.
        let mut again = transaction(2, &["users"], 4);
        again.mutation_ids = vec![MutationId::new("mutation-owned").unwrap()];
        log.append(again).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let scope = scope_for([table("tasks")]);
        let scope_hash = *scope.scope_hash().as_bytes();
        let authorization = ChangeAuthorization::new(ScopedChangePlan {
            plan_id: plan_id(),
            activated_scope: test_activated_scope(&codec, scope.clone()),
            expires_at_unix_micros: 10_000,
            from_cursor_exclusive: cursor_at(&codec, 0),
            through_cursor_inclusive: cursor_at(&codec, 2),
            table_starts: BTreeMap::from([(table("tasks"), None)]),
            acknowledgeable_mutation_ids:
                ResolvedMutationAcknowledgements::from_test_receipt_bindings(
                    source_id(),
                    epoch(),
                    &scope,
                    "device-1",
                    &BTreeSet::from([
                        MutationId::new("mutation-owned").unwrap(),
                        MutationId::new("mutation-other-scope").unwrap(),
                    ]),
                    [
                        MutationReceiptBinding {
                            mutation_id: MutationId::new("mutation-owned").unwrap(),
                            source_id: source_id(),
                            source_epoch: epoch(),
                            authorization_generation: scope.generation(),
                            authorization_policy_hash: *scope.policy_hash().as_bytes(),
                            authorization_scope_hash: scope_hash,
                            device_id: "device-1".to_owned(),
                        },
                        MutationReceiptBinding {
                            mutation_id: MutationId::new("mutation-other-scope").unwrap(),
                            source_id: source_id(),
                            source_epoch: epoch(),
                            authorization_generation: scope.generation(),
                            authorization_policy_hash: *scope.policy_hash().as_bytes(),
                            authorization_scope_hash: [8; 32],
                            device_id: "device-1".to_owned(),
                        },
                    ],
                )
                .unwrap(),
            revocation_barrier: None,
        })
        .unwrap();
        let request = ChangeBatchRequest {
            plan_id: plan_id(),
            protocol_version: 1,
            from_cursor_exclusive: cursor_at(&codec, 0),
            through_cursor_inclusive: Some(cursor_at(&codec, 2)),
            tables: vec![TableSubscription {
                table_id: table("tasks"),
                snapshot_cursor: None,
            }],
            max_batch_bytes: 1024 * 1024,
            max_transactions: 100,
            long_poll_timeout_ms: 0,
            accept_transaction_fragmentation: true,
            accept_revocation_barrier: true,
            stream: false,
            device_id: "device-1".to_owned(),
            pending_mutation_ids: BTreeSet::from([
                MutationId::new("mutation-owned").unwrap(),
                MutationId::new("mutation-other-scope").unwrap(),
            ]),
        };
        let builder = ChangeBatchBuilder::new(&log, &authorization).unwrap();
        let mut wrong_device = request.clone();
        wrong_device.device_id = "device-other".to_owned();
        assert!(matches!(
            builder.build_at_with_stream_id(&wrong_device, 1_000, vec![3; 16]),
            Err(ChangeBatchError::InvalidMutationAcknowledgementProof)
        ));
        let batch = builder
            .build_at_with_stream_id(&request, 1_000, vec![3; 16])
            .unwrap();
        let frames = frames(&batch.to_bytes());
        // The transactions leave this scope no operation, so no fragment
        // carries them, and only the batch end says they committed the
        // mutation.
        assert!(!frames.iter().any(|frame| matches!(
            frame.body,
            Some(change_stream_frame::Body::TransactionFragment(_))
        )));
        let Some(change_stream_frame::Body::BatchEnd(end)) = &frames.last().unwrap().body else {
            panic!("batch must end with its end");
        };
        assert_eq!(end.acknowledged_mutation_ids, ["mutation-owned"]);
        assert_eq!(batch.relevant_transactions, 0);
        assert_eq!(batch.relevant_operations, 0);
    }

    #[test]
    #[ignore = "requires CALQUE_TEST_DATABASE_URL with wal_level=logical"]
    fn live_receipt_resolution_composes_with_the_exact_change_request() {
        let url = std::env::var("CALQUE_TEST_DATABASE_URL")
            .expect("CALQUE_TEST_DATABASE_URL must be set");
        let mut postgres = Client::connect(&url, postgres::NoTls).unwrap();
        install_metadata(&mut postgres).unwrap();
        let mutation_id = format!("mutation-change-{}", Uuid::new_v4());
        let scope = scope_for([table("tasks")]);
        postgres
            .execute(
                "INSERT INTO calque_internal.mutation_receipts \
                 (mutation_id, source_id, source_epoch, authorization_generation, \
                  authorization_policy_hash, authorization_scope_hash, \
                  command_name, command_version, canonical_input_sha256, \
                  server_transaction_id, device_id, client_instance_id) \
                 VALUES ($1, $2, $3, $4, $5, $6, 'test.command', 1, $7, $8, $9, 'client-1')",
                &[
                    &mutation_id,
                    &source_id().as_str(),
                    &epoch().as_str(),
                    &i64::try_from(scope.generation()).unwrap(),
                    &scope.policy_hash().as_bytes().as_slice(),
                    &scope.scope_hash().as_bytes().as_slice(),
                    &[1_u8; 32].as_slice(),
                    &[2_u8; 16].as_slice(),
                    &"device-1",
                ],
            )
            .unwrap();

        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let mut canonical = transaction(1, &["users"], 1);
        canonical.mutation_ids = vec![MutationId::new(mutation_id.clone()).unwrap()];
        log.append(canonical).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let mut wire = wire_request(&codec);
        wire.pending_mutation_ids = vec![mutation_id.clone()];
        let incoming = parse_change_request(wire).unwrap();
        let proof = resolve_acknowledgements(
            &mut postgres,
            &source_id(),
            &epoch(),
            &scope,
            incoming.device_id(),
            incoming.pending_mutation_ids(),
        )
        .unwrap();
        let authorization = ChangeAuthorization::new(ScopedChangePlan {
            plan_id: plan_id(),
            activated_scope: test_activated_scope(&codec, scope),
            expires_at_unix_micros: 10_000,
            from_cursor_exclusive: cursor_at(&codec, 0),
            through_cursor_inclusive: cursor_at(&codec, 1),
            table_starts: BTreeMap::from([(table("tasks"), Some(cursor_at(&codec, 0)))]),
            acknowledgeable_mutation_ids: proof,
            revocation_barrier: None,
        })
        .unwrap();
        let batch = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .build_at_with_stream_id(incoming.request(), 1_000, vec![4; 16])
            .unwrap();
        let acknowledged = frames(&batch.to_bytes())
            .into_iter()
            .find_map(|frame| match frame.body {
                Some(change_stream_frame::Body::BatchEnd(end)) => {
                    Some(end.acknowledged_mutation_ids)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(acknowledged.as_slice(), std::slice::from_ref(&mutation_id));
        postgres
            .execute(
                "DELETE FROM calque_internal.mutation_receipts WHERE mutation_id = $1",
                &[&mutation_id],
            )
            .unwrap();
    }

    #[test]
    fn change_batch_differs_from_legacy_cross_language_fixture_only_by_resolved_scope_identity() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1, &["tasks", "users"], 1)).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let authorization = authorization(&codec, 1);
        let batch = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .build_at_with_stream_id(&request(&codec, 1), 0, vec![0x11; 16])
            .unwrap();
        let hex = include_str!("../../../../spec/fixtures/change-batch.frame.hex").trim();
        let expected = hex
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect::<Vec<_>>();
        let mut actual_frames = frames(&batch.to_bytes());
        let Some(change_stream_frame::Body::BatchStart(start)) = actual_frames[0].body.as_mut()
        else {
            panic!("batch must start with metadata");
        };
        let generation = start.generation.as_mut().unwrap();
        // The fixture's batch carries authorization generation 0 and this
        // scope hash, which the client's copy of the fixture checks too.
        generation.authorization_generation = 0;
        generation.authorization_scope_hash = [
            0xf9, 0x15, 0x4d, 0xb5, 0x12, 0xdf, 0xe8, 0x02, 0xaa, 0x41, 0xf3, 0x58, 0x90, 0x9f,
            0x83, 0x3e, 0x79, 0x8c, 0x07, 0x7f, 0x0b, 0xfa, 0x6a, 0x2a, 0xf7, 0x1c, 0x91, 0xab,
            0x27, 0x97, 0xcf, 0x36,
        ]
        .to_vec();
        actual_frames[0].body_sha256 = change_frame_body_sha256(&actual_frames[0])
            .unwrap()
            .to_vec();
        let (end, covered) = actual_frames.split_last_mut().unwrap();
        let mut digest = BatchFramesDigest::new();
        for frame in covered.iter() {
            digest.push(&frame.body_sha256.as_slice().try_into().unwrap());
        }
        let Some(change_stream_frame::Body::BatchEnd(batch_end)) = end.body.as_mut() else {
            panic!("batch must end with its end");
        };
        batch_end.frames_sha256 = digest.finish().to_vec();
        end.body_sha256 = change_frame_body_sha256(end).unwrap().to_vec();
        let normalized = actual_frames
            .iter()
            .flat_map(|frame| encode_frame_with_limit(frame, MAX_FRAME_BYTES).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(normalized, expected);
    }

    fn live_authorization(
        codec: &CursorCodec,
        scope: AuthorizedScope,
        acknowledgeable: &[&str],
    ) -> ChangeAuthorization {
        let pending = acknowledgeable
            .iter()
            .map(|id| MutationId::new(*id).unwrap())
            .collect::<BTreeSet<_>>();
        let bindings = pending
            .iter()
            .map(|mutation_id| MutationReceiptBinding {
                mutation_id: mutation_id.clone(),
                source_id: source_id(),
                source_epoch: epoch(),
                authorization_generation: scope.generation(),
                authorization_policy_hash: *scope.policy_hash().as_bytes(),
                authorization_scope_hash: *scope.scope_hash().as_bytes(),
                device_id: "device-1".to_owned(),
            })
            .collect::<Vec<_>>();
        let acknowledgeable_mutation_ids =
            ResolvedMutationAcknowledgements::from_test_receipt_bindings(
                source_id(),
                epoch(),
                &scope,
                "device-1",
                &pending,
                bindings,
            )
            .unwrap();
        let table_starts = scope
            .permitted_tables()
            .map(|table_id| (table_id.clone(), None))
            .collect();
        ChangeAuthorization::new(ScopedChangePlan {
            plan_id: plan_id(),
            activated_scope: test_activated_scope(codec, scope),
            expires_at_unix_micros: 10_000,
            from_cursor_exclusive: cursor_at(codec, 0),
            through_cursor_inclusive: cursor_at(codec, 0),
            table_starts,
            acknowledgeable_mutation_ids,
            revocation_barrier: None,
        })
        .unwrap()
    }

    fn live_request_for(
        codec: &CursorCodec,
        authorization: &ChangeAuthorization,
        pending: &[&str],
    ) -> ChangeBatchRequest {
        ChangeBatchRequest {
            tables: authorization
                .table_starts
                .keys()
                .map(|table_id| TableSubscription {
                    table_id: table_id.clone(),
                    snapshot_cursor: None,
                })
                .collect(),
            pending_mutation_ids: pending
                .iter()
                .map(|id| MutationId::new(*id).unwrap())
                .collect(),
            ..live_request(codec, 0)
        }
    }

    fn batch_end(batch: &ChangeBatch) -> BatchEnd {
        match frames(&batch.to_bytes()).pop().unwrap().body {
            Some(change_stream_frame::Body::BatchEnd(end)) => end,
            _ => panic!("batch must end with its end"),
        }
    }

    #[test]
    fn one_scope_shares_each_transaction_encoding_and_keeps_acknowledgements_its_own() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1, &["tasks"], 8)).unwrap();
        let mut acknowledged = transaction(2, &["users", "tasks"], 8);
        acknowledged.mutation_ids = vec![MutationId::new("mutation-owned").unwrap()];
        log.append(acknowledged).unwrap();
        log.append(transaction(3, &["secrets"], 8)).unwrap();
        log.append(transaction(4, &["users"], 8)).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let scope = scope_for([table("tasks"), table("users")]);
        let owner = live_authorization(&codec, scope.clone(), &["mutation-owned"]);
        let other = live_authorization(&codec, scope, &[]);
        let encoding = ScopeEncoding::new(ScopeEncodingKey::of(&owner));
        assert_eq!(ScopeEncodingKey::of(&other), *encoding.key());

        let owner_batch = ChangeBatchBuilder::new(&log, &owner)
            .unwrap()
            .with_shared_encoding(&encoding)
            .unwrap()
            .build_at_with_stream_id(
                &live_request_for(&codec, &owner, &["mutation-owned"]),
                9_999,
                vec![1; 16],
            )
            .unwrap();
        let encoded = encoding.encoded_bytes();
        assert!(encoded > 0);
        let other_request = live_request_for(&codec, &other, &[]);
        let other_batch = ChangeBatchBuilder::new(&log, &other)
            .unwrap()
            .with_shared_encoding(&encoding)
            .unwrap()
            .build_at_with_stream_id(&other_request, 9_999, vec![2; 16])
            .unwrap();
        let direct = ChangeBatchBuilder::new(&log, &other)
            .unwrap()
            .build_at_with_stream_id(&other_request, 9_999, vec![2; 16])
            .unwrap();

        // The second build encoded nothing: its transaction frames are the
        // first build's buffers, and its whole batch is the bytes a build of
        // its own would have sent.
        assert_eq!(encoding.encoded_bytes(), encoded);
        let shared = |batch: &ChangeBatch| {
            batch.chunks[1..batch.chunks.len() - 1]
                .iter()
                .map(|chunk| chunk.as_ptr())
                .collect::<Vec<_>>()
        };
        assert_eq!(shared(&owner_batch).len(), 3);
        assert_eq!(shared(&owner_batch), shared(&other_batch));
        assert_eq!(other_batch.to_bytes(), direct.to_bytes());
        assert_eq!(other_batch.relevant_transactions, 3);
        assert_eq!(other_batch.relevant_operations, 4);

        assert_eq!(
            batch_end(&owner_batch).acknowledged_mutation_ids,
            ["mutation-owned"]
        );
        assert!(batch_end(&other_batch).acknowledged_mutation_ids.is_empty());

        // An evicted frame stays counted while a batch still carries it.
        encoding.evict_through(4);
        assert_eq!(encoding.encoded_bytes(), encoded);
        drop(owner_batch);
        assert_eq!(encoding.encoded_bytes(), encoded);
        drop(other_batch);
        assert_eq!(encoding.encoded_bytes(), 0);
    }

    #[test]
    fn a_build_refuses_the_encoding_of_another_scope_table_set_or_source_epoch() {
        let directory = TempDir::new().unwrap();
        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let both = scope_for([table("tasks"), table("users")]);
        let authorization = live_authorization(&codec, both.clone(), &[]);
        let refused = |encoding: &ScopeEncoding, authorization: &ChangeAuthorization| {
            matches!(
                ChangeBatchBuilder::new(&log, authorization)
                    .unwrap()
                    .with_shared_encoding(encoding),
                Err(ChangeBatchError::SharedEncodingMismatch)
            )
        };
        let encoding = ScopeEncoding::new(ScopeEncodingKey::of(&authorization));
        assert!(!refused(&encoding, &authorization));

        // Another scope over the same table.
        let projected = live_authorization(&codec, projected_scope(), &[]);
        let tasks_only = live_authorization(&codec, scope_for([table("tasks")]), &[]);
        let projected_key = ScopeEncodingKey::of(&projected);
        let projected_encoding = ScopeEncoding::new(projected_key);
        assert!(refused(&projected_encoding, &tasks_only));
        assert!(refused(&encoding, &projected));

        // The same scope with fewer tables subscribed.
        let mut fewer = authorization.clone();
        fewer.table_starts.remove(&table("users"));
        assert!(refused(&encoding, &fewer));

        // The same scope and tables in another source epoch.
        let mut elsewhere = authorization.clone();
        elsewhere.source_epoch = SourceEpoch::new("epoch-2").unwrap();
        assert!(refused(
            &ScopeEncoding::new(ScopeEncodingKey::of(&elsewhere)),
            &authorization
        ));
    }

    #[test]
    fn a_request_whose_tables_start_past_its_cursor_is_encoded_for_itself() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1, &["tasks", "users"], 1)).unwrap();
        log.append(transaction(2, &["tasks", "users"], 1)).unwrap();
        let codec = CursorCodec::new([7; 32]).unwrap();
        let authorization = authorization(&codec, 2);
        let encoding = ScopeEncoding::new(ScopeEncodingKey::of(&authorization));
        let request = request(&codec, 2);
        let shared = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .with_shared_encoding(&encoding)
            .unwrap()
            .build_at_with_stream_id(&request, 9_999, vec![1; 16])
            .unwrap();
        let direct = ChangeBatchBuilder::new(&log, &authorization)
            .unwrap()
            .build_at_with_stream_id(&request, 9_999, vec![1; 16])
            .unwrap();
        // Tasks starts at 1, so the first transaction carries users alone.
        assert_eq!(shared.relevant_operations, 3);
        assert_eq!(shared.to_bytes(), direct.to_bytes());
        assert_eq!(encoding.encoded_bytes(), 0);
    }
}

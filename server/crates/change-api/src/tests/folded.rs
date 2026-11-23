//! Fenced batches built for a request that accepts compacted batches.

use super::receiver::{receive, Tables};
use super::*;
use crate::fold::tests::Draw;

/// The tasks table: an int8 key, column 1, and two int8 columns, all of
/// them permitted.
fn tasks_scope() -> AuthorizedScope {
    let mut registered = registered_table(table("tasks"));
    registered.column_ids = (1..=3).map(ColumnId::new).collect();
    let key = registered.inspection.columns[0].clone();
    for attribute_number in 2..=3 {
        registered.inspection.columns.push(ColumnInspection {
            attribute_number,
            name: format!("value_{attribute_number}"),
            primary_key_ordinal: None,
            ..key.clone()
        });
    }
    AuthorizationPolicy::compile(
        PolicyDefinition {
            generation: 7,
            tables: vec![TablePermitDefinition {
                table_id: registered.table_id.clone(),
                permitted_columns: registered.column_ids.clone(),
            }],
        },
        std::slice::from_ref(&registered),
    )
    .unwrap()
    .scope()
}

fn mutation_ids(ids: &[&str]) -> BTreeSet<MutationId> {
    ids.iter().map(|id| MutationId::new(*id).unwrap()).collect()
}

fn fenced_authorization(
    codec: &CursorCodec,
    fence: u64,
    acknowledgeable: &[&str],
) -> ChangeAuthorization {
    fenced_authorization_with_barrier(codec, fence, acknowledgeable, None)
}

fn fenced_authorization_with_barrier(
    codec: &CursorCodec,
    fence: u64,
    acknowledgeable: &[&str],
    barrier_at: Option<u64>,
) -> ChangeAuthorization {
    let scope = tasks_scope();
    let revocation_barrier = barrier_at.map(|sequence| minted_barrier(codec, &scope, sequence));
    let pending = mutation_ids(acknowledgeable);
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
    ChangeAuthorization::new(ScopedChangePlan {
        plan_id: plan_id(),
        activated_scope: test_activated_scope(codec, scope),
        expires_at_unix_micros: 10_000,
        from_cursor_exclusive: cursor_at(codec, 0),
        through_cursor_inclusive: cursor_at(codec, fence),
        table_starts: BTreeMap::from([(table("tasks"), Some(cursor_at(codec, 0)))]),
        acknowledgeable_mutation_ids,
        revocation_barrier,
    })
    .unwrap()
}

fn folded_request(codec: &CursorCodec, from: u64, fence: u64) -> ChangeBatchRequest {
    ChangeBatchRequest {
        from_cursor_exclusive: cursor_at(codec, from),
        tables: vec![TableSubscription {
            table_id: table("tasks"),
            snapshot_cursor: Some(cursor_at(codec, 0)),
        }],
        accept_compacted_batches: true,
        ..request(codec, fence)
    }
}

fn key(id: i64) -> Vec<u8> {
    encode_primary_key(&PrimaryKey {
        codec_version: sync_protocol::PRIMARY_KEY_CODEC_VERSION,
        parts: vec![WireColumnValue {
            column_id: 1,
            value: Some(WireCellValue {
                kind: Some(cell_value::Kind::Int64Value(id)),
            }),
        }],
    })
    .unwrap()
}

fn operation(id: i64, kind: CanonicalOperationKind) -> CanonicalOperation {
    CanonicalOperation {
        table_id: table("tasks"),
        primary_key: key(id),
        kind,
    }
}

fn insert(id: i64, title: i64, done: i64) -> CanonicalOperation {
    operation(
        id,
        CanonicalOperationKind::InsertFullRow {
            columns: vec![
                column(1, CellValue::Int64(id)),
                column(2, CellValue::Int64(title)),
                column(3, CellValue::Int64(done)),
            ],
        },
    )
}

fn patch(id: i64, columns: &[(u32, i64)]) -> CanonicalOperation {
    operation(
        id,
        CanonicalOperationKind::UpdatePatch {
            changed_columns: columns
                .iter()
                .map(|(column_id, value)| column(*column_id, CellValue::Int64(*value)))
                .collect(),
        },
    )
}

fn delete(id: i64) -> CanonicalOperation {
    operation(id, CanonicalOperationKind::DeleteByKey)
}

fn truncate() -> CanonicalOperation {
    CanonicalOperation {
        table_id: table("tasks"),
        primary_key: Vec::new(),
        kind: CanonicalOperationKind::TruncateAuthorizedTable {
            reason: "truncate".to_owned(),
        },
    }
}

/// A transaction on a table the scope does not permit.
fn elsewhere() -> CanonicalOperation {
    CanonicalOperation {
        table_id: table("secrets"),
        ..truncate()
    }
}

fn committed(sequence: u64, operations: Vec<CanonicalOperation>) -> CanonicalTransaction {
    let mut transaction = transaction(sequence, &[], 0);
    transaction.operations = operations;
    transaction
}

/// A log held in memory, since the file log syncs each append and these
/// tests write hundreds of logs.
struct MemoryLog {
    source_id: SourceId,
    source_epoch: SourceEpoch,
    records: Vec<CanonicalRecord>,
}

impl MemoryLog {
    /// `transactions` committed at sequences 1, 2, and so on.
    fn new(transactions: impl IntoIterator<Item = CanonicalTransaction>) -> Self {
        let records = transactions
            .into_iter()
            .enumerate()
            .map(|(index, transaction)| CanonicalRecord {
                position: position(index as u64 + 1),
                entry: CanonicalEntry::Transaction(transaction),
            })
            .collect();
        Self {
            source_id: source_id(),
            source_epoch: epoch(),
            records,
        }
    }
}

fn position(sequence: u64) -> CursorPosition {
    CursorPosition {
        source_epoch: epoch(),
        sequence,
    }
}

impl CanonicalLogRead for MemoryLog {
    fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        Ok(position(self.records.len() as u64))
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        Ok(position(1))
    }

    fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        _max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        let through = through_sequence_inclusive.unwrap_or(u64::MAX);
        let records = self
            .records
            .iter()
            .filter(|record| {
                (from_sequence_exclusive + 1..=through).contains(&record.position.sequence)
            })
            .take(max_records)
            .cloned()
            .collect::<Vec<_>>();
        Ok(CanonicalPage {
            stored_bytes: records.len() as u64,
            records,
        })
    }
}

fn log_of(transactions: &[Vec<CanonicalOperation>]) -> MemoryLog {
    MemoryLog::new(
        transactions
            .iter()
            .enumerate()
            .map(|(index, operations)| committed(index as u64 + 1, operations.clone())),
    )
}

/// One batch as a receiver reads it.
struct Read {
    through: u64,
    fragments: Vec<TransactionFragment>,
    end: BatchEnd,
}

impl Read {
    fn operations(&self) -> impl Iterator<Item = &Operation> {
        self.fragments
            .iter()
            .flat_map(|fragment| &fragment.operations)
    }

    fn transactions(&self) -> usize {
        self.fragments
            .iter()
            .filter(|fragment| fragment.first_fragment)
            .count()
    }
}

/// The batches of a fenced read from cursor 0 to `fence`, each requested
/// where the one before it ended.
fn read_to_fence(
    log: &MemoryLog,
    authorization: &ChangeAuthorization,
    codec: &CursorCodec,
    fence: u64,
    shape: impl Fn(ChangeBatchRequest) -> ChangeBatchRequest,
) -> Vec<Read> {
    let builder = ChangeBatchBuilder::new(log, authorization).unwrap();
    let mut from = 0;
    let mut reads = Vec::new();
    loop {
        let request = shape(folded_request(codec, from, fence));
        let batch = builder.build_at(&request, 9_999).unwrap();
        assert!(batch.stats.encoded_bytes <= request.max_batch_bytes as u64);
        let through = codec
            .decode(&batch.through_cursor_inclusive)
            .unwrap()
            .sequence;
        assert!(through > from, "the batch from {from} covers nothing");
        let mut fragments = Vec::new();
        let mut end = None;
        for frame in frames(&batch.to_bytes()) {
            match frame.body {
                Some(change_stream_frame::Body::TransactionFragment(fragment)) => {
                    fragments.push(fragment)
                }
                Some(change_stream_frame::Body::BatchEnd(batch_end)) => end = Some(batch_end),
                _ => {}
            }
        }
        let read = Read {
            through,
            fragments,
            end: end.unwrap(),
        };
        assert_eq!(
            read.end.relevant_operation_count,
            read.operations().count() as u64
        );
        assert_eq!(
            read.end.relevant_transaction_count,
            read.transactions() as u64
        );
        reads.push(read);
        from = through;
        if batch.reached_requested_fence {
            return reads;
        }
    }
}

/// Where the committed transactions through `through` leave a receiver.
fn committed_through(transactions: &[Vec<CanonicalOperation>], through: u64) -> Tables {
    let mut tables = Tables::new();
    for operation in transactions[..through as usize]
        .iter()
        .flatten()
        .filter(|operation| operation.table_id == table("tasks"))
    {
        receive(&mut tables, &wire_operation(operation.clone()).unwrap()).unwrap();
    }
    tables
}

/// Transactions drawn against the rows they leave: inserts of absent rows,
/// patches of present ones, some of them carrying the key's own value as a
/// full-row patch does, deletes, now and then a moved key or a truncate, and
/// transactions on a table out of scope.
fn workload(draw: &mut Draw) -> Vec<Vec<CanonicalOperation>> {
    let mut present = BTreeSet::<i64>::new();
    let mut transactions = Vec::new();
    for _ in 0..1 + draw.below(40) {
        let mut operations = Vec::new();
        match draw.below(25) {
            0 => {
                operations.push(truncate());
                present.clear();
            }
            1..=4 => operations.push(elsewhere()),
            _ => {
                for _ in 0..1 + draw.below(4) {
                    let id = draw.below(8) as i64;
                    if !present.contains(&id) {
                        operations.push(insert(id, draw.below(100) as i64, 0));
                        present.insert(id);
                        continue;
                    }
                    match draw.below(12) {
                        0..=1 => {
                            operations.push(delete(id));
                            present.remove(&id);
                        }
                        2 => {
                            let to = 8 + draw.below(8) as i64;
                            if present.insert(to) {
                                operations.push(patch(id, &[(1, to)]));
                                present.remove(&id);
                            }
                        }
                        3 => operations
                            .push(patch(id, &[(1, id), (2, draw.below(100) as i64), (3, 1)])),
                        _ => {
                            let column_id = 2 + draw.below(2) as u32;
                            operations.push(patch(id, &[(column_id, draw.below(100) as i64)]));
                        }
                    }
                }
            }
        }
        transactions.push(operations);
    }
    transactions
}

#[test]
fn each_folded_batch_leaves_a_receiver_where_the_transactions_it_covers_leave_it() {
    let codec = CursorCodec::new([7; 32]).unwrap();
    let (mut folded_operations, mut committed_operations) = (0, 0);
    for seed in 1..=1_000_u64 {
        let mut draw = Draw(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let transactions = workload(&mut draw);
        let fence = transactions.len() as u64;
        let log = log_of(&transactions);
        let authorization = fenced_authorization(&codec, fence, &[]);
        // A batch reserves 4 KiB for its end, so this leaves room for a few
        // transactions, and most reads take several batches.
        let max_batch_bytes = 5_500 + draw.below(2_000) as usize;
        let reads = read_to_fence(&log, &authorization, &codec, fence, |request| {
            ChangeBatchRequest {
                max_batch_bytes,
                ..request
            }
        });
        let mut received = Tables::new();
        for read in &reads {
            assert!(read.transactions() <= 1, "seed {seed}");
            for operation in read.operations() {
                receive(&mut received, operation)
                    .unwrap_or_else(|()| panic!("seed {seed} refused {operation:?}"));
            }
            assert_eq!(
                received,
                committed_through(&transactions, read.through),
                "seed {seed} at {}",
                read.through
            );
            folded_operations += read.operations().count();
        }
        let committed = read_to_fence(&log, &authorization, &codec, fence, |request| {
            ChangeBatchRequest {
                max_batch_bytes,
                accept_compacted_batches: false,
                ..request
            }
        });
        committed_operations += committed
            .iter()
            .map(|read| read.operations().count())
            .sum::<usize>();
    }
    // Batches this small fold a few transactions each, so a third fewer
    // operations shows the folds ran.
    assert!(folded_operations * 3 < committed_operations * 2);
}

#[test]
fn a_hot_row_is_sent_once_with_the_last_transaction_as_its_provenance() {
    let codec = CursorCodec::new([7; 32]).unwrap();
    let mut transactions = vec![vec![insert(1, 0, 0)]];
    transactions.extend((1..200).map(|title| vec![patch(1, &[(2, title)])]));
    let log = log_of(&transactions);
    let authorization = fenced_authorization(&codec, 200, &[]);
    let reads = read_to_fence(&log, &authorization, &codec, 200, |request| request);
    assert_eq!(reads.len(), 1);
    let [fragment] = reads[0].fragments.as_slice() else {
        panic!("one fragment carries the fold");
    };
    assert_eq!(fragment.transaction_id, b"tx-200");
    assert_eq!(
        fragment.transaction_cursor,
        Some(cursor(&cursor_at(&codec, 200)))
    );
    assert_eq!(fragment.commit_lsn, "0/C9");
    assert_eq!(
        fragment.operations,
        [wire_operation(insert(1, 199, 0)).unwrap()]
    );
    assert!(reads[0].end.reached_requested_fence);
}

#[test]
fn a_truncate_or_a_moved_key_is_sent_alone_as_committed() {
    let codec = CursorCodec::new([7; 32]).unwrap();
    let transactions = vec![
        vec![insert(1, 0, 0)],
        vec![patch(1, &[(2, 1)])],
        vec![truncate()],
        vec![insert(2, 0, 0)],
        vec![patch(2, &[(1, 3)])],
        vec![patch(3, &[(2, 1)])],
        vec![patch(3, &[(2, 2)])],
    ];
    let log = log_of(&transactions);
    let authorization = fenced_authorization(&codec, 7, &[]);
    let reads = read_to_fence(&log, &authorization, &codec, 7, |request| request);
    assert_eq!(
        reads.iter().map(|read| read.through).collect::<Vec<_>>(),
        [2, 3, 4, 5, 7]
    );
    assert_eq!(
        reads
            .iter()
            .map(|read| read.operations().cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        [
            vec![wire_operation(insert(1, 1, 0)).unwrap()],
            vec![wire_operation(truncate()).unwrap()],
            vec![wire_operation(insert(2, 0, 0)).unwrap()],
            vec![wire_operation(patch(2, &[(1, 3)])).unwrap()],
            vec![wire_operation(patch(3, &[(2, 2)])).unwrap()],
        ]
    );
}

#[test]
fn a_batch_ends_before_the_transaction_that_would_outgrow_it() {
    let codec = CursorCodec::new([7; 32]).unwrap();
    // Each row outweighs the room a batch keeps for its end, so a bound that
    // missed one would show.
    let transactions = (1..=4)
        .map(|id| {
            vec![operation(
                id,
                CanonicalOperationKind::InsertFullRow {
                    columns: vec![
                        column(1, CellValue::Int64(id)),
                        column(2, CellValue::Text("x".repeat(5_000))),
                        column(3, CellValue::Int64(0)),
                    ],
                },
            )]
        })
        .collect::<Vec<_>>();
    let log = log_of(&transactions);
    let authorization = fenced_authorization(&codec, 4, &[]);
    let reads = read_to_fence(&log, &authorization, &codec, 4, |request| {
        ChangeBatchRequest {
            max_batch_bytes: 16 * 1024,
            ..request
        }
    });
    assert_eq!(
        reads.iter().map(|read| read.through).collect::<Vec<_>>(),
        [2, 4]
    );
}

#[test]
fn transactions_a_receiver_refuses_are_sent_as_committed_for_it_to_refuse() {
    let codec = CursorCodec::new([7; 32]).unwrap();
    let transactions = vec![
        vec![insert(1, 0, 0)],
        vec![patch(1, &[(2, 1)])],
        vec![insert(1, 2, 0)],
    ];
    let log = log_of(&transactions);
    let authorization = fenced_authorization(&codec, 3, &[]);
    let reads = read_to_fence(&log, &authorization, &codec, 3, |request| request);
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].transactions(), 3);
    assert_eq!(
        reads[0].operations().cloned().collect::<Vec<_>>(),
        transactions
            .into_iter()
            .flatten()
            .map(|operation| wire_operation(operation).unwrap())
            .collect::<Vec<_>>()
    );
}

#[test]
fn rows_the_batch_inserts_and_deletes_send_nothing_and_still_move_the_cursor() {
    let codec = CursorCodec::new([7; 32]).unwrap();
    let transactions = vec![vec![insert(1, 0, 0)], vec![elsewhere()], vec![delete(1)]];
    let log = log_of(&transactions);
    let authorization = fenced_authorization(&codec, 3, &[]);
    let reads = read_to_fence(&log, &authorization, &codec, 3, |request| request);
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].through, 3);
    assert!(reads[0].fragments.is_empty());
    assert!(reads[0].end.reached_requested_fence);
}

#[test]
fn a_folded_batch_acknowledges_the_mutations_of_every_transaction_it_covers() {
    let codec = CursorCodec::new([7; 32]).unwrap();
    let log = MemoryLog::new(
        [
            (1, vec![insert(1, 0, 0)], "mutation-1"),
            (2, vec![elsewhere()], "mutation-2"),
            (3, vec![patch(1, &[(2, 1)])], "mutation-3"),
        ]
        .map(|(sequence, operations, mutation)| CanonicalTransaction {
            mutation_ids: vec![MutationId::new(mutation).unwrap()],
            ..committed(sequence, operations)
        }),
    );
    let authorization =
        fenced_authorization(&codec, 3, &["mutation-1", "mutation-2", "mutation-3"]);
    let reads = read_to_fence(&log, &authorization, &codec, 3, |request| {
        ChangeBatchRequest {
            pending_mutation_ids: mutation_ids(&["mutation-1", "mutation-2", "mutation-3"]),
            ..request
        }
    });
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].transactions(), 1);
    assert_eq!(
        reads[0].end.acknowledged_mutation_ids,
        ["mutation-1", "mutation-2", "mutation-3"]
    );
}

#[test]
fn a_live_request_or_one_without_fragmentation_cannot_ask_for_compacted_batches() {
    let codec = CursorCodec::new([7; 32]).unwrap();
    let mut compacted = wire_request(&codec);
    compacted.accept_compacted_batches = true;
    assert!(
        parse_change_request(compacted.clone())
            .unwrap()
            .request()
            .accept_compacted_batches
    );
    let mut live = compacted.clone();
    live.through_cursor_inclusive = None;
    let mut unfragmented = compacted;
    unfragmented.accept_transaction_fragmentation = false;
    for request in [live, unfragmented] {
        assert!(matches!(
            parse_change_request(request),
            Err(ChangeBatchError::InvalidLimits)
        ));
    }
}

#[test]
fn a_folded_batch_ends_on_the_revocation_barrier_and_folds_nothing_after_it() {
    let codec = CursorCodec::new([7; 32]).unwrap();
    let transactions = [
        vec![insert(1, 10, 0)],
        vec![patch(1, &[(2, 11)])],
        vec![insert(2, 20, 0)],
        vec![patch(1, &[(3, 1)]), delete(2)],
    ];
    let log = log_of(&transactions);
    let authorization = fenced_authorization_with_barrier(&codec, 4, &[], Some(3));
    let batch = ChangeBatchBuilder::new(&log, &authorization)
        .unwrap()
        .build_at(&folded_request(&codec, 0, 4), 9_999)
        .unwrap();

    assert!(batch.revocation_barrier_emitted);
    assert!(!batch.reached_requested_fence);
    assert_eq!(
        codec
            .decode(&batch.through_cursor_inclusive)
            .unwrap()
            .sequence,
        3
    );
    let frames = frames(&batch.to_bytes());
    let mut operations = Vec::new();
    let mut bodies = Vec::new();
    for frame in &frames {
        bodies.push(match frame.body.as_ref() {
            Some(change_stream_frame::Body::BatchStart(_)) => "start",
            Some(change_stream_frame::Body::TransactionFragment(fragment)) => {
                operations.extend(fragment.operations.iter().cloned());
                "fragment"
            }
            Some(change_stream_frame::Body::RevocationBarrier(_)) => "barrier",
            Some(change_stream_frame::Body::BatchEnd(_)) => "end",
            _ => "other",
        });
    }
    assert_eq!(bodies, ["start", "fragment", "barrier", "end"]);
    assert_eq!(batch.relevant_transactions, 1);
    let mut received = Tables::new();
    for operation in &operations {
        receive(&mut received, operation).unwrap();
    }
    assert_eq!(received, committed_through(&transactions, 3));
}

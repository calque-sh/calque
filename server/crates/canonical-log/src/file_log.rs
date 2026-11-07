//! The canonical log on the local filesystem: one immutable checksummed JSON
//! segment per committed transaction or barrier, under a JSON manifest. The
//! `calque` binary uses the PostgreSQL log; this one backs tests that need a
//! canonical log without a database.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use super::*;

pub(super) const FORMAT_VERSION: u32 = 4;
const MANIFEST_FILE: &str = "manifest.json";
pub(super) const SEGMENTS_DIRECTORY: &str = "segments";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Manifest {
    pub(super) format_version: u32,
    source_id: SourceId,
    source_epoch: SourceEpoch,
    minimum_retained_sequence: u64,
    head_sequence: u64,
    pub(super) segments: Vec<SegmentMetadata>,
    retired_transactions: Vec<RetiredTransactionMetadata>,
    #[serde(default)]
    retention_leases: Vec<RetentionLease>,
    #[serde(default)]
    next_snapshot_build_lease_token: i64,
    #[serde(default)]
    snapshot_build_retention_leases: Vec<FileSnapshotBuildRetentionLease>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RetentionLease {
    plan_id: PlanId,
    minimum_sequence: u64,
    expires_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FileSnapshotBuildRetentionLease {
    build_id: SnapshotBuildId,
    owner_id: SnapshotBuildOwnerId,
    token: i64,
    minimum_sequence: u64,
    expires_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct SegmentMetadata {
    sequence: u64,
    pub(super) file_name: String,
    bytes: u64,
    sha256: String,
    transaction_key: TransactionKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RetiredTransactionMetadata {
    original_sequence: u64,
    transaction_key: TransactionKey,
    entry_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SegmentEnvelope {
    format_version: u32,
    record: CanonicalRecord,
    record_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DurabilityPoint {
    SegmentFileSynced,
    SegmentPublished,
    SegmentDirectorySynced,
    ManifestFileSynced,
    ManifestPublished,
    ManifestDirectorySynced,
    RetentionManifestCommitted,
    RetentionFileDeleted(u64),
    RetentionDirectorySynced,
}

pub struct FileCanonicalLog {
    root: PathBuf,
    pub(super) manifest: Manifest,
    transaction_index: BTreeMap<String, usize>,
    retired_transaction_index: BTreeMap<String, usize>,
}

impl FileCanonicalLog {
    pub fn open(
        root: impl AsRef<Path>,
        source_id: SourceId,
        source_epoch: SourceEpoch,
    ) -> Result<Self, LogError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join(SEGMENTS_DIRECTORY))?;

        let manifest_path = root.join(MANIFEST_FILE);
        let manifest = if manifest_path.exists() {
            let bytes = read_limited(&manifest_path)?;
            let manifest: Manifest = serde_json::from_slice(&bytes)?;
            if manifest.format_version != FORMAT_VERSION {
                return Err(LogError::ManifestCorrupt(format!(
                    "unsupported format version {}",
                    manifest.format_version
                )));
            }
            if manifest.source_id != source_id {
                return Err(LogError::ManifestCorrupt(format!(
                    "source ID is {}, expected {}",
                    manifest.source_id, source_id
                )));
            }
            if manifest.source_epoch != source_epoch {
                return Err(LogError::EpochMismatch {
                    expected: source_epoch,
                    actual: manifest.source_epoch,
                });
            }
            manifest
        } else {
            let manifest = Manifest {
                format_version: FORMAT_VERSION,
                source_id,
                source_epoch,
                minimum_retained_sequence: 1,
                head_sequence: 0,
                segments: Vec::new(),
                retired_transactions: Vec::new(),
                retention_leases: Vec::new(),
                next_snapshot_build_lease_token: 0,
                snapshot_build_retention_leases: Vec::new(),
            };
            write_manifest(&root, &manifest)?;
            manifest
        };

        let mut log = Self {
            root,
            manifest,
            transaction_index: BTreeMap::new(),
            retired_transaction_index: BTreeMap::new(),
        };
        log.validate_and_index()?;
        log.recover_unindexed_tail()?;
        Ok(log)
    }

    pub fn source_id(&self) -> &SourceId {
        &self.manifest.source_id
    }

    pub fn source_epoch(&self) -> &SourceEpoch {
        &self.manifest.source_epoch
    }

    pub fn head(&self) -> CursorPosition {
        CursorPosition {
            source_epoch: self.manifest.source_epoch.clone(),
            sequence: self.manifest.head_sequence,
        }
    }

    pub fn minimum_retained(&self) -> CursorPosition {
        CursorPosition {
            source_epoch: self.manifest.source_epoch.clone(),
            sequence: self.manifest.minimum_retained_sequence,
        }
    }

    /// Durably pins canonical history needed by an issued bootstrap plan.
    ///
    /// `minimum_sequence` is the first record the client may need, normally
    /// `min(P_i, resident_cursor) + 1`. Duplicate plan IDs are idempotent only
    /// for the exact same range and expiry.
    pub fn acquire_retention_lease(
        &mut self,
        plan_id: PlanId,
        minimum_sequence: u64,
        expires_ms: i64,
        now_ms: i64,
    ) -> Result<(), LogError> {
        if expires_ms <= now_ms
            || minimum_sequence < self.manifest.minimum_retained_sequence
            || minimum_sequence > self.manifest.head_sequence.saturating_add(1)
        {
            return Err(LogError::InvalidRetentionLease);
        }
        let mut next = self.manifest.clone();
        next.retention_leases
            .retain(|lease| lease.expires_ms > now_ms);
        if let Some(existing) = next
            .retention_leases
            .iter()
            .find(|lease| lease.plan_id == plan_id)
        {
            if existing.minimum_sequence == minimum_sequence && existing.expires_ms == expires_ms {
                return Ok(());
            }
            return Err(LogError::RetentionLeaseConflict);
        }
        next.retention_leases.push(RetentionLease {
            plan_id,
            minimum_sequence,
            expires_ms,
        });
        next.retention_leases
            .sort_by(|left, right| left.plan_id.cmp(&right.plan_id));
        write_manifest(&self.root, &next)?;
        self.manifest = next;
        Ok(())
    }

    pub(super) fn acquire_snapshot_build_retention_at(
        &mut self,
        build_id: SnapshotBuildId,
        owner_id: SnapshotBuildOwnerId,
        ttl: Duration,
        now_ms: i64,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        let expires_ms = snapshot_build_lease_expiry(now_ms, ttl)?;
        let mut next = self.manifest.clone();
        next.snapshot_build_retention_leases
            .retain(|lease| lease.expires_ms > now_ms);
        if let Some(existing) = next
            .snapshot_build_retention_leases
            .iter()
            .find(|lease| lease.build_id == build_id)
        {
            if existing.owner_id != owner_id {
                return Err(LogError::SnapshotBuildRetentionLeaseConflict);
            }
            let token = next
                .next_snapshot_build_lease_token
                .checked_add(1)
                .filter(|token| *token > 0)
                .ok_or(LogError::SnapshotBuildRetentionTokenExhausted)?;
            let index = next
                .snapshot_build_retention_leases
                .iter()
                .position(|lease| lease.build_id == build_id)
                .ok_or(LogError::InvalidSnapshotBuildRetentionLease)?;
            next.next_snapshot_build_lease_token = token;
            next.snapshot_build_retention_leases[index].token = token;
            next.snapshot_build_retention_leases[index].expires_ms = expires_ms;
            let capability = file_snapshot_build_lease_capability(
                &next,
                &next.snapshot_build_retention_leases[index],
            );
            write_manifest(&self.root, &next)?;
            self.manifest = next;
            return Ok(capability);
        }
        let token = next
            .next_snapshot_build_lease_token
            .checked_add(1)
            .filter(|token| *token > 0)
            .ok_or(LogError::SnapshotBuildRetentionTokenExhausted)?;
        let minimum_sequence = next
            .head_sequence
            .checked_add(1)
            .ok_or(LogError::SequenceExhausted)?;
        if minimum_sequence < next.minimum_retained_sequence {
            return Err(LogError::InvalidSnapshotBuildRetentionLease);
        }
        next.next_snapshot_build_lease_token = token;
        next.snapshot_build_retention_leases
            .push(FileSnapshotBuildRetentionLease {
                build_id: build_id.clone(),
                owner_id: owner_id.clone(),
                token,
                minimum_sequence,
                expires_ms,
            });
        next.snapshot_build_retention_leases
            .sort_by(|left, right| left.build_id.cmp(&right.build_id));
        write_manifest(&self.root, &next)?;
        self.manifest = next;
        Ok(SnapshotBuildRetentionLease {
            source_id: self.manifest.source_id.clone(),
            source_epoch: self.manifest.source_epoch.clone(),
            build_id,
            owner_id,
            token,
            minimum_sequence,
            expires_at_unix_millis: expires_ms,
        })
    }

    pub(super) fn renew_snapshot_build_retention_at(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
        ttl: Duration,
        now_ms: i64,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        if lease.source_id != self.manifest.source_id
            || lease.source_epoch != self.manifest.source_epoch
        {
            return Err(LogError::SnapshotBuildRetentionLeaseRejected);
        }
        let expires_ms = snapshot_build_lease_expiry(now_ms, ttl)?;
        let mut next = self.manifest.clone();
        let Some(stored) = next
            .snapshot_build_retention_leases
            .iter_mut()
            .find(|stored| stored.build_id == lease.build_id)
        else {
            return Err(LogError::SnapshotBuildRetentionLeaseRejected);
        };
        if stored.owner_id != lease.owner_id
            || stored.token != lease.token
            || stored.minimum_sequence != lease.minimum_sequence
            || stored.expires_ms <= now_ms
        {
            return Err(LogError::SnapshotBuildRetentionLeaseRejected);
        }
        stored.expires_ms = expires_ms;
        write_manifest(&self.root, &next)?;
        self.manifest = next;
        let mut renewed = lease.clone();
        renewed.expires_at_unix_millis = expires_ms;
        Ok(renewed)
    }

    pub(super) fn release_snapshot_build_retention_at(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
        now_ms: i64,
    ) -> Result<(), LogError> {
        if lease.source_id != self.manifest.source_id
            || lease.source_epoch != self.manifest.source_epoch
        {
            return Err(LogError::SnapshotBuildRetentionLeaseRejected);
        }
        let mut next = self.manifest.clone();
        let Some(index) = next
            .snapshot_build_retention_leases
            .iter()
            .position(|stored| {
                stored.build_id == lease.build_id
                    && stored.owner_id == lease.owner_id
                    && stored.token == lease.token
                    && stored.minimum_sequence == lease.minimum_sequence
                    && stored.expires_ms > now_ms
            })
        else {
            return Err(LogError::SnapshotBuildRetentionLeaseRejected);
        };
        next.snapshot_build_retention_leases.remove(index);
        write_manifest(&self.root, &next)?;
        self.manifest = next;
        Ok(())
    }

    /// Durably appends and indexes a complete committed transaction.
    ///
    /// A caller may acknowledge PostgreSQL progress only after this returns.
    pub fn append(&mut self, transaction: CanonicalTransaction) -> Result<AppendOutcome, LogError> {
        self.append_entry_with_hook(CanonicalEntry::Transaction(transaction), &mut |_| Ok(()))
    }

    /// Durably appends the dedicated marker transaction as a canonical barrier.
    pub fn append_barrier(&mut self, barrier: CanonicalBarrier) -> Result<AppendOutcome, LogError> {
        self.append_entry_with_hook(CanonicalEntry::Barrier(barrier), &mut |_| Ok(()))
    }

    #[cfg(test)]
    pub(super) fn append_with_hook<F>(
        &mut self,
        transaction: CanonicalTransaction,
        hook: &mut F,
    ) -> Result<AppendOutcome, LogError>
    where
        F: FnMut(DurabilityPoint) -> Result<(), LogError>,
    {
        self.append_entry_with_hook(CanonicalEntry::Transaction(transaction), hook)
    }

    fn append_entry_with_hook<F>(
        &mut self,
        entry: CanonicalEntry,
        hook: &mut F,
    ) -> Result<AppendOutcome, LogError>
    where
        F: FnMut(DurabilityPoint) -> Result<(), LogError>,
    {
        // A previous append can have durably published its segment while failing
        // to publish the manifest. Recover it before allocating another sequence.
        self.recover_unindexed_tail()?;

        if entry.key().source_epoch != self.manifest.source_epoch {
            return Err(LogError::EpochMismatch {
                expected: self.manifest.source_epoch.clone(),
                actual: entry.key().source_epoch.clone(),
            });
        }
        validate_entry(&entry)?;

        let transaction_key = transaction_key_string(entry.key())?;
        if let Some(index) = self
            .retired_transaction_index
            .get(&transaction_key)
            .copied()
        {
            let retired = &self.manifest.retired_transactions[index];
            if entry_sha256(&entry)? == retired.entry_sha256 {
                return Ok(AppendOutcome::AlreadyPresent(CursorPosition {
                    source_epoch: self.manifest.source_epoch.clone(),
                    sequence: retired.original_sequence,
                }));
            }
            return Err(LogError::DuplicateTransactionConflict);
        }
        if let Some(index) = self.transaction_index.get(&transaction_key).copied() {
            let metadata = &self.manifest.segments[index];
            let record = self.read_segment(metadata)?;
            if record.entry == entry {
                return Ok(AppendOutcome::AlreadyPresent(record.position));
            }
            return Err(LogError::DuplicateTransactionConflict);
        }

        let sequence = self
            .manifest
            .head_sequence
            .checked_add(1)
            .ok_or(LogError::SequenceExhausted)?;
        let position = CursorPosition {
            source_epoch: self.manifest.source_epoch.clone(),
            sequence,
        };
        let record = CanonicalRecord {
            position: position.clone(),
            entry,
        };
        let metadata = self.write_segment_with_hook(&record, hook)?;

        let mut next_manifest = self.manifest.clone();
        next_manifest.head_sequence = sequence;
        next_manifest.segments.push(metadata);
        write_manifest_with_hook(&self.root, &next_manifest, hook)?;
        self.manifest = next_manifest;
        self.transaction_index
            .insert(transaction_key, self.manifest.segments.len() - 1);

        Ok(AppendOutcome::Appended(position))
    }

    /// Reads a bounded page of complete canonical records.
    ///
    /// The page stops before either limit is exceeded. If the first eligible
    /// record alone exceeds `max_bytes`, the caller receives an explicit error
    /// rather than an empty page that could cause a retry loop.
    pub fn read_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<Vec<CanonicalRecord>, LogError> {
        Ok(self
            .read_page_after_bounded(
                from_sequence_exclusive,
                through_sequence_inclusive,
                max_records,
                max_bytes,
            )?
            .records)
    }

    /// Reads a bounded page and reports the exact checksummed segment bytes
    /// charged against a caller's cross-page scan budget.
    pub fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        if max_records == 0 || max_bytes == 0 {
            return Err(LogError::InvalidReadLimit);
        }
        if from_sequence_exclusive.saturating_add(1) < self.manifest.minimum_retained_sequence {
            return Err(LogError::BeforeRetention {
                requested: from_sequence_exclusive,
                minimum: self.manifest.minimum_retained_sequence,
            });
        }
        let through = through_sequence_inclusive
            .unwrap_or(self.manifest.head_sequence)
            .min(self.manifest.head_sequence);
        let mut records = Vec::new();
        let mut page_bytes = 0_u64;
        let first_eligible = self
            .manifest
            .segments
            .partition_point(|metadata| metadata.sequence <= from_sequence_exclusive);
        for metadata in &self.manifest.segments[first_eligible..] {
            if metadata.sequence <= through {
                if records.len() == max_records {
                    break;
                }
                let next_page_bytes = page_bytes
                    .checked_add(metadata.bytes)
                    .ok_or(LogError::RecordTooLarge)?;
                if next_page_bytes > max_bytes {
                    if records.is_empty() {
                        return Err(LogError::ReadLimitTooSmall {
                            next_record_bytes: metadata.bytes,
                            max_bytes,
                        });
                    }
                    break;
                }
                records.push(self.read_segment(metadata)?);
                page_bytes = next_page_bytes;
            } else {
                break;
            }
        }
        Ok(CanonicalPage {
            records,
            stored_bytes: page_bytes,
        })
    }

    #[cfg(test)]
    pub(super) fn read_after(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
    ) -> Result<Vec<CanonicalRecord>, LogError> {
        self.read_after_bounded(
            from_sequence_exclusive,
            through_sequence_inclusive,
            usize::MAX,
            u64::MAX,
        )
    }

    /// Advances the retention floor before deleting obsolete immutable files.
    /// Readers reopening after a crash therefore either retain extra bytes or
    /// observe the new floor; they never trust a range whose segment is absent.
    pub fn retain_from(&mut self, minimum_sequence: u64) -> Result<(), LogError> {
        self.retain_from_with_hook(minimum_sequence, &mut |_| Ok(()))
    }

    /// Deterministic clock variant for the retention owner and crash tests.
    pub fn retain_from_at(&mut self, minimum_sequence: u64, now_ms: i64) -> Result<(), LogError> {
        self.retain_from_at_with_hook(minimum_sequence, now_ms, &mut |_| Ok(()))
    }

    pub(super) fn retain_from_with_hook<F>(
        &mut self,
        minimum_sequence: u64,
        hook: &mut F,
    ) -> Result<(), LogError>
    where
        F: FnMut(DurabilityPoint) -> Result<(), LogError>,
    {
        let now_ms = current_time_millis()?;
        self.retain_from_at_with_hook(minimum_sequence, now_ms, hook)
    }

    fn retain_from_at_with_hook<F>(
        &mut self,
        minimum_sequence: u64,
        now_ms: i64,
        hook: &mut F,
    ) -> Result<(), LogError>
    where
        F: FnMut(DurabilityPoint) -> Result<(), LogError>,
    {
        let active_lease_floor = self
            .manifest
            .retention_leases
            .iter()
            .filter(|lease| lease.expires_ms > now_ms)
            .map(|lease| lease.minimum_sequence)
            .chain(
                self.manifest
                    .snapshot_build_retention_leases
                    .iter()
                    .filter(|lease| lease.expires_ms > now_ms)
                    .map(|lease| lease.minimum_sequence),
            )
            .min();
        let minimum_sequence = minimum_sequence
            .max(self.manifest.minimum_retained_sequence)
            .min(self.manifest.head_sequence.saturating_add(1));
        let minimum_sequence =
            active_lease_floor.map_or(minimum_sequence, |floor| minimum_sequence.min(floor));
        let obsolete_metadata: Vec<_> = self
            .manifest
            .segments
            .iter()
            .filter(|segment| segment.sequence < minimum_sequence)
            .cloned()
            .collect();
        let mut next_manifest = self.manifest.clone();
        for metadata in &obsolete_metadata {
            let record = self.read_segment(metadata)?;
            next_manifest
                .retired_transactions
                .push(RetiredTransactionMetadata {
                    original_sequence: metadata.sequence,
                    transaction_key: metadata.transaction_key.clone(),
                    entry_sha256: entry_sha256(&record.entry)?,
                });
        }
        next_manifest
            .segments
            .retain(|segment| segment.sequence >= minimum_sequence);
        next_manifest.minimum_retained_sequence = minimum_sequence;
        next_manifest
            .retention_leases
            .retain(|lease| lease.expires_ms > now_ms);
        next_manifest
            .snapshot_build_retention_leases
            .retain(|lease| lease.expires_ms > now_ms);
        write_manifest_with_hook(&self.root, &next_manifest, hook)?;
        self.manifest = next_manifest;
        // Once the manifest is durable, all subsequent errors must leave the
        // live object consistent with the new retained index.
        self.rebuild_transaction_index()?;
        hook(DurabilityPoint::RetentionManifestCommitted)?;

        for metadata in obsolete_metadata {
            let file_name = metadata.file_name;
            let sequence = parse_segment_file_name(&file_name).ok_or_else(|| {
                LogError::ManifestCorrupt(format!(
                    "obsolete segment has non-canonical file name {file_name}"
                ))
            })?;
            match fs::remove_file(self.root.join(SEGMENTS_DIRECTORY).join(file_name)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(LogError::Io(error)),
            }
            hook(DurabilityPoint::RetentionFileDeleted(sequence))?;
        }
        sync_directory(&self.root.join(SEGMENTS_DIRECTORY))?;
        hook(DurabilityPoint::RetentionDirectorySynced)?;
        Ok(())
    }

    fn validate_and_index(&mut self) -> Result<(), LogError> {
        if self.manifest.minimum_retained_sequence == 0 {
            return Err(LogError::ManifestCorrupt(
                "minimum retained sequence must be at least 1".to_owned(),
            ));
        }
        let now_ms = current_time_millis()?;
        let mut lease_ids = std::collections::BTreeSet::new();
        for lease in &self.manifest.retention_leases {
            if !lease_ids.insert(&lease.plan_id)
                || lease.minimum_sequence == 0
                || lease.minimum_sequence > self.manifest.head_sequence.saturating_add(1)
                || (lease.expires_ms > now_ms
                    && lease.minimum_sequence < self.manifest.minimum_retained_sequence)
            {
                return Err(LogError::ManifestCorrupt(
                    "bootstrap retention lease is duplicate or outside retained history".to_owned(),
                ));
            }
        }
        if self.manifest.next_snapshot_build_lease_token < 0 {
            return Err(LogError::ManifestCorrupt(
                "snapshot-build retention token is negative".to_owned(),
            ));
        }
        let mut build_ids = std::collections::BTreeSet::new();
        let mut build_tokens = std::collections::BTreeSet::new();
        for lease in &self.manifest.snapshot_build_retention_leases {
            if !build_ids.insert(&lease.build_id)
                || !build_tokens.insert(lease.token)
                || lease.token <= 0
                || lease.token > self.manifest.next_snapshot_build_lease_token
                || lease.minimum_sequence == 0
                || lease.minimum_sequence > self.manifest.head_sequence.saturating_add(1)
                || (lease.expires_ms > now_ms
                    && lease.minimum_sequence < self.manifest.minimum_retained_sequence)
            {
                return Err(LogError::ManifestCorrupt(
                    "snapshot-build retention lease is duplicate or outside retained history"
                        .to_owned(),
                ));
            }
        }
        let mut expected = self.manifest.minimum_retained_sequence;
        for (index, metadata) in self.manifest.segments.iter().enumerate() {
            if metadata.file_name != segment_file_name(metadata.sequence) {
                return Err(LogError::ManifestCorrupt(format!(
                    "segment {} has non-canonical file name {}",
                    metadata.sequence, metadata.file_name
                )));
            }
            if metadata.sequence != expected {
                return Err(LogError::SequenceGap {
                    expected,
                    actual: metadata.sequence,
                });
            }
            self.read_segment(metadata)?;
            if index + 1 < self.manifest.segments.len() {
                expected = expected.checked_add(1).ok_or(LogError::SequenceExhausted)?;
            }
        }
        let expected_head = self.manifest.segments.last().map_or(
            self.manifest.minimum_retained_sequence.saturating_sub(1),
            |segment| segment.sequence,
        );
        if self.manifest.head_sequence != expected_head {
            return Err(LogError::ManifestCorrupt(format!(
                "head is {}, indexed tail is {}",
                self.manifest.head_sequence, expected_head
            )));
        }
        self.rebuild_transaction_index()
    }

    fn rebuild_transaction_index(&mut self) -> Result<(), LogError> {
        self.transaction_index.clear();
        self.retired_transaction_index.clear();
        for (index, segment) in self.manifest.segments.iter().enumerate() {
            let key = transaction_key_string(&segment.transaction_key)?;
            if self.transaction_index.insert(key, index).is_some() {
                return Err(LogError::ManifestCorrupt(
                    "duplicate transaction key in segment index".to_owned(),
                ));
            }
        }
        for (index, retired) in self.manifest.retired_transactions.iter().enumerate() {
            if retired.original_sequence >= self.manifest.minimum_retained_sequence
                || retired.transaction_key.source_epoch != self.manifest.source_epoch
                || retired.entry_sha256.len() != 64
                || !retired
                    .entry_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(LogError::ManifestCorrupt(
                    "retired transaction metadata is invalid".to_owned(),
                ));
            }
            let key = transaction_key_string(&retired.transaction_key)?;
            if self.transaction_index.contains_key(&key)
                || self.retired_transaction_index.insert(key, index).is_some()
            {
                return Err(LogError::ManifestCorrupt(
                    "duplicate transaction key across retained and retired indexes".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn recover_unindexed_tail(&mut self) -> Result<(), LogError> {
        self.remove_stale_segment_temporaries()?;
        while let Some(next_sequence) = self.manifest.head_sequence.checked_add(1) {
            let path = self
                .root
                .join(SEGMENTS_DIRECTORY)
                .join(segment_file_name(next_sequence));
            if !path.exists() {
                break;
            }
            let metadata = metadata_for_existing_segment(&path, next_sequence)?;
            let record = self.read_segment(&metadata)?;
            let key = transaction_key_string(record.entry.key())?;
            if self.transaction_index.contains_key(&key)
                || self.retired_transaction_index.contains_key(&key)
            {
                return Err(LogError::DuplicateTransactionConflict);
            }
            let mut next_manifest = self.manifest.clone();
            next_manifest.head_sequence = next_sequence;
            next_manifest.segments.push(metadata);
            write_manifest(&self.root, &next_manifest)?;
            self.manifest = next_manifest;
            self.transaction_index
                .insert(key, self.manifest.segments.len() - 1);
        }
        if let Some(actual) = self.first_unindexed_sequence()? {
            let expected = self
                .manifest
                .head_sequence
                .checked_add(1)
                .ok_or(LogError::SequenceExhausted)?;
            return Err(LogError::SequenceGap { expected, actual });
        }
        Ok(())
    }

    fn remove_stale_segment_temporaries(&self) -> Result<(), LogError> {
        let segments = self.root.join(SEGMENTS_DIRECTORY);
        let mut removed = false;
        for entry in fs::read_dir(&segments)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            if parse_temporary_segment_file_name(file_name).is_some() {
                fs::remove_file(entry.path())?;
                removed = true;
            }
        }
        if removed {
            sync_directory(&segments)?;
        }
        Ok(())
    }

    fn first_unindexed_sequence(&self) -> Result<Option<u64>, LogError> {
        let mut first = None;
        for entry in fs::read_dir(self.root.join(SEGMENTS_DIRECTORY))? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            let Some(sequence) = parse_segment_file_name(file_name) else {
                continue;
            };
            if sequence > self.manifest.head_sequence {
                first = Some(first.map_or(sequence, |current: u64| current.min(sequence)));
            }
        }
        Ok(first)
    }

    pub(super) fn write_segment_with_hook<F>(
        &self,
        record: &CanonicalRecord,
        hook: &mut F,
    ) -> Result<SegmentMetadata, LogError>
    where
        F: FnMut(DurabilityPoint) -> Result<(), LogError>,
    {
        let record_bytes = serde_json::to_vec(record)?;
        let envelope = SegmentEnvelope {
            format_version: FORMAT_VERSION,
            record: record.clone(),
            record_sha256: sha256_hex(&record_bytes),
        };
        let bytes = serde_json::to_vec(&envelope)?;
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(LogError::RecordTooLarge);
        }

        let file_name = segment_file_name(record.position.sequence);
        let segments = self.root.join(SEGMENTS_DIRECTORY);
        let destination = segments.join(&file_name);
        let temporary = segments.join(format!(".{file_name}.tmp"));
        write_new_file(&temporary, &bytes)?;
        hook(DurabilityPoint::SegmentFileSynced)?;
        if destination.exists() {
            return Err(LogError::ManifestCorrupt(format!(
                "refusing to replace existing canonical segment {file_name}"
            )));
        }
        fs::rename(&temporary, &destination)?;
        hook(DurabilityPoint::SegmentPublished)?;
        sync_directory(&segments)?;
        hook(DurabilityPoint::SegmentDirectorySynced)?;

        Ok(SegmentMetadata {
            sequence: record.position.sequence,
            file_name,
            bytes: bytes.len() as u64,
            sha256: sha256_hex(&bytes),
            transaction_key: record.entry.key().clone(),
        })
    }

    fn read_segment(&self, metadata: &SegmentMetadata) -> Result<CanonicalRecord, LogError> {
        let path = self.root.join(SEGMENTS_DIRECTORY).join(&metadata.file_name);
        let bytes = read_limited(&path).map_err(|error| LogError::SegmentCorrupt {
            sequence: metadata.sequence,
            reason: error.to_string(),
        })?;
        if bytes.len() as u64 != metadata.bytes || sha256_hex(&bytes) != metadata.sha256 {
            return Err(LogError::SegmentCorrupt {
                sequence: metadata.sequence,
                reason: "file size or SHA-256 differs from manifest".to_owned(),
            });
        }
        let envelope: SegmentEnvelope =
            serde_json::from_slice(&bytes).map_err(|error| LogError::SegmentCorrupt {
                sequence: metadata.sequence,
                reason: error.to_string(),
            })?;
        if envelope.format_version != FORMAT_VERSION {
            return Err(LogError::SegmentCorrupt {
                sequence: metadata.sequence,
                reason: "unsupported segment format".to_owned(),
            });
        }
        let record_bytes = serde_json::to_vec(&envelope.record)?;
        if sha256_hex(&record_bytes) != envelope.record_sha256 {
            return Err(LogError::SegmentCorrupt {
                sequence: metadata.sequence,
                reason: "record checksum mismatch".to_owned(),
            });
        }
        if envelope.record.position.sequence != metadata.sequence
            || envelope.record.position.source_epoch != self.manifest.source_epoch
            || envelope.record.entry.key().source_epoch != self.manifest.source_epoch
            || envelope.record.entry.key() != &metadata.transaction_key
        {
            return Err(LogError::SegmentCorrupt {
                sequence: metadata.sequence,
                reason: "record identity differs from manifest".to_owned(),
            });
        }
        validate_entry(&envelope.record.entry).map_err(|error| LogError::SegmentCorrupt {
            sequence: metadata.sequence,
            reason: error.to_string(),
        })?;
        Ok(envelope.record)
    }
}

impl SnapshotBuildRetention for FileCanonicalLog {
    /// The Postgres-fenced log's deadline: only tests build this log, in that
    /// one's place. It has to cover an acquire's two `sync_all`s and a
    /// directory fsync on a loaded shared disk.
    fn maximum_retention_operation_duration(&self) -> Duration {
        RETENTION_OPERATION_DEADLINE
    }

    fn acquire_snapshot_build_retention(
        &mut self,
        build_id: SnapshotBuildId,
        owner_id: SnapshotBuildOwnerId,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        self.acquire_snapshot_build_retention_at(build_id, owner_id, ttl, current_time_millis()?)
    }

    fn renew_snapshot_build_retention(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        self.renew_snapshot_build_retention_at(lease, ttl, current_time_millis()?)
    }

    fn release_snapshot_build_retention(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
    ) -> Result<(), LogError> {
        self.release_snapshot_build_retention_at(lease, current_time_millis()?)
    }
}

impl CanonicalLogRead for FileCanonicalLog {
    fn source_id(&self) -> &SourceId {
        FileCanonicalLog::source_id(self)
    }

    fn source_epoch(&self) -> &SourceEpoch {
        FileCanonicalLog::source_epoch(self)
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        Ok(FileCanonicalLog::head(self))
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        Ok(FileCanonicalLog::minimum_retained(self))
    }

    fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        FileCanonicalLog::read_page_after_bounded(
            self,
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )
    }
}

impl CanonicalLogPlanRetention for FileCanonicalLog {
    fn acquire_plan_retention_lease(
        &mut self,
        plan_id: PlanId,
        minimum_sequence: u64,
        expires_ms: i64,
        now_ms: i64,
    ) -> Result<(), LogError> {
        FileCanonicalLog::acquire_retention_lease(
            self,
            plan_id,
            minimum_sequence,
            expires_ms,
            now_ms,
        )
    }
}

fn file_snapshot_build_lease_capability(
    manifest: &Manifest,
    lease: &FileSnapshotBuildRetentionLease,
) -> SnapshotBuildRetentionLease {
    SnapshotBuildRetentionLease {
        source_id: manifest.source_id.clone(),
        source_epoch: manifest.source_epoch.clone(),
        build_id: lease.build_id.clone(),
        owner_id: lease.owner_id.clone(),
        token: lease.token,
        minimum_sequence: lease.minimum_sequence,
        expires_at_unix_millis: lease.expires_ms,
    }
}

fn metadata_for_existing_segment(path: &Path, sequence: u64) -> Result<SegmentMetadata, LogError> {
    let bytes = read_limited(path)?;
    let envelope: SegmentEnvelope = serde_json::from_slice(&bytes)?;
    if envelope.format_version != FORMAT_VERSION || envelope.record.position.sequence != sequence {
        return Err(LogError::SegmentCorrupt {
            sequence,
            reason: "orphan segment identity is invalid".to_owned(),
        });
    }
    let record_bytes = serde_json::to_vec(&envelope.record)?;
    if sha256_hex(&record_bytes) != envelope.record_sha256 {
        return Err(LogError::SegmentCorrupt {
            sequence,
            reason: "orphan record checksum mismatch".to_owned(),
        });
    }
    validate_entry(&envelope.record.entry).map_err(|error| LogError::SegmentCorrupt {
        sequence,
        reason: error.to_string(),
    })?;
    Ok(SegmentMetadata {
        sequence,
        file_name: segment_file_name(sequence),
        bytes: bytes.len() as u64,
        sha256: sha256_hex(&bytes),
        transaction_key: envelope.record.entry.key().clone(),
    })
}

pub(super) fn write_manifest(root: &Path, manifest: &Manifest) -> Result<(), LogError> {
    write_manifest_with_hook(root, manifest, &mut |_| Ok(()))
}

fn write_manifest_with_hook<F>(
    root: &Path,
    manifest: &Manifest,
    hook: &mut F,
) -> Result<(), LogError>
where
    F: FnMut(DurabilityPoint) -> Result<(), LogError>,
{
    let bytes = serde_json::to_vec_pretty(manifest)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(LogError::ManifestTooLarge);
    }
    let temporary = root.join(format!(".{MANIFEST_FILE}.tmp"));
    write_replaceable_file(&temporary, &bytes)?;
    hook(DurabilityPoint::ManifestFileSynced)?;
    fs::rename(&temporary, root.join(MANIFEST_FILE))?;
    hook(DurabilityPoint::ManifestPublished)?;
    sync_directory(root)?;
    hook(DurabilityPoint::ManifestDirectorySynced)?;
    Ok(())
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), io::Error> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn write_replaceable_file(path: &Path, bytes: &[u8]) -> Result<(), io::Error> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn read_limited(path: &Path) -> Result<Vec<u8>, io::Error> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > MAX_RECORD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file exceeds canonical record limit",
        ));
    }
    fs::read(path)
}

fn current_time_millis() -> Result<i64, LogError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| LogError::SystemClockInvalid)?;
    i64::try_from(duration.as_millis()).map_err(|_| LogError::SystemClockInvalid)
}

fn snapshot_build_lease_expiry(now_ms: i64, ttl: Duration) -> Result<i64, LogError> {
    let ttl_ms = snapshot_build_lease_ttl_millis(ttl)?;
    now_ms
        .checked_add(ttl_ms)
        .ok_or(LogError::InvalidSnapshotBuildRetentionLease)
}

fn sync_directory(path: &Path) -> Result<(), io::Error> {
    File::open(path)?.sync_all()
}

pub(super) fn segment_file_name(sequence: u64) -> String {
    format!("{sequence:020}.json")
}

fn parse_segment_file_name(file_name: &str) -> Option<u64> {
    let digits = file_name.strip_suffix(".json")?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn parse_temporary_segment_file_name(file_name: &str) -> Option<u64> {
    let canonical = file_name.strip_prefix('.')?.strip_suffix(".tmp")?;
    parse_segment_file_name(canonical)
}

fn validate_entry(entry: &CanonicalEntry) -> Result<(), LogError> {
    validate_encoded_entry(entry, entry.encode().len())
}

fn entry_sha256(entry: &CanonicalEntry) -> Result<String, serde_json::Error> {
    serde_json::to_vec(entry).map(|bytes| sha256_hex(&bytes))
}

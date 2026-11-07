use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{
    Arc, Mutex, MutexGuard, OnceLock, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard,
};

use authz::AuthorizationIdentity;
use bytes::Bytes;
use sync_types::{SourceEpoch, TableId};

use crate::{ChangeAuthorization, ChangeBatchError};

/// What makes two builds encode a transaction to the same frames: one source
/// epoch, one authorization scope and one subscribed table set. The scope's
/// identity holds its scope hash, which covers a partitioned scope's tenant,
/// so two tenants of one policy never share an encoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScopeEncodingKey {
    source_epoch: SourceEpoch,
    identity: AuthorizationIdentity,
    tables: BTreeSet<TableId>,
}

impl ScopeEncodingKey {
    pub fn of(authorization: &ChangeAuthorization) -> Self {
        Self {
            source_epoch: authorization.source_epoch.clone(),
            identity: authorization.scope.identity(),
            tables: authorization.table_starts.keys().cloned().collect(),
        }
    }

    pub(crate) fn matches(&self, authorization: &ChangeAuthorization) -> bool {
        self.source_epoch == authorization.source_epoch
            && self.identity == authorization.scope.identity()
            && self.tables.iter().eq(authorization.table_starts.keys())
    }

    pub(crate) fn tables(&self) -> &BTreeSet<TableId> {
        &self.tables
    }
}

/// One sealed frame and its body digest.
#[derive(Debug, Clone)]
pub(crate) struct SharedFrame {
    pub(crate) bytes: Bytes,
    pub(crate) body_sha256: [u8; 32],
}

/// The frames one transaction is sent as to one scope.
#[derive(Debug)]
pub(crate) struct EncodedTransaction {
    pub(crate) frames: Vec<SharedFrame>,
    pub(crate) operation_count: u64,
    pub(crate) encoded_bytes: usize,
}

#[derive(Default)]
struct Slot {
    /// `None` is a record this scope is sent nothing of.
    encoded: OnceLock<Option<Arc<EncodedTransaction>>>,
    /// Held by the build encoding the record.
    encoding: Mutex<()>,
}

/// Every transaction one scope has been sent, encoded by the first build that
/// needed it and shared by every later build of the scope.
///
/// A record is encoded at most once while it is held: a build that needs a
/// record another build is encoding waits for that encoding. An encoding that
/// fails is not kept, so every build that reaches the record reports it. A
/// build that finds the record encoded takes no lock but the map's read lock.
pub struct ScopeEncoding {
    key: ScopeEncodingKey,
    slots: RwLock<BTreeMap<u64, Arc<Slot>>>,
    encoded_bytes: Arc<AtomicU64>,
}

impl ScopeEncoding {
    pub fn new(key: ScopeEncodingKey) -> Self {
        Self::counted_in(key, Arc::default())
    }

    /// An encoding whose frames are counted in `encoded_bytes` with those of
    /// every other encoding given it.
    pub fn counted_in(key: ScopeEncodingKey, encoded_bytes: Arc<AtomicU64>) -> Self {
        Self {
            key,
            slots: RwLock::new(BTreeMap::new()),
            encoded_bytes,
        }
    }

    pub fn key(&self) -> &ScopeEncodingKey {
        &self.key
    }

    /// The bytes of every frame encoded and not yet dropped, whether this
    /// encoding still holds it or only a batch that carries it does.
    pub fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes.load(Ordering::Relaxed)
    }

    /// Whether no record is held, encoded or being encoded.
    pub fn is_empty(&self) -> bool {
        read(&self.slots).is_empty()
    }

    /// Drops every record at or below `sequence`. A build already holding one
    /// keeps it until it ends.
    pub fn evict_through(&self, sequence: u64) {
        let evicted = {
            let mut slots = write(&self.slots);
            let kept = slots.split_off(&sequence.saturating_add(1));
            std::mem::replace(&mut *slots, kept)
        };
        drop(evicted);
    }

    pub(crate) fn get_or_encode(
        &self,
        sequence: u64,
        encode: impl FnOnce() -> Result<Option<EncodedTransaction>, ChangeBatchError>,
    ) -> Result<Option<Arc<EncodedTransaction>>, ChangeBatchError> {
        let held = read(&self.slots).get(&sequence).cloned();
        let slot =
            held.unwrap_or_else(|| Arc::clone(write(&self.slots).entry(sequence).or_default()));
        if let Some(encoded) = slot.encoded.get() {
            return Ok(encoded.clone());
        }
        let _encoding = lock(&slot.encoding);
        if let Some(encoded) = slot.encoded.get() {
            return Ok(encoded.clone());
        }
        let encoded = encode()?.map(|mut encoded| {
            for frame in &mut encoded.frames {
                frame.bytes = Bytes::from_owner(Counted::new(
                    std::mem::take(&mut frame.bytes),
                    &self.encoded_bytes,
                ));
            }
            Arc::new(encoded)
        });
        Ok(slot.encoded.get_or_init(|| encoded).clone())
    }
}

/// A frame's bytes, counted from their encoding until the last batch that
/// carries them is written or dropped.
struct Counted {
    bytes: Bytes,
    encoded_bytes: Arc<AtomicU64>,
}

impl Counted {
    fn new(bytes: Bytes, encoded_bytes: &Arc<AtomicU64>) -> Self {
        encoded_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Self {
            bytes,
            encoded_bytes: Arc::clone(encoded_bytes),
        }
    }
}

impl AsRef<[u8]> for Counted {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.encoded_bytes
            .fetch_sub(self.bytes.len() as u64, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for ScopeEncoding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScopeEncoding")
            .field("key", &self.key)
            .field("encoded_bytes", &self.encoded_bytes())
            .finish()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

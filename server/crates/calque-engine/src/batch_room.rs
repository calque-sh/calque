//! The bytes of change batches built from the log that the process holds at
//! once, and that each plan does.
//!
//! A batch built from the change tail sends its scope's shared frames, which
//! the tail's bound counts until they are written. A batch built from the log
//! is the client's own, so its build takes room here for the most it may make
//! before it reads, gives back what it did not make, and holds the rest until
//! its last byte is written or its client leaves. A plan holds at most one
//! largest batch, so a client that stops reading, or opens connection after
//! connection, holds up its own plan's builds and leaves the process's room
//! to the others.

use std::sync::Arc;

use axum::body::Bytes;
use change_api::ChangeBatchError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// What one plan's batches built from the log hold at once.
const PLAN_ROOM_BYTES: usize = sync_protocol::MAX_CHANGE_BATCH_BYTES as usize;

pub(crate) struct LogBatchRoom {
    room: Arc<Semaphore>,
    bound: usize,
}

/// The room one plan's batches share.
#[derive(Clone)]
pub(crate) struct PlanRoom {
    room: Arc<Semaphore>,
    bound: usize,
}

impl PlanRoom {
    pub(crate) fn new() -> Self {
        Self::with_bound(PLAN_ROOM_BYTES)
    }

    fn with_bound(bound: usize) -> Self {
        Self {
            room: Arc::new(Semaphore::new(bound)),
            bound,
        }
    }
}

impl LogBatchRoom {
    pub(crate) fn new(bound: usize) -> Self {
        Self {
            room: Arc::new(Semaphore::new(bound)),
            bound,
        }
    }

    /// Room in `plan`'s and the process's for a batch of at most `bytes`, if
    /// it is free now. A batch larger than either room is refused.
    pub(crate) fn try_reserve(
        &self,
        plan: &PlanRoom,
        bytes: usize,
    ) -> Result<Option<BatchRoom>, ChangeBatchError> {
        let permits = self.permits(plan, bytes)?;
        let Ok(plan) = Arc::clone(&plan.room).try_acquire_many_owned(permits) else {
            return Ok(None);
        };
        Ok(Arc::clone(&self.room)
            .try_acquire_many_owned(permits)
            .ok()
            .map(|process| BatchRoom { process, plan }))
    }

    /// The same room, once it is free. The plan's comes first, so a plan
    /// whose own batches hold its room waits without holding the process's.
    pub(crate) async fn reserve(
        &self,
        plan: &PlanRoom,
        bytes: usize,
    ) -> Result<BatchRoom, ChangeBatchError> {
        let permits = self.permits(plan, bytes)?;
        let plan = acquire(&plan.room, permits).await;
        let process = acquire(&self.room, permits).await;
        Ok(BatchRoom { process, plan })
    }

    fn permits(&self, plan: &PlanRoom, bytes: usize) -> Result<u32, ChangeBatchError> {
        u32::try_from(bytes)
            .ok()
            .filter(|_| bytes <= self.bound && bytes <= plan.bound)
            .ok_or(ChangeBatchError::InvalidLimits)
    }

    #[cfg(test)]
    fn free(&self) -> usize {
        self.room.available_permits()
    }
}

async fn acquire(room: &Arc<Semaphore>, permits: u32) -> OwnedSemaphorePermit {
    match Arc::clone(room).acquire_many_owned(permits).await {
        Ok(permit) => permit,
        Err(_) => unreachable!("a batch room is never closed"),
    }
}

pub(crate) struct BatchRoom {
    process: OwnedSemaphorePermit,
    plan: OwnedSemaphorePermit,
}

impl BatchRoom {
    /// Gives back the room `chunks` do not use, and holds the rest until the
    /// last of them is dropped. Every batch is written in order, so its last
    /// chunk is the last dropped.
    pub(crate) fn hold(mut self, chunks: &mut [Bytes]) {
        let used = chunks.iter().map(Bytes::len).sum::<usize>();
        if let Some(unused) = self.process.num_permits().checked_sub(used) {
            drop(self.process.split(unused));
            drop(self.plan.split(unused));
        }
        if let Some(last) = chunks.last_mut() {
            *last = Bytes::from_owner(Held {
                bytes: std::mem::take(last),
                _room: self,
            });
        }
    }
}

struct Held {
    bytes: Bytes,
    _room: BatchRoom,
}

impl AsRef<[u8]> for Held {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn a_held_batch_keeps_its_bytes_and_gives_back_the_rest() {
        let room = LogBatchRoom::new(100);
        let plan = PlanRoom::with_bound(100);
        let reserved = room.try_reserve(&plan, 60).unwrap().unwrap();
        assert_eq!((room.free(), plan.room.available_permits()), (40, 40));
        assert!(room.try_reserve(&plan, 41).unwrap().is_none());

        let mut chunks = vec![Bytes::from(vec![1; 10]), Bytes::from(vec![2; 15])];
        reserved.hold(&mut chunks);
        assert_eq!((room.free(), plan.room.available_permits()), (75, 75));
        assert_eq!(&chunks[1][..], &[2; 15]);

        // A copy of the last chunk keeps the room until it is dropped too.
        let written = chunks.pop().unwrap();
        let copy = written.clone();
        drop(chunks);
        drop(written);
        assert_eq!(room.free(), 75);
        drop(copy);
        assert_eq!((room.free(), plan.room.available_permits()), (100, 100));
    }

    #[tokio::test]
    async fn a_build_waits_for_room_until_a_held_batch_is_dropped() {
        let room = Arc::new(LogBatchRoom::new(100));
        let plan = PlanRoom::with_bound(100);
        let mut held = vec![Bytes::from(vec![0; 80])];
        room.try_reserve(&plan, 100)
            .unwrap()
            .unwrap()
            .hold(&mut held);
        let waiting = tokio::spawn({
            let room = Arc::clone(&room);
            let plan = PlanRoom::with_bound(100);
            async move { room.reserve(&plan, 50).await.unwrap() }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        drop(held);
        let reserved = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the build gets room once the batch is dropped")
            .unwrap();
        assert_eq!(room.free(), 50);
        drop(reserved);
        assert_eq!(room.free(), 100);
    }

    /// A plan whose batch holds its room waits for it, and takes none of the
    /// process's room while it does; another plan's build goes ahead.
    #[tokio::test]
    async fn a_plan_waits_for_its_own_room_and_leaves_the_rest_to_others() {
        let room = Arc::new(LogBatchRoom::new(100));
        let plan = PlanRoom::with_bound(40);
        let mut held = vec![Bytes::from(vec![0; 30])];
        room.try_reserve(&plan, 40)
            .unwrap()
            .unwrap()
            .hold(&mut held);
        assert!(room.try_reserve(&plan, 20).unwrap().is_none());
        let waiting = tokio::spawn({
            let room = Arc::clone(&room);
            let plan = plan.clone();
            async move { room.reserve(&plan, 20).await.unwrap() }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        assert_eq!(room.free(), 70);
        let other = room
            .try_reserve(&PlanRoom::with_bound(40), 40)
            .unwrap()
            .expect("another plan's build has room");
        drop(other);
        drop(held);
        let reserved = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the plan's build gets room once its batch is dropped")
            .unwrap();
        assert_eq!((room.free(), plan.room.available_permits()), (80, 20));
        drop(reserved);
    }

    #[tokio::test]
    async fn a_batch_larger_than_either_room_is_refused_and_takes_none_of_it() {
        let room = LogBatchRoom::new(100);
        let plan = PlanRoom::with_bound(50);
        for bytes in [51, 101] {
            assert!(matches!(
                room.try_reserve(&plan, bytes),
                Err(ChangeBatchError::InvalidLimits)
            ));
            assert!(matches!(
                room.reserve(&plan, bytes).await,
                Err(ChangeBatchError::InvalidLimits)
            ));
        }
        assert_eq!((room.free(), plan.room.available_permits()), (100, 50));
        let whole = room.reserve(&plan, 50).await.unwrap();
        assert_eq!((room.free(), plan.room.available_permits()), (50, 0));
        drop(whole);
        assert_eq!((room.free(), plan.room.available_permits()), (100, 50));
    }
}

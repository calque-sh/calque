//! The side of the fence lock change builds take.
//!
//! Every open change stream builds a batch per commit, so thousands of builds
//! take the lock's shared side at once. A shared side given back takes its
//! semaphore's waiter mutex, and with thousands of builds on every worker
//! thread that mutex serializes them. A build takes the shared side of its
//! worker thread's shard alone; a fence takes every shard's exclusive side,
//! so it still waits for every build in flight, and no build starts while it
//! holds them.

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

const SHARDS: usize = 64;

pub(crate) struct BuildFence {
    shards: Box<[RwLock<()>]>,
}

pub(crate) struct Fenced<'a> {
    _shards: Vec<RwLockWriteGuard<'a, ()>>,
}

impl BuildFence {
    pub(crate) fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| RwLock::new(())).collect(),
        }
    }

    /// Held for one build.
    pub(crate) async fn build(&self) -> RwLockReadGuard<'_, ()> {
        self.shards[thread_shard() % self.shards.len()].read().await
    }

    /// Held for one fence, once no build holds any shard.
    pub(crate) async fn fence(&self) -> Fenced<'_> {
        let mut shards = Vec::with_capacity(self.shards.len());
        for shard in &*self.shards {
            shards.push(shard.write().await);
        }
        Fenced { _shards: shards }
    }
}

/// A shard per thread, so the builds of one worker share a shard and those
/// of two seldom do.
fn thread_shard() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static SHARD: usize = NEXT.fetch_add(1, Ordering::Relaxed);
    }
    SHARD.with(|shard| *shard)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;

    /// Holds a build on its own thread, so each holds its own shard, until
    /// told to give it back.
    fn held_build(
        fence: &Arc<BuildFence>,
    ) -> (std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>) {
        let (holding, held) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let fence = Arc::clone(fence);
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime.block_on(async {
                let _build = fence.build().await;
                holding.send(()).unwrap();
                released.recv().unwrap();
            });
        });
        held.recv().unwrap();
        (release, thread)
    }

    #[tokio::test]
    async fn a_fence_waits_for_every_build_in_flight() {
        let fence = Arc::new(BuildFence::new());
        let builds: Vec<_> = (0..SHARDS + 1).map(|_| held_build(&fence)).collect();
        let fencing = tokio::spawn({
            let fence = Arc::clone(&fence);
            async move {
                let _fenced = fence.fence().await;
            }
        });
        for (release, thread) in builds {
            tokio::time::sleep(Duration::from_millis(5)).await;
            assert!(!fencing.is_finished(), "the fence ran beside a build");
            release.send(()).unwrap();
            thread.join().unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), fencing)
            .await
            .expect("the fence runs once the builds end")
            .unwrap();
    }

    #[tokio::test]
    async fn no_build_starts_while_a_fence_holds() {
        let fence = Arc::new(BuildFence::new());
        let fenced = fence.fence().await;
        let builds: Vec<_> = (0..SHARDS + 1)
            .map(|_| {
                let fence = Arc::clone(&fence);
                std::thread::spawn(move || {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .build()
                        .unwrap()
                        .block_on(async {
                            tokio::time::timeout(Duration::from_millis(50), fence.build())
                                .await
                                .is_err()
                        })
                })
            })
            .collect();
        for build in builds {
            assert!(build.join().unwrap(), "a build started under a fence");
        }
        drop(fenced);
        let _build = tokio::time::timeout(Duration::from_secs(5), fence.build())
            .await
            .expect("a build starts once the fence ends");
    }
}

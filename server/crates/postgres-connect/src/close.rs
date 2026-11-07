use std::{
    ops::{Deref, DerefMut},
    sync::mpsc,
    thread,
};

/// A blocking connection that may be dropped on any thread.
///
/// `postgres::Client`'s `Drop` sends the terminate message by building a
/// current-thread runtime and `block_on`ing it, which panics on a thread that
/// is driving a tokio runtime. A connection shared through an `Arc`, or held
/// by a task the runtime drops as it shuts down, can lose its last owner on
/// such a thread, so this closes it on a thread of its own.
pub struct DropOffRuntime<C: Send + 'static> {
    /// `None` only once `Drop` has taken the connection away.
    connection: Option<C>,
}

impl<C: Send + 'static> DropOffRuntime<C> {
    pub fn new(connection: C) -> Self {
        Self {
            connection: Some(connection),
        }
    }
}

impl<C: Send + 'static> Deref for DropOffRuntime<C> {
    type Target = C;

    fn deref(&self) -> &C {
        self.connection
            .as_ref()
            .expect("the connection is held until it is dropped")
    }
}

impl<C: Send + 'static> DerefMut for DropOffRuntime<C> {
    fn deref_mut(&mut self) -> &mut C {
        self.connection
            .as_mut()
            .expect("the connection is held until it is dropped")
    }
}

impl<C: Send + 'static> Drop for DropOffRuntime<C> {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };
        // The connection is handed over *after* the thread exists, so a thread
        // that could not be spawned leaves it here instead of dropping it
        // inside the closure on this thread.
        let (handover, incoming) = mpsc::channel::<C>();
        match thread::Builder::new()
            .name("postgres-close".to_owned())
            .spawn(move || drop(incoming.recv()))
        {
            Ok(closer) => {
                // The receiver lives on the new thread until it has the value,
                // so this send cannot fail.
                let _ = handover.send(connection);
                // Joining keeps the terminate message ordered before the
                // process exits. It costs one round trip.
                let _ = closer.join();
            }
            Err(error) => {
                // Threads are unavailable only under real resource exhaustion.
                // Leaking one connection until the process exits is strictly
                // better than aborting inside a shutdown path.
                tracing::warn!(
                    %error,
                    "could not close a PostgreSQL connection on a thread of its own; leaking it"
                );
                std::mem::forget(connection);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stands in for `postgres::Client`, whose `Drop` builds a current-thread
    /// runtime and `block_on`s it to send the terminate message.
    struct RuntimeDrivingDrop;

    impl Drop for RuntimeDrivingDrop {
        fn drop(&mut self) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a current-thread runtime must build");
            runtime.block_on(async {});
        }
    }

    /// A task's last owner can die on a worker thread, as the change tail's
    /// follower does when the runtime shuts down under it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_connection_closes_when_dropped_from_the_runtime() {
        drop(DropOffRuntime::new(RuntimeDrivingDrop));
    }

    /// The same drop on the runtime's own thread, which is where
    /// `axum::serve` returning from a SIGINT leaves the router's state.
    #[test]
    fn the_connection_closes_when_dropped_inside_block_on() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("a multi-thread runtime must build");
        let connection = DropOffRuntime::new(RuntimeDrivingDrop);
        runtime.block_on(async move {
            drop(connection);
        });
    }

    #[test]
    fn the_connection_is_reachable_until_it_is_dropped() {
        let mut connection = DropOffRuntime::new(7_u32);
        *connection += 1;
        assert_eq!(*connection, 8);
    }
}

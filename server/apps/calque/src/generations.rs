//! Serving a source across its schema generations, which `calque
//! dev` and `calque serve` share. A move swaps in the routes served between
//! generations, retires the current generation, prepares the source again and
//! swaps the next generation's routes in once it serves.

use std::{future::Future, future::IntoFuture as _, path::Path, time::Duration};

use axum::Router;
use calque_engine::{
    BetweenGenerations, CdcStopRemedy, GenerationRoutes, LocalBackend, LocalSourceRuntime,
    StateError, StateStore,
};
use tokio_util::sync::CancellationToken;

use crate::describe;
use crate::lifecycle::{
    bounded_stop, shutdown_after, shutdown_runtime, with_state_remedy, CommandError,
};
use crate::listen::HttpListener;
use crate::log_sink::print_line;

/// How often a source is checked for a configuration edit and a schema
/// change, and a move that did not start is tried again.
const SUPERVISE_INTERVAL: Duration = Duration::from_secs(1);

/// One schema generation a process serves.
pub(crate) struct Generation {
    pub(crate) runtime: LocalSourceRuntime,
    pub(crate) backend: LocalBackend,
    pub(crate) cancellation: CancellationToken,
    pub(crate) routes: Router,
    pub(crate) schema_generation: u64,
}

impl Generation {
    /// Stops serving: the backend is retired, so a change request under one of
    /// its plans is refused and no build publishes, then the background work
    /// and the runtime stop.
    pub(crate) async fn retire<E: CommandError>(self) -> Result<(), E> {
        self.backend.retire().await;
        self.stop().await
    }

    /// Stops the background work and the runtime as the process stops. A
    /// retirement would first wait out the writes and builds in flight, which
    /// hang on a PostgreSQL that stopped answering.
    async fn stop<E: CommandError>(self) -> Result<(), E> {
        self.cancellation.cancel();
        shutdown_runtime(self.runtime).await
    }

    fn stopped_on_schema_change(&self) -> bool {
        !self.runtime.is_healthy() && self.runtime.failure_remedy() == CdcStopRemedy::MoveSchema
    }
}

/// Stops the generation served as the process stops. A stop on a schema
/// change that the supervisor had not yet acted on is recorded as a pending
/// move, so the next start moves the source.
pub(crate) async fn stop_serving<S: GenerationSource>(
    source: &S,
    generation: Generation,
) -> Result<(), S::Error> {
    let stopped_on_schema_change = generation.stopped_on_schema_change();
    let stopped = generation.stop::<S::Error>().await;
    if stopped_on_schema_change || stopped.as_ref().is_err_and(CommandError::moves_schema) {
        record_move_pending(source);
    }
    stopped
}

/// A source served across its schema generations.
pub(crate) trait GenerationSource {
    type Error: CommandError + From<std::io::Error> + From<StateError> + std::error::Error;

    /// Prepares the source and starts serving it. The source moves to its
    /// next schema generation when what it is asked to serve differs from
    /// what it served, and when `force_move` holds.
    async fn start_generation(&mut self, force_move: bool) -> Result<Generation, Self::Error>;

    /// The routes served while no generation is.
    fn between_generations(&self, state: BetweenGenerations) -> Router;

    /// Whether the configuration names other tables or columns than the
    /// generation served, taking them when it does.
    fn configuration_changed(&mut self) -> bool;

    fn state_store(&self) -> &StateStore;

    /// The state directory as the command was given it, which a remedy
    /// names.
    fn state_dir(&self) -> &Path;

    fn database_url_env(&self) -> &str;
}

/// Starts the next generation. The source moves when `force_move` holds or
/// a stop on a schema change is pending, and a stop on one met here is
/// recorded as pending.
async fn start<S: GenerationSource>(
    source: &mut S,
    force_move: bool,
) -> Result<Generation, S::Error> {
    let pending = source.state_store().schema_move_pending();
    let started = source.start_generation(force_move || pending).await;
    match &started {
        Ok(_) if pending => {
            if let Err(error) = source.state_store().clear_schema_move_pending() {
                tracing::warn!(%error, "the source moved, but its pending move was not cleared, so the next start moves it again");
            }
        }
        Err(error) if error.moves_schema() => record_move_pending(source),
        _ => {}
    }
    started
}

fn record_move_pending(source: &impl GenerationSource) {
    if let Err(error) = source.state_store().record_schema_move_pending() {
        tracing::warn!(%error, "the source's pending move was not recorded");
    }
}

/// How the first generation a process serves started.
pub(crate) enum FirstGeneration {
    Serving(Generation),
    /// The enrolled source does not bind the configuration, and is served
    /// once its schema or the configuration does.
    Suspended {
        force_move: bool,
        refusal: String,
    },
}

impl FirstGeneration {
    pub(crate) fn routes(&self, source: &impl GenerationSource) -> Router {
        match self {
            Self::Serving(generation) => generation.routes.clone(),
            Self::Suspended { .. } => source.between_generations(BetweenGenerations::Suspended),
        }
    }

    /// Stops a generation whose serving could not begin and returns `error`.
    pub(crate) async fn stop_after<E: CommandError>(self, error: E) -> E {
        let Self::Serving(generation) = self else {
            return error;
        };
        generation.backend.retire().await;
        generation.cancellation.cancel();
        shutdown_after(generation.runtime, error).await
    }
}

/// Starts the first generation a process serves. A stop on a schema change
/// moves the source to its next schema generation, since the state recorded
/// what the stop saw and a start at the same generation would stop on it
/// again. When `may_suspend`, a refusal that suspends the source leaves it
/// suspended; otherwise it stops the process.
pub(crate) async fn first_generation<S: GenerationSource>(
    source: &mut S,
    may_suspend: bool,
) -> Result<FirstGeneration, S::Error> {
    let mut force_move = false;
    loop {
        match start(source, force_move).await {
            Ok(generation) => return Ok(FirstGeneration::Serving(generation)),
            Err(error) if error.moves_schema() && !force_move => {
                print_line(format_args!(
                    "The source's schema changed ({}): moving to the next schema generation",
                    error
                        .stopped_on()
                        .map_or_else(|| describe(&error), str::to_owned)
                        .trim_end_matches('.')
                ));
                force_move = true;
            }
            Err(error) if may_suspend && error.suspends_schema() => {
                let refusal = describe(&with_state_remedy(
                    error,
                    source.state_dir(),
                    source.database_url_env(),
                ));
                print_suspended(&refusal);
                return Ok(FirstGeneration::Suspended {
                    force_move,
                    refusal,
                });
            }
            Err(error) => return Err(error),
        }
    }
}

/// Serves `router` on `listener` until `stop`, and returns the server's
/// result and the generation served when it stopped. Until then, the source
/// moves to its next schema generation when its configuration changes or its
/// runtime stops on a schema change, and is suspended while the live schema
/// does not bind the configuration.
pub(crate) async fn serve_generations<S: GenerationSource>(
    source: &mut S,
    first: FirstGeneration,
    routes: &GenerationRoutes,
    listener: HttpListener,
    router: Router,
    stop: impl Future<Output = ()>,
) -> (Result<(), S::Error>, Option<Generation>) {
    let server_stop = CancellationToken::new();
    let mut server = tokio::spawn(
        axum::serve(listener, router)
            .with_graceful_shutdown(bounded_stop(server_stop.clone().cancelled_owned()))
            .into_future(),
    );
    let (serving, served) = supervise(source, first, routes, stop, &mut server).await;
    server_stop.cancel();
    let result = match served {
        Some(joined) => joined,
        None => server.await,
    };
    let result = match result {
        Ok(result) => result.map_err(S::Error::from),
        Err(error) => Err(S::Error::blocking_task(error.to_string())),
    };
    (result, serving)
}

type Served = Result<std::io::Result<()>, tokio::task::JoinError>;

async fn supervise<S: GenerationSource>(
    source: &mut S,
    first: FirstGeneration,
    routes: &GenerationRoutes,
    stop: impl Future<Output = ()>,
    server: &mut tokio::task::JoinHandle<std::io::Result<()>>,
) -> (Option<Generation>, Option<Served>) {
    // `moving` is `Some(force)` while the source is to move; `force` moves it
    // even when its catalog shows nothing to move for.
    let (mut serving, mut moving, mut last_refusal) = match first {
        FirstGeneration::Serving(generation) => (Some(generation), None, None),
        FirstGeneration::Suspended {
            force_move,
            refusal,
        } => (None, Some(force_move), Some(refusal)),
    };
    let mut tick = tokio::time::interval(SUPERVISE_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => return (serving, None),
            joined = &mut *server => return (serving, Some(joined)),
            _ = tick.tick() => {}
        }
        if source.configuration_changed() {
            print_line("Configuration changed: moving to the next schema generation");
            moving = Some(moving.unwrap_or(false));
        }
        if let Some(generation) = &serving {
            if generation.stopped_on_schema_change() {
                print_line(format_args!(
                    "The source's schema changed ({}): moving to the next schema generation",
                    generation
                        .runtime
                        .failure()
                        .unwrap_or_else(|| "persistent CDC stopped".to_owned())
                        .trim_end_matches('.')
                ));
                record_move_pending(source);
                moving = Some(true);
            }
        }
        let Some(force_move) = moving else {
            continue;
        };
        if let Some(generation) = serving.take() {
            routes.replace(source.between_generations(BetweenGenerations::Preparing));
            // A generation that stopped on a schema change fails its stop with
            // that change, which the move has already printed.
            match generation.retire::<S::Error>().await {
                Err(error) if !error.moves_schema() => {
                    tracing::warn!(%error, "stopping the retired schema generation failed");
                }
                _ => {}
            }
        }
        match start(source, force_move).await {
            Ok(generation) => {
                routes.replace(generation.routes.clone());
                print_line(format_args!(
                    "Schema generation {}: serving",
                    generation.schema_generation
                ));
                serving = Some(generation);
                moving = None;
                last_refusal = None;
            }
            Err(error) => {
                let suspended = error.suspends_schema();
                if error.moves_schema() {
                    moving = Some(true);
                }
                routes.replace(source.between_generations(if suspended {
                    BetweenGenerations::Suspended
                } else {
                    BetweenGenerations::Preparing
                }));
                let message = describe(&with_state_remedy(
                    error,
                    source.state_dir(),
                    source.database_url_env(),
                ));
                if last_refusal.as_deref() != Some(message.as_str()) {
                    if suspended {
                        print_suspended(&message);
                    } else {
                        print_line(format_args!(
                            "The next schema generation did not start, retrying every second: {message}"
                        ));
                    }
                    last_refusal = Some(message);
                }
            }
        }
    }
}

fn print_suspended(refusal: &str) {
    print_line(format_args!(
        "Schema suspended: {refusal} Calque serves the source again once its schema or the configuration binds, and nothing needs calque destroy."
    ));
}

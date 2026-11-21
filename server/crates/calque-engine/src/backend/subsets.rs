//! The parts of the served tables pages ask for, and the background
//! derivation of an artifact of each at the newest cut.

use snapshot_builder::SnapshotBuildPlan;
use sync_types::ArtifactId;

use super::*;
use crate::subset::{derive_subset, MAX_SUBSETS};

/// The table sets source-wide pages were planned for that are part of the
/// served tables, and the source they were asked of.
#[derive(Default)]
pub(super) struct SubsetDemand {
    state: StdMutex<DemandState>,
}

#[derive(Default)]
struct DemandState {
    /// Least recently planned first, at most [`MAX_SUBSETS`].
    sets: Vec<BTreeSet<TableId>>,
    /// The source the last re-encode was spawned for.
    served: Option<Served>,
    /// A derivation is spawned and has not read `sets` yet.
    scheduled: bool,
}

#[derive(Clone)]
struct Served {
    prepared: PreparedLocalSource,
    tables: BTreeSet<TableId>,
}

impl SubsetDemand {
    fn state(&self) -> std::sync::MutexGuard<'_, DemandState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Makes `prepared` the source later derivations narrow.
    pub(super) fn serve(&self, prepared: &PreparedLocalSource) {
        let Ok(plan) = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope) else {
            return;
        };
        self.state().served = Some(Served {
            prepared: prepared.clone(),
            tables: plan
                .tables()
                .iter()
                .map(|table| table.table_id().clone())
                .collect(),
        });
    }

    /// Notes a plan of `required` that installed `installed`, and answers
    /// whether a derivation should be spawned for it.
    fn note(&self, required: BTreeSet<TableId>, installed: &BTreeSet<TableId>, held: bool) -> bool {
        let mut state = self.state();
        if state
            .served
            .as_ref()
            .is_none_or(|served| served.tables == required)
        {
            return false;
        }
        let derive = *installed != required && !held;
        match state.sets.iter().position(|set| *set == required) {
            Some(index) => {
                let set = state.sets.remove(index);
                state.sets.push(set);
            }
            None => {
                if state.sets.len() == MAX_SUBSETS {
                    state.sets.remove(0);
                }
                state.sets.push(required);
            }
        }
        derive && !std::mem::replace(&mut state.scheduled, true)
    }

    fn unschedule(&self) {
        self.state().scheduled = false;
    }

    /// The sets a re-encode derives after its own, most recently planned
    /// first. A derivation spawned since still runs, and finds them
    /// published.
    pub(super) fn sets(&self) -> Vec<BTreeSet<TableId>> {
        self.state().sets.iter().rev().cloned().collect()
    }

    /// The sets to derive, most recently planned first, and the source they
    /// are parts of.
    fn take(&self) -> Option<(Vec<BTreeSet<TableId>>, PreparedLocalSource)> {
        let mut state = self.state();
        state.scheduled = false;
        let served = state.served.clone()?;
        Some((state.sets.iter().rev().cloned().collect(), served.prepared))
    }

    fn forget(&self, tables: &BTreeSet<TableId>) {
        self.state().sets.retain(|set| set != tables);
    }
}

impl LocalBackend {
    /// Notes a source-wide plan whose page asked for part of the served
    /// tables. When the plan had to install more than the page asked for,
    /// and the page did not hold that artifact already, the artifact of
    /// exactly its tables is derived in the background for the next page.
    pub(super) fn note_planned_tables(&self, plan: &BootstrapPlan) {
        if self.inner.tenants.is_some() || self.inner.reencoder.is_none() {
            return;
        }
        let [artifact] = plan.artifacts.as_slice() else {
            return;
        };
        if !plan.resident_tables.is_empty() {
            return;
        }
        let installed: BTreeSet<_> = artifact.tables.iter().cloned().collect();
        let required: BTreeSet<_> = plan.required_tables.iter().cloned().collect();
        let held = plan.diagnostics.selected_cache_hints > 0;
        if self.inner.subsets.note(required, &installed, held) {
            self.spawn_subsets();
        }
    }

    fn spawn_subsets(&self) {
        let Some(reencoder) = self.inner.reencoder.as_ref() else {
            self.inner.subsets.unschedule();
            return;
        };
        // The running re-encode's token: a re-cut or a fence that cancels it
        // cancels this too, and the re-encode it starts derives every set.
        let cancellation = reencoder.current();
        let backend = self.clone();
        let spawned = std::thread::Builder::new()
            .name("calque-subset-derive".to_owned())
            .spawn(move || {
                crate::background::lower_current_thread("the subset derivation");
                backend.run_subsets(&cancellation);
            });
        if let Err(error) = spawned {
            self.inner.subsets.unschedule();
            tracing::warn!(%error, "no subset artifact was derived");
        }
    }

    fn run_subsets(&self, cancellation: &CancellationToken) {
        let Some(reencoder) = self.inner.reencoder.as_ref() else {
            return;
        };
        let _serial = reencoder
            .serial
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some((sets, prepared)) = self.inner.subsets.take() else {
            return;
        };
        if cancellation.is_cancelled() {
            return;
        }
        let runtime = match crate::background::current_thread_runtime() {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(%error, "no subset artifact was derived");
                return;
            }
        };
        let replaced = self.derive_subsets(&runtime, &prepared, &sets, cancellation);
        self.retire_replaced(&runtime, &replaced, cancellation);
    }

    /// Derives the artifact of each of `sets` at the newest cut published for
    /// `prepared`, and returns what their re-encodes replace. A set the
    /// served tables no longer hold is forgotten.
    pub(super) fn derive_subsets(
        &self,
        runtime: &tokio::runtime::Runtime,
        prepared: &PreparedLocalSource,
        sets: &[BTreeSet<TableId>],
        cancellation: &CancellationToken,
    ) -> Vec<ArtifactId> {
        let Some(reencoder) = self.inner.reencoder.as_ref() else {
            return Vec::new();
        };
        let generation = prepared.policy.generation();
        let derived = (|| {
            let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope)
                .map_err(|_| crate::LocalSnapshotError::InvalidPlan)?;
            let query = published_query_for(prepared)?;
            let published = SqliteArtifactCatalog::open(&reencoder.config.catalog_path)
                .and_then(|catalog| catalog.published(&query))?;
            Ok::<_, MaintenanceError>(
                newest_cut(&published, &self.inner.activated).map(|cut| (plan, query, cut)),
            )
        })();
        let (plan, query, cut) = match derived {
            Ok(Some(derived)) => derived,
            Ok(None) => return Vec::new(),
            Err(error) => {
                tracing::warn!(generation, %error, "no subset artifact was derived");
                return Vec::new();
            }
        };
        let mut replaced = Vec::new();
        for tables in sets {
            if cancellation.is_cancelled() {
                break;
            }
            let outcome = runtime.block_on(derive_subset(
                ReencodeRequest {
                    state_root: &reencoder.config.state_root,
                    catalog_path: &reencoder.config.catalog_path,
                    store: self.inner.store.as_ref(),
                    query: &query,
                    cut: &cut.cursor,
                    fence: &self.inner.fence_lock,
                    cancellation,
                },
                &plan,
                tables,
                || admits_reencode(&current_serving(&self.inner.serving), generation),
                |_| {},
            ));
            match outcome {
                Ok((outcomes, ids)) => {
                    tracing::info!(
                        target: "calque::preparation",
                        generation,
                        tables = tables.len(),
                        subset = ?outcomes.subset,
                        zstd = ?outcomes.encodes.as_ref().map(|encodes| &encodes.zstd),
                        brotli = ?outcomes.encodes.as_ref().and_then(|encodes| encodes
                            .brotli
                            .as_ref()
                            .map(|outcome| outcome.as_ref().map_err(ToString::to_string))),
                        "subset derivation finished"
                    );
                    replaced.extend(ids);
                }
                Err(ReencodeError::Cancelled) => {
                    tracing::info!(generation, "subset derivation cancelled");
                    break;
                }
                Err(ReencodeError::Plan(error)) => {
                    tracing::info!(generation, %error, "a planned table set is no longer served");
                    self.inner.subsets.forget(tables);
                }
                Err(error) => {
                    tracing::warn!(generation, %error, "the subset artifact was not derived");
                }
            }
        }
        replaced
    }

    /// Retires each of `replaced` once no plan pins it.
    pub(super) fn retire_replaced(
        &self,
        runtime: &tokio::runtime::Runtime,
        replaced: &[ArtifactId],
        cancellation: &CancellationToken,
    ) {
        let Some(reencoder) = self.inner.reencoder.as_ref() else {
            return;
        };
        if replaced.is_empty() {
            return;
        }
        match runtime.block_on(crate::reencode::retire_replaced(
            &reencoder.config.catalog_path,
            replaced,
            cancellation,
        )) {
            Ok(()) | Err(ReencodeError::Cancelled) => {}
            Err(error) => {
                tracing::warn!(%error, "a replaced artifact was not retired");
            }
        }
    }
}

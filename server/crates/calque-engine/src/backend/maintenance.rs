//! Background work on the served generation: publishing, re-encoding,
//! re-cutting, and trimming the log behind them.

use super::*;

/// Whether a re-encode made under `generation` may still be published: that
/// generation is still served, and its artifact is ready. A fence moves the
/// first and a failed fence the second, both under the fence lock.
pub(super) fn admits_reencode(serving: &ServingAuthorization, generation: u64) -> bool {
    serving.generation() == generation && serving.artifact() == ARTIFACT_READY
}

/// What a re-cut made under `generation` must find when it would be
/// published: the rule a re-encode is published under, asked under the same
/// fence lock.
pub(super) fn recut_admission(
    fence_lock: &Arc<RwLock<()>>,
    serving: impl Fn() -> Arc<ServingAuthorization> + Send + Sync + 'static,
    generation: u64,
) -> PublicationAdmission {
    PublicationAdmission {
        lock: Arc::clone(fence_lock),
        admit: Arc::new(move || admits_reencode(&serving(), generation)),
    }
}

/// The background re-cut's configuration, and the source it re-cuts: the
/// served generation's, replaced once a fenced generation's artifact is
/// published.
pub(super) struct Recutter {
    pub(super) config: RecutConfig,
    pub(super) prepared: StdMutex<PreparedLocalSource>,
}

/// What one maintenance pass carries to the next.
#[derive(Default)]
pub(super) struct MaintenanceState {
    pub(super) last_recut_started: Option<Instant>,
    /// A re-cut was started and its encodes have not been seen to finish.
    pub(super) recutting: bool,
    pub(super) tail: TailMeter,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum MaintenanceError {
    #[error(transparent)]
    Catalog(#[from] artifact_catalog::CatalogError),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error(transparent)]
    Snapshot(#[from] crate::LocalSnapshotError),
    #[error(transparent)]
    Backend(#[from] LocalBackendError),
}

impl MaintenanceError {
    pub(super) const fn class(&self) -> OperationErrorClass {
        match self {
            Self::Catalog(_) => OperationErrorClass::ArtifactCatalog,
            Self::Log(_) => OperationErrorClass::CanonicalLog,
            Self::Snapshot(_) => OperationErrorClass::ArtifactBuild,
            Self::Backend(_) => OperationErrorClass::Internal,
        }
    }
}

/// Re-encodes the artifact published for `request.query` under `generation`,
/// then encodes its brotli artifact, packed or plain, and publishes each only
/// if [`admits_reencode`] holds for what `serving` holds when the catalog
/// asks, under the fence lock.
pub(super) async fn reencode_generation(
    request: ReencodeRequest<'_>,
    serving: &StdRwLock<Arc<ServingAuthorization>>,
    generation: u64,
    on_stage: impl FnMut(ReencodeStage),
) -> Result<ReencodeOutcomes, ReencodeError> {
    reencode_and_publish_brotli(
        request,
        || admits_reencode(&current_serving(serving), generation),
        on_stage,
    )
    .await
}

impl LocalBackend {
    /// Publishes the fenced generation's artifact off the request path, and
    /// marks the generation servable, or failed, when that finishes.
    pub(super) fn spawn_publication(
        &self,
        prepared: PreparedLocalSource,
        serving: Arc<ServingAuthorization>,
    ) {
        let Some(revocation) = self.inner.revocation.as_ref() else {
            serving.set_artifact(ARTIFACT_FAILED);
            return;
        };
        let publisher = Arc::clone(&revocation.publisher);
        let published = Arc::clone(&serving);
        let backend = self.clone();
        let spawned = std::thread::Builder::new()
            .name("calque-generation-publish".to_owned())
            .spawn(move || {
                let outcome = publisher.publish(&prepared);
                if let Err(failure) = &outcome {
                    tracing::error!(
                        generation = prepared.policy.generation(),
                        error = %failure.detail,
                        state_must_be_destroyed = failure.state_must_be_destroyed,
                        "the fenced generation's artifact was not published"
                    );
                }
                published.set_artifact(if outcome.is_ok() {
                    ARTIFACT_READY
                } else {
                    ARTIFACT_FAILED
                });
                if outcome.is_ok() {
                    if let Some(recutter) = backend.inner.recut.as_ref() {
                        *recutter
                            .prepared
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = prepared.clone();
                    }
                    backend.spawn_reencode(&prepared);
                }
            });
        if spawned.is_err() {
            serving.set_artifact(ARTIFACT_FAILED);
        }
    }

    /// Re-encodes the artifact published for `prepared` on a thread of its
    /// own, after cancelling any re-encode still running. The result is
    /// published only while `prepared`'s generation is still served and its
    /// artifact is ready.
    pub fn spawn_reencode(&self, prepared: &PreparedLocalSource) {
        self.spawn_reencode_in(prepared, false);
    }

    /// [`Self::spawn_reencode`] in the background class, for a re-cut's
    /// artifact.
    pub(super) fn spawn_recut_reencode(&self, prepared: &PreparedLocalSource) {
        self.spawn_reencode_in(prepared, true);
    }

    pub(super) fn spawn_reencode_in(&self, prepared: &PreparedLocalSource, lowered: bool) {
        let Some(reencoder) = self.inner.reencoder.as_ref() else {
            return;
        };
        let generation = prepared.policy.generation();
        let query = match published_query_for(prepared) {
            Ok(query) => query,
            Err(error) => {
                tracing::warn!(generation, %error, "the published artifact was not re-encoded");
                return;
            }
        };
        let cancellation = reencoder.replace();
        let span = reencoder.begin_encoding();
        let backend = self.clone();
        let spawned = std::thread::Builder::new()
            .name("calque-artifact-reencode".to_owned())
            .spawn(move || {
                if lowered {
                    crate::background::lower_current_thread("the re-encode");
                }
                backend.run_reencode(generation, &query, &cancellation, &span);
            });
        if let Err(error) = spawned {
            tracing::warn!(generation, %error, "the published artifact was not re-encoded");
        }
    }

    pub(super) fn run_reencode(
        &self,
        generation: u64,
        query: &PublishedArtifactQuery,
        cancellation: &CancellationToken,
        span: &EncodingSpan,
    ) {
        let Some(reencoder) = self.inner.reencoder.as_ref() else {
            return;
        };
        let _serial = reencoder
            .serial
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cancellation.is_cancelled() {
            return;
        }
        let runtime = match crate::background::current_thread_runtime() {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(generation, %error, "the published artifact was not re-encoded");
                return;
            }
        };
        let newest = SqliteArtifactCatalog::open(&reencoder.config.catalog_path)
            .and_then(|catalog| catalog.published(query))
            .map(|published| newest_cut(&published, &self.inner.activated));
        let cut = match newest {
            Ok(Some(newest)) => newest.cursor,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(generation, %error, "the published artifact was not re-encoded");
                return;
            }
        };
        let outcome = runtime.block_on(reencode_generation(
            ReencodeRequest {
                state_root: &reencoder.config.state_root,
                catalog_path: &reencoder.config.catalog_path,
                store: self.inner.store.as_ref(),
                query,
                cut: &cut,
                fence: &self.inner.fence_lock,
                cancellation,
            },
            &self.inner.serving,
            generation,
            |stage| {
                if stage == ReencodeStage::Retiring {
                    span.close();
                }
            },
        ));
        match outcome {
            Ok(ReencodeOutcomes { zstd, brotli }) => {
                tracing::info!(
                    target: "calque::preparation",
                    generation,
                    outcome = ?zstd,
                    "re-encode finished"
                );
                match brotli {
                    Some(Ok(outcome)) => tracing::info!(
                        target: "calque::preparation",
                        generation,
                        ?outcome,
                        "brotli encode finished"
                    ),
                    Some(Err(error)) => tracing::warn!(
                        generation,
                        %error,
                        "the brotli artifact was not published"
                    ),
                    None => {}
                }
            }
            Err(ReencodeError::Cancelled) => {
                tracing::info!(generation, "re-encode cancelled");
            }
            Err(error) => {
                tracing::warn!(generation, %error, "the published artifact was not re-encoded");
            }
        }
    }

    /// Starts the thread that, every `MAINTENANCE_INTERVAL`, measures the
    /// canonical log against the newest cut of the served scope: it re-cuts
    /// the scope once the tail is due, retires the older cuts' artifacts no
    /// plan pins, and trims the log behind the oldest cut still needed.
    pub fn spawn_recut(&self) {
        let Some(recutter) = self.inner.recut.as_ref() else {
            return;
        };
        let cancellation = recutter.config.cancellation.clone();
        let backend = self.clone();
        let spawned = std::thread::Builder::new()
            .name("calque-log-maintenance".to_owned())
            .spawn(move || backend.run_maintenance(&cancellation));
        if let Err(error) = spawned {
            tracing::warn!(%error, "the canonical log is neither trimmed nor re-cut");
        }
    }

    pub(super) fn run_maintenance(&self, cancellation: &CancellationToken) {
        let Some(recutter) = self.inner.recut.as_ref() else {
            return;
        };
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(%error, "the canonical log is neither trimmed nor re-cut");
                return;
            }
        };
        let mut state = MaintenanceState::default();
        while !cancellation.is_cancelled() {
            if let Err(error) = self.maintain(recutter, &runtime, &mut state) {
                self.report_maintenance_error(&error);
            }
            runtime.block_on(async {
                tokio::select! {
                    () = cancellation.cancelled() => {}
                    () = tokio::time::sleep(MAINTENANCE_INTERVAL) => {}
                }
            });
        }
    }

    /// One maintenance pass. Everything that reads the plans and pins or
    /// moves the log floor holds the catalog lock, which a bootstrap holds
    /// from its log read through its retention lease, so no plan is ever cut
    /// from under its own planning.
    pub(super) fn maintain(
        &self,
        recutter: &Recutter,
        runtime: &tokio::runtime::Runtime,
        state: &mut MaintenanceState,
    ) -> Result<(), MaintenanceError> {
        if let Some(tenants) = self.inner.tenants.as_ref() {
            return self.maintain_tenants(tenants);
        }
        let encoding = self
            .inner
            .reencoder
            .as_ref()
            .is_some_and(Reencoder::is_encoding);
        if state.recutting && !encoding {
            state.recutting = false;
            self.inner.metrics.record_artifact_recut_in_progress(false);
        }
        let fences = recutter.config.publisher.fences();
        let serving = self.serving();
        let prepared = recutter
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let generation = prepared.policy.generation();
        if serving.artifact() != ARTIFACT_READY || serving.generation() != generation {
            return Ok(());
        }
        let query = published_query_for(&prepared)?;
        let now = now_unix_micros()?;
        let now_ms = now / 1_000;
        let newest = {
            let mut catalog = self.inner.catalog.blocking_lock();
            let published = catalog.published(&query)?;
            let Some(newest) = newest_cut(&published, &self.inner.activated) else {
                return Ok(());
            };
            // A store or catalog that keeps failing here must not stop the
            // trim below, or the log would grow for as long as it fails.
            if let Err(error) =
                self.retire_older_cuts(&mut catalog, runtime, &published, &newest, now_ms)
            {
                self.report_maintenance_error(&error);
            }
            newest
        };

        let tail = self.inner.tail.as_deref();
        let floor = retention_floor(newest.sequence, tail.and_then(ChangeTail::head));
        let bounds = self.inner.log.blocking_lock().bounds()?;
        let retained = self.trim_log(bounds.minimum_retained.sequence, floor)?;
        let head = bounds.head.sequence;
        let tail_bytes = state
            .tail
            .measure(newest.sequence, head, |after, through| {
                self.inner
                    .log
                    .blocking_lock()
                    .entry_bytes_between(after, through)
            })?;
        let tail_entries = head.saturating_sub(newest.sequence);
        let policy = recutter.config.policy;
        self.inner.metrics.record_canonical_log_retention(
            tail_entries,
            tail_bytes,
            head.saturating_add(1).saturating_sub(retained),
            policy.threshold_bytes(newest.decoded_bytes),
        );

        // A due re-cut does not wait for the newest cut's encodes, and cancels
        // them: under steady writes the tail stays bounded by the interval
        // however long an encode would take, and a re-cut's build never runs
        // beside the encode of the cut it replaces.
        if !policy.due(tail_bytes, newest.decoded_bytes)
            || !policy.rested(
                newest.cut_at,
                now,
                state.last_recut_started.map(|started| started.elapsed()),
            )
        {
            return Ok(());
        }
        self.cancel_reencode();
        let started = Instant::now();
        state.last_recut_started = Some(started);
        state.recutting = true;
        self.inner.metrics.record_artifact_recut_in_progress(true);
        tracing::info!(
            target: "calque::preparation",
            generation,
            tail_entries,
            tail_bytes,
            "re-cut started"
        );
        let backend = self.clone();
        let admission = recut_admission(
            &self.inner.fence_lock,
            move || backend.serving(),
            generation,
        );
        match recutter
            .config
            .publisher
            .recut(&prepared, admission, fences)
        {
            Ok(manifest) => {
                self.inner.metrics.record_artifact_recut();
                tracing::info!(
                    target: "calque::preparation",
                    generation,
                    artifact = %manifest.manifest().artifact_id,
                    tail_entries,
                    tail_bytes,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "re-cut published"
                );
                self.spawn_recut_reencode(&prepared);
            }
            Err(failure) => {
                state.recutting = false;
                self.inner.metrics.record_artifact_recut_in_progress(false);
                if failure.preempted {
                    tracing::info!(
                        target: "calque::preparation",
                        generation,
                        "the re-cut gave way to a fenced generation"
                    );
                } else {
                    tracing::warn!(
                        generation,
                        error = %failure.detail,
                        "the re-cut was not published"
                    );
                    self.inner
                        .metrics
                        .record_error(OperationErrorClass::ArtifactBuild);
                    // The newest cut is still the one served: its encodes,
                    // cancelled when the re-cut started, go on.
                    self.spawn_recut_reencode(&prepared);
                }
            }
        }
        Ok(())
    }

    /// A partitioned scope's pass. There is no source-wide cut to re-cut or
    /// retire: the log is kept for every tenant's cut and running build, the
    /// tenant follower and the change tail, and each plan's lease keeps it
    /// for the plan.
    pub(super) fn maintain_tenants(&self, tenants: &Tenants) -> Result<(), MaintenanceError> {
        let floor = retention_floor(
            tenants.retained_after(),
            self.inner.tail.as_deref().and_then(ChangeTail::head),
        );
        let bounds = self.inner.log.blocking_lock().bounds()?;
        let retained = self.trim_log(bounds.minimum_retained.sequence, floor)?;
        let head = bounds.head.sequence;
        // The tail gauges follow the warm tenant whose log since its cut is
        // longest against its artifact.
        let tail = tenants.longest_tail();
        self.inner.metrics.record_canonical_log_retention(
            tail.as_ref()
                .map_or(0, |tail| head.saturating_sub(tail.cut_sequence)),
            tail.as_ref().map_or(0, |tail| tail.tail_bytes),
            head.saturating_add(1).saturating_sub(retained),
            tail.as_ref().map_or(0, |tail| tail.threshold_bytes),
        );
        Ok(())
    }

    /// Trims the log from `retained` toward `floor` under the catalog lock,
    /// and drops what it trimmed from the change tail.
    pub(super) fn trim_log(&self, retained: u64, floor: u64) -> Result<u64, LogError> {
        let tail = self.inner.tail.as_deref();
        trim(retained, floor, |minimum| {
            let _catalog = self.inner.catalog.blocking_lock();
            let mut log = self.inner.log.blocking_lock();
            log.retain_from(minimum)?;
            let retained = log.minimum_retained()?.sequence;
            if let Some(tail) = tail {
                tail.trimmed(retained);
            }
            Ok(retained)
        })
    }

    /// Retires every published cut but the newest that no plan pins, and
    /// deletes the retired cuts' objects.
    pub(super) fn retire_older_cuts(
        &self,
        catalog: &mut SqliteArtifactCatalog,
        runtime: &tokio::runtime::Runtime,
        published: &[ValidatedArtifactManifest],
        newest: &NewestCut,
        now_ms: i64,
    ) -> Result<(), MaintenanceError> {
        for manifest in published {
            let manifest = manifest.manifest();
            if manifest.pit.snapshot_cursor == newest.cursor {
                continue;
            }
            match catalog.retire(&manifest.artifact_id, now_ms, "a newer cut replaces it") {
                Ok(_) | Err(artifact_catalog::CatalogError::ArtifactPinnedByPlan { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        runtime.block_on(catalog.delete_retired(self.inner.store.as_ref(), now_ms))?;
        Ok(())
    }

    pub(super) fn report_maintenance_error(&self, error: &MaintenanceError) {
        tracing::warn!(%error, "canonical-log maintenance failed; the next pass retries");
        self.inner.metrics.record_error(error.class());
    }

    pub(super) fn cancel_reencode(&self) {
        if let Some(reencoder) = self.inner.reencoder.as_ref() {
            reencoder.cancel();
        }
    }
}

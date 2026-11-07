use std::{
    sync::mpsc::{self, RecvTimeoutError},
    thread,
    time::{Duration, SystemTime},
};

use tokio_postgres::Client;

use crate::{guard::PostgresConnectionFamily, DatabaseUrl, OperationsMetrics};

/// How often the sampler reads the lag, and how long one reading may take.
#[derive(Clone, Copy)]
struct Timing {
    sample_interval: Duration,
    /// Bounds the whole connection, authentication included, not only the
    /// socket's connect.
    connect_timeout: Duration,
    query_timeout: Duration,
}

const TIMING: Timing = Timing {
    sample_interval: Duration::from_secs(1),
    connect_timeout: Duration::from_secs(2),
    query_timeout: Duration::from_secs(1),
};

const SLOT_LAG_SQL: &str = "SELECT (pg_catalog.pg_current_wal_lsn() - confirmed_flush_lsn)::bigint
   FROM pg_catalog.pg_replication_slots
  WHERE slot_name = $1";

/// Samples the replication slot's lag, `pg_current_wal_lsn() -
/// confirmed_flush_lsn`, into `calque_cdc_lag_bytes`, and the sample's time
/// into `calque_cdc_lag_sample_timestamp_seconds`, a second after each
/// previous sample, on a thread and a source connection of its own. Neither
/// is exported before the first successful sample.
///
/// The CDC thread is the only one that advances visible state, so it never
/// waits on this: a source that refuses, stalls or authenticates slowly
/// delays only the gauge. A failed sample leaves the last reading and its
/// sample time in place and drops the connection, and the next sample
/// connects again.
///
/// Dropping the sampler stops the thread without joining it; a sample in
/// flight finishes within its timeouts.
pub(crate) struct SlotLagSampler {
    _stop: mpsc::Sender<()>,
}

impl SlotLagSampler {
    pub(crate) fn start(
        source: DatabaseUrl,
        slot_name: String,
        metrics: OperationsMetrics,
    ) -> std::io::Result<Self> {
        Self::start_with(source, slot_name, metrics, TIMING)
    }

    fn start_with(
        source: DatabaseUrl,
        slot_name: String,
        metrics: OperationsMetrics,
        timing: Timing,
    ) -> std::io::Result<Self> {
        let (stop, stopped) = mpsc::channel::<()>();
        thread::Builder::new()
            .name("calque-slot-lag".to_owned())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                let mut client = None;
                loop {
                    match runtime.block_on(sample(&mut client, &source, &slot_name, timing)) {
                        Ok(Some(lag)) => metrics.record_cdc_lag_sample(lag, SystemTime::now()),
                        Ok(None) => {}
                        Err(()) => client = None,
                    }
                    if !matches!(
                        stopped.recv_timeout(timing.sample_interval),
                        Err(RecvTimeoutError::Timeout)
                    ) {
                        return;
                    }
                }
            })?;
        Ok(Self { _stop: stop })
    }
}

/// One reading: `Err` when the source failed or did not answer in time, and
/// `Ok(None)` when it answered without a lag, as before the slot exists.
async fn sample(
    client: &mut Option<Client>,
    source: &DatabaseUrl,
    slot_name: &str,
    timing: Timing,
) -> Result<Option<u64>, ()> {
    let client = match client {
        Some(client) => client,
        None => {
            let endpoint = source
                .endpoint(PostgresConnectionFamily::SlotLag)
                .map_err(drop)?;
            let (connected, connection) =
                tokio::time::timeout(timing.connect_timeout, endpoint.connect_async())
                    .await
                    .map_err(drop)?
                    .map_err(drop)?;
            tokio::spawn(connection);
            client.insert(connected)
        }
    };
    let row = tokio::time::timeout(
        timing.query_timeout,
        client.query_opt(SLOT_LAG_SQL, &[&slot_name]),
    )
    .await
    .map_err(drop)?
    .map_err(drop)?;
    let lag: Option<i64> = row.and_then(|row| row.get(0));
    Ok(lag.map(|lag| u64::try_from(lag).unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::{Instant, UNIX_EPOCH},
    };

    use postgres::{Client as SyncClient, NoTls as SyncNoTls};

    use super::*;
    use crate::test_support::silent_listener;

    fn live_database_url() -> Option<String> {
        std::env::var("CALQUE_SOURCE_DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("TEST_DATABASE_URL").ok())
    }

    fn gauge(metrics: &OperationsMetrics) -> Option<u64> {
        metrics
            .encode()
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("calque_cdc_lag_bytes "))
            .map(|value| value.parse().unwrap())
    }

    fn sampled_at(metrics: &OperationsMetrics) -> Option<f64> {
        metrics
            .encode()
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("calque_cdc_lag_sample_timestamp_seconds "))
            .map(|value| value.parse().unwrap())
    }

    fn unix_seconds(time: SystemTime) -> f64 {
        time.duration_since(UNIX_EPOCH).unwrap().as_secs_f64()
    }

    /// The sampler's timing scaled down, so that a test waits out a stalled
    /// connection in a tenth of a second rather than two.
    const QUICK: Timing = Timing {
        sample_interval: Duration::from_millis(50),
        connect_timeout: Duration::from_millis(100),
        query_timeout: Duration::from_millis(50),
    };

    /// Waits for the listener's second connection, which the sampler opens
    /// only after the sample on the first one failed. The deadline is far
    /// above `QUICK`'s 150 ms, so a loaded host does not fail the test.
    fn wait_for_a_failed_sample(accepted: &AtomicUsize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while accepted.load(Ordering::SeqCst) < 2 {
            assert!(
                Instant::now() < deadline,
                "the sampler did not connect again after a stalled connection"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn a_lag_that_was_never_sampled_is_absent_from_the_scrape() {
        let (url, accepted) = silent_listener();
        let metrics = OperationsMetrics::new();

        let sampler = SlotLagSampler::start_with(
            DatabaseUrl::parse(&url).unwrap(),
            "a_slot".to_owned(),
            metrics.clone(),
            QUICK,
        )
        .unwrap();
        wait_for_a_failed_sample(&accepted);
        drop(sampler);

        let scrape = metrics.encode().unwrap();
        assert!(
            !scrape.contains("calque_cdc_lag"),
            "a lag no sample measured is exported:\n{scrape}"
        );
    }

    #[test]
    fn a_source_that_stops_answering_keeps_the_last_reading_and_its_sample_time() {
        let (url, accepted) = silent_listener();
        let metrics = OperationsMetrics::new();
        metrics.record_cdc_lag_sample(4_242, UNIX_EPOCH + Duration::from_secs(1_700_000_000));

        let started = Instant::now();
        let sampler = SlotLagSampler::start_with(
            DatabaseUrl::parse(&url).unwrap(),
            "a_slot".to_owned(),
            metrics.clone(),
            QUICK,
        )
        .unwrap();
        assert!(started.elapsed() < Duration::from_millis(100));
        wait_for_a_failed_sample(&accepted);
        assert_eq!(gauge(&metrics), Some(4_242));
        assert_eq!(sampled_at(&metrics), Some(1_700_000_000.0));

        let dropped = Instant::now();
        drop(sampler);
        assert!(dropped.elapsed() < Duration::from_millis(100));
    }

    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn the_gauge_reports_the_slots_lag_behind_a_waiting_backlog() {
        let url = live_database_url().expect("live source database URL");
        let mut source = SyncClient::connect(&url, SyncNoTls).unwrap();
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let slot = format!("slot_lag_{suffix}");
        let table = format!("slot_lag_{suffix}");
        source
            .query_one(
                "SELECT pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .unwrap();
        source
            .batch_execute(&format!(
                "CREATE TABLE public.{table} (id bigint PRIMARY KEY, value text NOT NULL);
                 INSERT INTO public.{table} SELECT id, repeat('x', 512)
                   FROM generate_series(1, 256) AS id;"
            ))
            .unwrap();
        let lag = |source: &mut SyncClient| -> u64 {
            let lag: i64 = source
                .query_one(
                    "SELECT (pg_catalog.pg_current_wal_lsn() - confirmed_flush_lsn)::bigint
                       FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&slot],
                )
                .unwrap()
                .get(0);
            u64::try_from(lag).unwrap()
        };
        let lowest = lag(&mut source);
        assert!(
            lowest >= 64 * 1024,
            "the backlog left only {lowest} bytes of lag"
        );

        let metrics = OperationsMetrics::new();
        let started = SystemTime::now();
        let sampler = SlotLagSampler::start(
            DatabaseUrl::parse(&url).unwrap(),
            slot.clone(),
            metrics.clone(),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let sample = loop {
            match gauge(&metrics) {
                Some(sample) if sample > 0 => break sample,
                _ => {
                    assert!(Instant::now() < deadline, "no sample within 10 s");
                    thread::sleep(Duration::from_millis(20));
                }
            }
        };
        let sampled = sampled_at(&metrics).expect("a sample's time is exported with it");
        let read = SystemTime::now();
        let highest = lag(&mut source);
        drop(sampler);
        assert!(
            (lowest..=highest).contains(&sample),
            "the sample {sample} is not the slot's lag, which lay in [{lowest}, {highest}]"
        );
        assert!(
            (unix_seconds(started)..=unix_seconds(read)).contains(&sampled),
            "the sample time {sampled} lies outside the sampler's run, [{}, {}]",
            unix_seconds(started),
            unix_seconds(read)
        );

        source
            .query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&slot])
            .unwrap();
        source
            .batch_execute(&format!("DROP TABLE public.{table}"))
            .unwrap();
    }
}

//! The vendored driver's `reply_timeout` (`vendor/tokio-postgres/CALQUE_PATCH.md`)
//! against a live PostgreSQL.

use std::{
    env, io,
    time::{Duration, Instant},
};

use postgres_connect::Endpoint;

fn database_url() -> Option<String> {
    env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| env::var("CALQUE_CONTROL_DATABASE_URL").ok())
}

fn client_with_reply_timeout(reply_timeout: Duration) -> postgres::Client {
    let url = database_url().expect("live database URL");
    let mut endpoint = Endpoint::parse(&url).unwrap();
    endpoint.config_mut().reply_timeout(reply_timeout);
    endpoint.connect().unwrap()
}

#[test]
#[ignore = "requires TEST_DATABASE_URL or CALQUE_CONTROL_DATABASE_URL"]
fn a_reply_that_does_not_start_within_the_timeout_fails_the_connection() {
    let mut client = client_with_reply_timeout(Duration::from_millis(700));
    let started = Instant::now();
    let error = client.batch_execute("SELECT pg_sleep(2)").unwrap_err();
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(700) && elapsed < Duration::from_millis(1_800),
        "the wait ended after {elapsed:?}"
    );
    let source = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<io::Error>())
        .expect("an I/O error");
    assert_eq!(source.kind(), io::ErrorKind::TimedOut);
    assert!(client.is_closed());
}

/// The wait restarts at each message, and a connection idle between requests
/// does not count its idle time against the next one.
#[test]
#[ignore = "requires TEST_DATABASE_URL or CALQUE_CONTROL_DATABASE_URL"]
fn messages_that_keep_arriving_and_idle_time_between_requests_do_not_run_it_out() {
    let mut client = client_with_reply_timeout(Duration::from_millis(700));
    client.batch_execute("SELECT pg_sleep(0.3)").unwrap();
    let started = Instant::now();
    client
        .batch_execute(
            "DO $$ BEGIN
               FOR i IN 1..4 LOOP
                 PERFORM pg_sleep(0.4);
                 RAISE NOTICE 'still working';
               END LOOP;
             END $$",
        )
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(1_600));
    std::thread::sleep(Duration::from_millis(1_000));
    client.batch_execute("SELECT 1").unwrap();
}

#[test]
#[ignore = "requires TEST_DATABASE_URL or CALQUE_CONTROL_DATABASE_URL"]
fn without_a_reply_timeout_a_slow_reply_is_waited_for() {
    let url = database_url().expect("live database URL");
    let mut client = Endpoint::parse(&url).unwrap().connect().unwrap();
    client.batch_execute("SELECT pg_sleep(1)").unwrap();
}

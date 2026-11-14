# Calque tokio-postgres patch

This directory vendors the published `tokio-postgres` 0.7.18 crate from
upstream commit `f1cb6ec0d5766b136cbd68f3010d64142a5daa66`. The downloaded
crate archive had SHA-256
`a528f7d280f6d5b9cd149635c8705b0dd049754bc67d81d31fa25169a93809d3`.

The patch changes two things.

## The connect timeout bounds the whole attempt

In `src/connect.rs`, `connect_timeout` bounds the whole connection attempt on
each address, as libpq's `connect_timeout` does, instead of the socket
connect alone. The attempt covers the socket, TLS negotiation, the startup
and authentication exchange and the `target_session_attrs` check. One that
runs out returns the same `Error::connect` with `io::ErrorKind::TimedOut`
that a socket connect out of time already returned.

Every PostgreSQL connection Calque opens through `postgres` or
`tokio-postgres` carries a `connect_timeout`. Upstream applies it to the socket
alone, so a peer that accepts the connection and never answers the startup
message holds the attempt forever. Docker's port proxy does this while its
container is unreachable: a running `calque` stuck there in change capture's
reconnect never re-established its session and could not stop on SIGINT or
SIGTERM. `server/apps/calque/tests/dev_startup.rs` and
`server/apps/calque/tests/dev_stop_live.rs` drive that peer through the binary.

## A reply timeout

`Config::reply_timeout` (`src/config.rs`, applied in `src/connect_raw.rs` and
`src/connection.rs`) fails the connection with `io::ErrorKind::TimedOut` once
a request has awaited its reply that long without the server sending
anything. The wait restarts at each message, and pauses while the connection
holds messages its client has not taken. It is unset by default, is not read
from connection strings, and libpq has no equivalent.

`postgres_connect::Endpoint::bound_replies` sets it a margin past a
session's `statement_timeout`, on every connection that serves a source, so a
request whose server or proxy stops answering returns instead of waiting on
the socket forever.
`server/crates/postgres-connect/tests/reply_timeout_live.rs` covers the
driver, and the lease store's, canonical log's and source's tests cover the
connections.

Remove the `[patch.crates-io]` override once a maintained upstream release
bounds the whole connect attempt and offers a reply timeout, and those tests
pass unchanged.

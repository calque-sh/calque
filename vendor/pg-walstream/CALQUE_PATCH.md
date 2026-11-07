# Calque pg_walstream patch

This directory vendors the published `pg_walstream` 0.8.0 crate from upstream
tag commit `784e6441cb78affd73c040bc419c2cc3085eebc1`. The downloaded crate archive
had SHA-256
`e7ab7c4175ec27f84dab31b6dab80340ef84fa3de886d10f1b3fb708729bfc0d`.

The dependency remains the maintained PostgreSQL protocol and replication
transport boundary. Calque does not own a second pgoutput parser or ACK
transport. The local patch is limited to preserving one discarded protocol
fact, exposing observer-only feedback boundaries, and one read of data that
has already reached the connection:

- `StreamingReplicationMessage::data_xid` retains the transaction or
  subtransaction XID prefixed to every streamed Relation, Type, Insert, Update,
  Delete, Truncate, and Message frame;
- `xid` retains its existing upstream meaning: the top-level XID selected by
  Stream Start;
- the inverse encoder emits `data_xid`, so PostgreSQL bytes with different top
  and subtransaction XIDs remain byte-identical after parse/re-encode;
- `new_streaming_data` supports exact fixtures while `new_streaming` remains
  source-compatible for top-level streamed data;
- two crate-private stream-stop helpers use the same connection-backend feature
  gate as their only callers, avoiding a parser-only dead-code warning without
  changing the public API or runtime behavior.
- `send_standby_status_update_observed` delegates to the same sole feedback
  implementation while reporting encoded, CopyData-queued, pre-flush, and
  flushed boundaries. The original method uses a shared no-op observer.
- the observer is synchronous instrumentation only: it cannot change feedback
  bytes, authorize a write, or create another socket path. `CopyDataQueued`
  deliberately does not claim that libpq has written bytes to the OS socket.
- `try_get_copy_data` on the libpq backend returns a CopyData message that has
  already reached the connection, queued, buffered inside libpq or waiting on
  the non-blocking socket, and `None` when no complete message could be read:
  none has arrived, or `PQgetCopyData` reported an error (-2), which the drain
  it shares with `get_copy_data_async` treats as nothing to read. It never
  waits, and it does not return that error. Calque's
  ingest loop uses it to append every source transaction already received as
  one group without waiting for more. The native backend, which Calque does
  not compile, does not have it.

The patch is additive and intended for upstream submission. Remove the
`[patch.crates-io]` override only after a maintained upstream release exposes
both identities, the feedback observer contract and a non-blocking read of
received CopyData, and the Calque protocol and process-takeover fixtures pass
unchanged.

Calque runs the fork with protocol 1 and streaming disabled, against
PostgreSQL 18 only.

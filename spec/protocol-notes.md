# Wire-protocol implementation notes

The version-1 `.proto` contracts are frozen; *Unknown fields and compatibility* below states what may still change.

## Transport framing

For HTTP streaming and stored test fixtures, use this deterministic outer framing:

```text
4 bytes  payload length, unsigned big-endian
N bytes  one serialized protobuf top-level message
```

Rules:

- No frame may exceed the negotiated/server-configured limit.
- Reject a length before allocating if it exceeds that limit.
- A zero-length frame is invalid.
- Truncated frames are retryable transport failures, never partially applied data.
- HTTP compression, if used for change streams, wraps the framed byte stream; frame boundaries are defined after decoding.
  The server compresses only a fenced request's response, with `Content-Encoding: zstd`
  when the request's `Accept-Encoding` names `zstd` with a weight above zero: one frame for
  the whole body, flushed after each batch so each batch decodes as it arrives. A live
  response and a refusal go uncompressed, and every response that carries batches says
  `Vary: Accept-Encoding`.
- The snapshot artifact protocol is separate. The manifest's decoded object
  names its format in `decoded.format`, and the bootstrap plan's
  `artifactRuntime` carries the same discriminant, which the planner copies
  from `manifest.decoded.format` rather than re-deriving it. The protocol
  defines one format, `directColumnar`. A plan selects only artifacts built for
  the decoder the request declares.
- `artifactRuntime` also carries `identity` (the
  two header strings `schemaHash` and `collectionSetHash` do not pin), `schema`
  (the physical schema the decoder re-hashes to `schemaHash` before reading
  payload), and per table `columns` (the wire column ids a change contract
  binds). All three come from the immutable manifest: its `decoded` carries
  `identity` and `schema`, every one of its `tables[]` carries `columns`, and
  the planner copies them verbatim, so a plan can only say what the bytes' own
  manifest says. `@calque/client` requires all three to open an artifact and
  bind its change stream.
  `spec/bootstrap-plan.direct-columnar.example.json` is the vector both sides
  check: `bootstrap-api` round-trips it through `BootstrapPlan` and validates
  it against the schema, the client parser reads it as a plan.
- **Compatibility of the columnar manifest.** `identity`, `schema`, and every
  table's non-empty `columns` are required by
  `spec/artifact-manifest.schema.json` and carry no serde default, so a
  manifest persisted without them is refused by
  `ValidatedArtifactManifest::parse` instead of being migrated. Artifacts are
  immutable and content-addressed, so the repair for a manifest that cannot
  describe its own bytes is to derive the artifact again and register the new
  manifest, never to default the missing facts. `calque serve` publishes
  direct-columnar manifests into `.calque-serve/catalog.sqlite3`, and
  `calque backup|restore|verify-restore` treat that directory as durable state:
  it is rebuilt by `calque restore` from a backup that has the three fields, or
  by a fresh `serve` against the source. `calque dev`'s state directory is
  disposable: `docker compose down -v && rm -rf .calque-dev` discards it, and
  the next `pnpm demo` prepares it again.
- A bootstrap request's `requiredTables` may spell a table either as its wire
  `tableId` or as the source relation `schema.table`. The server resolves a
  relation to the wire id of the enrolled table with that source identity
  before planning and refuses an unenrolled relation as an invalid request.
  The plan it returns always names wire ids: `requiredTables`,
  `artifacts[].tables`, and `artifactRuntime.tables[].tableId` are the ids the
  change stream is subscribed with, and each runtime table carries the
  `sourceSchema` and `sourceTable` it was resolved from. Generated bindings
  therefore name relations, not wire ids, and an application build is bound
  to no one database identity or timeline.
- A bootstrap request that renews a plan carries
  `residentStateProof`, the `nextResidentStateProof` of the plan in force;
  `residentTables`, the wire ids of the tables the client holds; and
  `residentCursor`, the cursor through which it has applied every change to
  them. `residentTables` without the proof, or with `residentCursor` absent,
  and `residentCursor` without `residentTables`, are invalid requests, and so
  is a cursor this source did not sign for its epoch or one earlier than the
  proof's cursor. A cursor outside the retained log, or a proof whose cache
  key is not the one the bearer's scope gives now, is answered `409`
  `RESNAPSHOT_REQUIRED`. A plan that renews every table it requires names no
  artifact, and its `residentCursor`, `activationCursor` and change stream's
  `fromCursorExclusive` and `throughCursorInclusive` are the claimed cursor.

Recommended starting limits:

```text
frame payload                 <= 4 MiB
single encoded operation      <= 2 MiB unless table profile raises it
transaction staged bytes      <= source/table configured ceiling
batch decoded bytes           <= 16 MiB
live buffered bytes/client    bounded by apply-latency feedback
```

A transaction larger than one frame is split into consecutive `TransactionFragment` frames. Frames from two transactions must not interleave in version 1. The client applies nothing until the final fragment and all integrity/order checks pass.

## Cursor rules

- `Cursor.token` is opaque outside the server cursor codec.
- Never compare cursor bytes lexicographically in client code.
- Batch continuity is exact equality: the next `from_cursor_exclusive` must equal the prior trusted `through_cursor_inclusive`.
- `BatchEnd.through_cursor_inclusive` may advance across canonical transactions with no relevant authorized operation.
- A source epoch/generation mismatch is a structured rebuild condition, not a best-effort replay.

## Transaction rules

- Operations are in canonical PostgreSQL transaction order.
- All authorized operations for all requested resident/staging tables touched by one PostgreSQL transaction stay in one logical transaction envelope.
- A transaction filtering leaves with no operation for this client sends no fragment. `BatchEnd.acknowledged_mutation_ids` names the request's pending mutation ids that a transaction the batch covers committed, whether or not that transaction sent a fragment.
- A `TransactionFragment` carries at least one operation and nothing that depends on the client, so every client of one authorization scope and table set is sent the same bytes for it.
- Streamed PostgreSQL transaction fragments have already been assembled/normalized server-side; client fragmentation is only a transport concern.
- A client cursor advances only inside the engine generation that applies the complete logical transaction, and only once it applies completely.

## Value encoding

The protobuf `CellValue` union is the logical wire form, not PostgreSQL’s native binary type representation. A schema generation fixes:

```text
PostgreSQL type/domain
→ canonical logical type
→ protobuf CellValue member
→ direct-columnar column kind
```

Do not silently coerce unsupported values. Unsupported type, range, collation, timestamp, decimal, or JSON semantics must fail schema enrollment/build or use an explicitly versioned codec.

## Checksums

`body_sha256` uses this version-2 preimage:

```text
ASCII "calque.change.body-sha256.v2\0"
|| selected body protobuf field tag as u8 (10 through 15)
|| known-field body byte length as unsigned u32 big-endian
|| deterministic protobuf serialization of the selected body's known fields
```

The tag byte is the body's own protobuf field number, so adding a body to the
`oneof` widens the legal range without changing the formula or the bytes any
existing body hashes to.

Unknown protobuf fields are excluded because Prost does not retain them and
they cannot affect version-1 semantics. RustCrypto `sha2` and browser Web Crypto
implement the hash; the shared heartbeat vector is
`3354e0618a1b1bc8398e9b660a5f6af1f02c6a2702f8f2dc4f4e4a67edab420e`.
Receivers must verify the digest before accepting a frame into stream state.
A receiver may take it over the selected body's bytes as they arrived rather
than over a fresh serialization of the decoded body: a conformant sender's
body is that serialization, so the digest is the same. The page does, and
refuses a frame that selects a body more than once, whose decoded body no
single run of its bytes carries. A body carrying fields the page does not know
then verifies when its sender's digest covers them, and the page ignores them.

Version 1 put a per-response `frame_sequence` in the preimage, so a frame's
bytes differed for every response that carried it. Version 2 drops it, and a
batch binds its frames instead in `BatchEnd`:

```text
frame_count   = the number of frames before BatchEnd, BatchStart included
frames_sha256 = SHA-256( ASCII "calque.change.batch-frames-sha256.v1\0"
                         || body_sha256 of each of those frames, in order )
```

A receiver refuses a `BatchEnd` whose count or digest is not the one it
computed over the frames it accepted since `BatchStart`, so a frame dropped,
repeated or moved within a batch, or taken from another batch, fails the
batch. The shared vector for two copies of the heartbeat above is
`4e3e251e441747e7acca24728d1d2b71b04638551ebcb611bbc1c3541c9a24ca`, and for
no frames `0038f268dd7e088a6f73143de821a658ecb6fcf0238f50f837e85c4ca73d8ba2`.

Artifact hashes are independent and always SHA-256 over decoded/encoded artifact bytes as declared by the manifest.

## Unknown fields and compatibility

- A v1 reader may preserve/ignore unknown protobuf fields only when doing so cannot weaken invariants.
- Unknown enum values affecting operation semantics are fatal protocol errors.
- Adding an optional informational field is potentially backward compatible.
- Changing cursor meaning, operation semantics, value codec, transaction boundaries, table identity, or authorization meaning requires a new protocol/generation.
- Never reuse a removed field number.
- A new required semantic alternative, such as a `ChangeStreamFrame` body, operation kind or mutation outcome a v1 reader does not know, reaches the wire only for a reader that negotiated it, or under a new protocol version.

## Streamed responses

A `ChangeRequest` that sets `stream` asks for one open response instead of
one batch. On a live request the response carries a batch each time the
canonical head moves. On a fenced catch-up it carries the catch-up's batches
back to back and ends after the batch that reaches the fence.

- A request may ask only with a non-zero `long_poll_timeout_ms`, which is how
  long the response stays open. The server refuses one with a zero wait as
  invalid (422, `REQUEST_INVALID`).
- The body is a sequence of batches. Each is a complete frame sequence, from
  `BatchStart` to `BatchEnd`, with its own `stream_id` in `BatchStart` and
  its own frame count and frame digest in `BatchEnd`, exactly as a one-batch
  response carries. Batch k+1's
  `from_cursor_exclusive` is batch k's `through_cursor_inclusive`, so the
  cursor rule above is the only check that spans batches.
- The first batch is the one a long-poll of the same request would answer
  with, built before the response starts, so a refusal is the HTTP status a
  long-poll gets. A live stream builds each later batch when the head or a
  revocation wakes it; a fenced one builds the next as soon as the previous
  is handed to the body. Each later batch is authorized as a request of its
  own: the grant, the revocation ceiling and the acknowledgeable mutation ids
  are resolved again for that build. A later build that does not move the
  cursor is not written, unless it ends on a revocation barrier.
- The response ends after a complete batch: when `long_poll_timeout_ms` has
  passed, when the grant expires, when a newer request of the same plan
  arrives, after a batch that ends on a revocation barrier, after a fenced
  batch that reaches its fence, or when a later build is refused. A refused
  build writes nothing; the client's next request meets the refusal with its
  status. No `Heartbeat` and no `StreamError` frame is written into a
  streamed body.
- A body that ends inside a batch is a truncated batch, and the reader
  applies none of it. A body the transport cut is asked for again from the
  mount's cursor; one the server ended inside a batch is refused, as a
  truncated one-batch response is.
- A server that does not know the field answers with one batch and ends the
  body, which a streaming reader reads as a stream that ended.
- A server holds a bounded number of open responses. One at its bound refuses
  a new stream with 503, `INTERNAL_RETRYABLE` and a `retry_after_ms` of 1,000,
  before it builds anything.
- The response carries `X-Accel-Buffering: no`. A proxy that buffers it holds
  every batch until the response ends, which turns the stream's latency into
  its lifetime.

## Revocation barrier

`ChangeStreamFrame.revocation_barrier` (field 15) delivers an authorization
revocation at an ordered, authenticated position in the stream.

- **The body is negotiated, so a reader that does not know it is never sent
  one.** A new required semantic alternative must be negotiated (the last
  bullet of *Unknown fields and compatibility*), and
  `ChangeRequest.accept_revocation_barrier` (field 14) is that negotiation,
  alongside `accept_transaction_fragmentation`. A server puts a barrier frame
  on the wire only for a request that set it. A client that did not set it is
  still fenced by the barrier rather than served past it: the scan stops at the
  barrier's position as it would otherwise, and standing on it without being
  able to say so is refused as the coarse expired authorization (`change-http`
  answers 401 with `resnapshot_required`). Absence therefore costs the ordered
  signal, never the revocation. `@calque/worker` sets the flag only when a
  barrier verification key is configured, because a mount with no key refuses
  every barrier anyway.
- **The addition is additive on the wire and fail-closed in a v1 reader.** A
  reader that does not know field 15 parses the frame, drops the unknown body,
  and is left with no body at all -- Prost does not retain unknown fields, and
  `@bufbuild/protobuf` is read with `readUnknownFields: false`. A frame with no
  body fails `validate_change_stream_frame` at `changeStreamFrame.body` and its
  `body_sha256` cannot be recomputed, so the barrier is refused as a malformed
  frame instead of being skipped as an informational field: ignoring a
  revocation would weaken an authorization invariant.
  `server/crates/sync-protocol/tests/revocation_barrier.rs` models such a
  reader and proves both refusals.
- **No existing field changes meaning.** Cursor semantics, operation
  semantics, the value codec, transaction boundaries and table identity are
  untouched, so no protocol or generation bump is required by the fourth bullet
  of that section.
- **The barrier is authenticated by the authorization authority, not by the
  change server.** `signature` is Ed25519 over the v2 preimage below,
  verified against a key the application configures on the client. A key
  carried in the bootstrap plan would let the server that delivers a revocation
  also mint it, so there is no wire field for one. A receiver with no
  configured key refuses every barrier.

```text
ASCII "calque.change.revocation-barrier.v2\0"
|| u32 BE len(barrier_id)                     || barrier_id
|| u32 BE len(signing_key_id)                 || signing_key_id
|| u32 BE len(barrier_cursor.token)           || barrier_cursor.token
|| u32 BE len(revoked_authorization_scope_hash) || revoked_authorization_scope_hash
|| u64 BE revoked_authorization_generation
|| u64 BE effective_authorization_generation
|| u64 BE issued_at_unix_micros
```

Every variable-length field is length-prefixed, so no two distinct barriers
share a preimage by moving a boundary. Ed25519 is deterministic, so `ring` and
WebCrypto sign the same barrier to the same 64 bytes, and the golden vectors in
`spec/fixtures/protocol-v1/` are generated independently by each language and
compared byte for byte.

Ordering rules, enforced identically in `sync-protocol` and `@calque/protocol`:

- A barrier may appear only between transactions, never inside one.
- No body but `BatchEnd` may follow it in the same batch, so a second barrier,
  a heartbeat, a further fragment and a stream error are all refused by one
  rule. An error after a barrier would leave the batch incomplete, so the
  barrier would never be reported and the revocation would reach the caller
  disguised as a transport failure it may retry.
- `BatchEnd.through_cursor_inclusive` must equal `barrier_cursor`, so "before
  the barrier" and "after the barrier" are total: the batch cannot end
  anywhere else, and nothing can be delivered past it.
- `revoked_authorization_generation` and `revoked_authorization_scope_hash`
  must equal the stream's own, so a barrier minted for another scope or
  generation is a stream violation rather than a revocation of this one. A
  barrier names no plan: it states something true of every plan of its scope
  and generation, so one signed barrier serves them all. The v1 preimage began
  with a plan id, and field 5 that carried it is reserved.

## Required protocol tests

- Rust encode → TypeScript decode and reverse.
- Golden frame bytes for every message/operation/value kind.
- Unknown fields and unknown enums.
- Maximum lengths and integer boundaries.
- Fragment loss, duplication, reordering, and truncation.
- Duplicate complete batch replay.
- Batch continuity gaps/overlap.
- One huge transaction fragmented across many frames.
- Mutation-only transaction with zero row operations.
- Patch with omitted unchanged/TOAST columns.
- Corrupt primary-key/value codecs.
- Streamed responses: a batch that does not start where the previous one
  ended, a body cut inside a batch, a stream asked for with no wait, a fenced
  stream that ends after the batch that reaches its fence, and a server that
  answers a stream with one batch.
- Revocation barrier: forged and truncated signatures, a barrier no configured
  key can authenticate, a barrier for another scope or authorization
  generation, a barrier followed by a further body -- fragment, heartbeat,
  duplicate barrier or stream error -- a batch that ends off the barrier's
  cursor, a malformed or oversized frame carrying one, a request that did not
  negotiate the body, and a v1 reader that does not know the body, which must
  refuse rather than skip.

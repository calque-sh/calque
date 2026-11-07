# Calque threat model

This document is the human review surface;
[`threat-model.json`](./threat-model.json) is the machine-readable register.
The model does not claim that a control is in place: implementation and release
work must prove the controls named here. The `INV-*` IDs are those of
[`docs/design/architecture-guidelines.md`](../design/architecture-guidelines.md).

The tree does not implement the deployment preflight that `TM-025` and
`TM-037` require, the canary scans of `TM-016`, the SBOM and build provenance
of `BND-SUPPLY-CHAIN` and `TM-022`, the fuzz targets of `TM-033`, the release
verifier of `TM-032`, or the candidate-binding digests of `TM-034`. Nor is
there an evidence index or a process that reviews a release candidate, which
`ASM-008`, `CMP-BUILD-RELEASE-PIPELINE` and the release question below
assume.

Authorities, in order: `MANIFESTO.md`, then
`docs/design/architecture-guidelines.md`. A conflict is resolved in favor of the
earlier document.

## Security objectives

- `OBJ-001`: disclose and execute only within an application-authorized grant and
  the current online authorization generation.
- `OBJ-002`: never expose partial, mixed-cursor, corrupt, unauthenticated, or
  wrong-scope state as application data.
- `OBJ-003`: preserve canonical order, durable fencing, and convergence through
  replay, crash, hostile input, and stale ownership.
- `OBJ-004`: keep infrastructure and signing secrets out of clients, artifacts,
  URLs, caches, observability, errors, and images.
- `OBJ-005`: bound resource use before allocation or durable growth and reject
  excess work without partial publication.
- `OBJ-006`: bind mutation authority, idempotency, effects, and canonical echo in
  one transaction; optimistic state is never canonical truth.

## Trust assignment

Trust is purpose-specific. “Trusted” means Calque relies on the component for
the listed fact under the self-host assumptions; it does not mean invulnerable.

| Component | Assignment | Trusted only for | Never trusted for |
|---|---|---|---|
| `CMP-POSTGRESQL-SOURCE` | trusted | committed business truth and transaction order | protocol validation or resistance to privileged compromise |
| `CMP-APP-BACKEND` | trusted | principal authentication, business authorization, projection and command policy | browser erasure or cross-source atomicity |
| `CMP-AUTH-GENERATION-STORE` | trusted | durable monotonic authorization generation | browser cleanup or signature verification |
| `CMP-CALQUE-SERVER` | conditional | behavior of the verified candidate, current-owner fencing, bounded handling | deciding application business permission |
| `CMP-CANONICAL-LOG-CONTROL` | conditional | recoverable canonical state after integrity and ownership checks | validity after inconsistent restore |
| `CMP-PRIVATE-OBJECT-STORE` | conditional | durable private byte storage | artifact authenticity or authorization |
| `CMP-ARTIFACT-CATALOG` | trusted | published artifact identity and authenticated metadata | business permission or unverified object bytes |
| `CMP-COMMAND-EXECUTOR` | conditional | declared, validated command semantics | implicit read-to-write authority or arbitrary SQL |
| `CMP-VISIBILITY-PROJECTION` | trusted | application-maintained membership at a source transaction | generic live joins or cross-source truth |
| `CMP-BROWSER-RUNTIME` | hostile | no security-authoritative fact | authorization, completeness, authenticity, erasure, secret custody, or self-reported evidence |
| `CMP-HTTP-CACHE-CDN` | hostile | transport optimization only | tenant isolation, freshness, revocation, integrity, or secret storage |
| `CMP-NETWORK-DNS-TLS-EDGE` | conditional | configured authenticated transport | application authorization or canonical order |
| `CMP-BUILD-RELEASE-PIPELINE` | conditional | digest-bound immutable candidate construction | mutable inputs or self-issued review acceptance |
| `CMP-OBSERVABILITY-BACKUP` | conditional | access-controlled retention and recovery | receiving bearer material or deciding authorization |

The actors are `ACT-OPERATOR`, `ACT-APP-BACKEND`,
`ACT-HONEST-BROWSER`, `ACT-HOSTILE-BROWSER`, `ACT-NETWORK-ATTACKER`,
`ACT-TENANT-ATTACKER`, and `ACT-SUPPLY-CHAIN-ATTACKER`. A hostile browser
controls its code, workers, state, storage, extensions, DevTools, requests,
timing, and every byte it has received. The model assumes an attacker cannot
forge signatures from uncompromised keys or defeat correctly used maintained
cryptography and TLS.

## Boundary map

| Boundary | Crossing | Fail-closed result |
|---|---|---|
| `BND-BROWSER-SERVER` | bootstrap, capability, range, stream, resume, status, command | reject before disclosure/effect; never publish partial state |
| `BND-NETWORK-EDGE` | every public connection | no plaintext or unauthenticated fallback |
| `BND-ORIGIN-CACHE` | origins, redirects, ranges, browser/shared caches | reject origin, identity, generation, or metadata mismatch |
| `BND-TENANT-SCOPE` | principal, source, typed scope, columns, cache and command identity | no capability or command for ambiguous/mismatched identity |
| `BND-AUTHORITY-GENERATION` | signed grants, keys, capabilities, monotonic generation | unknown key/algorithm/version/time/generation blocks work |
| `BND-POSTGRES-SOURCE` | COPY, CDC, relation metadata, epoch, WAL feedback | stop at last complete cursor; rebuild/operator action |
| `BND-OBJECT-CAPABILITY` | upload, catalog publish, scoped range delivery, GC | unpublished/mismatched/expired/revoked/corrupt objects stay inactive |
| `BND-COMMAND-EXECUTION` | command, principal, generation, mutation ID, effects, echo | reject or remain transport-ambiguous; CDC echo alone confirms |
| `BND-PROJECTION-AUTHORITY` | application-maintained projected membership and rows | ambiguity or incompatibility blocks publication/rebuilds |
| `BND-SUPPLY-CHAIN` | source, locks, fork, toolchains, images, SBOM, evidence | unpinned, mismatched, vulnerable, or unreviewed candidate cannot release |
| `BND-OPERATIONS-RECOVERY` | telemetry, backups, keys, restore, catalog, objects, operational commands | inconsistency keeps readiness false and creates a rebuild boundary |

The application backend is the business authority. PostgreSQL is the canonical
data and ordering authority. The authenticated catalog, not object-provider
metadata, binds artifact identity. The browser and cache are hostile. Trust is
never transferred merely by an unguessable URL, valid TLS session, client cache
key, successful HTTP response, or plausible rendered table.

## Revocation fences

`REV-SERVER-DISCLOSURE` starts when the application authority durably advances
the online monotonic authorization generation. From that point, no new forbidden
bootstrap, capability, range, stream open/resume, or command work may be
authorized under the old generation.

`REV-CONNECTED-CLIENT-VISIBILITY` starts only when a connected honest client
receives the authenticated ordered revocation barrier. It locks every affected
query before another affected transaction member, resolves pending mutations,
clears resident state, and rebuilds. Propagation between the durable server fence
and this receipt is measured; it is not described as instantaneous.

`REV-DISCONNECTED-EXPIRY` bounds stale usability for an honest disconnected
client by short-lived grant/lease expiry. Reconnect and resume reauthorize. None
of these fences retracts bytes already observed by a hostile browser.

## Residual data

On narrowing, logout, and shared-browser principal switch, an honest current
client must lock affected queries, cancel or reauthorize work, clear all
Calque-owned resident state, partition and invalidate cache identity, reject
stale responses, and rebuild safely. The same testing covers open downloads,
worker termination/reload, browser cache reuse, pending commands, logs, metrics,
traces, crash reports, support bundles, and backups.

Secrets are divided into `SEC-GRANT-SIGNING-KEY`,
`SEC-POSTGRES-CREDENTIAL`, `SEC-OBJECT-STORE-CREDENTIAL`,
`SEC-CONTROL-DATABASE-CREDENTIAL`, `SEC-TLS-PRIVATE-KEY`,
`SEC-DELIVERY-CAPABILITY`, and `SEC-BACKUP-KEY-MATERIAL`. Infrastructure and
signing secrets exist only in protected runtime/issuer/backup custody. A browser
may receive only a short-lived grant or delivery capability bound to its exact
purpose. Bearer values, sensitive scope values, credentials, and row payloads
are prohibited in ordinary logs, metrics, traces, errors, referrers, analytics,
source maps, image layers, and public evidence.

The project cannot promise physical erasure from browser-managed freed memory,
swap, crash reporting, extensions, screenshots, exports, DevTools, or hostile
copies. Operator-controlled privileged stores follow the documented access,
retention, backup, and incident-response policy.

## Self-host assumptions

- `ASM-001`: the operator correctly controls DNS, TLS, origins, firewalling, and
  private Docker networking.
- `ASM-002`: PostgreSQL 18, control state, and private S3-compatible storage are
  patched, access-controlled, and not deployed with default credentials.
- `ASM-003`: the application correctly authenticates, authorizes, maintains
  projections, and validates business commands.
- `ASM-004`: host root, container administrators, browser owners, database
  superusers, and storage administrators are privileged trust-boundary actors.
- `ASM-005`: clocks used for grants, expiry, TLS, and evidence stay
  synchronized, or readiness/protected work stops. Grant verification allows
  no clock skew ([`spec/dataset-grant-v1.md`](../../spec/dataset-grant-v1.md#the-clock)).
- `ASM-006`: secrets use protected runtime injection and documented rotation.
- `ASM-007`: backup/restore covers one consistency-checked recovery unit.
- `ASM-008`: deployed source, locks, fork, images, configuration, and evidence
  match the immutable reviewed candidate.
- `ASM-009`: finite rate and capacity limits are configured; no HA or horizontal
  scaling is assumed.
- `ASM-010`: the supported path is cloud-neutral Docker plus PostgreSQL and
  S3-compatible private storage; accelerators are optional.

## Abuse and failure register

In the JSON register every case carries its `INV-*` links, and every case but
`TM-011` names a verification; `TM-011` names none while every scope is
source-wide. “Fail” below always means fail before disclosure, effect, or
application visibility unless the row explicitly permits bounded
unavailability.

| ID | Abuse or failure | Required result |
|---|---|---|
| `TM-001` | mutate every signed grant claim or signature byte | strict maintained verification rejects |
| `TM-002` | typed-scope prefix, Unicode, delimiter, type, ordering, or collision confusion | canonical typed exact identity rejects |
| `TM-003` | expired, future, retired-key, wrong-version, old-generation grant | every protected operation rejects |
| `TM-004` | tamper client bindings, queries, dependencies, workers, cursors, and status | server scope cannot widen; false completeness stays invisible |
| `TM-005` | corrupt, partial, oversized, wrong-format/schema/scope/cursor artifact | bounds, structure, metadata, and digest fail before activation |
| `TM-006` | forge/reorder cursors, transactions, barriers, echoes, duplicates, or gaps | ordering/idempotency/rebuild rules fail closed |
| `TM-007` | TLS downgrade, invalid certificate, hostile redirect, plaintext fallback | connection fails before sending secrets/data |
| `TM-008` | CORS, CSRF, origin suffix, null origin, rebinding, Host confusion | exact origin and state-change policy rejects |
| `TM-009` | delay, duplicate, fragment, truncate, reorder, stall, or replay traffic | bounded unavailability; never false complete state |
| `TM-010` | substitute tenant/principal/source/scope/artifact/resume/mutation identity | exact full binding rejects |
| `TM-011` | overlap shared partitions or duplicate keys in a union | not served in the pre-alpha: every scope is source-wide |
| `TM-012` | race revocation against every authorize/I/O/command boundary | no new authorization after durable fence |
| `TM-013` | receive barrier amid transaction, callback, mutation, clear, rebuild | lock first; no post-receipt affected result |
| `TM-014` | disconnect through narrowing and resume with stale grant | expiry bounds honest usability; resume reauthorizes |
| `TM-015` | logout/shared-profile principal switch during every hydration/live state | prior Calque state never becomes new-principal visible |
| `TM-016` | leak rows, scopes, capabilities, credentials, or keys into privileged sinks | canary scans find none in prohibited sinks |
| `TM-017` | oversized, nested, malformed, compressed-expansion, slow protocol input | pre-allocation limits and bounded parsers reject |
| `TM-018` | authorized complete scope cannot fit client/server budget | structured capacity error, zero partial publication |
| `TM-019` | stampede builds, variants, ranges, reconnects, cache misses | fenced deduplication, quotas, limits, retirement stay bounded |
| `TM-020` | source sustained/burst rate exceeds consumers | ordered backpressure or explicit rebuild, no loss/skip/growth |
| `TM-021` | search public client outputs for long-lived secrets | only short-lived exact capability may appear |
| `TM-022` | dependency/build step exfiltrates build or deployment secrets | pinned isolated build, scan, provenance, review block it |
| `TM-023` | guess/share/replay object capability across binding dimensions | private store plus exact short-lived binding rejects |
| `TM-024` | poison caches with Vary/range/redirect/stale/metadata confusion | catalog identity and the plan's digest of each 4 MiB chunk reject before parse |
| `TM-025` | public bucket, list access, broad provider credentials, metadata trust | deployment readiness fails |
| `TM-026` | write with read grant or mutate command authority/payload | explicit current write capability rejects; overlay rolls back |
| `TM-027` | duplicate/retry/lost-response/mismatched mutation ID | one transactional effect; mismatched reuse rejects |
| `TM-028` | adversarial optimistic queue/commit/reject/echo/revoke/reload schedule | reference reducer equivalence; no false canonical state |
| `TM-029` | non-atomic or incomplete application visibility projection | application defect exposed; Calque does not invent joins |
| `TM-030` | adversarial projection keys/values/DDL/TOAST/truncate | exact PostgreSQL semantics or explicit stop/rebuild |
| `TM-031` | configure cross-source, arbitrary SQL, or CDC-time joins | compile/activation rejects unsupported model |
| `TM-032` | substitute mutable source, lock, fork, image, toolchain, evidence | candidate mismatch invalidates release evidence |
| `TM-033` | vulnerable or hand-written crypto/compression/URL/parser primitive | maintained pinned library, scans, fuzzing, review required |
| `TM-034` | deploy modified/stale tree with reviewed evidence | runtime digests fail immutable-candidate binding |
| `TM-035` | reach internal database/storage/metrics/admin ports | private networking/authentication deny access |
| `TM-036` | SSRF, traversal, local-file, redirect, scheme/address confusion | typed maintained URL/path handling and allowlists reject |
| `TM-037` | root containers, broad mounts/capabilities/credentials, no limits | secure-composition preflight fails |
| `TM-038` | restore recovery components from inconsistent points | readiness false, new epoch, safe rebuild |
| `TM-039` | stale owner appends/publishes/GCs/ACKs/sends after replacement | durable fence denies every side effect |
| `TM-040` | cancellation/reload/crash/tab/backpressure exposes plausible partial state | old-or-new complete visibility only |
| `TM-041` | signature algorithm/key/encoding/duplicate-claim/version confusion | strict allowlist and schema reject |
| `TM-042` | prototype pollution, nesting, type confusion, malformed command payload | bounded strict decoding before any effect |

## Explicit non-claims

- `NC-001`: no retroactive erasure of hostile-browser copies.
- `NC-002`: the browser/device is not a confidentiality boundary for authorized
  data.
- `NC-003`: no correction of wrong application business authorization,
  projections, or command policy.
- `NC-004`: no automatic failover, Byzantine tolerance, horizontal/multi-region
  continuity, or protection from privileged infrastructure administrators.
- `NC-005`: no cross-source query atomicity, remote predicates/pagination,
  arbitrary server SQL, generic CDC-time joins, or durable offline writes.
- `NC-006`: no authorization or integrity claim from URL secrecy, cache metadata,
  client cache keys, TLS alone, or unguessable identifiers.
- `NC-007`: no denial-of-service immunity beyond finite limits and bounded
  fail-closed behavior.
- `NC-008`: no invented crypto, compression, URL, PostgreSQL, protocol, or parser
  primitive, and no claim of zero vulnerabilities.
- `NC-009`: no compatibility/security claim outside PostgreSQL 18, headed stable
  Chrome, the one-tab 16 GiB client, and documented
  Docker/S3-compatible topology.
- `NC-010`: no correctness/security proof from speed, plausible paint, HTTP
  success, eventual convergence, or self-reported client state.

The release question remains: can this candidate produce a locally
plausible client result that is incomplete, unauthorized, corrupt, from the
wrong cursor, or not equal to the complete authorized PostgreSQL state? If yes
or uncertain, release is blocked.

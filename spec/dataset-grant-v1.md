# Dataset grant v1 — wire contract and compatibility decision

The signed dataset grant: minted by the application backend, verified by
Calque, never minted by Calque. `server/crates/dataset-grant` is the
verifier and the single definition of the payload. This repository holds two
issuers: `server/crates/dataset-grant-issuer` in Rust, and
`client/examples/grant-issuer/` in Node with `node:crypto` alone.

This document is written for an issuer in any language. Where it and the
verifier disagree, the verifier decides what Calque accepts, and the document is
the defect. Rule names below are identifiers in
`server/crates/dataset-grant/src/lib.rs` unless another file is named.

## The token

```text
base64url(payload) "." base64url(signature)
```

- Both halves use the URL-safe base64 alphabet (RFC 4648 §5) **without `=`
  padding**. The verifier decodes with `URL_SAFE_NO_PAD`, so a padded token is
  refused as `InvalidEncoding`.
- A token is at most 8192 bytes (`MAX_TOKEN_BYTES`), and its decoded payload at
  most 4096 bytes (`MAX_PAYLOAD_BYTES`). Within the payload bound a token is at
  most 5,549 bytes, so an issuer that respects the payload bound meets the
  token bound.
- `signature` is a 64-byte Ed25519 signature (RFC 8032, pure Ed25519, no
  prehash) over the signing input: the 23 ASCII bytes `calque.dataset-grant.v1`,
  one NUL byte (`0x00`), then the payload bytes exactly as they are encoded into
  the token (`GRANT_SIGNING_DOMAIN`, `grant_signing_input`). A signature over
  the bare payload, or over the domain without its NUL byte, is refused.
- There is no header and no algorithm field: Ed25519 is chosen by the verifier,
  not announced by the token.

The issuer hands the token to its browser. The client SDK takes it from the
application's `auth()` callback, sends it as `Authorization: Bearer <token>`,
and calls `auth()` again when the grant expires or its generation is retired.

## The payload

A JSON object in UTF-8:

```json
{
  "version": 1,
  "keyId": "…",
  "audience": "…",
  "subject": "…",
  "attributes": { "organization_id": { "uuid": "0a1b…" } },
  "authorizationGeneration": 9,
  "issuedAtUnixSeconds": 1800000000,
  "expiresAtUnixSeconds": 1800000300,
  "commands": [{ "collection": "tasks", "operations": ["update"] }]
}
```

`deny_unknown_fields` applies to the payload and to each `commands` entry. A
grant still names no table, column, row predicate, or scope: the read projection
is derived by `authz::AuthorizationPolicy` from server-side configuration.

| Field | JSON type | Rule |
| --- | --- | --- |
| `version` | integer | Exactly `1` (`GRANT_VERSION`). |
| `keyId` | string | Must equal the `keyId` of a key the deployment trusts. Key ids are 1 to 128 bytes of printable ASCII (`!` through `~`) other than `.` (`MAX_KEY_ID_BYTES`, `GrantVerificationKey::new`). |
| `audience` | string | Must equal the deployment's `--grant-audience` byte for byte. An audience is 1 to 256 bytes (`MAX_AUDIENCE_BYTES`, `GrantVerifier::new`). |
| `subject` | string | 1 to 512 bytes of UTF-8 with no control character, Unicode general category Cc (`validate_name` in `server/crates/authz/src/lib.rs`). |
| `attributes` | object | Optional; absent is `{}`. At most 64 entries (`MAX_GRANT_ATTRIBUTES`). Each name follows the `subject` rule and appears at most once, and each value is one of the typed values below. |
| `authorizationGeneration` | integer, 0 to 2^64 − 1 | Must equal the generation in force: the one `calque serve` prints at startup as `authorization_generation=`, until a revocation puts a later one in force. |
| `issuedAtUnixSeconds` | integer, 0 to 2^64 − 1 | Seconds since the Unix epoch. See the clock rules. |
| `expiresAtUnixSeconds` | integer, 0 to 2^64 − 1 | Later than `issuedAtUnixSeconds`, by at most 900 (`MAX_GRANT_LIFETIME_SECONDS`). |
| `commands` | array | Optional; absent is `[]`. The write capability below. |

The generation and the audience are facts of the deployment, not of the grant,
so an issuer cannot check them locally. A grant minted for the wrong one is
validly signed and then refused.

### Attribute values

Each value is an object with exactly one key, naming its type. `int8`, `bytes`
and `uuid` travel as strings, because a JavaScript number cannot hold every
64-bit integer (`GrantAttributeValue`, `into_principal_value`). Calque
verifies every attribute and carries it with the principal. A source-wide
scope evaluates none of them. A partitioned scope reads exactly one, the
attribute its configuration names, and the grant holder sees only the rows
whose partition column equals that value. That attribute must be present and
of the column's own type, `int2`, `int4`, `int8` or `uuid`: `int8` for a
`bigint` column. A grant without it, or with another type, verifies and is
then refused with `403`, so a grant can never fall back to every tenant's
rows.

| Value | Accepted |
| --- | --- |
| `{"bool": true}` | `true` or `false`. |
| `{"int2": -32768}` | A JSON integer from −32768 to 32767. |
| `{"int4": 2147483647}` | A JSON integer from −2147483648 to 2147483647. |
| `{"int8": "-9223372036854775808"}` | A string of decimal digits with an optional `+` or `-` sign, within a signed 64-bit integer. |
| `{"bytes": "00ff10"}` | A string of hex digits of even length, in either case. Empty is zero bytes. |
| `{"uuid": "0a1b2c3d4e5f6071829304a5b6c7d8e9"}` | A string of exactly 32 hex digits, in either case, with no hyphens. |

**A repeated attribute name is refused.** JSON lets an object repeat a key, and
a parser that kept the last value would verify a payload naming
`organization_id` twice as whichever value came last. The verifier refuses
such a payload while parsing it, as `DuplicateAttribute` (`unique_attributes`),
so the signed bytes have one meaning. Any other repeated name in the payload is
`MalformedPayload`, because the payload does not parse: a top-level field, a
field of a `commands` entry, or a second type key in one attribute value.

## `commands` — the write capability

A read grant never implies write permission: Calque runs generic row CRUD only
when a signed command capability permits it, and `commands` is that capability.

Each entry is `{"collection": "<id>", "operations": [...]}` with exactly those
two fields. `collection` is a collection id the deployment's configuration
declares. `operations` is a non-empty list drawn from `"insert"`, `"update"`
and `"delete"`, in lower case, without repeats.

**Capability is per collection and per operation, both.** Per collection alone
would make "may fix a typo in a title" the same authority as "may delete every
row of the collection". Per operation alone is not expressible: an operation
without a collection names nothing a command could execute against. The unit is
the *collection*, never a table or a column, because the collection is what an
application's own configuration declares, what a generated facade exposes, and
what a deployment maps to exactly one `crud:<table>` command.

Every malformed capability refuses the whole grant rather than narrowing it: an
empty operation set, a repeated operation, a repeated collection, an oversized
or non-graphic collection name, and any unknown operation or field. Silently
dropping a bad entry would let an issuer believe it had minted a capability no
verifier honours. At most 64 entries; a collection name is at most 128 bytes of
ASCII graphic characters.

A verified capability is authenticated data, not authority. A deployment may
only ever **narrow** it against what it mounts, and `calque serve` does that
(`MountedCommands::narrow` in `server/crates/calque-engine/src/backend.rs`): a
collection the deployment does not mount and an operation it does not advertise
reach nothing, however the grant spells them.

## The clock

The verifier reads its own clock, in whole Unix seconds, at every presentation:

- `now < issuedAtUnixSeconds` is refused as `NotYetValid`. There is no skew
  allowance, so an issuer whose clock runs ahead of the deployment's mints
  grants the deployment refuses until its own clock catches up.
- `now >= expiresAtUnixSeconds` is refused as `Expired`. A grant is valid on the
  half-open interval from `issuedAtUnixSeconds` to `expiresAtUnixSeconds`.
- The 900-second bound is on `expiresAtUnixSeconds − issuedAtUnixSeconds`, not
  on the time left. Stamping `issuedAtUnixSeconds` a few seconds early absorbs
  that much skew, and shortens the grant's usable life by the same amount.

A window over 900 seconds is refused, never truncated: truncating would let the
issuer believe a long-lived grant had been honoured.

## What the verifier refuses

`GrantVerifier::verify` checks in this order and stops at the first failure.
Nothing derived from the payload is trusted before the signature over it has
been checked.

1. A token over 8192 bytes: `TokenTooLarge`.
2. No `.`, or a second one: `InvalidFraming`.
3. A half that is not unpadded base64url: `InvalidEncoding`. A payload over
   4096 bytes: `TokenTooLarge`.
4. A payload that is not a JSON object of the fields above with their types, or
   that carries an unknown field or names one twice: `MalformedPayload`. An
   attribute name given twice: `DuplicateAttribute`. A `version` other than 1:
   `UnsupportedVersion`. A `keyId` the deployment does not trust:
   `UnknownKeyId`.
5. A signature that does not verify under that key: `InvalidSignature`.
6. Another audience: `AudienceMismatch`. Another generation:
   `AuthorizationGenerationMismatch`.
7. `expiresAtUnixSeconds <= issuedAtUnixSeconds`: `InvalidValidityWindow`. A
   window over 900 seconds: `LifetimeTooLong`. Then `NotYetValid` and `Expired`
   from the clock rules.
8. More than 64 attributes: `TooManyAttributes`. An `int8`, `bytes` or `uuid`
   string that does not parse as its type: `MalformedAttribute`. An `int2` or
   `int4` out of range, or a value of the wrong JSON type, was already
   `MalformedPayload` at step 4. A subject or attribute name that breaks the
   name rule: `Principal`.
9. More than 64 command capabilities: `TooManyCommandCapabilities`. A
   collection that is empty, over 128 bytes or not printable ASCII, an empty
   operation list, or a repeated operation: `MalformedCommandCapability`. A
   collection named twice: `DuplicateCommandCapability`.

Over HTTP every refusal is the same `401`, which does not say which check
failed (`resolve_at` in `server/crates/calque-engine/src/backend.rs`).

## The verification key

`calque serve --grant-verification-key-env NAME` names an environment variable.
The flag is required and repeats, once per trusted key. Each variable holds one
JSON object with exactly two fields (`VerificationKeyEnvironment` and
`load_grant_verifier` in `server/apps/calque/src/serve.rs`):

```json
{ "keyId": "grant-vectors-key", "publicKey": "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo" }
```

`publicKey` is the raw 32-byte Ed25519 public key in unpadded base64url, 43
characters. An object with an unknown field, or one that names a field twice,
stops `calque serve` at startup. Key ids must be distinct across the variables.
Trusting several keys at once is how a key rotates: the issuer starts signing
with the new key while the old one is still trusted, and a later deployment
drops the old one.

`calque-dev-grant` keeps its signing key in a PKCS#8 v2 document, the private
seed together with its public key, 83 bytes, created with mode `0600` on first
use. `GrantIssuer::from_pkcs8` reads it with ring's `Ed25519KeyPair::from_pkcs8`,
which requires v2, and Node's `createPrivateKey` reads the same file.

## Tenant orders

Under a partitioned scope the application backend can ask the deployment to
prepare a tenant before anyone opens it, to keep it prepared, to drop it at
once, or to report its state. It posts `{"order": "<token>"}` to
`POST /v1/tenants`. The order has the token's framing and size bounds, and is
signed with one of the deployment's grant keys under its own domain: the 22
ASCII bytes `calque.tenant-order.v1`, one NUL byte, then the payload
(`TENANT_ORDER_SIGNING_DOMAIN` in
`server/crates/dataset-grant/src/tenant_order.rs`). A dataset grant never
verifies as an order, and an order never verifies as a grant, so a browser
holding a grant cannot warm, pin, evict or inspect a tenant.

| Field | JSON type | Rule |
| --- | --- | --- |
| `version` | integer | Exactly `1`. |
| `keyId` | string | A key the deployment trusts, as in a grant. |
| `audience` | string | The deployment's `--grant-audience`, as in a grant. |
| `orderId` | string | 16 to 64 characters of the base64url alphabet, unique to this order. Both issuers use 16 random bytes, 22 characters. |
| `action` | string | `warm` prepares the tenant now if it has no current artifact; `pin` keeps it prepared whatever its recency; `unpin` hands it back to recency; `state` changes nothing; `evict` drops its artifact, pin and recency, and a build in progress, so its next open is cold. A plan granted before the evict keeps its artifact until the plan expires. |
| `tenant` | object | The tenant, as an attribute value of the partition column's type. |
| `issuedAtUnixSeconds` | integer | As in a grant. |
| `expiresAtUnixSeconds` | integer | Later than `issuedAtUnixSeconds`, by at most 300. |

An unknown field is refused. The deployment acts on an order id once: it
records the id in its tenant directory before anything the order changes,
keeps it until five minutes after the order expires, and answers the same
order presented again, before or after a restart, with `409` `REPLAYED`. The
answer to every action is the tenant's state, `cold`, `preparing` or `warm`,
with whether it is pinned and, once it has an artifact, the age of its cut in
seconds and its size in bytes. An order that does not verify is `401`, a
tenant of the wrong type is `400`, and a deployment whose scope is not
partitioned answers `503`. The Node issuer's `mintTenantOrder` signs one.

## How the issuers serialize

The verifier parses the payload with any field order and any whitespace, because
the signature covers whatever bytes were sent. Both issuers write the same bytes
anyway, and the token vectors pin them:

- the fields in the order of the table above, with no whitespace;
- `attributes` always present, `{}` when empty, its names in ascending order of
  their UTF-8 bytes;
- `commands` omitted when empty; its entries, and each entry's operations, in
  the order given;
- integers as plain JSON integers; strings with `"` and `\` escaped, U+0008,
  U+0009, U+000A, U+000C and U+000D as `\b`, `\t`, `\n`, `\f` and `\r`, the
  other characters below U+0020 as `\u00xx` in lower-case hex, and every other
  character as its UTF-8 bytes, `/` and non-ASCII included.

The Rust issuer gets these from `serde_json::to_vec` of `DatasetGrantClaims`
(`GrantIssuer::issue_claims`). ECMAScript's `JSON.stringify` escapes strings the
same way. The attribute order needs care in JavaScript: an object's own key
order puts integer-like keys first, and `Array.prototype.sort` compares UTF-16
code units, which disagrees with UTF-8 order above U+FFFF.

## Compatibility

A field may be added within v1 only if it is optional and its absence keeps
v1's meaning, as `commands` is: absent and empty both authorize no command.
`deny_unknown_fields` makes a verifier that does not know a field refuse a
payload carrying it, so a stale verifier fails a grant rather than honouring
it with part of its meaning ignored; deployments upgrade the verifier before
the issuer starts writing the field. A version bump is required for any other
change, and for any change that would let an older verifier accept a payload
while reading less of it than the issuer wrote.

## Golden vectors

`spec/fixtures/dataset-grant-v1/`, with `manifest.json` carrying each file's
size, SHA-256 and coverage.

**Token vectors.** `test-only-grant-key.pk8` is the Ed25519 key of RFC 8032
§7.1, TEST 1, in the 83-byte PKCS#8 v2 layout `calque-dev-grant` writes. Its
seed is published in the RFC, so no deployment may trust it; its file name and
its manifest entry both say so. `test-only-grant-key.public.raw` is its 32-byte
public key, and `test-only-verification-key.json` is the
`--grant-verification-key-env` value for it under `keyId` `grant-vectors-key`.
Ed25519 signatures are deterministic, so that key signs each payload to exactly
one token. `read-only.token`, `command-capability.token` and
`attribute-types.token` each sign the payload file of the same name, and each
manifest entry names its payload and its key. Every issuer must produce those
exact tokens. They verify at generation 9, one second after
`issuedAtUnixSeconds`.

**Payload vectors.** The payloads under `invalid/` are signed by the test, so
every negative vector is validly signed and its refusal proves the claim check
rather than the signature check. Each one's manifest entry names, as `refusal`,
the `GrantError` variant it is refused with. Framing, encoding and signature
negatives live in `dataset-grant/tests/grant_verification.rs`.

Covered: the read-only payload, the capability payload, the attribute-types
payload (every attribute type, the integer bounds, names whose UTF-8 order
differs from an object's key order, a subject that needs escapes, and a window
of exactly 900 seconds), truncated, oversized, cross-generation, a repeated
attribute name, and four malformed-capability shapes. `grant_vectors.rs` drives
all of them through the real verifier, checks that each negative vector is
refused with the `refusal` its manifest entry names, and asserts that the Rust
issuer serializes `command-capability.payload.json` and
`attribute-types.payload.json` and signs every token byte for byte.

## Cross-language checks

Each issuer is held to the frozen vectors:

- **Rust.** `server/crates/dataset-grant/tests/grant_vectors.rs` asserts that
  `dataset-grant-issuer` signs each frozen payload to its frozen token, that
  `GrantVerifier` verifies each token, and that the test-only key prints
  `test-only-verification-key.json`.
- **Node.** `scripts/grant-issuer-example.test.ts`, run by `pnpm verify`,
  asserts that the example serializes each positive vector's claims to its
  payload bytes, signs each payload to its token, and prints the same
  verification-key value. As an issuer it refuses to mint the claims of five
  of the eight negative vectors: the four malformed capabilities, and
  `oversized.payload.json`, whose subject breaks the 512-byte name rule before
  the payload reaches 4096 bytes. The other three are not claims an issuer can
  be asked for: `truncated.payload.json` is not JSON;
  `cross-generation.payload.json` is well-formed, refused only by a deployment
  whose generation in force differs; and `duplicate-attribute.payload.json`
  repeats a name that a JavaScript object holds once.
- **Live.** `scripts/serve-mutation-e2e.ts` (`pnpm serve:e2e`) gives one page a
  grant the Node example minted from the key file `calque-dev-grant` created,
  and that page loads only if a real `calque serve` verifies the grant.

The browser client treats a grant as an opaque bearer string. No vector may be
regenerated to make a new implementation pass: a disagreement is that
implementation's defect until the Rust side is shown wrong, and if the Rust
side is wrong the fix is a v2, not an edited fixture.

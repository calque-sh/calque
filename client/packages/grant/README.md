# @calque/grant

`calque serve` verifies Ed25519-signed dataset grants and mints none: your
application's backend signs them. `@calque/grant` is that backend's half, with
no dependency beyond `node:crypto`. It creates the signing key, mints grants
and tenant orders, and refuses every claim Calque's verifier would answer with
`401`. [`spec/dataset-grant-v1.md`](https://github.com/calque-sh/calque/blob/main/spec/dataset-grant-v1.md) is the
format it implements.

```sh
npm install @calque/grant
```

It is for a server. The signing key never reaches a browser, so nothing here
runs in one.

## Create the key

```sh
npx calque-grant keygen --key-file grant-key.pem --key-id app-1
```

It writes the private key to `grant-key.pem` as PKCS#8 PEM, readable by its
owner only, and prints the value `calque serve --grant-verification-key-env`
reads:

```json
{ "keyId": "app-1", "publicKey": "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo" }
```

Put that value in the environment variable the flag names, and the same
`publicKey` in the client's `grantVerificationKey`. Keep the key file in your
secret store. `keygen` never overwrites a file: a server configured with the
old key would refuse every grant signed with a new one.

In code, `generateSigningKey()` returns the same key as a `KeyObject`, as
PKCS#8 DER and PEM, and as the raw public key.

## Mint a grant

```ts
import { readFileSync } from "node:fs";
import { loadSigningKey, mintGrant } from "@calque/grant";

const key = loadSigningKey(readFileSync("grant-key.pem"));

const now = Math.floor(Date.now() / 1000);
const token = mintGrant(
  {
    keyId: "app-1",
    audience: "app.example.com",
    subject: user.id,
    attributes: { tenant_id: { int8: "42" } },
    authorizationGeneration: 1,
    issuedAtUnixSeconds: now,
    expiresAtUnixSeconds: now + 900,
    commands: [{ collection: "tasks", operations: ["update"] }],
  },
  key,
);
```

Return `token` from the page's `auth()` as `bearerToken`. A grant lasts at most
900 seconds, so mint one per request, not per session.

`loadSigningKey` takes PEM text, PEM bytes or DER bytes, from PKCS#8 version 1
or 2. A key from `calque-dev-grant` is a version 2 document, and it loads on
every supported Node, because the package reads the seed from that layout
instead of leaving it to the OpenSSL Node bundles.

`mintGrant` throws `GrantRefused` instead of signing a grant `calque serve`
would answer with `401`:

- a validity window that is empty or longer than 900 seconds;
- an unknown claim, an empty audience, or one over 256 bytes;
- a key id that is empty, longer than 128 bytes, or holds a `.` or a character
  outside printable ASCII;
- a subject or attribute name that is empty, longer than 512 bytes, or holds a
  control character;
- more than 64 attributes, or a value outside its type;
- more than 64 command capabilities, a collection named twice, an empty or
  repeated operation, or an operation other than `insert`, `update` and
  `delete`;
- a payload over 4096 bytes.

It cannot check `authorizationGeneration` or the audience against a
deployment: those are the deployment's facts, so a grant for the wrong ones is
signed and then refused by `calque serve`.

`int8`, `bytes` and `uuid` attributes travel as strings, because a JavaScript
number cannot hold every 64-bit integer, and a tenant key that lost its low
bits would authorize the wrong rows. `int8` is a decimal integer, `bytes` is
hex, and `uuid` is exactly 32 hex digits with no hyphens.

## Tenants

When a scope is partitioned by a grant attribute, each grant names one tenant
in that attribute, typed as the partition column is: an `int8` column takes
`{ int8: "42" }`, a `uuid` column takes 32 hex digits. A grant whose attribute
is missing or of another type is refused.

The backend can also warm, pin, unpin, inspect or evict a tenant.
`mintTenantOrder` signs such an order with the same key, under a signing domain
of its own so a browser's grant can never stand in for it, valid for at most
300 seconds:

```ts
import { mintTenantOrder } from "@calque/grant";

const order = mintTenantOrder(
  {
    keyId: "app-1",
    audience: "app.example.com",
    action: "warm",
    tenant: { int8: "42" },
    issuedAtUnixSeconds: now,
    expiresAtUnixSeconds: now + 60,
  },
  key,
);
await fetch(`${calque}/v1/tenants`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ order }),
});
```

The answer is the tenant's state, `cold`, `preparing` or `warm`, with whether it
is pinned and, once it has an artifact, the age of its cut and its size.

Each order carries an `orderId`, 16 random bytes in base64url unless you pass
one of 16 to 64 base64url characters. Calque acts on an order once: the same
order posted again, before it expires or after a restart, is answered `409`
`REPLAYED`, so mint a new order for each request.

## calque-grant mint

```sh
npx calque-grant mint --key-file grant-key.pem --key-id app-1 \
  --audience app.example.com --subject me --generation 1 \
  --lifetime-seconds 900 --command tasks=update
```

It prints one JSON document: the token, when it expires, the generation, the
commands and the verification-key value. It is for development and for
scripts; a backend mints in code.

| Flag                        | What it takes                                                                                 |
| --------------------------- | --------------------------------------------------------------------------------------------- |
| `--key-file <path>`         | The signing key, PEM or DER, PKCS#8.                                                          |
| `--key-id <id>`             | The id the grant and the verification key carry.                                              |
| `--audience <audience>`     | The audience `calque serve` was started with.                                                 |
| `--subject <subject>`       | Who the grant is issued to.                                                                   |
| `--generation <n>`          | The authorization generation the grant is valid for.                                          |
| `--lifetime-seconds <n>`    | How long it is valid, at most 900.                                                            |
| `--attribute <n>=<t>:<v>`   | A grant attribute; the type is `uuid`, `bytes`, `int8`, `int4`, `int2` or `bool`. Repeatable. |
| `--command <c>=<op>[+<op>]` | A write capability for a collection; without one the grant is read-only. Repeatable.          |

`calque-grant` exits 0 on success, 1 when a grant is refused or a file cannot
be read or written, and 2 on a usage error.

## Node versions

The package supports the Node releases with Node's October 2026 security
fixes: 26.12.1 or newer, 24.22.1 or newer on the 24 line, or 22.23.4 or newer
on the 22 line. It runs on Node 20 and newer, the first with
`String.prototype.isWellFormed`, which checks that text is well-formed
Unicode. It does not depend on the OpenSSL a Node bundles, and its tests pass
on Node 20, whose OpenSSL 3.0 cannot read a PKCS#8 version 2 key itself.

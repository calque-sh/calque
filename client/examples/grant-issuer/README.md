# Minting dataset grants in Node

`calque serve` verifies dataset grants and mints none; your application's
backend issues them. This example is that backend's half in Node, using
`node:crypto` and no dependency. [`spec/dataset-grant-v1.md`](../../../spec/dataset-grant-v1.md)
is the format it implements.

- `dataset-grant.mts` is the library: `loadSigningKey` reads a PKCS#8 Ed25519
  key, `mintGrant` signs a grant from explicit claims, and
  `verificationKeyEnvironment` prints the `{"keyId":…,"publicKey":…}` value
  `calque serve --grant-verification-key-env` reads.
- `mint-grant.mts` is `calque-dev-grant` in Node. It takes the same flags and
  prints the same JSON document, reading the `grant-key.pk8` that
  `calque-dev-grant` writes. It never creates a key file.

```sh
node client/examples/grant-issuer/mint-grant.mts --key-file grant-key.pk8 \
  --key-id quickstart --audience quickstart.example --generation 1 \
  --subject me --lifetime-seconds 900 --command tasks=update
```

Run it on Node 26.12.1 or newer, 24.22.1 or newer on the 24 line, or 22.23.4 or
newer on the 22 line: the releases with Node's October 2026 security fixes.
Node runs `.mts` files without a flag from 22.18 and 23.6, but the
`grant-key.pk8` `calque-dev-grant` writes is a PKCS#8 v2 document, which
carries the public key beside the private one, and only the OpenSSL 3.5 that
Node bundles from 22.20 and 24.5 reads it. OpenSSL 3.0 refuses it with
`wrong tag`, and no Node 23 release bundles 3.5. A backend behind a bundler, or
on an older Node, copies both files in and strips the types there; on an older
Node it also needs a PKCS#8 v1 key, which is the form `node:crypto` exports.

## Tenants

When a scope is partitioned by a grant attribute, each grant names one tenant
in that attribute, typed as the partition column is: an `int8` column takes
`{ int8: "42" }`, a `uuid` column takes 32 hex digits. A grant whose attribute
is missing or of another type is refused.

The backend can also warm, pin, unpin, inspect or evict a tenant.
`mintTenantOrder` signs such an order with the same key, under a signing
domain of its own so a browser's grant can never stand in for it, valid for at
most 300 seconds:

```ts
const order = mintTenantOrder(
  {
    keyId: "quickstart",
    audience: "quickstart.example",
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

The answer is the tenant's state, `cold`, `preparing` or `warm`, with whether
it is pinned and, once it has an artifact, the age of its cut and its size.

Each order carries an `orderId`, 16 random bytes in base64url unless you pass
one of 16 to 64 base64url characters. calque acts on an order once: the same
order posted again, before it expires or after a restart, is answered `409`
`REPLAYED`, so mint a new order for each request.

## What it refuses

`mintGrant` throws `GrantRefused` instead of signing a grant `calque serve`
would answer with 401:

- a validity window that is empty or longer than 900 seconds;
- an unknown claim, an empty audience, or one over 256 bytes;
- a key id that is empty, longer than 128 bytes, or holds a `.` or a
  character outside printable ASCII;
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

## How it is checked

`scripts/grant-issuer-example.test.ts` runs under `pnpm verify`. It mints each
frozen token in `spec/fixtures/dataset-grant-v1/` from that directory's
test-only key and asserts the exact bytes, which the Rust issuer's tests assert
it produces too. `scripts/serve-mutation-e2e.ts` gives one browser page a grant
this example minted, so a real `calque serve` verifies it.

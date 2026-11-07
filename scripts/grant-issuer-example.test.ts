/**
 * The Node grant issuer (`client/examples/grant-issuer`) against the frozen
 * dataset grant v1 vectors in `spec/fixtures/dataset-grant-v1/`: it must sign
 * each frozen payload to the frozen token, which `grant_vectors.rs` runs
 * through Calque's verifier, and refuse to mint any grant the verifier
 * refuses.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash, createPublicKey, verify } from "node:crypto";
import { readFileSync } from "node:fs";
import process from "node:process";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  GRANT_SIGNING_DOMAIN,
  GrantRefused,
  loadSigningKey,
  MAX_GRANT_LIFETIME_SECONDS,
  mintGrant,
  mintTenantOrder,
  TENANT_ORDER_SIGNING_DOMAIN,
  serializeClaims,
  signPayload,
  verificationKeyEnvironment,
  type DatasetGrantClaims,
  type GrantAttributeValue,
} from "../client/examples/grant-issuer/dataset-grant.mts";
import { mintGrantCommand } from "../client/examples/grant-issuer/mint-grant.mts";

const vectors = new URL("../spec/fixtures/dataset-grant-v1/", import.meta.url);
const vectorPath = (name: string): string =>
  fileURLToPath(new URL(name, vectors));
const vector = (name: string): Buffer => readFileSync(vectorPath(name));
const text = (name: string): string => vector(name).toString("utf8");

const KEY_ID = "grant-vectors-key";
const KEY_FILE = "test-only-grant-key.pk8";
const key = loadSigningKey(vector(KEY_FILE));

/** The token vectors: each payload file and the token it signs to. */
const TOKENS = [
  ["read-only.payload.json", "read-only.token"],
  ["command-capability.payload.json", "command-capability.token"],
  ["attribute-types.payload.json", "attribute-types.token"],
] as const;

/** A frozen payload, read back as the claims an issuer would be given. */
function claimsOf(payload: Buffer): DatasetGrantClaims {
  const { version, ...claims } = JSON.parse(payload.toString("utf8")) as {
    version: unknown;
  } & DatasetGrantClaims;
  assert.equal(version, 1);
  return claims;
}

/** A valid grant to vary one claim of at a time. */
const base: DatasetGrantClaims = claimsOf(vector("read-only.payload.json"));

const refused = (claims: DatasetGrantClaims, reason: RegExp): void => {
  assert.throws(
    () => mintGrant(claims, key),
    (error: unknown) =>
      error instanceof GrantRefused && reason.test(error.message),
  );
};

test("the manifest's sizes and digests are the files' own, and node is a producer", () => {
  const manifest = JSON.parse(text("manifest.json")) as {
    wire: { producers: readonly string[] };
    fixtures: readonly { file: string; bytes: number; sha256: string }[];
  };
  assert.deepEqual(manifest.wire.producers, ["rust", "node"]);
  for (const { file, bytes, sha256 } of manifest.fixtures) {
    const content = vector(file);
    assert.equal(content.length, bytes, file);
    assert.equal(
      createHash("sha256").update(content).digest("hex"),
      sha256,
      file,
    );
  }
  const listed = new Set(manifest.fixtures.map(({ file }) => file));
  for (const [payload, token] of TOKENS) {
    assert.ok(listed.has(payload), payload);
    assert.ok(listed.has(token), token);
  }
  assert.ok(listed.has(KEY_FILE));
});

test("the example signs each frozen payload to exactly its frozen token", () => {
  for (const [payload, token] of TOKENS)
    assert.equal(signPayload(vector(payload), key), text(token), token);
});

test("minting from the claims a payload states writes its exact bytes and token", () => {
  for (const [payload, token] of TOKENS) {
    const claims = claimsOf(vector(payload));
    assert.deepEqual(serializeClaims(claims), vector(payload), payload);
    assert.equal(mintGrant(claims, key), text(token), token);
  }
});

test("attribute order is UTF-8 byte order, whatever order the claims list them in", () => {
  const claims = claimsOf(vector("attribute-types.payload.json"));
  const reversed: Record<string, GrantAttributeValue> = {};
  for (const name of Object.keys(claims.attributes).reverse())
    reversed[name] = claims.attributes[name] as GrantAttributeValue;
  assert.equal(
    mintGrant({ ...claims, attributes: reversed }, key),
    text("attribute-types.token"),
  );
});

test("the verification-key value is the frozen one, over the frozen raw public key", () => {
  const environment = verificationKeyEnvironment(KEY_ID, key);
  assert.equal(environment, text("test-only-verification-key.json"));
  const { publicKey } = JSON.parse(environment) as { publicKey: string };
  assert.deepEqual(
    Buffer.from(publicKey, "base64url"),
    vector("test-only-grant-key.public.raw"),
  );
  assert.throws(() => verificationKeyEnvironment("has.dot", key), GrantRefused);
});

test("every negative vector an issuer can be handed is refused before signing", () => {
  const refusedByTheIssuer = [
    ["capability-empty-operations", /names no operation/],
    ["capability-unknown-operation", /names "truncate"/],
    ["capability-unknown-field", /exactly a collection and its operations/],
    ["capability-duplicate-collection", /named twice/],
    ["oversized", /subject must be 1 to 512 bytes/],
  ] as const;
  for (const [name, reason] of refusedByTheIssuer)
    refused(claimsOf(vector(`invalid/${name}.payload.json`)), reason);

  // Not a claims document at all: an issuer never holds these bytes.
  assert.throws(() => JSON.parse(text("invalid/truncated.payload.json")));
  // The generation in force is the deployment's fact, not the issuer's, so
  // this one mints; Calque refuses it at presentation against generation 9.
  assert.match(
    mintGrant(claimsOf(vector("invalid/cross-generation.payload.json")), key),
    /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{86}$/,
  );
  // Claims are a JavaScript object, which cannot hold a name twice: parsing
  // these bytes keeps the last value, and the example writes that one once.
  const repeated = claimsOf(vector("invalid/duplicate-attribute.payload.json"));
  assert.deepEqual(repeated.attributes, {
    organization_id: { uuid: "ffffffffffffffffffffffffffffffff" },
  });
  assert.equal(
    serializeClaims(repeated).toString("utf8").split('"organization_id"')
      .length,
    2,
  );

  // Every negative vector the manifest lists is one of the cases above.
  const manifest = JSON.parse(text("manifest.json")) as {
    fixtures: readonly { file: string }[];
  };
  assert.deepEqual(
    manifest.fixtures
      .map(({ file }) => file)
      .filter((file) => file.startsWith("invalid/"))
      .sort(),
    [
      ...refusedByTheIssuer.map(([name]) => name),
      "truncated",
      "cross-generation",
      "duplicate-attribute",
    ]
      .map((name) => `invalid/${name}.payload.json`)
      .sort(),
  );
});

test("the lifetime limit is the verifier's, to the second", () => {
  const issued = base.issuedAtUnixSeconds;
  const lasting = (seconds: number): DatasetGrantClaims => ({
    ...base,
    expiresAtUnixSeconds: issued + seconds,
  });
  assert.equal(MAX_GRANT_LIFETIME_SECONDS, 900);
  mintGrant(lasting(900), key);
  refused(lasting(901), /validity window is 901 s/);
  refused(lasting(0), /must be later than/);
  refused(lasting(-1), /must be later than/);
});

test("a malformed command capability refuses the whole grant", () => {
  const capability = (
    collection: string,
    operations: readonly string[],
  ): DatasetGrantClaims =>
    ({
      ...base,
      commands: [{ collection, operations }],
    }) as DatasetGrantClaims;
  mintGrant(capability("c".repeat(128), ["insert", "update", "delete"]), key);
  for (const [claims, reason] of [
    [capability("", ["update"]), /printable ASCII/],
    [capability("c".repeat(129), ["update"]), /printable ASCII/],
    [capability("my tasks", ["update"]), /printable ASCII/],
    [capability("tâches", ["update"]), /printable ASCII/],
    [capability("tasks", ["update", "update"]), /repeats update/],
    [capability("tasks", ["*"]), /names "\*"/],
    [capability("tasks", ["Update"]), /names "Update"/],
  ] as const)
    refused(claims, reason);

  const many = (count: number): DatasetGrantClaims => ({
    ...base,
    commands: Array.from({ length: count }, (_, index) => ({
      collection: `c${String(index)}`,
      operations: ["insert"] as const,
    })),
  });
  mintGrant(many(64), key);
  refused(many(65), /at most 64 command capabilities/);
  // Absent and empty are the same read-only grant, byte for byte.
  assert.equal(
    mintGrant({ ...base, commands: [] }, key),
    text("read-only.token"),
  );
});

test("a malformed attribute, name or key id is refused", () => {
  const attribute = (value: unknown, name = "tenant"): DatasetGrantClaims =>
    ({ ...base, attributes: { [name]: value } }) as DatasetGrantClaims;
  for (const [claims, reason] of [
    [attribute({ uuid: "ab" }), /32 hex digits/],
    [
      attribute({ uuid: "0a1b2c3d-4e5f-6071-8293-04a5b6c7d8e9" }),
      /32 hex digits/,
    ],
    [attribute({ int8: "9223372036854775808" }), /signed 64-bit/],
    [attribute({ int8: " 1" }), /signed 64-bit/],
    [attribute({ int8: 1 }), /signed 64-bit/],
    [attribute({ bytes: "odd" }), /even number of digits/],
    [attribute({ int2: 32_768 }), /-32768 to 32767/],
    [attribute({ int4: 1.5 }), /whole number/],
    [attribute({ bool: "true" }), /true or false/],
    [attribute({ text: "x" }), /unknown type text/],
    [attribute({ bool: true, int4: 1 }), /exactly one type/],
    [attribute({ bool: true }, ""), /attribute name must be/],
    [attribute({ bool: true }, "a\u0007b"), /attribute name must be/],
    [attribute({ bool: true }, "n".repeat(513)), /attribute name must be/],
    [{ ...base, subject: "member\n1" }, /subject must be/],
    [{ ...base, subject: "member-\ud800" }, /well-formed Unicode/],
    [{ ...base, keyId: "grant.vectors" }, /other than "\."/],
    [{ ...base, keyId: "k".repeat(129) }, /other than "\."/],
    [{ ...base, audience: "" }, /audience must be/],
    [{ ...base, authorizationGeneration: 2 ** 53 }, /whole number from 0/],
    [
      { ...base, version: 1 } as DatasetGrantClaims,
      /version is not a grant claim/,
    ],
  ] as const)
    refused(claims, reason);

  const attributes = (count: number, value: GrantAttributeValue) =>
    Object.fromEntries(
      Array.from({ length: count }, (_, index) => [`a${String(index)}`, value]),
    );
  mintGrant({ ...base, attributes: attributes(64, { bool: true }) }, key);
  refused(
    { ...base, attributes: attributes(65, { bool: true }) },
    /at most 64 attributes/,
  );
  // Every field valid, and the payload still over the verifier's 4 KiB.
  refused(
    { ...base, attributes: attributes(40, { bytes: "ab".repeat(48) }) },
    /Calque refuses one over 4096/,
  );
});

test("the command prints the document calque-dev-grant prints, for the token mintGrant mints", () => {
  const arguments_ = [
    "--key-file",
    vectorPath(KEY_FILE),
    "--key-id",
    KEY_ID,
    "--audience",
    "calque.vectors.test",
    "--subject",
    "member-1",
    "--generation",
    "9",
    "--lifetime-seconds",
    "300",
    "--attribute",
    "organization_id=uuid:0a1b2c3d4e5f6071829304a5b6c7d8e9",
    "--command",
    "projects=update",
    "--command",
    "tasks=insert+update+delete",
  ];
  assert.deepEqual(JSON.parse(mintGrantCommand(arguments_, 1_800_000_000)), {
    authorizationGeneration: 9,
    commands: ["projects:update", "tasks:insert+update+delete"],
    expiresAtUnixSeconds: 1_800_000_300,
    issuedAtUnixSeconds: 1_800_000_000,
    subject: "member-1",
    token: text("command-capability.token"),
    verificationKeyEnvironment: JSON.parse(
      text("test-only-verification-key.json"),
    ) as unknown,
  });

  // The file as a reader runs it: Node strips the types, and the clock is
  // the real one, so the token is checked against a re-mint at its own time.
  const command = fileURLToPath(
    new URL("../client/examples/grant-issuer/mint-grant.mts", import.meta.url),
  );
  const ran = spawnSync(process.execPath, [command, ...arguments_], {
    encoding: "utf8",
  });
  assert.equal(ran.status, 0, ran.stderr);
  const printed = JSON.parse(ran.stdout) as {
    issuedAtUnixSeconds: number;
    token: string;
  };
  assert.equal(
    printed.token,
    mintGrant(
      {
        ...claimsOf(vector("command-capability.payload.json")),
        issuedAtUnixSeconds: printed.issuedAtUnixSeconds,
        expiresAtUnixSeconds: printed.issuedAtUnixSeconds + 300,
      },
      key,
    ),
  );

  const tooLong = spawnSync(
    process.execPath,
    [
      command,
      ...arguments_.slice(0, 11),
      String(MAX_GRANT_LIFETIME_SECONDS + 1),
    ],
    { encoding: "utf8" },
  );
  assert.equal(tooLong.status, 1);
  assert.match(tooLong.stderr, /^mint-grant: the validity window is 901 s/);
  assert.equal(tooLong.stdout, "");
});

test("a tenant order is signed under its own domain, never the grant's", () => {
  const order = {
    keyId: KEY_ID,
    audience: "tenants.example",
    orderId: "order-0123456789ab",
    action: "warm",
    tenant: { int8: "9007199254740993" },
    issuedAtUnixSeconds: 1_800_000_000,
    expiresAtUnixSeconds: 1_800_000_300,
  } as const;
  const [payload, signature] = mintTenantOrder(order, key)
    .split(".")
    .map((half) => Buffer.from(half, "base64url"));
  assert.ok(payload !== undefined && signature !== undefined);
  assert.equal(
    payload.toString("utf8"),
    '{"version":1,"keyId":"grant-vectors-key","audience":"tenants.example","orderId":"order-0123456789ab","action":"warm","tenant":{"int8":"9007199254740993"},"issuedAtUnixSeconds":1800000000,"expiresAtUnixSeconds":1800000300}',
  );
  const publicKey = createPublicKey(key);
  const under = (domain: string): boolean =>
    verify(
      null,
      Buffer.concat([Buffer.from(domain, "utf8"), payload]),
      publicKey,
      signature,
    );
  assert.ok(under(TENANT_ORDER_SIGNING_DOMAIN));
  assert.ok(!under(GRANT_SIGNING_DOMAIN));

  assert.throws(
    () =>
      mintTenantOrder(
        { ...order, expiresAtUnixSeconds: order.issuedAtUnixSeconds + 301 },
        key,
      ),
    GrantRefused,
  );
  assert.throws(
    () =>
      mintTenantOrder({ ...order, action: "delete" as unknown as "warm" }, key),
    GrantRefused,
  );
  assert.throws(
    () => mintTenantOrder({ ...order, tenant: { int8: "1.5" } }, key),
    GrantRefused,
  );
  assert.throws(
    () => mintTenantOrder({ ...order, orderId: "short" }, key),
    GrantRefused,
  );

  // Left out, the id is random, so no two orders share one.
  const { orderId: _, ...unnamed } = order;
  const ids = new Set(
    [0, 1, 2].map((): unknown => {
      const [minted] = mintTenantOrder(unnamed, key).split(".");
      assert.ok(minted !== undefined);
      const claims = JSON.parse(
        Buffer.from(minted, "base64url").toString("utf8"),
      ) as { orderId: string };
      assert.match(claims.orderId, /^[A-Za-z0-9_-]{22}$/u);
      return claims.orderId;
    }),
  );
  assert.equal(ids.size, 3);
});

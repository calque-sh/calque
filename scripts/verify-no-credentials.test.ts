/**
 * `scripts/verify-no-credentials.ts` through its entry point, over a scratch
 * git repository. Every credential is generated when the test runs, so this
 * file carries none of the shapes it checks.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { generateKeyPairSync, randomBytes, type KeyObject } from "node:crypto";
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { loadSigningKey, mintGrant } from "@calque/grant";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const FIXTURE = "spec/fixtures/dataset-grant-v1";
const FIXTURE_KEY = `${FIXTURE}/test-only-grant-key.pk8`;

interface Scan {
  readonly status: number | null;
  readonly stdout: string;
  readonly stderr: string;
  /** The `<file>:<line> <kind>` lines a refusal lists, in its order. */
  readonly refused: readonly string[];
}

/**
 * Runs the gate over a fresh repository holding `files`, each added to the
 * index the way a commit would carry it. `withFixture` first copies the grant
 * vectors and their spec in from this repository.
 */
function scan(
  files: Readonly<Record<string, string | Buffer>>,
  withFixture = false,
): Scan {
  const root = mkdtempSync(join(tmpdir(), "calque-credentials-"));
  try {
    spawnSync("git", ["init", "-q"], { cwd: root });
    if (withFixture) {
      cpSync(join(repositoryRoot, FIXTURE), join(root, FIXTURE), {
        recursive: true,
      });
      cpSync(
        join(repositoryRoot, "spec/dataset-grant-v1.md"),
        join(root, "spec/dataset-grant-v1.md"),
      );
    }
    for (const [file, contents] of Object.entries(files)) {
      mkdirSync(dirname(join(root, file)), { recursive: true });
      writeFileSync(join(root, file), contents);
    }
    const added = spawnSync("git", ["add", "-A"], { cwd: root });
    assert.equal(added.status, 0, String(added.stderr));
    const result = spawnSync(
      process.execPath,
      [
        "--import",
        "./scripts/register.mjs",
        "scripts/verify-no-credentials.ts",
        root,
      ],
      { cwd: repositoryRoot, encoding: "utf8" },
    );
    return {
      status: result.status,
      stdout: result.stdout,
      stderr: result.stderr,
      refused: result.stderr
        .split("\n")
        .filter((line) => line.startsWith("  "))
        .map((line) => line.trim()),
    };
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

/** A fresh Ed25519 key as PKCS#8 DER: v1 as `node:crypto` writes it, v2 as `calque-dev-grant` does. */
function randomKey(layout: "v1" | "v2"): Buffer {
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  const v1 = privateKey.export({ format: "der", type: "pkcs8" });
  const publicRaw = Buffer.from(
    publicKey.export({ format: "jwk" }).x ?? "",
    "base64url",
  );
  const der =
    layout === "v1"
      ? v1
      : Buffer.concat([
          Buffer.from("3051020101", "hex"),
          v1.subarray(5),
          Buffer.from("812100", "hex"),
          publicRaw,
        ]);
  // A real key, not a lookalike: the package parses it.
  loadSigningKey(der);
  return der;
}

function freshGrant(key: KeyObject): string {
  const now = Math.floor(Date.now() / 1000);
  return mintGrant(
    {
      keyId: "production-1",
      audience: "app.example",
      subject: "alice",
      attributes: {},
      authorizationGeneration: 1,
      issuedAtUnixSeconds: now,
      expiresAtUnixSeconds: now + 900,
      commands: [{ collection: "tasks", operations: ["update"] }],
    },
    key,
  );
}

/** A `SecretToken`'s text: 32 random bytes, unpadded base64url. */
const secretToken = (): string => randomBytes(32).toString("base64url");

test("a grant freshly minted with a random key is refused by place, never printed", () => {
  const token = freshGrant(generateKeyPairSync("ed25519").privateKey);
  const result = scan({
    "docs/runbook.md": `# Calling the API\n\n    curl -H "Authorization: Bearer ${token}" https://app.example/v1/plan\n`,
  });
  assert.equal(result.status, 1, result.stdout);
  assert.deepEqual(result.refused, ["docs/runbook.md:3 grant"]);
  for (const half of token.split("."))
    assert.ok(!result.stderr.includes(half), "the refusal printed the token");
});

test("the RFC 8032 TEST 1 grant vectors and their key pass by content", () => {
  const tokens = readdirSync(join(repositoryRoot, FIXTURE)).filter((file) =>
    file.endsWith(".token"),
  );
  assert.ok(tokens.length > 0);
  const result = scan({}, true);
  assert.equal(result.status, 0, result.stderr);
  assert.match(
    result.stdout,
    new RegExp(
      `exempted by content: 1 ed25519-key, ${String(tokens.length)} grant\\)`,
      "u",
    ),
  );
});

test("a random key written over the fixture's key file is refused", () => {
  const result = scan({ [FIXTURE_KEY]: randomKey("v2") }, true);
  assert.equal(result.status, 1, result.stdout);
  assert.deepEqual(result.refused, [`${FIXTURE_KEY}:1 ed25519-key`]);
});

test("a random key is refused as DER, base64 and hex", () => {
  const key = randomKey("v1");
  const result = scan({
    "deploy/grant-key.pk8": randomKey("v2"),
    "deploy/grant-key.pem": `-----BEGIN PRIVATE KEY-----\n${key.toString("base64")}\n-----END PRIVATE KEY-----\n`,
    "deploy/seed.ts": `// signing key\nconst KEY = "${key.toString("hex")}";\n`,
  });
  assert.equal(result.status, 1, result.stdout);
  assert.deepEqual(result.refused, [
    "deploy/grant-key.pem:2 ed25519-key",
    "deploy/grant-key.pk8:1 ed25519-key",
    "deploy/seed.ts:2 ed25519-key",
  ]);
});

/** A plan token, or with an object an artifact capability, as the deployment signs it. */
function signed(fields: Readonly<Record<string, unknown>>): string {
  const payload = Buffer.from(
    JSON.stringify({ keyId: "0011223344556677", planId: "plan-1", ...fields }),
  ).toString("base64url");
  return `${payload}.${randomBytes(32).toString("base64url")}`;
}

test("a plan token, an artifact capability and each local secret are refused in the shape Calque writes", () => {
  const result = scan({
    "notes/trace.txt": `GET /v1/artifact-objects/a33f?cap=${signed({ objectId: "sha256:a33f" })} 200\n`,
    "notes/changes.json": `{"planToken":"${signed({ generation: 4 })}"}\n`,
    "notes/curl.sh": `curl -H "Authorization: Bearer ${secretToken()}" http://127.0.0.1:8787/v1/plan\n`,
    ".env.example": `CALQUE_ENDPOINT=http://127.0.0.1:8787\nCALQUE_BEARER_TOKEN=${secretToken()}\n`,
    "notes/secrets.json": `{"formatVersion":3,"cursorSecret":"${secretToken()}","proofSecret":"${secretToken()}","planSecret":"${secretToken()}","bearerToken":"${secretToken()}"}\n`,
  });
  assert.equal(result.status, 1, result.stdout);
  assert.deepEqual(result.refused, [
    ".env.example:2 local-secret",
    "notes/changes.json:1 plan",
    "notes/curl.sh:1 local-secret",
    "notes/secrets.json:1 local-secret",
    "notes/secrets.json:1 local-secret",
    "notes/secrets.json:1 local-secret",
    "notes/secrets.json:1 local-secret",
    "notes/trace.txt:1 cap",
  ]);
});

test("a signed value whose payload is not a grant's claims passes", () => {
  const payload = Buffer.from(
    JSON.stringify({ version: 1, keyId: "k", note: "not a grant" }),
  ).toString("base64url");
  const result = scan({
    "notes/other.txt": `${payload}.${randomBytes(64).toString("base64url")}\n`,
  });
  assert.equal(result.status, 0, result.stderr);
});

test("the tree's four placeholders pass with no allowlist entry", () => {
  const result = scan({
    "server/crates/mutation-http/src/lib.rs":
      '.header(header::AUTHORIZATION, "Bearer app-token")\n',
    "server/crates/bootstrap-http/src/lib.rs":
      '.header(header::AUTHORIZATION, "Bearer app-token")\n',
    "client/packages/client/src/vite.test.ts":
      'headers: { authorization: "Bearer calque-dev-proxy" },\n',
    "scripts/bench.test.ts":
      'sent("2", "http://127.0.0.1:8/v1/artifacts/a33f?cap=secret", "Fetch");\n',
  });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /exempted by content: none\)/u);
});

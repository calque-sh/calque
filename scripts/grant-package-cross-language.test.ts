/**
 * `@calque/grant` against Calque's Rust verifier: the packed `calque-grant`
 * binary creates a key, mints a grant under it, and `mintTenantOrder` signs an
 * order; `server/crates/dataset-grant/examples/verify_node_grant.rs` then verifies
 * both from the verification-key value `keygen` printed. The frozen vectors pin
 * the test-only key's bytes; this test covers a key the package generated.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { loadSigningKey, mintTenantOrder } from "@calque/grant";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const bin = join(root, "client/packages/grant/bin/calque-grant.js");

function calqueGrant(args: readonly string[]): string {
  const result = spawnSync(process.execPath, [bin, ...args], {
    encoding: "utf8",
  });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout;
}

test("a grant and a tenant order signed with a generated key verify under the Rust verifier", () => {
  const directory = mkdtempSync(join(tmpdir(), "calque-grant-cross-"));
  try {
    const keyFile = join(directory, "grant-key.pem");
    const keyId = "cross-language-key";
    const audience = "cross-language.example";
    const verificationKey = JSON.parse(
      calqueGrant(["keygen", "--key-file", keyFile, "--key-id", keyId]),
    ) as unknown;

    const now = Math.floor(Date.now() / 1000);
    const minted = JSON.parse(
      calqueGrant([
        "mint",
        "--key-file",
        keyFile,
        "--key-id",
        keyId,
        "--audience",
        audience,
        "--subject",
        "cross-language-subject",
        "--generation",
        "3",
        "--lifetime-seconds",
        "899",
        "--attribute",
        "tenant=int8:9007199254740993",
        "--command",
        "tasks=update+delete",
      ]),
    ) as { token: string; issuedAtUnixSeconds: number };
    assert.deepEqual(
      (minted as unknown as { verificationKeyEnvironment: unknown })
        .verificationKeyEnvironment,
      verificationKey,
    );

    const tenantOrder = mintTenantOrder(
      {
        keyId,
        audience,
        action: "pin",
        tenant: { int8: "9007199254740993" },
        issuedAtUnixSeconds: minted.issuedAtUnixSeconds,
        expiresAtUnixSeconds: minted.issuedAtUnixSeconds + 60,
      },
      loadSigningKey(readFileSync(keyFile)),
    );

    writeFileSync(
      join(directory, "fixture.json"),
      JSON.stringify({
        audience,
        generation: 3,
        nowUnixSeconds: Math.max(now, minted.issuedAtUnixSeconds),
        verificationKey,
        grant: minted.token,
        tenantOrder,
      }),
    );

    const cargo = spawnSync(
      "cargo",
      [
        "run",
        "-q",
        "-p",
        "dataset-grant",
        "--example",
        "verify_node_grant",
        "--",
        directory,
      ],
      { cwd: root, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
    );
    assert.equal(cargo.status, 0, `${cargo.stdout}\n${cargo.stderr}`);
    assert.equal(cargo.stdout, "verified\n");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

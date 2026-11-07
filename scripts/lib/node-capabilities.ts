import { createPrivateKey } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import zlib from "node:zlib";
import { fileURLToPath } from "node:url";

/**
 * The PKCS#8 v2 Ed25519 key the grant issuer example's tests sign with, the
 * frozen RFC 8032 TEST 1 key of the dataset grant vectors.
 */
export const testGrantKeyPath = join(
  resolve(dirname(fileURLToPath(import.meta.url)), "../.."),
  "spec/fixtures/dataset-grant-v1/test-only-grant-key.pk8",
);

/**
 * What the probes read from a Node runtime. `currentNodeRuntime` reads the one
 * running; a test hands in a runtime that lacks a capability.
 */
export interface NodeRuntime {
  readonly zstdCompressSync: unknown;
  /** `process.features.typescript`: `"strip"` or `"transform"` when on. */
  readonly typescript: unknown;
  /** Throws when the bundled OpenSSL cannot read the key. */
  readonly readPkcs8: (der: Buffer) => unknown;
}

export const currentNodeRuntime = (): NodeRuntime => ({
  zstdCompressSync: zlib.zstdCompressSync,
  typescript: process.features.typescript,
  readPkcs8: (der) =>
    createPrivateKey({ key: der, format: "der", type: "pkcs8" }),
});

/** `SEQUENCE (0x51 bytes) { INTEGER 1, ...`: version 1 is PKCS#8 v2. */
const PKCS8_V2_PREFIX = Buffer.from("3051020101", "hex");

/**
 * One line per capability the workspace needs and the runtime lacks, naming
 * the Node releases that bring it; empty when it has all three. Capabilities
 * are probed rather than a version, because the releases that bring them do
 * not line up.
 */
export function nodeCapabilityRefusals(
  runtime: NodeRuntime,
  pkcs8V2Key: Buffer,
): string[] {
  // A v1 key reads under OpenSSL 3.0 as well, so the probe needs a v2 key.
  if (!pkcs8V2Key.subarray(0, PKCS8_V2_PREFIX.length).equals(PKCS8_V2_PREFIX)) {
    throw new Error(
      "the key the OpenSSL probe reads is not a PKCS#8 v2 Ed25519 key, so the probe cannot tell OpenSSL 3.0 from 3.5",
    );
  }
  const refusals: string[] = [];
  if (typeof runtime.zstdCompressSync !== "function") {
    refusals.push(
      "`zlib.zstdCompressSync` is not a function: Node's built-in zstd arrived in 22.15 and 23.8, " +
        "and the client browser fixture compresses its artifacts with it.",
    );
  }
  if (runtime.typescript !== "strip" && runtime.typescript !== "transform") {
    refusals.push(
      `type stripping is off (\`process.features.typescript\` is ${String(runtime.typescript)}): ` +
        "Node strips types without a flag from 22.18 and 23.6, and the harness tests import the " +
        "grant issuer example's `.mts` files, which the harness loader leaves to Node.",
    );
  }
  try {
    runtime.readPkcs8(pkcs8V2Key);
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    refusals.push(
      `\`crypto.createPrivateKey\` refuses the PKCS#8 v2 test key (${reason}): the OpenSSL 3.5 ` +
        "that reads it is bundled from Node 22.20 and 24.5, and the grant issuer example's tests " +
        "sign with that key.",
    );
  }
  return refusals;
}

/** Throws, listing every missing capability, unless the running Node has all three. */
export function requireNodeCapabilities(): void {
  const refusals = nodeCapabilityRefusals(
    currentNodeRuntime(),
    readFileSync(testGrantKeyPath),
  );
  if (refusals.length > 0) {
    throw new Error(
      `Node ${process.versions.node} (${process.execPath}) cannot run this workspace:\n` +
        refusals.map((refusal) => `  - ${refusal}`).join("\n") +
        "\npackage.json's engines.node is the supported range and .nvmrc the pinned version.",
    );
  }
}

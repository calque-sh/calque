import assert from "node:assert/strict";
import { generateKeyPairSync } from "node:crypto";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  currentNodeRuntime,
  nodeCapabilityRefusals,
  requireNodeCapabilities,
  testGrantKeyPath,
  type NodeRuntime,
} from "./lib/node-capabilities.ts";

const key = readFileSync(testGrantKeyPath);

/** A runtime with all three capabilities, to take one away from at a time. */
const capable: NodeRuntime = {
  zstdCompressSync: () => Buffer.alloc(0),
  typescript: "strip",
  readPkcs8: () => undefined,
};

// What `createPrivateKey` throws for the v2 key under the OpenSSL 3.0 that
// Node bundles before 22.20 and 24.5.
const openssl30: NodeRuntime["readPkcs8"] = () => {
  throw new Error("error:068000A8:asn1 encoding routines::wrong tag");
};

test("the running Node has every capability the workspace needs", () => {
  assert.deepEqual(nodeCapabilityRefusals(currentNodeRuntime(), key), []);
  requireNodeCapabilities();
});

test("a Node without built-in zstd is refused, naming 22.15 and 23.8", () => {
  const refusals = nodeCapabilityRefusals(
    { ...capable, zstdCompressSync: undefined },
    key,
  );
  assert.equal(refusals.length, 1, refusals.join("\n"));
  assert.match(
    refusals[0] ?? "",
    /^`zlib\.zstdCompressSync` is not a function: .* 22\.15 and 23\.8,/,
  );
});

test("a Node with type stripping off is refused, naming 22.18 and 23.6", () => {
  for (const typescript of [false, undefined]) {
    const refusals = nodeCapabilityRefusals({ ...capable, typescript }, key);
    assert.equal(refusals.length, 1, refusals.join("\n"));
    assert.match(
      refusals[0] ?? "",
      new RegExp(
        `^type stripping is off \\(\`process\\.features\\.typescript\` is ${String(typescript)}\\): .* 22\\.18 and 23\\.6,`,
      ),
    );
  }
});

test("a Node whose OpenSSL cannot read the v2 key is refused, naming 22.20 and 24.5", () => {
  const refusals = nodeCapabilityRefusals(
    { ...capable, readPkcs8: openssl30 },
    key,
  );
  assert.equal(refusals.length, 1, refusals.join("\n"));
  assert.match(
    refusals[0] ?? "",
    /^`crypto\.createPrivateKey` refuses the PKCS#8 v2 test key \(error:068000A8:asn1 encoding routines::wrong tag\): .* Node 22\.20 and 24\.5,/,
  );
});

test("each missing capability gets its own line, in probe order", () => {
  const refusals = nodeCapabilityRefusals(
    { zstdCompressSync: undefined, typescript: false, readPkcs8: openssl30 },
    key,
  );
  assert.deepEqual(
    refusals.map((refusal) => refusal.slice(0, 24)),
    [
      "`zlib.zstdCompressSync` ",
      "type stripping is off (`",
      "`crypto.createPrivateKey",
    ],
  );
});

test("the OpenSSL probe refuses to run against a PKCS#8 v1 key, which OpenSSL 3.0 reads", () => {
  const v1 = generateKeyPairSync("ed25519").privateKey.export({
    format: "der",
    type: "pkcs8",
  });
  assert.throws(
    () => nodeCapabilityRefusals(capable, v1),
    /not a PKCS#8 v2 Ed25519 key/,
  );
});

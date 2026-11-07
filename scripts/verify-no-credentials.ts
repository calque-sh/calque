#!/usr/bin/env node
// Refuses a live Calque credential anywhere in the tree: a grant, a plan token,
// an artifact capability, a `calque dev` secret or an Ed25519 signing key.
//
// Each shape is matched by what its bytes decode to, so a placeholder such as
// `Bearer app-token` never matches. The only exemption is by content, never by
// path: the RFC 8032 §7.1 TEST 1 key, which signs the grant vectors in
// spec/fixtures/dataset-grant-v1/, and tokens that verify under it.
import { createPublicKey, verify, type KeyObject } from "node:crypto";
import { lstatSync, readFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

import { treePaths } from "./lib/tree-paths.ts";

type CredentialKind = "grant" | "plan" | "cap" | "local-secret" | "ed25519-key";

interface Credential {
  readonly kind: CredentialKind;
  /** Byte offset of the match in the file. */
  readonly offset: number;
  /** True only for the RFC 8032 TEST 1 key and tokens it signed. */
  readonly rfcTestVector: boolean;
}

/** RFC 8032 §7.1 TEST 1 secret key, the 32-byte seed. */
const RFC_8032_TEST_1_SEED = Buffer.from(
  "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
  "hex",
);
/** RFC 8032 §7.1 TEST 1 public key. */
const RFC_8032_TEST_1_PUBLIC_KEY: KeyObject = createPublicKey({
  key: {
    kty: "OKP",
    crv: "Ed25519",
    x: Buffer.from(
      "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
      "hex",
    ).toString("base64url"),
  },
  format: "jwk",
});

// spec/dataset-grant-v1.md: the signature covers this domain, its NUL byte,
// then the payload, and a payload is at most 4096 bytes.
const GRANT_SIGNING_DOMAIN = Buffer.from("calque.dataset-grant.v1\0", "utf8");
const MAX_PAYLOAD_CHARACTERS = Math.ceil((4096 * 4) / 3);
const ED25519_SIGNATURE_CHARACTERS = 86;
/** The claims every v1 payload carries; `attributes` and `commands` may be absent. */
const REQUIRED_CLAIMS = [
  "version",
  "keyId",
  "audience",
  "subject",
  "authorizationGeneration",
  "issuedAtUnixSeconds",
  "expiresAtUnixSeconds",
] as const;

/**
 * `payload.signature`, both unpadded base64url. The lookbehind starts a
 * candidate only where a base64url run starts, which keeps the scan linear
 * over long encoded blobs.
 */
const GRANT = new RegExp(
  `(?<![A-Za-z0-9_-])([A-Za-z0-9_-]{16,${MAX_PAYLOAD_CHARACTERS}})\\.([A-Za-z0-9_-]{${ED25519_SIGNATURE_CHARACTERS}})(?![A-Za-z0-9_-])`,
  "gu",
);
/**
 * A plan token or artifact capability the deployment signed: `payload.tag`,
 * both unpadded base64url, the tag a 32-byte HMAC over a JSON payload that
 * names its key and plan. A capability's payload also names its object.
 */
const SIGNED = new RegExp(
  `(?<![A-Za-z0-9_-])([A-Za-z0-9_-]{16,${MAX_PAYLOAD_CHARACTERS}})\\.([A-Za-z0-9_-]{43})(?![A-Za-z0-9_-])`,
  "gu",
);
// A `SecretToken` is 32 random bytes written as 43 unpadded base64url
// characters: the `calque dev` bearer, cursor, proof and plan secrets under
// the labels they are written with.
const LOCAL_SECRET =
  /(?:\bBearer\s+|\bCALQUE_BEARER_TOKEN=|"(?:bearerToken|cursorSecret|proofSecret|planSecret)"\s*:\s*")([A-Za-z0-9_-]{43})(?![A-Za-z0-9_-])/gu;
const SECRET_TOKEN_BYTES = 32;

/**
 * The PKCS#8 Ed25519 private key from its algorithm identifier on:
 * `SEQUENCE { OID 1.3.101.112 }`, then the OCTET STRING that wraps the
 * 32-byte seed's OCTET STRING. RFC 8410 §7; v1 and v2 (RFC 5958) share it.
 */
const ED25519_PRIVATE_KEY_ALGORITHM = Buffer.from(
  "300506032b657004220420",
  "hex",
);
/** `SEQUENCE`, a length byte, `INTEGER` version 0 or 1: the 5 bytes before it. */
function isPkcs8Header(bytes: Buffer, algorithmAt: number): boolean {
  const start = algorithmAt - 5;
  return (
    start >= 0 &&
    bytes[start] === 0x30 &&
    bytes[start + 2] === 0x02 &&
    bytes[start + 3] === 0x01 &&
    (bytes[start + 4] === 0x00 || bytes[start + 4] === 0x01)
  );
}
/** A key document's first 64 base64 characters: the header, algorithm and seed. */
const ED25519_KEY_BASE64 =
  /(?<![A-Za-z0-9+/_-])[A-Za-z0-9+/_-]{8}BQYDK2VwBCIE[A-Za-z0-9+/_-]{44}/gu;
const ED25519_KEY_HEX =
  /(?<![0-9A-Fa-f])30[0-9A-Fa-f]{2}0201(?:00|01)300506032[Bb]657004220420[0-9A-Fa-f]{64}/gu;

/** `text` decoded, or undefined unless it is the canonical unpadded base64url of `bytes` bytes. */
function base64url(text: string, bytes?: number): Buffer | undefined {
  const decoded = Buffer.from(text, "base64url");
  if (decoded.toString("base64url") !== text) return undefined;
  if (bytes !== undefined && decoded.length !== bytes) return undefined;
  return decoded;
}

function jsonObject(payload: Buffer): object | undefined {
  let value: unknown;
  try {
    value = JSON.parse(payload.toString("utf8"));
  } catch {
    return undefined;
  }
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value
    : undefined;
}

function isGrantPayload(payload: Buffer): boolean {
  const claims = jsonObject(payload);
  return (
    claims !== undefined &&
    REQUIRED_CLAIMS.every((claim) => Object.hasOwn(claims, claim))
  );
}

function signedKind(payload: Buffer): "plan" | "cap" | undefined {
  const fields = jsonObject(payload);
  if (
    fields === undefined ||
    !Object.hasOwn(fields, "keyId") ||
    !Object.hasOwn(fields, "planId")
  )
    return undefined;
  return Object.hasOwn(fields, "objectId") ? "cap" : "plan";
}

function seedCredential(seed: Buffer, offset: number): Credential | undefined {
  if (seed.length !== RFC_8032_TEST_1_SEED.length) return undefined;
  return {
    kind: "ed25519-key",
    offset,
    rfcTestVector: seed.equals(RFC_8032_TEST_1_SEED),
  };
}

/** Every credential-shaped value in one file's bytes, in no particular order. */
function credentialsIn(bytes: Buffer): Credential[] {
  const found: Credential[] = [];
  const text = bytes.toString("latin1");

  for (const match of text.matchAll(GRANT)) {
    const payload = base64url(match[1] ?? "");
    const signature = base64url(match[2] ?? "", 64);
    if (!payload || !signature || !isGrantPayload(payload)) continue;
    found.push({
      kind: "grant",
      offset: match.index,
      rfcTestVector: verify(
        null,
        Buffer.concat([GRANT_SIGNING_DOMAIN, payload]),
        RFC_8032_TEST_1_PUBLIC_KEY,
        signature,
      ),
    });
  }
  for (const match of text.matchAll(SIGNED)) {
    const payload = base64url(match[1] ?? "");
    const kind = payload && signedKind(payload);
    if (kind && base64url(match[2] ?? "", SECRET_TOKEN_BYTES))
      found.push({ kind, offset: match.index, rfcTestVector: false });
  }
  for (const match of text.matchAll(LOCAL_SECRET)) {
    if (base64url(match[1] ?? "", SECRET_TOKEN_BYTES))
      found.push({
        kind: "local-secret",
        offset: match.index,
        rfcTestVector: false,
      });
  }

  for (
    let at = bytes.indexOf(ED25519_PRIVATE_KEY_ALGORITHM);
    at !== -1;
    at = bytes.indexOf(ED25519_PRIVATE_KEY_ALGORITHM, at + 1)
  ) {
    const seedAt = at + ED25519_PRIVATE_KEY_ALGORITHM.length;
    if (!isPkcs8Header(bytes, at)) continue;
    const credential = seedCredential(
      bytes.subarray(seedAt, seedAt + 32),
      at - 5,
    );
    if (credential) found.push(credential);
  }
  for (const match of text.matchAll(ED25519_KEY_BASE64)) {
    const document = Buffer.from(
      match[0].replaceAll("-", "+").replaceAll("_", "/"),
      "base64",
    );
    if (
      !isPkcs8Header(document, 5) ||
      !document.subarray(5, 16).equals(ED25519_PRIVATE_KEY_ALGORITHM)
    )
      continue;
    const credential = seedCredential(document.subarray(16, 48), match.index);
    if (credential) found.push(credential);
  }
  for (const match of text.matchAll(ED25519_KEY_HEX)) {
    const credential = seedCredential(
      Buffer.from(match[0].slice(32), "hex"),
      match.index,
    );
    if (credential) found.push(credential);
  }
  return found;
}

function lineOf(bytes: Buffer, offset: number): number {
  let line = 1;
  for (let at = bytes.indexOf(0x0a); at !== -1 && at < offset;) {
    line += 1;
    at = bytes.indexOf(0x0a, at + 1);
  }
  return line;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const root = path.resolve(
    process.argv[2] ??
      path.join(path.dirname(fileURLToPath(import.meta.url)), ".."),
  );
  const started = performance.now();
  const refused: string[] = [];
  const exempt = new Map<CredentialKind, number>();
  let scanned = 0;
  for (const file of treePaths(root)) {
    const absolute = path.join(root, file);
    let stat;
    try {
      stat = lstatSync(absolute);
    } catch {
      continue; // listed by the index but deleted from the working tree
    }
    // A symlink's target is scanned under its own path; a gitlink is a directory.
    if (!stat.isFile()) continue;
    scanned += 1;
    const bytes = readFileSync(absolute);
    for (const credential of credentialsIn(bytes)) {
      if (credential.rfcTestVector) {
        exempt.set(credential.kind, (exempt.get(credential.kind) ?? 0) + 1);
      } else {
        refused.push(
          `${file}:${String(lineOf(bytes, credential.offset))} ${credential.kind}`,
        );
      }
    }
  }
  const exempted =
    [...exempt]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([kind, count]) => `${String(count)} ${kind}`)
      .join(", ") || "none";
  const summary = `${String(scanned)} files in ${String(Math.round(performance.now() - started))} ms; RFC 8032 TEST 1 values exempted by content: ${exempted}`;
  if (refused.length === 0) {
    process.stdout.write(`Credentials: PASS (${summary})\n`);
    process.exit(0);
  }
  refused.sort();
  process.stderr.write(
    `Credentials: FAIL — ${String(refused.length)} credential-shaped value(s) in the tree (${summary}).\n\n` +
      "Each is a value Calque would accept, printed by place, never by value. Remove it, rotate it\n" +
      "if it was ever real, and sign a test vector with the RFC 8032 TEST 1 key instead:\n\n",
  );
  for (const place of refused) process.stderr.write(`  ${place}\n`);
  process.stderr.write("\n");
  process.exit(1);
}

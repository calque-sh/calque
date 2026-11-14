import { Buffer } from "node:buffer";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import type { DatasetGrantClaims } from "./claims.js";

const vectors = new URL(
  "../../../../spec/fixtures/dataset-grant-v1/",
  import.meta.url,
);

export const KEY_ID = "grant-vectors-key";
export const KEY_FILE = "test-only-grant-key.pk8";

/** The token vectors: each payload file and the token it signs to. */
export const TOKENS = [
  ["read-only.payload.json", "read-only.token"],
  ["command-capability.payload.json", "command-capability.token"],
  ["attribute-types.payload.json", "attribute-types.token"],
] as const;

export const vectorPath = (name: string): string =>
  fileURLToPath(new URL(name, vectors));
export const vector = (name: string): Buffer => readFileSync(vectorPath(name));
export const text = (name: string): string => vector(name).toString("utf8");

/** A frozen payload, read back as the claims an issuer would be given. */
export function claimsOf(payload: Buffer): DatasetGrantClaims {
  const { version, ...claims } = JSON.parse(payload.toString("utf8")) as {
    version: unknown;
  } & DatasetGrantClaims;
  if (version !== 1) throw new Error(`payload version ${String(version)}`);
  return claims;
}

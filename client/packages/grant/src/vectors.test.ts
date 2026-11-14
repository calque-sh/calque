import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";

import {
  GRANT_SIGNING_DOMAIN,
  GrantRefused,
  serializeClaims,
  type GrantAttributeValue,
} from "./claims.js";
import { loadSigningKey, verificationKeyEnvironment } from "./key.js";
import { mintGrant, signUnder } from "./mint.js";
import {
  claimsOf,
  KEY_FILE,
  KEY_ID,
  text,
  TOKENS,
  vector,
} from "./test-vectors.js";

const key = loadSigningKey(vector(KEY_FILE));

describe("the frozen dataset grant v1 vectors", () => {
  it("are the files the manifest lists, and node is a producer", () => {
    const manifest = JSON.parse(text("manifest.json")) as {
      wire: { producers: readonly string[] };
      fixtures: readonly { file: string; bytes: number; sha256: string }[];
    };
    expect(manifest.wire.producers).toEqual(["rust", "node"]);
    for (const { file, bytes, sha256 } of manifest.fixtures) {
      expect(vector(file).length, file).toBe(bytes);
      expect(
        createHash("sha256").update(vector(file)).digest("hex"),
        file,
      ).toBe(sha256);
    }
    const listed = new Set(manifest.fixtures.map(({ file }) => file));
    for (const [payload, token] of TOKENS) {
      expect(listed.has(payload), payload).toBe(true);
      expect(listed.has(token), token).toBe(true);
    }
    expect(listed.has(KEY_FILE)).toBe(true);
  });

  it("sign each frozen payload to exactly its frozen token", () => {
    expect(GRANT_SIGNING_DOMAIN).toBe("calque.dataset-grant.v1\0");
    for (const [payload, token] of TOKENS)
      expect(signUnder(GRANT_SIGNING_DOMAIN, vector(payload), key), token).toBe(
        text(token),
      );
  });

  it("are reproduced byte for byte from the claims a payload states", () => {
    for (const [payload, token] of TOKENS) {
      const claims = claimsOf(vector(payload));
      expect(serializeClaims(claims), payload).toEqual(vector(payload));
      expect(mintGrant(claims, key), token).toBe(text(token));
    }
  });

  it("order attributes by UTF-8 bytes, whatever order the claims list them in", () => {
    const claims = claimsOf(vector("attribute-types.payload.json"));
    const reversed: Record<string, GrantAttributeValue> = {};
    for (const name of Object.keys(claims.attributes).reverse())
      reversed[name] = claims.attributes[name] as GrantAttributeValue;
    expect(mintGrant({ ...claims, attributes: reversed }, key)).toBe(
      text("attribute-types.token"),
    );
  });

  it("give the frozen verification-key value over the frozen raw public key", () => {
    const environment = verificationKeyEnvironment(KEY_ID, key);
    expect(environment).toBe(text("test-only-verification-key.json"));
    const { publicKey } = JSON.parse(environment) as { publicKey: string };
    expect(Buffer.from(publicKey, "base64url")).toEqual(
      vector("test-only-grant-key.public.raw"),
    );
    expect(() => verificationKeyEnvironment("has.dot", key)).toThrow(
      GrantRefused,
    );
  });

  it("include every invalid payload an issuer can be handed, each refused", () => {
    const refusedByTheIssuer = [
      ["capability-empty-operations", /names no operation/],
      ["capability-unknown-operation", /names "truncate"/],
      ["capability-unknown-field", /exactly a collection and its operations/],
      ["capability-duplicate-collection", /named twice/],
      ["oversized", /subject must be 1 to 512 bytes/],
    ] as const;
    for (const [name, reason] of refusedByTheIssuer) {
      const claims = claimsOf(vector(`invalid/${name}.payload.json`));
      expect(() => mintGrant(claims, key), name).toThrow(GrantRefused);
      expect(() => mintGrant(claims, key), name).toThrow(reason);
    }

    // Not a claims document at all: an issuer never holds these bytes.
    expect(() => JSON.parse(text("invalid/truncated.payload.json"))).toThrow();
    // The generation in force is the deployment's fact, not the issuer's, so
    // this one mints; Calque refuses it at presentation against generation 9.
    expect(
      mintGrant(claimsOf(vector("invalid/cross-generation.payload.json")), key),
    ).toMatch(/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{86}$/u);
    // Claims are a JavaScript object, which cannot hold a name twice: parsing
    // these bytes keeps the last value, and the package writes that one once.
    const repeated = claimsOf(
      vector("invalid/duplicate-attribute.payload.json"),
    );
    expect(repeated.attributes).toEqual({
      organization_id: { uuid: "ffffffffffffffffffffffffffffffff" },
    });
    expect(
      serializeClaims(repeated).toString("utf8").split('"organization_id"')
        .length,
    ).toBe(2);

    // Every invalid vector the manifest lists is one of the cases above.
    const manifest = JSON.parse(text("manifest.json")) as {
      fixtures: readonly { file: string }[];
    };
    expect(
      manifest.fixtures
        .map(({ file }) => file)
        .filter((file) => file.startsWith("invalid/"))
        .sort(),
    ).toEqual(
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
});

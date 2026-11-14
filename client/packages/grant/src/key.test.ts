import { Buffer } from "node:buffer";
import { createPublicKey, sign, verify } from "node:crypto";
import { describe, expect, it } from "vitest";

import {
  generateSigningKey,
  loadSigningKey,
  verificationKeyEnvironment,
} from "./key.js";
import { KEY_FILE, KEY_ID, text, vector } from "./test-vectors.js";

const V1_PREFIX = "302e020100300506032b657004220420";

describe("generateSigningKey", () => {
  it("returns an Ed25519 key as PKCS#8 version 1 DER and PEM, with its raw public key", () => {
    const generated = generateSigningKey();
    expect(generated.privateKey.asymmetricKeyType).toBe("ed25519");
    expect(generated.pkcs8).toHaveLength(48);
    expect(generated.pkcs8.subarray(0, 16).toString("hex")).toBe(V1_PREFIX);
    expect(generated.pem).toMatch(
      /^-----BEGIN PRIVATE KEY-----\n[A-Za-z0-9+/=\n]+-----END PRIVATE KEY-----\n$/u,
    );
    const publicKey = Buffer.from(generated.publicKey, "base64url");
    expect(publicKey).toHaveLength(32);
    expect(generated.publicKey).toMatch(/^[A-Za-z0-9_-]{43}$/u);
    expect(
      JSON.parse(verificationKeyEnvironment("k", generated.privateKey)),
    ).toEqual({
      keyId: "k",
      publicKey: generated.publicKey,
    });
  });

  it("returns a different key each time", () => {
    expect(generateSigningKey().publicKey).not.toBe(
      generateSigningKey().publicKey,
    );
  });
});

describe("loadSigningKey", () => {
  const generated = generateSigningKey();
  const message = Buffer.from("the same message");
  const signsLike = (key: ReturnType<typeof loadSigningKey>): void => {
    expect(
      verify(
        null,
        message,
        createPublicKey(generated.privateKey),
        sign(null, message, key),
      ),
    ).toBe(true);
  };

  it("reads what generateSigningKey wrote, as DER bytes, PEM text and PEM bytes", () => {
    signsLike(loadSigningKey(generated.pkcs8));
    signsLike(loadSigningKey(generated.pem));
    signsLike(loadSigningKey(Buffer.from(generated.pem)));
    signsLike(loadSigningKey(`\n${generated.pem.replace(/\n/gu, "\r\n")}`));
  });

  it("reads the PKCS#8 version 2 document calque-dev-grant writes", () => {
    const v2 = vector(KEY_FILE);
    expect(v2).toHaveLength(83);
    expect(v2[4]).toBe(1);
    expect(verificationKeyEnvironment(KEY_ID, loadSigningKey(v2))).toBe(
      text("test-only-verification-key.json"),
    );
    const pem = `-----BEGIN PRIVATE KEY-----\n${v2.toString("base64")}\n-----END PRIVATE KEY-----\n`;
    expect(verificationKeyEnvironment(KEY_ID, loadSigningKey(pem))).toBe(
      text("test-only-verification-key.json"),
    );
  });

  it("reads version 2 from its seed, so OpenSSL's support for it does not matter, and refuses a public key that is not the seed's", () => {
    const v2 = vector(KEY_FILE);
    const native = loadSigningKey(v2);
    const rewrapped = loadSigningKey(
      Buffer.concat([Buffer.from(V1_PREFIX, "hex"), v2.subarray(16, 48)]),
    );
    expect(sign(null, Buffer.from("m"), rewrapped)).toEqual(
      sign(null, Buffer.from("m"), native),
    );

    const mismatched = Buffer.from(v2);
    mismatched.writeUInt8(mismatched.readUInt8(82) ^ 1, 82);
    expect(() => loadSigningKey(mismatched)).toThrow(
      /public key is not the one its private key derives/u,
    );
  });

  it("refuses what is not a PKCS#8 Ed25519 private key, naming what it wanted", () => {
    const ed25519Public = createPublicKey(generated.privateKey).export({
      format: "pem",
      type: "spki",
    });
    for (const bad of [
      Buffer.alloc(0),
      Buffer.from("not a key"),
      generated.pkcs8.subarray(0, 40),
      ed25519Public,
      generated.pem.replace("PRIVATE KEY", "RSA PRIVATE KEY"),
      "-----BEGIN PRIVATE KEY-----\n!!!\n-----END PRIVATE KEY-----\n",
    ])
      expect(() => loadSigningKey(bad), String(bad)).toThrow(
        /not a PKCS#8 Ed25519 private key in PEM or DER/u,
      );
  });

  it("refuses a key of another algorithm", () => {
    const other = generateSigningKey();
    const x25519 = Buffer.from(other.pkcs8);
    x25519[11] = 0x6e;
    expect(() => loadSigningKey(x25519)).toThrow(
      /Ed25519 private key|Ed25519 key/u,
    );
  });
});

describe("verificationKeyEnvironment", () => {
  it("prints keyId then publicKey, and refuses a key id the verifier would", () => {
    const { privateKey, publicKey } = generateSigningKey();
    expect(verificationKeyEnvironment("quickstart", privateKey)).toBe(
      `{"keyId":"quickstart","publicKey":"${publicKey}"}`,
    );
    for (const id of ["", "a.b", "x".repeat(129)])
      expect(() => verificationKeyEnvironment(id, privateKey)).toThrow(
        /keyId must be 1 to 128/u,
      );
  });
});

import { Buffer } from "node:buffer";
import {
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  type KeyObject,
} from "node:crypto";

import { keyId } from "./claims.js";

/** A fresh Ed25519 signing key, in every form a backend stores it in. */
export interface GeneratedSigningKey {
  readonly privateKey: KeyObject;
  /** The private key as a PKCS#8 version 1 DER document. */
  readonly pkcs8: Buffer;
  /** The same document as PEM text, which a secret store holds as a string. */
  readonly pem: string;
  /** The raw 32-byte public key in unpadded base64url. */
  readonly publicKey: string;
}

export function ed25519(key: KeyObject): KeyObject {
  if (key.asymmetricKeyType !== "ed25519")
    throw new TypeError(
      `a grant is signed with an Ed25519 key, not ${String(key.asymmetricKeyType)}`,
    );
  return key;
}

function rawPublicKey(key: KeyObject): string {
  const { x } = createPublicKey(key).export({ format: "jwk" });
  if (x === undefined) throw new TypeError("the key exported no public half");
  return x;
}

/**
 * A new Ed25519 key pair from the platform's random source. The private key is
 * written as PKCS#8 version 1, which every Node that supports Ed25519 reads.
 */
export function generateSigningKey(): GeneratedSigningKey {
  const { privateKey } = generateKeyPairSync("ed25519");
  return {
    privateKey,
    pkcs8: privateKey.export({ format: "der", type: "pkcs8" }),
    pem: privateKey.export({ format: "pem", type: "pkcs8" }).toString(),
    publicKey: rawPublicKey(privateKey),
  };
}

const PKCS8_V1_PREFIX = Buffer.from("302e020100300506032b657004220420", "hex");
const PKCS8_V2_PREFIX = Buffer.from("3051020101300506032b657004220420", "hex");
const PKCS8_V2_PUBLIC_KEY_PREFIX = Buffer.from("812100", "hex");
const SEED_BYTES = 32;
const PKCS8_V2_BYTES =
  PKCS8_V2_PREFIX.length +
  SEED_BYTES +
  PKCS8_V2_PUBLIC_KEY_PREFIX.length +
  SEED_BYTES;

function fromSeed(seed: Buffer): KeyObject {
  return createPrivateKey({
    key: Buffer.concat([PKCS8_V1_PREFIX, seed]),
    format: "der",
    type: "pkcs8",
  });
}

/**
 * The seed of an Ed25519 PKCS#8 version 2 document in the layout
 * `calque-dev-grant` and ring write: the algorithm identifier, the seed, then
 * the public key as the `[1]` field. Undefined for any other bytes. A public
 * key that is not the seed's is refused.
 */
function versionTwoSeed(der: Buffer): Buffer | undefined {
  if (der.length !== PKCS8_V2_BYTES) return undefined;
  const seedAt = PKCS8_V2_PREFIX.length;
  const publicAt = seedAt + SEED_BYTES + PKCS8_V2_PUBLIC_KEY_PREFIX.length;
  if (
    !der.subarray(0, seedAt).equals(PKCS8_V2_PREFIX) ||
    !der
      .subarray(seedAt + SEED_BYTES, publicAt)
      .equals(PKCS8_V2_PUBLIC_KEY_PREFIX)
  )
    return undefined;
  const seed = der.subarray(seedAt, seedAt + SEED_BYTES);
  const embedded = der.subarray(publicAt).toString("base64url");
  if (rawPublicKey(fromSeed(seed)) !== embedded)
    throw new TypeError(
      "the key document's public key is not the one its private key derives",
    );
  return seed;
}

function fromDer(der: Buffer): KeyObject {
  // A version 2 document in the layout above is read from its seed on every
  // Node, because OpenSSL before 3.5, which Node bundles up to 22.19 and
  // 24.4, refuses version 2.
  const seed = versionTwoSeed(der);
  return seed === undefined
    ? createPrivateKey({ key: der, format: "der", type: "pkcs8" })
    : fromSeed(seed);
}

const PEM_LABEL =
  /^-----BEGIN PRIVATE KEY-----\s*([A-Za-z0-9+/=\s]+?)\s*-----END PRIVATE KEY-----\s*$/u;

/**
 * The private key from a PKCS#8 Ed25519 document, as PEM text or DER bytes,
 * version 1 or 2. `calque-grant keygen` writes PEM; `calque-dev-grant` writes
 * DER, as version 2. A document of another kind of key is refused.
 */
export function loadSigningKey(document: Uint8Array | string): KeyObject {
  const bytes =
    typeof document === "string"
      ? Buffer.from(document, "utf8")
      : Buffer.from(document);
  const text = bytes.toString("latin1").trimStart();
  let key: KeyObject;
  try {
    if (text.startsWith("-----BEGIN")) {
      const body = PEM_LABEL.exec(text)?.[1];
      if (body === undefined)
        throw new TypeError(
          "the PEM document is not a PRIVATE KEY block of PKCS#8",
        );
      key = fromDer(Buffer.from(body.replace(/\s+/gu, ""), "base64"));
    } else {
      key = fromDer(bytes);
    }
  } catch (cause) {
    throw new TypeError(
      `the document is not a PKCS#8 Ed25519 private key in PEM or DER: ${cause instanceof Error ? cause.message : String(cause)}`,
      { cause },
    );
  }
  return ed25519(key);
}

/**
 * The `{"keyId":…,"publicKey":…}` value a deployment's
 * `--grant-verification-key-env` variable holds for this key: the raw 32-byte
 * public key, unpadded base64url.
 */
export function verificationKeyEnvironment(id: string, key: KeyObject): string {
  return JSON.stringify({
    keyId: keyId(id),
    publicKey: rawPublicKey(ed25519(key)),
  });
}

import type { RevocationBarrier } from "./gen/change_stream_pb.js";
import {
  ProtocolValidationError,
  REVOCATION_BARRIER_SIGNATURE_BYTES,
} from "./validation.js";

/**
 * The v2 signing preimage and Ed25519 verification of a revocation barrier,
 * held to the server's by the golden vectors under `spec/fixtures/protocol-v1`.
 *
 * Barriers are signed by the authority that mints dataset grants, not by the
 * change server, and the client takes the key from its configuration rather
 * than the frame, so a change server can neither invent a revocation nor move
 * one. A barrier that is never sent is bounded only by grant expiry.
 */

const SIGNING_DOMAIN = new TextEncoder().encode(
  "calque.change.revocation-barrier.v2\0",
);

const REVOCATION_BARRIER_PUBLIC_KEY_BYTES = 32;

/** The Ed25519 key a receiver accepts revocation barriers from. */
export interface RevocationBarrierVerificationKey {
  readonly keyId: string;
  /** Raw 32-byte Ed25519 public key. */
  readonly publicKey: Uint8Array;
}

/**
 * Builds the v2 signing preimage. A barrier names no plan: it states
 * something true of every plan of its generation and scope.
 *
 * ```text
 * ASCII "calque.change.revocation-barrier.v2\0"
 * || u32 BE len(barrierId)     || barrierId
 * || u32 BE len(signingKeyId)  || signingKeyId
 * || u32 BE len(barrierCursor) || barrierCursor.token
 * || u32 BE len(scopeHash)     || revokedAuthorizationScopeHash
 * || u64 BE revokedAuthorizationGeneration
 * || u64 BE effectiveAuthorizationGeneration
 * || u64 BE issuedAtUnixMicros
 * ```
 *
 * Every variable-length field is length-prefixed, so no two distinct barriers
 * can produce the same bytes by moving a boundary between two fields.
 */
export function revocationBarrierSigningPreimage(
  barrier: RevocationBarrier,
): Uint8Array<ArrayBuffer> {
  const cursor = barrier.barrierCursor;
  if (cursor === undefined) {
    throw new ProtocolValidationError(
      "revocationBarrier.barrierCursor",
      "is required",
    );
  }
  const encoder = new TextEncoder();
  const fields: readonly Uint8Array[] = [
    encoder.encode(barrier.barrierId),
    encoder.encode(barrier.signingKeyId),
    cursor.token,
    barrier.revokedAuthorizationScopeHash,
  ];
  const length =
    SIGNING_DOMAIN.byteLength +
    fields.reduce((total, field) => total + 4 + field.byteLength, 0) +
    24;
  const preimage = new Uint8Array(length);
  const view = new DataView(preimage.buffer);
  preimage.set(SIGNING_DOMAIN, 0);
  let offset = SIGNING_DOMAIN.byteLength;
  for (const field of fields) {
    view.setUint32(offset, field.byteLength, false);
    offset += 4;
    preimage.set(field, offset);
    offset += field.byteLength;
  }
  view.setBigUint64(offset, barrier.revokedAuthorizationGeneration, false);
  view.setBigUint64(
    offset + 8,
    barrier.effectiveAuthorizationGeneration,
    false,
  );
  view.setBigUint64(offset + 16, barrier.issuedAtUnixMicros, false);
  return preimage;
}

/**
 * Verifies one barrier against the configured authorization-authority key.
 * Every failure throws, including a signature of the wrong width, a key id
 * this deployment does not accept, and a runtime without Ed25519.
 */
export async function verifyRevocationBarrier(
  barrier: RevocationBarrier,
  key: RevocationBarrierVerificationKey,
): Promise<void> {
  if (barrier.signature.byteLength !== REVOCATION_BARRIER_SIGNATURE_BYTES) {
    throw new ProtocolValidationError(
      "revocationBarrier.signature",
      `must contain exactly ${REVOCATION_BARRIER_SIGNATURE_BYTES} bytes`,
    );
  }
  if (key.publicKey.byteLength !== REVOCATION_BARRIER_PUBLIC_KEY_BYTES) {
    throw new ProtocolValidationError(
      "revocationBarrierVerificationKey.publicKey",
      `must contain exactly ${REVOCATION_BARRIER_PUBLIC_KEY_BYTES} bytes`,
    );
  }
  if (barrier.signingKeyId !== key.keyId) {
    throw new ProtocolValidationError(
      "revocationBarrier.signingKeyId",
      "does not match the configured revocation barrier key",
    );
  }
  const preimage = revocationBarrierSigningPreimage(barrier);
  let verified: boolean;
  try {
    const publicKey = await crypto.subtle.importKey(
      "raw",
      key.publicKey.slice(),
      { name: "Ed25519" },
      false,
      ["verify"],
    );
    verified = await crypto.subtle.verify(
      { name: "Ed25519" },
      publicKey,
      barrier.signature.slice(),
      preimage,
    );
  } catch (error) {
    // A runtime without Ed25519 in Web Crypto cannot authenticate a barrier,
    // so it refuses one. Reporting "unverifiable" as "invalid" is the safe
    // direction: the client rebuilds rather than keeps reading.
    throw new ProtocolValidationError(
      "revocationBarrier.signature",
      `could not be verified by this runtime: ${String(error)}`,
    );
  }
  if (!verified) {
    throw new ProtocolValidationError(
      "revocationBarrier.signature",
      "failed authorization-authority verification",
    );
  }
}

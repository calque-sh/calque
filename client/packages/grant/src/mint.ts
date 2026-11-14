import { Buffer } from "node:buffer";
import { randomBytes, sign, type KeyObject } from "node:crypto";

import {
  attributeValue,
  audience,
  GRANT_SIGNING_DOMAIN,
  keyId,
  refuse,
  serializeClaims,
  unsigned,
  type DatasetGrantClaims,
  type GrantAttributeValue,
} from "./claims.js";
import { ed25519 } from "./key.js";

/**
 * Prefixed to a tenant order's payload before signing, so that neither a
 * dataset grant nor a tenant order verifies as the other.
 */
export const TENANT_ORDER_SIGNING_DOMAIN = "calque.tenant-order.v1\0";

/** The longest validity window Calque accepts for a tenant order: 5 minutes. */
export const MAX_TENANT_ORDER_LIFETIME_SECONDS = 5 * 60;

/**
 * `warm` prepares the tenant now, `pin` keeps it warm whatever its recency,
 * `unpin` hands it back to recency, `state` changes nothing, and `evict` drops
 * its artifact, pin and recency so its next open is cold.
 */
export type TenantAction = "warm" | "pin" | "unpin" | "state" | "evict";
const TENANT_ACTIONS: ReadonlySet<unknown> = new Set([
  "warm",
  "pin",
  "unpin",
  "state",
  "evict",
]);

/** Everything a tenant order states except its version, which is always 1. */
export interface TenantOrderClaims {
  readonly keyId: string;
  readonly audience: string;
  /**
   * Unique to this order, which Calque acts on once: 16 to 64 base64url
   * characters. Left out, it is 16 random bytes.
   */
  readonly orderId?: string;
  readonly action: TenantAction;
  /** The tenant, typed as the grant attribute that names it. */
  readonly tenant: GrantAttributeValue;
  readonly issuedAtUnixSeconds: number;
  readonly expiresAtUnixSeconds: number;
}

/**
 * `base64url(payload).base64url(signature)`, both unpadded, where the
 * signature is Ed25519 over the domain, then the payload.
 */
export function signUnder(
  domain: string,
  payload: Uint8Array,
  key: KeyObject,
): string {
  if (key.type !== "private")
    throw new TypeError("a grant is signed with a private key");
  const signature = sign(
    null,
    Buffer.concat([Buffer.from(domain, "utf8"), payload]),
    ed25519(key),
  );
  return `${Buffer.from(payload).toString("base64url")}.${signature.toString("base64url")}`;
}

/**
 * A signed grant for exactly these claims, or a `GrantRefused` for claims
 * Calque's verifier would answer with 401.
 */
export function mintGrant(claims: DatasetGrantClaims, key: KeyObject): string {
  return signUnder(GRANT_SIGNING_DOMAIN, serializeClaims(claims), key);
}

/**
 * The order the backend posts as `{"order": …}` to `POST /v1/tenants`, signed
 * with the grant key. Only the backend holds that key, so a browser cannot
 * warm, pin or inspect a tenant.
 */
export function mintTenantOrder(
  claims: TenantOrderClaims,
  key: KeyObject,
): string {
  const { action } = claims;
  if (!TENANT_ACTIONS.has(action))
    refuse(
      `action ${JSON.stringify(action)} is not warm, pin, unpin, state or evict`,
    );
  const orderId = claims.orderId ?? randomBytes(16).toString("base64url");
  if (!/^[A-Za-z0-9_-]{16,64}$/u.test(orderId))
    refuse("orderId must be 16 to 64 base64url characters");
  const issued = unsigned("issuedAtUnixSeconds", claims.issuedAtUnixSeconds);
  const expires = unsigned("expiresAtUnixSeconds", claims.expiresAtUnixSeconds);
  if (expires <= issued || expires - issued > MAX_TENANT_ORDER_LIFETIME_SECONDS)
    refuse(
      `a tenant order is valid for 1 to ${String(MAX_TENANT_ORDER_LIFETIME_SECONDS)} s`,
    );
  const payload = [
    `"version":1`,
    `"keyId":${JSON.stringify(keyId(claims.keyId))}`,
    `"audience":${JSON.stringify(audience(claims.audience))}`,
    `"orderId":${JSON.stringify(orderId)}`,
    `"action":${JSON.stringify(action)}`,
    `"tenant":${attributeValue("tenant", claims.tenant)}`,
    `"issuedAtUnixSeconds":${String(issued)}`,
    `"expiresAtUnixSeconds":${String(expires)}`,
  ].join(",");
  return signUnder(
    TENANT_ORDER_SIGNING_DOMAIN,
    Buffer.from(`{${payload}}`, "utf8"),
    key,
  );
}

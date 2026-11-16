/**
 * The cookie that names the stored copy a page holds: the id of the key it is
 * sealed under and its decoded SHA-256. Every request that installs an
 * artifact sends the digest as its only hint, so the server plans that cut
 * while it still keeps it, and `renderBootstrap` leaves out the preload of an
 * artifact the page will read from disk. Neither value says anything of the
 * key or of a row. A cookie that is wrong costs a download, never a wrong
 * row: the page reads a copy only after its own checks.
 *
 * The starter reads the cookie before the client library loads, so its
 * parser lives in `bootstrap-start.ts`, which imports nothing.
 */

import {
  STORED_ROWS_COOKIE,
  heldCopy,
  hintedCopy,
  type StoredRowsHint,
} from "./bootstrap-start.js";

export { STORED_ROWS_COOKIE, heldCopy, hintedCopy, type StoredRowsHint };

/** 400 days, the longest a browser keeps a cookie. */
const COOKIE_MAX_AGE_SECONDS = 400 * 24 * 60 * 60;

/** As the store derives it from the raw key. */
const KEY_ID_LABEL = "calque.stored-rows.key-id.v1\0";

/**
 * The id the store keeps beside a copy sealed under `encoded`, a plan's
 * `storedRowsKey`, or undefined for anything that is not 32 bytes of
 * unpadded base64url.
 */
export async function storedRowsKeyId(
  encoded: string,
): Promise<string | undefined> {
  if (!/^[A-Za-z0-9_-]{43}$/u.test(encoded)) return undefined;
  const binary = atob(`${encoded.replaceAll("-", "+").replaceAll("_", "/")}=`);
  const label = new TextEncoder().encode(KEY_ID_LABEL);
  const named = new Uint8Array(label.byteLength + binary.length);
  named.set(label);
  for (let index = 0; index < binary.length; index += 1)
    named[label.byteLength + index] = binary.charCodeAt(index);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", named));
  return Array.from(digest.subarray(0, 16), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

/**
 * Names `copy` as the one the page holds, or, with none, says it holds none
 * the next load would read. `Lax`, so a page reached from a link on another
 * site still sends it.
 */
export function hintStoredRows(copy: StoredRowsHint | undefined): void {
  if (typeof document === "undefined") return;
  const secure = globalThis.location.protocol === "https:" ? "; Secure" : "";
  const value = copy ? `${copy.keyId}.${copy.decodedSha256}` : "";
  const age = copy ? COOKIE_MAX_AGE_SECONDS : 0;
  document.cookie = `${STORED_ROWS_COOKIE}=${value}; Path=/; Max-Age=${String(age)}; SameSite=Lax${secure}`;
}

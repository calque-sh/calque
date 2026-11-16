import { importStoredRowsKey } from "@calque/worker/columnar";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  heldCopy,
  hintedCopy,
  hintStoredRows,
  storedRowsKeyId,
} from "./stored-rows-hint.js";

const ID = "0123456789abcdef0123456789abcdef";
const DIGEST = "89abcdef".repeat(8);
const VALUE = `${ID}.${DIGEST}`;

const KEYS = [
  "c3RvcmVkLXJvd3Mta2V5LWZpeHR1cmUtMzItYnl0ZXM",
  "_-_-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "-------------------------------------------",
];

describe("the cookie that names a page's stored copy", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("names the key id the store keeps beside the copy", async () => {
    for (const key of KEYS)
      expect(await storedRowsKeyId(key)).toBe(
        (await importStoredRowsKey(key)).keyId,
      );
    expect(await storedRowsKeyId(`${KEYS[0] ?? ""}=`)).toBeUndefined();
    expect(await storedRowsKeyId("short")).toBeUndefined();
  });

  it("is read only from its own name, holding a key id and a decoded SHA-256", () => {
    const copy = { keyId: ID, decodedSha256: DIGEST };
    expect(hintedCopy(`a=1;calque-stored-rows=${VALUE}; b=2`)).toEqual(copy);
    expect(
      hintedCopy(`calque-stored-rows=${ID}; calque-stored-rows=${VALUE}`),
    ).toEqual(copy);
    for (const cookie of [
      `calque-stored-rows=${VALUE}x`,
      `calque-stored-rows=${ID}`,
      `calque-stored-rows=${ID}.${DIGEST.toUpperCase()}`,
      `calque-stored-rows=${ID}.sha256:${DIGEST}`,
      `xcalque-stored-rows=${VALUE}`,
      "calque-stored-rows=",
      undefined,
    ])
      expect(hintedCopy(cookie)).toBeUndefined();
  });

  it("is the page's own cookie, and none outside a page", () => {
    expect(heldCopy()).toBeUndefined();
    vi.stubGlobal("document", { cookie: `calque-stored-rows=${VALUE}` });
    expect(heldCopy()).toEqual({ keyId: ID, decodedSha256: DIGEST });
  });

  it("is set for a copy and expired for none, sent on a link from another site", () => {
    const written: string[] = [];
    vi.stubGlobal("location", { protocol: "https:" });
    vi.stubGlobal("document", {
      set cookie(value: string) {
        written.push(value);
      },
    });
    hintStoredRows({ keyId: ID, decodedSha256: DIGEST });
    hintStoredRows(undefined);
    expect(written).toEqual([
      `calque-stored-rows=${VALUE}; Path=/; Max-Age=34560000; SameSite=Lax; Secure`,
      "calque-stored-rows=; Path=/; Max-Age=0; SameSite=Lax; Secure",
    ]);
  });
});

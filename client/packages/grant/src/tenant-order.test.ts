import { Buffer } from "node:buffer";
import { createPublicKey, verify } from "node:crypto";
import { describe, expect, it } from "vitest";

import { GrantRefused } from "./claims.js";
import { loadSigningKey } from "./key.js";
import {
  MAX_TENANT_ORDER_LIFETIME_SECONDS,
  mintTenantOrder,
  type TenantOrderClaims,
} from "./mint.js";
import { KEY_FILE, KEY_ID, vector } from "./test-vectors.js";

const key = loadSigningKey(vector(KEY_FILE));
const order: TenantOrderClaims = {
  keyId: KEY_ID,
  audience: "tenants.example",
  orderId: "order-0123456789ab",
  action: "warm",
  tenant: { int8: "9007199254740993" },
  issuedAtUnixSeconds: 1_800_000_000,
  expiresAtUnixSeconds: 1_800_000_300,
};

const refused = (claims: TenantOrderClaims, reason: RegExp): void =>
  expect(() => mintTenantOrder(claims, key)).toThrow(reason);

describe("mintTenantOrder", () => {
  it("signs the exact payload under its own domain, never the grant's", () => {
    const [payload, signature] = mintTenantOrder(order, key)
      .split(".")
      .map((half) => Buffer.from(half, "base64url"));
    expect(payload).toBeDefined();
    expect(signature).toBeDefined();
    expect(payload?.toString("utf8")).toBe(
      '{"version":1,"keyId":"grant-vectors-key","audience":"tenants.example","orderId":"order-0123456789ab","action":"warm","tenant":{"int8":"9007199254740993"},"issuedAtUnixSeconds":1800000000,"expiresAtUnixSeconds":1800000300}',
    );
    const publicKey = createPublicKey(key);
    const under = (domain: string): boolean =>
      verify(
        null,
        Buffer.concat([Buffer.from(domain, "utf8"), payload as Buffer]),
        publicKey,
        signature as Buffer,
      );
    expect(under("calque.tenant-order.v1\0")).toBe(true);
    expect(under("calque.dataset-grant.v1\0")).toBe(false);
    expect(under("")).toBe(false);
  });

  it("is valid for 1 to 300 seconds", () => {
    expect(MAX_TENANT_ORDER_LIFETIME_SECONDS).toBe(300);
    mintTenantOrder({ ...order, expiresAtUnixSeconds: 1_800_000_001 }, key);
    mintTenantOrder({ ...order, expiresAtUnixSeconds: 1_800_000_300 }, key);
    for (const expires of [1_800_000_301, 1_800_000_000, 1_799_999_999])
      refused(
        { ...order, expiresAtUnixSeconds: expires },
        /valid for 1 to 300 s/u,
      );
  });

  it("refuses an action, tenant, order id, key id or audience Calque would", () => {
    refused(
      { ...order, action: "delete" as unknown as "warm" },
      /is not warm, pin, unpin, state or evict/u,
    );
    refused({ ...order, tenant: { int8: "1.5" } }, /signed 64-bit/u);
    refused({ ...order, orderId: "short" }, /16 to 64 base64url/u);
    refused({ ...order, orderId: "a".repeat(65) }, /16 to 64 base64url/u);
    refused({ ...order, keyId: "a.b" }, /other than "\."/u);
    refused({ ...order, audience: "" }, /audience must be/u);
    expect(() =>
      mintTenantOrder({ ...order, tenant: { int8: "1.5" } }, key),
    ).toThrow(GrantRefused);
    for (const action of ["warm", "pin", "unpin", "state", "evict"] as const)
      mintTenantOrder({ ...order, action }, key);
  });

  it("gives an order without an id a random 16-byte one", () => {
    const unnamed: TenantOrderClaims = { ...order };
    delete (unnamed as { orderId?: string }).orderId;
    const ids = new Set(
      [0, 1, 2].map((): string => {
        const [minted] = mintTenantOrder(unnamed, key).split(".");
        const claims = JSON.parse(
          Buffer.from(minted ?? "", "base64url").toString("utf8"),
        ) as { orderId: string };
        expect(claims.orderId).toMatch(/^[A-Za-z0-9_-]{22}$/u);
        return claims.orderId;
      }),
    );
    expect(ids.size).toBe(3);
  });
});

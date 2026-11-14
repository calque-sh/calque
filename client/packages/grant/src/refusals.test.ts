import { describe, expect, it } from "vitest";

import {
  GrantRefused,
  MAX_GRANT_LIFETIME_SECONDS,
  type DatasetGrantClaims,
  type GrantAttributeValue,
} from "./claims.js";
import { loadSigningKey } from "./key.js";
import { mintGrant } from "./mint.js";
import { claimsOf, KEY_FILE, text, vector } from "./test-vectors.js";

const key = loadSigningKey(vector(KEY_FILE));
const base: DatasetGrantClaims = claimsOf(vector("read-only.payload.json"));

function refuses(claims: DatasetGrantClaims, reason: RegExp): void {
  let thrown: unknown;
  try {
    mintGrant(claims, key);
  } catch (error) {
    thrown = error;
  }
  expect(thrown).toBeInstanceOf(GrantRefused);
  expect((thrown as GrantRefused).message).toMatch(reason);
}

describe("the validity window", () => {
  const issued = base.issuedAtUnixSeconds;
  const lasting = (seconds: number): DatasetGrantClaims => ({
    ...base,
    expiresAtUnixSeconds: issued + seconds,
  });

  it("is the verifier's, to the second", () => {
    expect(MAX_GRANT_LIFETIME_SECONDS).toBe(900);
    mintGrant(lasting(900), key);
    mintGrant(lasting(1), key);
    refuses(lasting(901), /validity window is 901 s/u);
    refuses(lasting(0), /must be later than/u);
    refuses(lasting(-1), /must be later than/u);
  });

  it("takes whole seconds from 0 to the largest safe integer", () => {
    for (const bad of [-1, 1.5, Number.NaN, 2 ** 53, "1"])
      refuses(
        { ...base, issuedAtUnixSeconds: bad as number },
        /issuedAtUnixSeconds must be a whole number/u,
      );
  });
});

describe("a command capability", () => {
  const capability = (
    collection: string,
    operations: readonly string[],
  ): DatasetGrantClaims =>
    ({ ...base, commands: [{ collection, operations }] }) as DatasetGrantClaims;

  it("is exact names and the three operations, once each", () => {
    mintGrant(capability("c".repeat(128), ["insert", "update", "delete"]), key);
    for (const [claims, reason] of [
      [capability("", ["update"]), /printable ASCII/u],
      [capability("c".repeat(129), ["update"]), /printable ASCII/u],
      [capability("my tasks", ["update"]), /printable ASCII/u],
      [capability("tâches", ["update"]), /printable ASCII/u],
      [capability("tasks", []), /names no operation/u],
      [capability("tasks", ["update", "update"]), /repeats update/u],
      [capability("tasks", ["*"]), /names "\*"/u],
      [capability("tasks", ["Update"]), /names "Update"/u],
      [
        {
          ...base,
          commands: [
            { collection: "tasks", operations: ["insert"] },
            { collection: "tasks", operations: ["delete"] },
          ],
        } as DatasetGrantClaims,
        /named twice/u,
      ],
      [
        {
          ...base,
          commands: [{ collection: "tasks", operations: ["insert"], extra: 1 }],
        } as unknown as DatasetGrantClaims,
        /exactly a collection and its operations/u,
      ],
      [
        { ...base, commands: "tasks" } as unknown as DatasetGrantClaims,
        /commands must be a list/u,
      ],
    ] as const)
      refuses(claims, reason);
  });

  it("is at most 64 collections, and an empty list is a read-only grant", () => {
    const many = (count: number): DatasetGrantClaims => ({
      ...base,
      commands: Array.from({ length: count }, (_, index) => ({
        collection: `c${String(index)}`,
        operations: ["insert"] as const,
      })),
    });
    mintGrant(many(64), key);
    refuses(many(65), /at most 64 command capabilities/u);
    expect(mintGrant({ ...base, commands: [] }, key)).toBe(
      text("read-only.token"),
    );
  });
});

describe("attributes, names and the key id", () => {
  const attribute = (value: unknown, name = "tenant"): DatasetGrantClaims =>
    ({ ...base, attributes: { [name]: value } }) as DatasetGrantClaims;

  it("refuse a value outside its type", () => {
    for (const [claims, reason] of [
      [attribute({ uuid: "ab" }), /32 hex digits/u],
      [
        attribute({ uuid: "0a1b2c3d-4e5f-6071-8293-04a5b6c7d8e9" }),
        /32 hex digits/u,
      ],
      [attribute({ int8: "9223372036854775808" }), /signed 64-bit/u],
      [attribute({ int8: "-9223372036854775809" }), /signed 64-bit/u],
      [attribute({ int8: " 1" }), /signed 64-bit/u],
      [attribute({ int8: 1 }), /signed 64-bit/u],
      [attribute({ bytes: "odd" }), /even number of digits/u],
      [attribute({ int2: 32_768 }), /-32768 to 32767/u],
      [attribute({ int4: 2_147_483_648 }), /whole number/u],
      [attribute({ int4: 1.5 }), /whole number/u],
      [attribute({ bool: "true" }), /true or false/u],
      [attribute({ text: "x" }), /unknown type text/u],
      [attribute({ bool: true, int4: 1 }), /exactly one type/u],
      [attribute("x"), /must be an object naming one type/u],
      [
        { ...base, attributes: [] } as unknown as DatasetGrantClaims,
        /attributes must be an object/u,
      ],
    ] as const)
      refuses(claims, reason);
    mintGrant(attribute({ int8: "9223372036854775807" }), key);
    mintGrant(attribute({ int8: "-9223372036854775808" }), key);
    mintGrant(attribute({ int4: -2_147_483_648 }), key);
  });

  it("refuse a name that is empty, long, a control character or ill-formed", () => {
    for (const [claims, reason] of [
      [attribute({ bool: true }, ""), /attribute name must be/u],
      [attribute({ bool: true }, "a\u0007b"), /attribute name must be/u],
      [attribute({ bool: true }, "n".repeat(513)), /attribute name must be/u],
      [{ ...base, subject: "" }, /subject must be/u],
      [{ ...base, subject: "member\n1" }, /subject must be/u],
      [{ ...base, subject: "member-\ud800" }, /well-formed Unicode/u],
      [{ ...base, audience: "" }, /audience must be/u],
      [{ ...base, audience: "a".repeat(257) }, /audience must be/u],
    ] as const)
      refuses(claims, reason);
    mintGrant({ ...base, subject: "s".repeat(512) }, key);
    mintGrant({ ...base, audience: "a".repeat(256) }, key);
  });

  it('refuse a key id that is empty, over 128 bytes, or holds "." or a non-graphic character', () => {
    for (const id of ["", "grant.vectors", "k".repeat(129), "a b", "clé"])
      refuses({ ...base, keyId: id }, /other than "\."/u);
    mintGrant({ ...base, keyId: "k".repeat(128) }, key);
  });

  it("refuse an unknown claim, a generation beyond 2^53 - 1 and more than 64 attributes", () => {
    refuses(
      { ...base, version: 1 } as DatasetGrantClaims,
      /version is not a grant claim/u,
    );
    refuses(
      { ...base, authorizationGeneration: 2 ** 53 },
      /whole number from 0/u,
    );
    const attributes = (count: number, value: GrantAttributeValue) =>
      Object.fromEntries(
        Array.from({ length: count }, (_, index) => [
          `a${String(index)}`,
          value,
        ]),
      );
    mintGrant({ ...base, attributes: attributes(64, { bool: true }) }, key);
    refuses(
      { ...base, attributes: attributes(65, { bool: true }) },
      /at most 64 attributes/u,
    );
  });

  it("refuse a payload over the verifier's 4 KiB with every field valid", () => {
    const attributes = Object.fromEntries(
      Array.from({ length: 40 }, (_, index) => [
        `a${String(index)}`,
        { bytes: "ab".repeat(48) },
      ]),
    );
    refuses({ ...base, attributes }, /Calque refuses one over 4096/u);
  });
});

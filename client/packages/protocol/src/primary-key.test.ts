import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import {
  CellValueSchema,
  ColumnValueSchema,
  PrimaryKeySchema,
} from "./gen/change_stream_pb.js";
import {
  decodePrimaryKey,
  encodePrimaryKey,
  PRIMARY_KEY_CODEC_VERSION,
  PRIMARY_KEY_MAX_BYTES,
} from "./primary-key.js";

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}

function compositeKey() {
  return create(PrimaryKeySchema, {
    codecVersion: PRIMARY_KEY_CODEC_VERSION,
    parts: [
      create(ColumnValueSchema, {
        columnId: 7,
        value: create(CellValueSchema, {
          kind: { case: "int64Value", value: -42n },
        }),
      }),
      create(ColumnValueSchema, {
        columnId: 2,
        value: create(CellValueSchema, {
          kind: { case: "boolValue", value: true },
        }),
      }),
      create(ColumnValueSchema, {
        columnId: 5,
        value: create(CellValueSchema, {
          kind: { case: "blobValue", value: Uint8Array.of(0xca, 0xfe) },
        }),
      }),
      create(ColumnValueSchema, {
        columnId: 9,
        value: create(CellValueSchema, {
          kind: { case: "uuid16", value: new Uint8Array(16).fill(0xab) },
        }),
      }),
    ],
  });
}

describe("primary key codec", () => {
  it("matches the Rust composite-key golden vector", () => {
    const encoded = encodePrimaryKey(compositeKey());
    expect(hex(encoded)).toBe(
      "0801120608071202105312060802120238011208080512043202cafe1216080912125210abababababababababababababababab",
    );
    expect(decodePrimaryKey(encoded)).toEqual(compositeKey());
  });

  it("rejects unknown fields, malformed bytes, and oversized keys", () => {
    const canonical = encodePrimaryKey(compositeKey());
    const unknown = new Uint8Array(canonical.byteLength + 3);
    unknown.set(canonical);
    unknown.set([0x98, 0x06, 0x01], canonical.byteLength);
    expect(() => decodePrimaryKey(unknown)).toThrow(/not canonical/u);
    expect(() => decodePrimaryKey(Uint8Array.of(0x08))).toThrow(/malformed/u);
    expect(() =>
      decodePrimaryKey(new Uint8Array(PRIMARY_KEY_MAX_BYTES + 1)),
    ).toThrow(/exceeding limit/u);
  });

  it("matches the Rust text-key golden vector", () => {
    const key = create(PrimaryKeySchema, {
      codecVersion: PRIMARY_KEY_CODEC_VERSION,
      parts: [
        create(ColumnValueSchema, {
          columnId: 7,
          value: create(CellValueSchema, {
            kind: { case: "int64Value", value: -42n },
          }),
        }),
        create(ColumnValueSchema, {
          columnId: 3,
          value: create(CellValueSchema, {
            kind: { case: "textValue", value: "é-1" },
          }),
        }),
      ],
    });
    const encoded = encodePrimaryKey(key);
    expect(hex(encoded)).toBe("08011206080712021053120a080312062a04c3a92d31");
    expect(decodePrimaryKey(encoded)).toEqual(key);
  });

  it("rejects a text part that is not well-formed Unicode", () => {
    for (const text of ["\ud800", "a\udc00b"]) {
      const key = create(PrimaryKeySchema, {
        codecVersion: PRIMARY_KEY_CODEC_VERSION,
        parts: [
          create(ColumnValueSchema, {
            columnId: 1,
            value: create(CellValueSchema, {
              kind: { case: "textValue", value: text },
            }),
          }),
        ],
      });
      expect(() => encodePrimaryKey(key)).toThrow(/well-formed Unicode/u);
    }
  });

  it("accepts exactly the five v1 key value kinds", () => {
    const cases = [
      { case: "int64Value", value: -1n } as const,
      { case: "boolValue", value: false } as const,
      { case: "textValue", value: "" } as const,
      { case: "blobValue", value: Uint8Array.of(1, 2) } as const,
      { case: "uuid16", value: new Uint8Array(16) } as const,
    ];
    for (const [index, kind] of cases.entries()) {
      const key = create(PrimaryKeySchema, {
        codecVersion: PRIMARY_KEY_CODEC_VERSION,
        parts: [
          create(ColumnValueSchema, {
            columnId: index + 1,
            value: create(CellValueSchema, { kind }),
          }),
        ],
      });
      expect(() => encodePrimaryKey(key)).not.toThrow();
    }
  });

  it("rejects every non-v1 key value kind", () => {
    const unsupported = [
      { case: "nullValue", value: 0 } as const,
      { case: "uint64Value", value: 1n } as const,
      { case: "float64Value", value: 1 } as const,
      { case: "decimalText", value: "1.0" } as const,
      { case: "timestampUnixMicros", value: 1n } as const,
      { case: "jsonText", value: "{}" } as const,
      { case: "dateIso8601", value: "2026-07-14" } as const,
      { case: "timeIso8601", value: "12:00:00" } as const,
      { case: "intervalIso8601", value: "PT1S" } as const,
    ];
    for (const kind of unsupported) {
      const key = create(PrimaryKeySchema, {
        codecVersion: PRIMARY_KEY_CODEC_VERSION,
        parts: [
          create(ColumnValueSchema, {
            columnId: 1,
            value: create(CellValueSchema, { kind }),
          }),
        ],
      });
      expect(() => encodePrimaryKey(key)).toThrow(
        /must not be null|not a v1 key type/u,
      );
    }
  });

  it("rejects duplicate columns and malformed UUID parts", () => {
    const key = compositeKey();
    const first = key.parts[0];
    if (first === undefined) throw new Error("fixture part missing");
    first.value = create(CellValueSchema, {
      kind: { case: "boolValue", value: true },
    });
    const second = key.parts[1];
    if (second === undefined) throw new Error("fixture part missing");
    second.columnId = first.columnId;
    expect(() => encodePrimaryKey(key)).toThrow(/must be unique/u);

    second.columnId = 2;
    second.value = create(CellValueSchema, {
      kind: { case: "uuid16", value: new Uint8Array(15) },
    });
    expect(() => encodePrimaryKey(key)).toThrow(/exactly 16 bytes/u);
  });
});

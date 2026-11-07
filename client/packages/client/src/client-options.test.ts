import { describe, expect, it, vi } from "vitest";
import { CLIENT_METADATA_RESERVE_BYTES } from "./limits.js";

import {
  CLIENT_CONSTRUCTION_LIMITS,
  createGeneratedClientSchema,
  sealInternalSyncClientOptions,
  sealSyncClientOptions,
} from "./client-options.js";
import type { SyncClientOptions } from "./client.js";
import type {
  GeneratedClientCollectionDescriptor,
  GeneratedClientSchemaDescriptor,
} from "./generated-schema.js";
import { resolveClientSchema } from "./schema-registry.js";

function options(
  descriptor: GeneratedClientSchemaDescriptor = {
    collections: [],
  },
) {
  const auth = () => ({ bearerToken: "token" });
  const worker = () => undefined as never;
  const fetcher = vi.fn<typeof fetch>();
  const schema = createGeneratedClientSchema(descriptor);
  return {
    value: {
      endpoint: "https://sync.test/root",
      auth,
      schema,
      memoryBudgetBytes: 32 * 1024 * 1024,
      deviceId: "device-a",
      fetch: fetcher,
    } satisfies SyncClientOptions,
    auth,
    worker,
    fetcher,
  };
}

function seal(value: unknown): GeneratedClientSchemaDescriptor {
  return resolveClientSchema(
    createGeneratedClientSchema(value as GeneratedClientSchemaDescriptor),
  ).descriptor;
}

function collection(
  id: string,
  columnCount = 1,
  primaryKeyCount = 1,
): GeneratedClientCollectionDescriptor {
  const columns = Array.from({ length: columnCount }, (_, index) => ({
    name: `column_${index + 1}`,
    kind: "int64" as const,
  }));
  return {
    id,
    tableId: `public.${id}`,
    columns,
    primaryKey: columns.slice(0, primaryKeyCount).map(({ name }) => name),
  };
}

describe("SyncClient construction sealing", () => {
  it("binds the default browser fetch to its platform receiver", async () => {
    const original = Object.getOwnPropertyDescriptor(globalThis, "fetch");
    const platformFetch = vi.fn(function (this: unknown) {
      if (this !== globalThis) throw new TypeError("Illegal invocation");
      return Promise.resolve(new Response(null, { status: 204 }));
    });
    try {
      Object.defineProperty(globalThis, "fetch", {
        configurable: true,
        writable: true,
        value: platformFetch,
      });
      const source = options().value;
      const sealed = sealSyncClientOptions({
        endpoint: source.endpoint,
        auth: source.auth,
        schema: source.schema,
        memoryBudgetBytes: source.memoryBudgetBytes,
        deviceId: source.deviceId,
      });

      const response = await sealed.fetch("https://sync.test/readyz");

      expect(response.status).toBe(204);
      expect(platformFetch).toHaveBeenCalledOnce();
    } finally {
      if (original) Object.defineProperty(globalThis, "fetch", original);
      else Reflect.deleteProperty(globalThis, "fetch");
    }
  });

  it("accepts only an exact opaque generated schema capability", () => {
    const descriptor = {
      collections: [collection("preferences")],
    };
    const schema = createGeneratedClientSchema(descriptor);
    expect(Reflect.ownKeys(schema)).toEqual([]);
    expect(Object.getPrototypeOf(schema)).toBeNull();
    expect(Object.isFrozen(schema)).toBe(true);

    descriptor.collections[0] = collection("changed");
    expect(
      sealSyncClientOptions({ ...options().value, schema }).schema,
    ).toEqual({
      collections: [collection("preferences")],
    });

    for (const forged of [
      { collections: [] },
      Object.create(schema),
      new Proxy(schema, {}),
      structuredClone(schema),
      JSON.parse(JSON.stringify(schema)),
    ]) {
      expect(() =>
        sealSyncClientOptions({
          ...options().value,
          schema: forged as never,
        }),
      ).toThrowError(
        /^SyncClientOptions schema is invalid: pass the `schema` that calque.generated.ts exports/u,
      );
    }
  });

  it("captures every top-level data property once and owns one frozen schema", () => {
    const callerColumn = { name: "column_1", kind: "int64" as const };
    const callerWritable = ["column_1"];
    const callerCollection = {
      id: "preferences",
      tableId: "public.preferences",
      columns: [callerColumn],
      primaryKey: ["column_1"],
      writes: {
        operations: ["update" as const],
        writableColumns: callerWritable,
      },
    };
    const callerCollections: GeneratedClientCollectionDescriptor[] = [
      callerCollection,
    ];
    const source = options({
      collections: callerCollections,
    });
    const descriptorReads = new Map<PropertyKey, number>();
    let ownKeyReads = 0;
    let ordinaryReads = 0;
    const hostile = new Proxy(source.value, {
      get: () => {
        ordinaryReads += 1;
        throw new Error("ordinary option reads are forbidden");
      },
      ownKeys: (target) => {
        ownKeyReads += 1;
        return Reflect.ownKeys(target);
      },
      getOwnPropertyDescriptor: (target, key) => {
        descriptorReads.set(key, (descriptorReads.get(key) ?? 0) + 1);
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });

    const sealed = sealSyncClientOptions(hostile);
    expect(ordinaryReads).toBe(0);
    expect(ownKeyReads).toBe(1);
    expect([...descriptorReads.values()]).toEqual(Array(6).fill(1));
    expect(sealed.auth).toBe(source.auth);
    expect(sealed.fetch).toBe(source.fetcher);
    expect(sealed.endpointUrl.href).toBe("https://sync.test/root/");
    expect(sealed.deviceId).toBe("device-a");
    expect(sealed.memoryBudgetBytes).toBe(32 * 1024 * 1024);
    expect(sealed.clientHeapReserveBytes).toBe(4 * 1024 * 1024);
    expect(sealed.schemaRetainedBytes).toBeGreaterThan(0);

    callerCollection.id = "changed";
    callerColumn.name = "changed";
    callerWritable.push("later");
    callerCollections.push(collection("later"));
    expect(sealed.schema).toEqual({
      collections: [
        {
          ...collection("preferences"),
          writes: { operations: ["update"], writableColumns: ["column_1"] },
        },
      ],
    });
    const sealedCollection = sealed.schema.collections[0];
    expect(Object.isFrozen(sealed)).toBe(true);
    expect(Object.isFrozen(sealed.schema)).toBe(true);
    expect(Object.isFrozen(sealed.schema.collections)).toBe(true);
    expect(Object.isFrozen(sealedCollection)).toBe(true);
    expect(Object.isFrozen(sealedCollection?.columns)).toBe(true);
    expect(Object.isFrozen(sealedCollection?.columns[0])).toBe(true);
    expect(Object.isFrozen(sealedCollection?.primaryKey)).toBe(true);
    expect(Object.isFrozen(sealedCollection?.writes)).toBe(true);
    expect(Object.isFrozen(sealedCollection?.writes?.operations)).toBe(true);
    expect(Object.isFrozen(sealedCollection?.writes?.writableColumns)).toBe(
      true,
    );
  });

  it("rejects top-level accessors and unknown properties without invoking them", () => {
    const source = options().value as SyncClientOptions &
      Record<string, unknown>;
    const endpointGetter = vi.fn(() => "https://secret.invalid");
    const accessor = Object.defineProperty({ ...source }, "endpoint", {
      configurable: true,
      enumerable: true,
      get: endpointGetter,
    });
    expect(() =>
      sealSyncClientOptions(accessor as SyncClientOptions),
    ).toThrowError(
      new TypeError(
        "SyncClientOptions endpoint must be a data property, not a getter or setter",
      ),
    );
    expect(endpointGetter).not.toHaveBeenCalled();

    const withoutSchema: Record<string, unknown> = { ...source };
    delete withoutSchema.schema;
    expect(() =>
      sealSyncClientOptions(withoutSchema as unknown as SyncClientOptions),
    ).toThrowError(
      new TypeError('SyncClientOptions is missing required key "schema"'),
    );
    expect(() =>
      sealSyncClientOptions({
        ...source,
        [Symbol("tag")]: true,
      } as SyncClientOptions),
    ).toThrowError(
      new TypeError('SyncClientOptions has unknown key "Symbol(tag)"'),
    );
    source.unknown = "value";
    expect(() => sealSyncClientOptions(source)).toThrowError(
      new TypeError('SyncClientOptions has unknown key "unknown"'),
    );
  });

  it("defaults the memory budget and keeps one device id in localStorage", () => {
    const stored = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => stored.get(key) ?? null,
      setItem: (key: string, value: string) => stored.set(key, value),
    });
    try {
      const { endpoint, auth, schema, fetch } = options().value;
      const source = { endpoint, auth, schema, fetch };
      const first = sealSyncClientOptions(source);
      const second = sealSyncClientOptions(source);
      expect(first.memoryBudgetBytes).toBe(1024 * 1024 * 1024);
      expect(first.deviceId).toMatch(/^[0-9a-f-]{36}$/u);
      expect(second.deviceId).toBe(first.deviceId);
      stored.set("calque-device-id", "not a device id");
      expect(sealSyncClientOptions(source).deviceId).not.toBe(
        "not a device id",
      );
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("takes the memory budget of the page's calque-bootstrap element when its options name none", () => {
    const { endpoint, auth, schema, fetch, deviceId } = options().value;
    const source = { endpoint, auth, schema, fetch, deviceId };
    const page = (text: string) =>
      vi.stubGlobal("document", {
        getElementById: (id: string) =>
          id === "calque-bootstrap" ? { textContent: text } : null,
      });
    try {
      page('{"requiredTables":[],"memoryBudgetBytes":67108864}');
      expect(sealSyncClientOptions(source).memoryBudgetBytes).toBe(
        64 * 1024 * 1024,
      );
      expect(
        sealSyncClientOptions({ ...source, memoryBudgetBytes: 32 * 1024 ** 2 })
          .memoryBudgetBytes,
      ).toBe(32 * 1024 * 1024);
      for (const text of ['{"memoryBudgetBytes":"67108864"}', "{", "null"]) {
        page(text);
        expect(sealSyncClientOptions(source).memoryBudgetBytes).toBe(
          1024 * 1024 * 1024,
        );
      }
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("takes the heap reserve the page's starter asked for only with the page's budget", () => {
    const { endpoint, auth, schema, fetch, deviceId } = options().value;
    const source = { endpoint, auth, schema, fetch, deviceId };
    const reserve = 6 * 1024 * 1024;
    const page = (elements: Readonly<Record<string, unknown>>) =>
      vi.stubGlobal("document", {
        getElementById: (id: string) =>
          id in elements ? { textContent: JSON.stringify(elements[id]) } : null,
      });
    try {
      page({
        "calque-bootstrap": {
          requiredTables: [],
          memoryBudgetBytes: 64 * 1024 * 1024,
          clientHeapReserveBytes: reserve,
        },
      });
      expect(sealSyncClientOptions(source)).toMatchObject({
        memoryBudgetBytes: 64 * 1024 * 1024,
        clientHeapReserveBytes: reserve,
      });
      expect(
        sealSyncClientOptions({ ...source, memoryBudgetBytes: 32 * 1024 ** 2 })
          .clientHeapReserveBytes,
      ).toBe(4 * 1024 * 1024);

      // A plan the backend rendered is what the starter sends first.
      page({
        "calque-plan": {
          memoryBudgetBytes: 128 * 1024 * 1024,
          clientHeapReserveBytes: reserve,
        },
        "calque-bootstrap": {
          requiredTables: [],
          memoryBudgetBytes: 64 * 1024 * 1024,
        },
      });
      expect(sealSyncClientOptions(source)).toMatchObject({
        memoryBudgetBytes: 128 * 1024 * 1024,
        clientHeapReserveBytes: reserve,
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("uses captured dense collection extents without getters or ordinary proxy reads", () => {
    const getter = vi.fn(() => collection("secret"));
    const accessorCollections: unknown[] = [];
    Object.defineProperty(accessorCollections, 0, {
      configurable: true,
      enumerable: true,
      get: getter,
    });
    accessorCollections.length = 1;
    expect(() => seal({ collections: accessorCollections })).toThrowError(
      new TypeError("ClientSchema collections are invalid"),
    );
    expect(getter).not.toHaveBeenCalled();

    const sparse = new Array(1);
    expect(() => seal({ collections: sparse })).toThrowError(
      new TypeError("ClientSchema collections are invalid"),
    );

    const extra = [collection("one")] as typeof sparse & { extra?: boolean };
    extra.extra = true;
    expect(() => seal({ collections: extra })).toThrowError(
      new TypeError("ClientSchema collections are invalid"),
    );

    let arrayGets = 0;
    let collectionGets = 0;
    const descriptorReads = new Map<PropertyKey, number>();
    const collectionDescriptorReads = new Map<PropertyKey, number>();
    const proxiedCollection = new Proxy(collection("one"), {
      get: () => {
        collectionGets += 1;
        throw new Error("collection values must be descriptor-captured");
      },
      getOwnPropertyDescriptor: (target, key) => {
        collectionDescriptorReads.set(
          key,
          (collectionDescriptorReads.get(key) ?? 0) + 1,
        );
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });
    const proxiedCollections = new Proxy([proxiedCollection], {
      get: () => {
        arrayGets += 1;
        throw new Error("array values must be descriptor-captured");
      },
      getOwnPropertyDescriptor: (target, key) => {
        descriptorReads.set(key, (descriptorReads.get(key) ?? 0) + 1);
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });
    expect(seal({ collections: proxiedCollections }).collections).toHaveLength(
      1,
    );
    expect(arrayGets).toBe(0);
    expect(collectionGets).toBe(0);
    expect(descriptorReads.get("length")).toBe(1);
    expect(descriptorReads.get("0")).toBe(1);
    expect([...collectionDescriptorReads.values()]).toEqual([1, 1, 1, 1]);
  });

  it("enforces every collection limit at the exact and one-over boundary", () => {
    const collections = Array.from(
      { length: CLIENT_CONSTRUCTION_LIMITS.collections },
      (_, index) => collection(`collection_${index}`),
    );
    expect(seal({ collections }).collections).toHaveLength(
      CLIENT_CONSTRUCTION_LIMITS.collections,
    );
    expect(() =>
      seal({
        collections: [...collections, collection("one_over")],
      }),
    ).toThrowError(new TypeError("ClientSchema collections are invalid"));
    const oversizedOwnKeys = vi.fn(() => {
      throw new Error("oversized arrays must fail before key enumeration");
    });
    const oversizedProxy = new Proxy(
      new Array(CLIENT_CONSTRUCTION_LIMITS.collections + 1),
      { ownKeys: oversizedOwnKeys },
    );
    expect(() => seal({ collections: oversizedProxy })).toThrowError(
      new TypeError("ClientSchema collections are invalid"),
    );
    expect(oversizedOwnKeys).not.toHaveBeenCalled();

    const { columnsPerCollection, primaryKeyColumns } =
      CLIENT_CONSTRUCTION_LIMITS;
    expect(
      seal({
        collections: [collection("wide", columnsPerCollection)],
      }).collections[0]?.columns,
    ).toHaveLength(columnsPerCollection);
    expect(() =>
      seal({
        collections: [collection("wide", columnsPerCollection + 1)],
      }),
    ).toThrowError(new TypeError("ClientSchema collection is invalid"));

    expect(
      seal({
        collections: [
          collection("keyed", primaryKeyColumns, primaryKeyColumns),
        ],
      }).collections[0]?.primaryKey,
    ).toHaveLength(primaryKeyColumns);
    expect(() =>
      seal({
        collections: [
          collection("keyed", primaryKeyColumns + 1, primaryKeyColumns + 1),
        ],
      }),
    ).toThrowError(new TypeError("ClientSchema collection is invalid"));
  });

  it("enforces identifier and aggregate schema byte limits exactly", () => {
    const exactIdentifier = "n".repeat(
      CLIENT_CONSTRUCTION_LIMITS.identifierCodeUnits,
    );
    expect(
      seal({
        collections: [{ ...collection("one"), id: exactIdentifier }],
      }).collections[0]?.id,
    ).toBe(exactIdentifier);
    expect(() =>
      seal({
        collections: [{ ...collection("one"), id: `${exactIdentifier}n` }],
      }),
    ).toThrowError(new TypeError("ClientSchema collection is invalid"));

    // Every identifier is charged once per occurrence. Sixteen collections of
    // 1,024 maximal identifiers each -- id, tableId, 1,021 column names and
    // one primary-key name -- add up to the aggregate limit exactly, and one
    // two-byte character in place of an ASCII one passes it by one byte.
    const identifierBytes = CLIENT_CONSTRUCTION_LIMITS.identifierCodeUnits;
    const maximal = (prefix: string, last = "x") =>
      prefix.padEnd(identifierBytes - 1, "x") + last;
    const aggregate = (last: string) => ({
      collections: Array.from({ length: 16 }, (_, collectionIndex) => {
        const columns = Array.from({ length: 1_021 }, (_, columnIndex) => ({
          name: maximal(
            `c${columnIndex}_`,
            collectionIndex === 0 && columnIndex === 1 ? last : "x",
          ),
          kind: "int64" as const,
        }));
        return {
          id: maximal(`id${collectionIndex}_`),
          tableId: maximal(`table${collectionIndex}_`),
          columns,
          primaryKey: [maximal("c0_")],
        };
      }),
    });
    expect(16 * 1_024 * identifierBytes).toBe(
      CLIENT_CONSTRUCTION_LIMITS.aggregateSchemaBytes,
    );
    expect(seal(aggregate("x")).collections).toHaveLength(16);
    expect(() => seal(aggregate("é"))).toThrowError(
      new TypeError("ClientSchema collection is invalid"),
    );
  });

  it("reads an endpoint that is a path against the page's URL, and never as another host", () => {
    vi.stubGlobal("location", { href: "https://app.test/issues?id=7" });
    try {
      expect(
        sealSyncClientOptions({ ...options().value, endpoint: "/_calque/" })
          .endpointUrl.href,
      ).toBe("https://app.test/_calque/");
      for (const endpoint of ["//sync.test/root", "/\\sync.test/root"])
        expect(() =>
          sealSyncClientOptions({ ...options().value, endpoint }),
        ).toThrowError(
          new TypeError(
            "SyncClientOptions endpoint is not an absolute URL, or a path on a page",
          ),
        );
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("rejects invalid scalar, endpoint, and schema values before use", () => {
    const limit = CLIENT_CONSTRUCTION_LIMITS.endpointCodeUnits;
    for (const [endpoint, refusal] of [
      ["", `must be a string of 1 to ${limit} characters`],
      [
        `https://sync.test/${"a".repeat(limit)}`,
        `must be a string of 1 to ${limit} characters`,
      ],
      ["https://sync.test/\u0000", "must not contain control characters"],
      ["not-a-url", "is not an absolute URL, or a path on a page"],
      ["/_calque/", "is not an absolute URL, or a path on a page"],
      [
        "http://sync.test",
        "must be an https: URL, or an http: URL on localhost or 127.0.0.1",
      ],
      [
        "https://user:secret@sync.test",
        "must not contain a user name or password",
      ],
      ["https://sync.test/#fragment", "must not contain a fragment"],
    ] as const) {
      expect(() =>
        sealSyncClientOptions({ ...options().value, endpoint }),
      ).toThrowError(new TypeError(`SyncClientOptions endpoint ${refusal}`));
    }
    expect(
      sealSyncClientOptions({
        ...options().value,
        endpoint: "http://localhost:3000",
      }).endpointUrl.href,
    ).toBe("http://localhost:3000/");

    for (const patch of [
      { auth: "not-a-function" },
      { worker: "not-a-function" },
      { fetch: "not-a-function" },
      { memoryBudgetBytes: "1073741824" },
      { memoryBudgetBytes: 0 },
      { memoryBudgetBytes: Number.MAX_SAFE_INTEGER + 1 },
      { deviceId: "" },
      { deviceId: "bad\u0000device" },
      { deviceId: "é".repeat(513) },
    ]) {
      expect(() =>
        sealSyncClientOptions({
          ...options().value,
          ...patch,
        } as unknown as SyncClientOptions),
      ).toThrow(TypeError);
    }

    expect(() => seal({ schemaGeneration: 1, collections: [] })).toThrowError(
      new TypeError('ClientSchema has unknown key "schemaGeneration"'),
    );
    expect(() => seal({})).toThrowError(
      new TypeError('ClientSchema is missing required key "collections"'),
    );
    expect(() => seal({ collections: [], views: [] })).toThrowError(
      new TypeError('ClientSchema has unknown key "views"'),
    );
    expect(() =>
      seal({ collections: [], tables: [{ name: "x" }] }),
    ).toThrowError(new TypeError('ClientSchema has unknown key "tables"'));
    expect(() => seal(null)).toThrowError(
      new TypeError("ClientSchema must be an object"),
    );

    expect(
      sealSyncClientOptions({
        ...options().value,
        deviceId: "é".repeat(512),
      }).deviceId,
    ).toHaveLength(512);
  });

  it("rejects below-minimum budgets before construction and precharges the schema exactly", () => {
    const source = options().value;
    const schemaRetainedBytes =
      sealSyncClientOptions(source).schemaRetainedBytes;
    const exactReserve = CLIENT_METADATA_RESERVE_BYTES + schemaRetainedBytes;
    // The default heap reserve is the whole of a budget this small, so the
    // smallest budget is the metadata pool plus an empty schema.
    expect(
      sealSyncClientOptions({ ...source, memoryBudgetBytes: exactReserve })
        .clientHeapReserveBytes,
    ).toBe(exactReserve);
    expect(() =>
      sealSyncClientOptions({
        ...source,
        memoryBudgetBytes: exactReserve - 1,
      }),
    ).toThrowError(
      new TypeError(
        `SyncClientOptions memoryBudgetBytes must be at least ${exactReserve}`,
      ),
    );

    const sealed = sealInternalSyncClientOptions({
      ...source,
      clientHeapReserveBytes: exactReserve,
    });
    expect(sealed.clientHeapReserveBytes).toBe(exactReserve);
    expect(sealed.schemaRetainedBytes).toBe(schemaRetainedBytes);
    expect(() =>
      sealInternalSyncClientOptions({
        ...source,
        clientHeapReserveBytes: exactReserve - 1,
      }),
    ).toThrowError(
      new TypeError(
        `SyncClientOptions clientHeapReserveBytes must be at least ${exactReserve}`,
      ),
    );
    expect(() =>
      sealInternalSyncClientOptions({
        ...source,
        clientHeapReserveBytes: source.memoryBudgetBytes + 1,
      }),
    ).toThrowError(
      new TypeError(
        "InternalSyncClientOptions clientHeapReserveBytes must not exceed memoryBudgetBytes",
      ),
    );
  });

  it("rejects a genuine generated schema above the client reserve without reinspecting it", () => {
    const sharedColumns = collection("shared", 16).columns;
    let collectionDescriptorReads = 0;
    const collections = Array.from(
      { length: CLIENT_CONSTRUCTION_LIMITS.collections },
      (_, index) =>
        new Proxy(
          {
            id: `collection_${index}`,
            tableId: `public.collection_${index}`,
            columns: sharedColumns,
            primaryKey: ["column_1"],
          },
          {
            getOwnPropertyDescriptor: (target, key) => {
              collectionDescriptorReads += 1;
              return Reflect.getOwnPropertyDescriptor(target, key);
            },
          },
        ),
    );
    const schema = createGeneratedClientSchema({
      collections,
    });
    const readsAfterGeneration = collectionDescriptorReads;
    expect(() =>
      sealSyncClientOptions({
        ...options().value,
        schema,
      }),
    ).toThrow("client heap reserve");
    expect(collectionDescriptorReads).toBe(readsAfterGeneration);
  });
});

describe("generated collection descriptors", () => {
  const collection = {
    id: "issues",
    tableId: "public.issues",
    columns: [
      { name: "id", kind: "uuid" as const },
      { name: "title", kind: "text" as const },
    ],
    primaryKey: ["id"],
    writes: { operations: ["update" as const], writableColumns: ["title"] },
  };

  it("seals collections into the frozen descriptor", () => {
    const sealed = seal({ collections: [collection] });
    expect(sealed.collections).toEqual([collection]);
    expect(Object.isFrozen(sealed.collections)).toBe(true);
    expect(Object.isFrozen(sealed.collections[0]?.columns)).toBe(true);
  });

  it("rejects duplicate ids, unknown kinds, and keys or writable columns outside the collection", () => {
    expect(() =>
      seal({
        collections: [collection, { ...collection, id: "issues_copy" }],
      }),
    ).toThrow("duplicate collection table IDs");
    expect(() =>
      seal({
        collections: [collection, { ...collection, tableId: "public.other" }],
      }),
    ).toThrow("duplicate collection IDs");
    expect(() =>
      seal({
        collections: [
          {
            ...collection,
            columns: [{ name: "id", kind: "varchar" as never }],
          },
        ],
      }),
    ).toThrow("collection is invalid");
    expect(() =>
      seal({
        collections: [{ ...collection, primaryKey: ["missing"] }],
      }),
    ).toThrow("collection is invalid");
    expect(() =>
      seal({
        collections: [
          {
            ...collection,
            writes: {
              operations: ["update" as const],
              writableColumns: ["missing"],
            },
          },
        ],
      }),
    ).toThrow("collection is invalid");
    for (const operations of [[], ["update", "update"], ["upsert"]])
      expect(() =>
        seal({
          collections: [
            {
              ...collection,
              writes: { operations: operations as never, writableColumns: [] },
            },
          ],
        }),
      ).toThrow("collection is invalid");
    expect(() =>
      seal({
        collections: [
          {
            ...collection,
            writes: {
              id: "issue.update",
              operations: ["update" as const],
              writableColumns: ["title"],
            } as never,
          },
        ],
      }),
    ).toThrow('ClientSchema collection writes has unknown key "id"');
  });
});

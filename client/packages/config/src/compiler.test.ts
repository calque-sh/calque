import { readFile } from "node:fs/promises";
import { describe, expect, test } from "vitest";
import { compileConfig, ConfigError } from "./compiler.js";
import type { CalqueConfigDefinition } from "./domain.js";

const fixtureUrl = new URL(
  "../../../../tests/fixtures/config/incident-ops.config.json",
  import.meta.url,
);
const load = async (): Promise<CalqueConfigDefinition> =>
  JSON.parse(await readFile(fixtureUrl, "utf8")) as CalqueConfigDefinition;
const clone = <T>(value: T): T => structuredClone(value);

describe("compileConfig", () => {
  test("returns the manifest deeply frozen, every declaration sorted", async () => {
    const definition = await load();
    const manifest = compileConfig(definition);
    expect(manifest.schemaVersion).toBe("calque.config.v1");
    expect(manifest.collections[0]?.columns.map(({ name }) => name)).toEqual([
      "id",
      "organization_id",
      "status",
      "title",
    ]);
    expect(manifest.commands[0]?.operations).toEqual(["delete", "update"]);
    expect(Object.isFrozen(manifest)).toBe(true);
    expect(Object.isFrozen(manifest.collections[0]?.columns)).toBe(true);
  });

  test("normalizes declaration order without changing meaningful key order", async () => {
    const left = await load();
    const right = clone(left) as {
      sources: CalqueConfigDefinition["sources"];
      scopes: CalqueConfigDefinition["scopes"];
      collections: CalqueConfigDefinition["collections"];
      commands: CalqueConfigDefinition["commands"];
    } & CalqueConfigDefinition;
    right.sources = [...right.sources].reverse();
    right.scopes = [...right.scopes].reverse();
    right.collections = [...right.collections].reverse().map((collection) => ({
      ...collection,
      columns: [...collection.columns].reverse(),
    }));
    right.commands = [...right.commands].reverse().map((command) => ({
      ...command,
      operations: [...command.operations].reverse(),
      writableColumns: [...command.writableColumns].reverse(),
    }));
    expect(compileConfig(right)).toEqual(compileConfig(left));
  });

  test("owns the compiled graph", async () => {
    const definition = await load();
    const manifest = compileConfig(definition);
    (definition.sources[0] as { id: string }).id = "changed";
    expect(manifest.sources[0]?.id).toBe("primary");
  });

  test("refuses a type calque serve does not enroll, naming the ones it does", async () => {
    for (const postgresType of ["pg_catalog.text", "interval", "int4[]"]) {
      const definition = clone(await load());
      const column = definition.collections[0]?.columns[0];
      if (!column) throw new Error("fixture column is missing");
      (column as { postgresType: string }).postgresType = postgresType;
      expect(() => compileConfig(definition)).toThrow(
        new ConfigError(
          "$.collections[0].columns[0].postgresType",
          `${postgresType} is not a type calque serve enrolls; it must be one of bool, int2, int4, int8, float4, float8, numeric, text, varchar, bpchar, uuid, bytea, json, jsonb, date, time, timestamp, timestamptz, by its pg_catalog name as calque init writes it`,
        ),
      );
    }
  });

  test("rejects a second source and an unselected primary key", async () => {
    const definition = clone(await load()) as unknown as {
      sources: unknown[];
    };
    definition.sources.push({
      id: "other",
      databaseUrlEnv: "OTHER_DATABASE_URL",
      publication: "other",
      slot: "other",
    });
    expect(() =>
      compileConfig(definition as unknown as CalqueConfigDefinition),
    ).toThrow("$.sources: must hold exactly one source");

    const missingKey = clone(await load()) as unknown as {
      collections: Array<Record<string, unknown>>;
    };
    const collection = missingKey.collections[0] as unknown as {
      columns: Array<{ name: string }>;
    };
    collection.columns = collection.columns.filter(({ name }) => name !== "id");
    expect(() =>
      compileConfig(missingKey as unknown as CalqueConfigDefinition),
    ).toThrow(/key column id is not selected/);
  });

  interface PartitionedDraft {
    readonly scope: Record<string, unknown>;
    readonly issues: Record<string, unknown>;
    readonly plans: Record<string, unknown>;
    readonly definition: CalqueConfigDefinition;
  }

  /**
   * `issues` keyed by `(tenant_id, id)` and partitioned on `tenant_id`, and
   * `plans` shared, under one partitioned scope. Editing `scope`, `issues` or
   * `plans` edits `definition`.
   */
  const partitioned = async (): Promise<PartitionedDraft> => {
    const base = clone(await load());
    const scope: Record<string, unknown> = {
      ...base.scopes[0],
      kind: "partitioned",
      attribute: "tenant_id",
    };
    const issues: Record<string, unknown> = {
      ...base.collections[0],
      columns: [
        { name: "tenant_id", postgresType: "int8", nullable: false },
        ...(base.collections[0]?.columns ?? []),
      ],
      primaryKey: ["tenant_id", "id"],
      partitionColumn: "tenant_id",
    };
    const plans: Record<string, unknown> = {
      ...base.collections[0],
      id: "plans",
      relation: { schema: "public", table: "plans" },
      columns: [
        { name: "id", postgresType: "int8", nullable: false },
        { name: "name", postgresType: "text", nullable: false },
      ],
      primaryKey: ["id"],
      shared: true,
    };
    const definition = {
      ...base,
      scopes: [scope],
      collections: [issues, plans],
    } as unknown as CalqueConfigDefinition;
    return { scope, issues, plans, definition };
  };

  test("compiles a partitioned scope of partitioned and shared collections", async () => {
    const manifest = compileConfig((await partitioned()).definition);
    expect(manifest.scopes[0]).toEqual({
      id: "organization",
      source: "primary",
      dataVersion: 1,
      kind: "partitioned",
      attribute: "tenant_id",
    });
    const [issues, plans] = manifest.collections;
    expect(issues?.partitionColumn).toBe("tenant_id");
    expect(issues?.shared).toBeUndefined();
    expect(plans?.shared).toBe(true);
    expect(plans?.partitionColumn).toBeUndefined();
  });

  test("compiles the partitioned manifest calque serve is tested on", async () => {
    const definition = JSON.parse(
      await readFile(
        new URL(
          "../../../../tests/fixtures/config/serve-tenants.config.json",
          import.meta.url,
        ),
        "utf8",
      ),
    ) as CalqueConfigDefinition;
    const manifest = compileConfig(definition);
    expect(manifest.scopes[0]?.kind).toBe("partitioned");
    expect(
      manifest.collections.map(({ id, partitionColumn, shared }) => [
        id,
        partitionColumn ?? (shared === true ? "shared" : undefined),
      ]),
    ).toEqual([
      ["priorities", "shared"],
      ["projects", "tenant_id"],
      ["tasks", "tenant_id"],
    ]);
  });

  test("refuses a partitioned scope whose collections do not each say how they are partitioned", async () => {
    const refusal = async (
      edit: (draft: PartitionedDraft) => void,
    ): Promise<string> => {
      const draft = await partitioned();
      edit(draft);
      try {
        compileConfig(draft.definition);
      } catch (error) {
        if (error instanceof ConfigError) return error.message;
        throw error;
      }
      throw new Error("the configuration compiled");
    };
    expect(
      await refusal(({ plans }) => {
        delete plans.shared;
      }),
    ).toBe(
      "$.collections: collection plans is in partitioned scope organization, so it sets exactly one of partitionColumn, naming the column that holds the tenant, or shared: true",
    );
    expect(
      await refusal(({ issues }) => {
        issues.shared = true;
      }),
    ).toMatch(/collection issues is in partitioned scope organization/u);
    expect(
      await refusal(({ issues }) => {
        issues.primaryKey = ["id", "tenant_id"];
      }),
    ).toBe(
      "$.collections: collection issues is partitioned by tenant_id, which must be the first column of its primaryKey, not id",
    );
    expect(
      await refusal(({ issues }) => {
        issues.partitionColumn = "title";
      }),
    ).toMatch(/partitioned by title, which must be the first column/u);
    expect(
      await refusal(({ issues }) => {
        issues.columns = [
          { name: "tenant_id", postgresType: "text", nullable: false },
          ...(issues.columns as unknown[]).slice(1),
        ];
      }),
    ).toBe(
      "$.collections: collection issues is partitioned by tenant_id, of type text; a partition column is int2, int4, int8 or uuid",
    );
    expect(
      await refusal(({ plans }) => {
        plans.partitionColumn = "id";
        delete plans.shared;
        plans.columns = [
          { name: "id", postgresType: "uuid", nullable: false },
          { name: "name", postgresType: "text", nullable: false },
        ];
      }),
    ).toBe(
      "$.collections: the partition columns of scope organization have types int8, uuid; they must have one type, the type of grant attribute tenant_id",
    );
    expect(
      await refusal(({ issues }) => {
        delete issues.partitionColumn;
        issues.shared = true;
      }),
    ).toBe(
      "$.scopes: partitioned scope organization has no partitioned collection",
    );
    expect(
      await refusal(({ plans }) => {
        plans.shared = false;
      }),
    ).toBe("$.collections[1].shared: must be true when present");
    expect(
      await refusal(({ scope }) => {
        delete scope.attribute;
      }),
    ).toBe("$.scopes[0].attribute: must be a non-empty string");
    expect(
      await refusal(({ scope }) => {
        scope.attribute = "tenant\u0007";
      }),
    ).toMatch(/attribute: must be a grant attribute name/u);
    expect(
      await refusal(({ scope }) => {
        scope.kind = "source-wide";
        delete scope.attribute;
      }),
    ).toBe(
      "$.collections: collection issues sets partitionColumn, which only a partitioned scope reads; scope organization is source-wide",
    );
  });

  test("rejects ambiguous authorization declarations", async () => {
    // A scope is source-wide or partitioned. Any other kind is refused by
    // its kind, before the fields only that kind would carry are read.
    const partition = clone(await load()) as unknown as {
      scopes: Array<Record<string, unknown>>;
    };
    partition.scopes[0] = {
      ...partition.scopes[0],
      kind: "partition",
      keyColumns: ["organization_id"],
      immutableKeys: true,
    };
    expect(() =>
      compileConfig(partition as unknown as CalqueConfigDefinition),
    ).toThrow(/^\$\.scopes\[0\]\.kind: must be source-wide or partitioned$/);

    const projectionScope = clone(await load()) as unknown as {
      scopes: Array<Record<string, unknown>>;
    };
    projectionScope.scopes[0] = {
      ...projectionScope.scopes[0],
      kind: "projection",
      completeRows: true,
    };
    expect(() =>
      compileConfig(projectionScope as unknown as CalqueConfigDefinition),
    ).toThrow(/kind: must be source-wide or partitioned$/);

    const extraKey = clone(await load()) as unknown as {
      scopes: Array<Record<string, unknown>>;
    };
    extraKey.scopes[0] = {
      ...extraKey.scopes[0],
      keyColumns: ["organization_id"],
    };
    expect(() =>
      compileConfig(extraKey as unknown as CalqueConfigDefinition),
    ).toThrow(/^\$\.scopes\[0\]: contains unknown fields: keyColumns$/);

    const projectionCollection = clone(await load()) as unknown as {
      collections: Array<Record<string, unknown>>;
    };
    projectionCollection.collections[0] = {
      ...projectionCollection.collections[0],
      kind: "projection",
    };
    expect(() =>
      compileConfig(projectionCollection as unknown as CalqueConfigDefinition),
    ).toThrow(/kind: must be table/);

    const routed = clone(await load()) as unknown as {
      collections: Array<Record<string, unknown>>;
    };
    routed.collections[0] = {
      ...routed.collections[0],
      routingKey: ["organization_id"],
    };
    expect(() =>
      compileConfig(routed as unknown as CalqueConfigDefinition),
    ).toThrow(/^\$\.collections\[0\]: contains unknown fields: routingKey$/);
  });

  test("rejects implicit writes and references to unselected data", async () => {
    const definition = clone(await load()) as unknown as {
      commands: Array<Record<string, unknown>>;
    };
    definition.commands[0] = {
      ...definition.commands[0],
      writableColumns: ["secret"],
    };
    expect(() =>
      compileConfig(definition as unknown as CalqueConfigDefinition),
    ).toThrow(/references unselected column secret/);

    const duplicate = clone(await load()) as unknown as { commands: unknown[] };
    duplicate.commands.push(clone(duplicate.commands[0]));
    expect(() =>
      compileConfig(duplicate as unknown as CalqueConfigDefinition),
    ).toThrow(/must not contain duplicates/);

    const second = clone(await load()) as unknown as {
      commands: Array<Record<string, unknown>>;
    };
    second.commands.push({ ...second.commands[0], id: "edit-issue-again" });
    expect(() =>
      compileConfig(second as unknown as CalqueConfigDefinition),
    ).toThrow(
      new ConfigError(
        "$.commands",
        "commands edit-issue and edit-issue-again both write collection issues; a collection takes one crud command",
      ),
    );

    const identityWrite = clone(await load()) as unknown as {
      commands: Array<Record<string, unknown>>;
    };
    identityWrite.commands[0] = {
      ...identityWrite.commands[0],
      writableColumns: ["id"],
    };
    expect(() =>
      compileConfig(identityWrite as unknown as CalqueConfigDefinition),
    ).toThrow(/cannot mutate identity column id/);
  });

  test("refuses a collection whose id is not its relation's table name", async () => {
    // The server serves the collection as `issues`, and the client binds the
    // collection `tickets` only to a served table named `tickets`.
    const renamed = clone(await load()) as unknown as {
      collections: Array<Record<string, unknown>>;
    };
    renamed.collections[0] = { ...renamed.collections[0], id: "tickets" };
    expect(() =>
      compileConfig(renamed as unknown as CalqueConfigDefinition),
    ).toThrow(
      new ConfigError(
        "$.collections[0].id",
        "collection tickets reads public.issues, and a collection id must equal its relation's table name, issues",
      ),
    );
  });

  test("refuses a backend command, which Calque does not execute", async () => {
    const backend = clone(await load()) as unknown as {
      commands: Array<Record<string, unknown>>;
    };
    backend.commands.push({
      id: "resolve-issue",
      kind: "backend",
      executor: "application-backend",
      source: "primary",
      scope: "organization",
      collections: ["issues"],
      inputVersion: 1,
    });
    expect(() =>
      compileConfig(backend as unknown as CalqueConfigDefinition),
    ).toThrow(
      new ConfigError(
        "$.commands[1].kind",
        "backend commands are not supported; declare a crud command",
      ),
    );
  });

  test("refuses a collection whose adapter is not direct-columnar", async () => {
    const other = clone(await load()) as unknown as {
      collections: Array<Record<string, unknown>>;
    };
    other.collections[0] = { ...other.collections[0], adapter: "sqlite" };
    expect(() =>
      compileConfig(other as unknown as CalqueConfigDefinition),
    ).toThrow(
      new ConfigError("$.collections[0].adapter", "must be direct-columnar"),
    );
  });

  test("refuses insert on a command that cannot write every non-key column", async () => {
    // The fixture's collection selects organization_id and its command
    // cannot write it. Without insert that is a read-only column.
    const readOnly = clone(await load());
    expect(() => compileConfig(readOnly)).not.toThrow();

    const withInsert = clone(await load()) as unknown as {
      commands: Array<Record<string, unknown>>;
    };
    withInsert.commands[0] = {
      ...withInsert.commands[0],
      operations: ["insert", "update", "delete"],
    };
    expect(() =>
      compileConfig(withInsert as unknown as CalqueConfigDefinition),
    ).toThrow(
      new ConfigError(
        "$.commands[0].writableColumns",
        "command edit-issue lists insert, but its writableColumns omit organization_id: an insert writes every non-key column of issues, so add that column to writableColumns or remove insert from operations",
      ),
    );

    withInsert.commands[0] = {
      ...withInsert.commands[0],
      writableColumns: ["organization_id", "status", "title"],
    };
    expect(() =>
      compileConfig(withInsert as unknown as CalqueConfigDefinition),
    ).not.toThrow();
  });

  test("names databaseUrlEnv when a source carries its earlier name", async () => {
    const definition = clone(await load()) as unknown as {
      sources: [Record<string, unknown>];
    };
    const { databaseUrlEnv, ...rest } = definition.sources[0];
    definition.sources[0] = { ...rest, connectionEnv: databaseUrlEnv };
    expect(() =>
      compileConfig(definition as unknown as CalqueConfigDefinition),
    ).toThrow(
      "$.sources[0].connectionEnv: is now databaseUrlEnv; rename the field",
    );
  });

  test("rejects secrets, functions, custom prototypes, and unknown fields", async () => {
    const secret = clone(await load()) as unknown as {
      sources: Array<Record<string, unknown>>;
    };
    secret.sources[0] = {
      ...secret.sources[0],
      databaseUrlEnv: "postgres://secret@host/db",
    };
    expect(() =>
      compileConfig(secret as unknown as CalqueConfigDefinition),
    ).toThrow(/must name an environment variable/);

    const callback = clone(await load()) as unknown as Record<string, unknown>;
    callback.runtime = () => undefined;
    expect(() =>
      compileConfig(callback as unknown as CalqueConfigDefinition),
    ).toThrow(/contains unknown fields: runtime/);

    const inherited = clone(await load()) as unknown as Record<string, unknown>;
    Object.setPrototypeOf(inherited, { runtime: true });
    expect(() =>
      compileConfig(inherited as unknown as CalqueConfigDefinition),
    ).toThrow(ConfigError);

    const sparse = clone(await load()) as unknown as { commands: unknown[] };
    sparse.commands.length += 1;
    expect(() =>
      compileConfig(sparse as unknown as CalqueConfigDefinition),
    ).toThrow(/must not be sparse/);
  });

  test("rejects unsafe identifiers, duplicate keys, and unsupported formats", async () => {
    for (const applicationId of ["Incident_Ops", "incident--ops", "2fa"]) {
      const misnamed = clone(await load());
      (misnamed.application as { id: string }).id = applicationId;
      expect(() => compileConfig(misnamed)).toThrow(
        new ConfigError(
          "$.application.id",
          "must start with a lowercase letter and use lowercase letters and digits, joined by single '.', '_' or '-' (for example incident-ops)",
        ),
      );
    }

    const unsafe = clone(await load()) as unknown as {
      collections: Array<Record<string, unknown>>;
    };
    unsafe.collections[0] = {
      ...unsafe.collections[0],
      primaryKey: ["id", "id"],
    };
    expect(() =>
      compileConfig(unsafe as unknown as CalqueConfigDefinition),
    ).toThrow(/primaryKey: must not contain duplicates/);

    const unsupported = clone(await load()) as unknown as {
      collections: Array<Record<string, unknown>>;
    };
    unsupported.collections[0] = {
      ...unsupported.collections[0],
      encoding: "generic",
    };
    expect(() =>
      compileConfig(unsupported as unknown as CalqueConfigDefinition),
    ).toThrow(/not a supported configured format/);
  });
});

type JsonPatchOperation =
  | { op: "add" | "replace"; path: string; value: unknown }
  | { op: "remove"; path: string }
  | { op: "copy"; path: string; from: string };

/**
 * Applies the `add`, `replace`, `remove` and `copy` operations of an RFC 6902
 * JSON Patch whose paths need no escaping.
 */
function applyJsonPatch(document: unknown, patch: JsonPatchOperation[]): void {
  const at = (pointer: string): unknown =>
    pointer
      .split("/")
      .slice(1)
      .reduce<unknown>((value, key) => {
        const next = (value as Record<string, unknown>)[key];
        if (next === undefined) throw new Error(`no value at ${pointer}`);
        return next;
      }, document);
  for (const operation of patch) {
    const slash = operation.path.lastIndexOf("/");
    const parent = at(operation.path.slice(0, slash));
    const last = operation.path.slice(slash + 1);
    const value =
      operation.op === "copy"
        ? structuredClone(at(operation.from))
        : operation.op === "remove"
          ? undefined
          : operation.value;
    if (Array.isArray(parent)) {
      if (operation.op === "remove") parent.splice(Number(last), 1);
      else if (last === "-") parent.push(value);
      else if (operation.op === "replace") parent[Number(last)] = value;
      else parent.splice(Number(last), 0, value);
    } else {
      const object = parent as Record<string, unknown>;
      if (operation.op === "remove") {
        if (!(last in object)) throw new Error(`no value at ${operation.path}`);
        Reflect.deleteProperty(object, last);
      } else object[last] = value;
    }
  }
}

describe("the refusals compileConfig shares with calque serve", () => {
  const fixturePath = (name: string) =>
    new URL(`../../../../tests/fixtures/config/${name}`, import.meta.url);
  const read = async (name: string): Promise<unknown> =>
    JSON.parse(await readFile(fixturePath(name), "utf8"));

  test("refuses every manifest the shared fixture lists, and accepts its bases", async () => {
    type Case = { name: string; base: string; patch: JsonPatchOperation[] };
    const fixture = (await read("refusals.json")) as {
      bases: Record<string, string>;
      cases: Case[];
      accepted: Case[];
    };
    const bases = new Map<string, unknown>();
    for (const [name, file] of Object.entries(fixture.bases)) {
      const base = await read(file);
      expect(() =>
        compileConfig(structuredClone(base) as CalqueConfigDefinition),
      ).not.toThrow();
      bases.set(name, base);
    }
    const accepted = fixture.cases
      .filter(({ base, patch }) => {
        const manifest = structuredClone(bases.get(base));
        applyJsonPatch(manifest, patch);
        try {
          compileConfig(manifest as CalqueConfigDefinition);
          return true;
        } catch (error) {
          if (!(error instanceof ConfigError)) throw error;
          return false;
        }
      })
      .map(({ name }) => name);
    expect(accepted).toEqual([]);
    for (const { base, patch } of fixture.accepted) {
      const manifest = structuredClone(bases.get(base));
      applyJsonPatch(manifest, patch);
      expect(() =>
        compileConfig(manifest as CalqueConfigDefinition),
      ).not.toThrow();
    }
  });
});

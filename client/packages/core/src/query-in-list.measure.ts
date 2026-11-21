/**
 * Filtering a link table by a list of keys: 500 text ids over 21,000 links
 * joined to 30,000 items and grouped by two item columns. The list is written
 * as five `or` of `eq` queries of 100 ids, which the specification cap allows,
 * and as one `in-list` predicate. Run with
 * `pnpm --filter @calque/core measure --testNamePattern "link filter"`.
 * Timings are query-kernel diagnostics on the host that ran them, never
 * browser, cold-load or stable-paint evidence. `first` opens the artifact and
 * the engine afresh for every sample; `warm` repeats on one engine.
 */
import os from "node:os";

import { expect, test } from "vitest";

import {
  openDirectColumnarArtifact,
  type DirectColumnarSchema,
} from "./artifact.js";
import { encodeArtifact } from "./artifact.fixture.js";
import { OverlayEngine, artifactBase } from "./overlay.js";
import {
  ColumnarQueryEngine,
  MAX_QUERY_RESULT_BYTES,
  MAX_QUERY_RESULT_ROWS,
  type QueryPredicate,
  type QueryResult,
  type QuerySpecification,
} from "./query.js";

const LINKS = 21_000;
const ITEMS = 30_000;
const PARENTS = 3_000;
const LISTED = 500;
const SAMPLES = 9;
const WARMUPS = 3;

const SCHEMA: DirectColumnarSchema = {
  format: "calque-direct-columnar",
  version: 4,
  tables: [
    {
      name: "links",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "int64", nullable: false },
        { name: "parent_text", type: "utf8", nullable: false },
        { name: "parent_dict", type: "dict", nullable: false },
        { name: "item_id", type: "int64", nullable: false },
      ],
    },
    {
      name: "items",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "int64", nullable: false },
        { name: "kind", type: "dict", nullable: false },
        { name: "bucket", type: "dict", nullable: false },
      ],
    },
  ],
};
const COLUMNS = {
  links: ["id", "parent_text", "parent_dict", "item_id"],
  items: ["id", "kind", "bucket"],
};

/** mulberry32, so the fixture replays. */
function generator(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 2 ** 32;
  };
}

const OPTIONS = {
  maxRows: MAX_QUERY_RESULT_ROWS,
  maxBytes: MAX_QUERY_RESULT_BYTES,
};

function median(values: readonly number[]): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.floor(sorted.length / 2)] ?? Number.NaN;
}

const parentId = (index: number) =>
  `parent-${String(index).padStart(6, "0")}-${"p".repeat(11)}`;

const field = (source: string, column: string) => ({ source, column });

function query(where: QueryPredicate): QuerySpecification {
  return {
    version: 1,
    from: { collection: "links", as: "l" },
    joins: [
      {
        kind: "inner",
        collection: "items",
        as: "i",
        on: { left: field("l", "item_id"), right: field("i", "id") },
      },
    ],
    where,
    select: [
      { kind: "field", field: field("i", "kind"), as: "kind" },
      { kind: "field", field: field("i", "bucket"), as: "bucket" },
      { kind: "aggregate", operator: "count", as: "links" },
    ],
    groupBy: [field("i", "kind"), field("i", "bucket")],
    orderBy: [
      { by: "kind", direction: "asc" },
      { by: "bucket", direction: "asc" },
    ],
  };
}

const eq = (column: string, value: string): QueryPredicate => ({
  kind: "compare",
  operator: "eq",
  left: { kind: "field", field: field("l", column) },
  right: { kind: "literal", value },
});

async function engineOverFreshArtifact(bytes: ReturnType<typeof encode>) {
  const workspace = await openDirectColumnarArtifact(
    bytes.bytes.slice().buffer,
    {
      byteLength: bytes.byteLength,
      digest: bytes.digest,
      identity: bytes.identity,
      schema: SCHEMA,
    },
  );
  return new ColumnarQueryEngine(
    new OverlayEngine(
      (["links", "items"] as const).map((name) => ({
        name,
        columns: COLUMNS[name],
        base: artifactBase(workspace.table(name), COLUMNS[name]),
        primaryKey: ["id"],
      })),
    ),
    { capacityBytes: 1 << 30 },
  );
}

function encode() {
  const random = generator(0x1157);
  const links = Array.from({ length: LINKS }, (_unused, id) => {
    const parent = parentId(Math.floor(random() * PARENTS));
    return {
      id: BigInt(id),
      parent_text: parent,
      parent_dict: parent,
      item_id: BigInt(Math.floor(random() * ITEMS)),
    };
  });
  const items = Array.from({ length: ITEMS }, (_unused, id) => ({
    id: BigInt(id),
    kind: `kind-${id % 12}`,
    bucket: `bucket-${(id * 7) % 31}`,
  }));
  return encodeArtifact(
    SCHEMA,
    new Map<string, readonly Record<string, unknown>[]>([
      ["links", links],
      ["items", items],
    ]),
  );
}

/** Sums `count` per group over the pieces of a split query. */
function merged(results: readonly QueryResult[]): string {
  const groups = new Map<string, number>();
  for (const { rows } of results)
    for (const [kind, bucket, links] of rows)
      groups.set(
        `${String(kind)}|${String(bucket)}`,
        (groups.get(`${String(kind)}|${String(bucket)}`) ?? 0) + Number(links),
      );
  return JSON.stringify([...groups].sort(([a], [b]) => (a < b ? -1 : 1)));
}

function whole(result: QueryResult): string {
  return merged([result]);
}

test("link filter: 500 listed ids over 21,000 links joined to 30,000 items", async () => {
  const bytes = encode();
  const random = generator(0x2f);
  const ids = Array.from({ length: LISTED }, () =>
    parentId(Math.floor(random() * PARENTS)),
  );
  const distinct = [...new Set(ids)];
  const report: Record<string, unknown> = {
    host: {
      cores: os.availableParallelism(),
      loadAverage: os.loadavg(),
      node: process.version,
      platform: `${process.platform} ${process.arch}`,
    },
    links: LINKS,
    items: ITEMS,
    listed: LISTED,
    distinctListed: distinct.length,
    samples: SAMPLES,
  };
  for (const column of ["parent_text", "parent_dict"] as const) {
    const pieces = Array.from({ length: 5 }, (_unused, piece) =>
      distinct.slice(piece * 100, piece * 100 + 100),
    );
    const split = (): readonly QuerySpecification[] =>
      pieces.map((piece) =>
        query({
          kind: "or",
          predicates: piece.map((id) => eq(column, id)),
        }),
      );
    const single = (): QuerySpecification =>
      query({
        kind: "in-list",
        field: field("l", column),
        values: distinct,
      });
    const runSplit = (engine: ColumnarQueryEngine) =>
      split().map((specification) => engine.query(specification, OPTIONS));
    const runSingle = (engine: ColumnarQueryEngine) =>
      engine.query(single(), OPTIONS);
    const variants: [string, (engine: ColumnarQueryEngine) => string][] = [
      ["split", (engine) => merged(runSplit(engine))],
      ["inList", (engine) => whole(runSingle(engine))],
    ];
    const expected = merged(runSplit(await engineOverFreshArtifact(bytes)));
    for (const [name, run] of variants) {
      const first: number[] = [];
      for (let sample = 0; sample < SAMPLES; sample++) {
        const engine = await engineOverFreshArtifact(bytes);
        const started = performance.now();
        const answer = run(engine);
        first.push(performance.now() - started);
        expect(answer).toBe(expected);
      }
      const engine = await engineOverFreshArtifact(bytes);
      for (let warmup = 0; warmup < WARMUPS; warmup++) run(engine);
      const warm: number[] = [];
      for (let sample = 0; sample < SAMPLES; sample++) {
        const started = performance.now();
        run(engine);
        warm.push(performance.now() - started);
      }
      report[`${column} ${name}`] = {
        firstMedianMs: Number(median(first).toFixed(2)),
        warmMedianMs: Number(median(warm).toFixed(2)),
        firstMs: first.map((value) => Number(value.toFixed(2))),
        warmMs: warm.map((value) => Number(value.toFixed(2))),
      };
    }
  }
  console.log(JSON.stringify(report, null, 2));
}, 900_000);

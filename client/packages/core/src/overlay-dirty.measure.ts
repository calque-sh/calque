/**
 * The incident page's chart and open-issues queries over 10,000 issues, with
 * the overlay clean, with every row's title rewritten as a one-column patch,
 * and with the same rewrite delivered as the full new tuple pgoutput sends;
 * then both queries in each arm as one publication meets them, after a write
 * to one title. Run with
 * `pnpm --filter @calque/core measure --testNamePattern "issue overlay"`.
 * Timings are query-kernel diagnostics on the host that ran them, never
 * browser, cold-load or stable-paint evidence.
 */
import { expect, test } from "vitest";

import {
  openDirectColumnarArtifact,
  type DirectColumnarSchema,
} from "./artifact.js";
import { encodeArtifact } from "./artifact.fixture.js";
import {
  OverlayEngine,
  artifactBase,
  type OverlayBase,
  type SqlValue,
} from "./overlay.js";
import {
  ColumnarQueryEngine,
  MAX_QUERY_RESULT_BYTES,
  MAX_QUERY_RESULT_ROWS,
  type QuerySpecification,
} from "./query.js";
import { referenceQuery } from "./query-reference.fixture.js";

const ROWS = 10_000;
const SAMPLES = 30;
const WARMUPS = 5;
const STATUSES = [
  "unresolved",
  "investigating",
  "resolved",
  "ignored",
  "archived",
];

const SCHEMA: DirectColumnarSchema = {
  format: "calque-direct-columnar",
  version: 4,
  tables: [
    {
      name: "issues",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "uuid", nullable: false },
        { name: "project_id", type: "uuid", nullable: false },
        { name: "status", type: "utf8", nullable: false },
        { name: "priority", type: "int64", nullable: false },
        { name: "title", type: "utf8", nullable: false },
        { name: "event_count", type: "int64", nullable: false },
        { name: "updated_at", type: "timestamp", nullable: false },
        { name: "bench_index", type: "int64", nullable: false },
        { name: "is_sentinel", type: "bool", nullable: false },
      ],
    },
  ],
};
const COLUMNS = SCHEMA.tables[0]?.columns.map(({ name }) => name) ?? [];
const TITLE = COLUMNS.indexOf("title");

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

const field = (column: string) => ({ source: "issue", column });

const chart: QuerySpecification = {
  version: 1,
  from: { collection: "issues", as: "issue" },
  select: [
    { kind: "field", field: field("status"), as: "status" },
    { kind: "field", field: field("priority"), as: "priority" },
    { kind: "aggregate", operator: "count", as: "count" },
  ],
  groupBy: [field("status"), field("priority")],
  orderBy: [
    { by: "status", direction: "asc" },
    { by: "priority", direction: "asc" },
  ],
};

const open: QuerySpecification = {
  version: 1,
  from: { collection: "issues", as: "issue" },
  where: {
    kind: "and",
    predicates: [
      {
        kind: "compare",
        operator: "eq",
        left: { kind: "field", field: field("status") },
        right: { kind: "literal", value: "unresolved" },
      },
      {
        kind: "compare",
        operator: "eq",
        left: { kind: "field", field: field("is_sentinel") },
        right: { kind: "literal", value: false },
      },
    ],
  },
  select: ["id", "title", "status", "priority", "updated_at"].map((column) => ({
    kind: "field",
    field: field(column),
    as: column,
  })),
  orderBy: [
    { by: "priority", direction: "desc" },
    { by: "updated_at", direction: "desc" },
    { by: "id", direction: "asc" },
  ],
  limit: 40,
};

test("issue overlay: clean, one-column patches and full tuples", async () => {
  const random = generator(0x1550);
  const started = Date.parse("2026-01-01T00:00:00Z");
  const issues = Array.from({ length: ROWS }, (_unused, index) => ({
    id: Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256)),
    project_id: Uint8Array.from({ length: 16 }, (_u, byte) =>
      byte === 15 ? index % 8 : 7,
    ),
    status: STATUSES[index % STATUSES.length],
    priority: BigInt(1 + (index % 5)),
    title: `issue ${String(index)} title`,
    event_count: BigInt(index % 97),
    updated_at: new Date(started + index * 60_000).toISOString(),
    bench_index: BigInt(index),
    is_sentinel: false,
  }));
  const encoded = encodeArtifact(SCHEMA, new Map([["issues", issues]]));
  const overlayOf = async (): Promise<[OverlayEngine, OverlayBase]> => {
    const workspace = await openDirectColumnarArtifact(
      encoded.bytes.slice().buffer,
      {
        byteLength: encoded.byteLength,
        digest: encoded.digest,
        identity: encoded.identity,
        schema: SCHEMA,
      },
    );
    const base = artifactBase(workspace.table("issues"), COLUMNS);
    return [
      new OverlayEngine([{ name: "issues", columns: COLUMNS, base }]),
      base,
    ];
  };
  // Every row's title rewritten, as a patch naming the title alone or as the
  // whole new tuple; every other value read back from the base, so a full
  // tuple carries exactly the values the base already holds.
  const rewrite = (overlay: OverlayEngine, base: OverlayBase, full: boolean) =>
    overlay.applyCanonical({
      transactions: [
        {
          mutationIds: [],
          operations: issues.map((issue, row) => {
            const title: SqlValue = `${String(issue.title)} rewritten`;
            return {
              kind: "update" as const,
              collection: "issues",
              primaryKey: issue.id,
              key: [issue.id],
              changed: full
                ? COLUMNS.map((_name, column) => ({
                    column,
                    value: column === TITLE ? title : base.valueAt(column, row),
                  }))
                : [{ column: TITLE, value: title }],
            };
          }),
        },
      ],
    });
  // Applying the rewrite, each time onto a fresh overlay.
  const applyMs: Record<string, number> = {};
  for (const [mode, full] of Object.entries({ patched: false, tuples: true })) {
    const samples: number[] = [];
    for (let index = 0; index < 7; index++) {
      const [overlay, base] = await overlayOf();
      const before = performance.now();
      rewrite(overlay, base, full);
      samples.push(performance.now() - before);
    }
    applyMs[mode] = Number(median(samples).toFixed(3));
  }
  const arms: Record<string, OverlayEngine> = {};
  const [clean] = await overlayOf();
  arms.clean = clean;
  const [patched, patchedBase] = await overlayOf();
  rewrite(patched, patchedBase, false);
  arms.patched = patched;
  const [tuples, tuplesBase] = await overlayOf();
  rewrite(tuples, tuplesBase, true);
  arms.tuples = tuples;

  const schemas = [{ name: "issues", columns: COLUMNS }];
  const engines = Object.fromEntries(
    Object.entries(arms).map(([name, overlay]) => [
      name,
      new ColumnarQueryEngine(overlay, { capacityBytes: 1 << 30 }),
    ]),
  );
  for (const query of [chart, open]) {
    const expected = referenceQuery(patched, schemas, query);
    expect(engines.patched?.query(query, OPTIONS)).toEqual(expected);
    expect(engines.tuples?.query(query, OPTIONS)).toEqual(expected);
  }
  const timings: Record<string, readonly number[]> = {};
  for (const [name, query] of Object.entries({ chart, open }))
    for (const [arm, engine] of Object.entries(engines)) {
      for (let index = 0; index < WARMUPS; index++)
        engine.query(query, OPTIONS);
      timings[`${name}.${arm}`] = Array.from({ length: SAMPLES }, () => {
        const before = performance.now();
        engine.query(query, OPTIONS);
        return performance.now() - before;
      });
    }
  // One publication as the page meets it: a canonical update to one title
  // moves the generation, then both queries run over the new one.
  for (const [arm, overlay] of Object.entries(arms)) {
    const engine = engines[arm];
    if (!engine) continue;
    let written = 0;
    const publication = () => {
      const issue = issues[written++ % ROWS];
      if (!issue) return;
      overlay.applyCanonical({
        transactions: [
          {
            mutationIds: [],
            operations: [
              {
                kind: "update",
                collection: "issues",
                primaryKey: issue.id,
                key: [issue.id],
                changed: [{ column: TITLE, value: `published ${written}` }],
              },
            ],
          },
        ],
      });
      const before = performance.now();
      engine.query(chart, OPTIONS);
      engine.query(open, OPTIONS);
      return performance.now() - before;
    };
    for (let index = 0; index < WARMUPS; index++) publication();
    timings[`publication.${arm}`] = Array.from(
      { length: SAMPLES },
      () => publication() ?? Number.NaN,
    );
  }
  console.log(
    `ISSUE_OVERLAY_RESULT=${JSON.stringify({
      rows: ROWS,
      samples: SAMPLES,
      applyMs,
      medianMs: Object.fromEntries(
        Object.entries(timings).map(([name, values]) => [
          name,
          Number(median(values).toFixed(3)),
        ]),
      ),
    })}`,
  );
}, 120_000);

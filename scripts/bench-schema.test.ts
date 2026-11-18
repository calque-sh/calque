import assert from "node:assert/strict";
import {
  appendFileSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { CONFIG_PATH } from "./bench-calque.ts";
import { parseBenchArguments } from "./bench-conditions.ts";
import type { HashedFrame } from "./bench-frames.ts";
import { probeInvalidity } from "./bench-samples.ts";
import {
  LogFollower,
  loadBeforeChange,
  readMoveLog,
  rebuildsAfter,
  recoveryPaint,
  recoveryReachedMs,
  type SchemaResult,
  schemaDevArguments,
  schemaSetupSql,
  schemaSummary,
  withTriageNote,
} from "./bench-schema.ts";

const reference = "a".repeat(64);
const other = "b".repeat(64);

function frame(seq: number, timestampMs: number, grid: string): HashedFrame {
  return {
    seq,
    sessionId: 1,
    epochSeconds: timestampMs / 1_000,
    timestampMs,
    receivedMs: timestampMs,
    full: grid,
    grid,
  } as HashedFrame;
}

test("a schema run reads its own configuration, and each case starts from its shape", () => {
  const run = {
    directory: "/run",
    options: parseBenchArguments(["--schema-change", "config-add"]),
  };
  assert.deepEqual(schemaDevArguments(run), [
    "--config",
    "/run/calque.config.json",
  ]);
  assert.deepEqual(
    schemaDevArguments({ ...run, options: parseBenchArguments([]) }),
    [],
  );
  assert.match(schemaSetupSql("config-add"), /ADD COLUMN triage_note text/u);
  assert.match(schemaSetupSql("rewrite-whole"), /REPLICA IDENTITY FULL/u);
  assert.equal(schemaSetupSql("rename-suspend"), "");
});

test("config-add names triage_note on issues and nothing else", () => {
  const shipped = readFileSync(CONFIG_PATH, "utf8");
  const changed = JSON.parse(withTriageNote(shipped)) as {
    collections: { id: string; columns: { name: string }[] }[];
    commands: {
      id: string;
      collection: string;
      writableColumns: string[];
    }[];
  };
  const before = JSON.parse(shipped) as typeof changed;
  for (const collection of changed.collections) {
    const was = before.collections.find((each) => each.id === collection.id);
    assert.ok(was);
    const added = collection.columns.slice(was.columns.length);
    assert.deepEqual(
      added,
      collection.id === "issues"
        ? [{ name: "triage_note", postgresType: "text", nullable: true }]
        : [],
    );
  }
  for (const command of changed.commands) {
    const was = before.commands.find((each) => each.id === command.id);
    assert.ok(was);
    assert.deepEqual(
      command.writableColumns.slice(was.writableColumns.length),
      command.collection === "issues" ? ["triage_note"] : [],
    );
  }
  assert.throws(
    () => withTriageNote(withTriageNote(shipped)),
    /already names triage_note/u,
  );
});

test("a move's log is read for its own generation", () => {
  const lines = [
    "The source's schema changed (the column was renamed): moving to the next schema generation",
    "2026-11-18T10:00:00Z ERROR calque_engine::source: CDC runtime stopped without reconnecting failure=renamed",
    "Schema suspended: incidents.issues has no column priority.",
    "2026-11-18T10:00:11Z  INFO source_runtime::slot_owner: the stream skips every transaction until the first cut of schema generation 3",
    "2026-11-18T10:00:12Z  INFO source_runtime::schema_generation: the stream serves schema generation 3 from its first cut",
    "Schema generation 3: serving",
    "Schema generation 30: serving",
  ].map((text, index) => ({ text, atMs: 1_000 + index * 100 }));
  assert.deepEqual(readMoveLog(lines, 3), {
    boundaries: 1,
    serving: 1,
    notStarted: 0,
    stops: 1,
    schemaStops: 1,
    otherErrors: [],
    suspendedMs: 1_200,
    movingMs: 1_000,
    boundaryMs: 1_300,
    firstCutMs: 1_400,
    servingMs: 1_500,
  });
  const failed = readMoveLog(
    [
      "2026-11-18T10:00:00Z ERROR calque_engine::source: CDC runtime stopped without reconnecting failure=lease",
      "2026-11-18T10:00:00Z ERROR calque::dev: something else",
      "The next schema generation did not start, retrying every second: no",
    ].map((text) => ({ text, atMs: 0 })),
    3,
  );
  assert.equal(failed.stops, 1);
  assert.equal(failed.schemaStops, 0);
  assert.equal(failed.notStarted, 1);
  assert.equal(failed.otherErrors.length, 1);
});

test("the load before a change is read as the rate PostgreSQL committed it", () => {
  assert.deepEqual(loadBeforeChange(2_190, 10_000, 220), {
    rate: 220,
    completed: 2_190,
    ms: 10_000,
    transactionsPerSecond: 219,
  });
  assert.equal(loadBeforeChange(0, 0, 220).transactionsPerSecond, 0);
});

test("the page rebuilt once when it left ready once after the change and came back", () => {
  const log = [
    { state: "initializing", atMs: 0 },
    { state: "ready", atMs: 100 },
    { state: "initializing", atMs: 2_000 },
    { state: "preparing", atMs: 2_100 },
    { state: "initializing", atMs: 3_000 },
    { state: "ready", atMs: 4_000 },
  ];
  assert.deepEqual(rebuildsAfter(log, 1_000), { rebuilds: 1, readyMs: 4_000 });
  // Not back to ready yet.
  assert.deepEqual(rebuildsAfter(log.slice(0, 5), 1_000), {
    rebuilds: 1,
    readyMs: null,
  });
  assert.deepEqual(rebuildsAfter(log.slice(0, 2), 1_000), {
    rebuilds: 0,
    readyMs: null,
  });
  assert.equal(
    rebuildsAfter(
      [
        ...log,
        { state: "initializing", atMs: 5_000 },
        { state: "ready", atMs: 6_000 },
      ],
      1_000,
    ).rebuilds,
    2,
  );
});

test("the recovery ends at the later of the page's ready and the stable paint of the reference grid", () => {
  // The grid never changed: the frame on screen at ready is the paint.
  const unchanged = [frame(0, 1_000, reference), frame(1, 9_000, reference)];
  assert.equal(recoveryPaint(unchanged, 5_000, reference, []).matchMs, 5_000);
  // The grid was redrawn after ready: its first frame showing the reference.
  const redrawn = [
    frame(0, 1_000, reference),
    frame(1, 4_000, other),
    frame(2, 5_500, reference),
    frame(3, 9_000, reference),
  ];
  assert.equal(recoveryPaint(redrawn, 5_000, reference, []).matchMs, 5_500);
  // A grid that leaves the reference within the hold is contradicted.
  const flicker = [
    frame(0, 1_000, reference),
    frame(1, 6_000, other),
    frame(2, 6_500, reference),
  ];
  assert.notEqual(
    recoveryPaint(flicker, 5_000, reference, []).contradiction,
    null,
  );
});

test("frames stamped out of order make a recovery invalid for the instrument alone", () => {
  // As a schema run recorded it: after ready, two frames of the reference
  // arrived stamped out of order, then a third showed it again.
  const ready = 10_000;
  const reordered = [
    frame(10, ready - 13_373, reference),
    frame(11, ready - 13_340, other),
    frame(12, ready + 128, reference),
    frame(13, ready + 119, reference),
    frame(14, ready + 948, reference),
  ];
  const judgement = recoveryPaint(reordered, ready, reference, []);
  assert.equal(judgement.matchMs, ready + 128);
  assert.equal(judgement.contradiction, null);
  assert.equal(
    judgement.droppedFrameSignals[0]?.kind,
    "non-increasing-timestamp",
  );
  const invalid = probeInvalidity(judgement, {
    sessionClockPassed: true,
    frameClockPassed: true,
    wallDriftMs: 0,
    hostSleptMs: 0,
  });
  assert.equal(invalid.length, 1);
  assert.ok(invalid.every((entry) => entry.instrument));
  assert.equal(
    recoveryReachedMs(reordered, ready, reference, judgement),
    ready + 128,
  );
  // A signal before the match leaves none; the reached value is the earliest
  // frame showing the reference.
  const early = [
    frame(0, ready - 1_000, other),
    frame(1, ready + 200, other),
    frame(2, ready + 150, reference),
    frame(3, ready + 300, reference),
  ];
  const unmatched = recoveryPaint(early, ready, reference, []);
  assert.equal(unmatched.matchMs, null);
  assert.equal(
    recoveryReachedMs(early, ready, reference, unmatched),
    ready + 150,
  );
  // Neither a match nor a signal: nothing was reached.
  const never = [frame(0, ready - 1_000, other), frame(1, ready + 500, other)];
  assert.equal(
    recoveryReachedMs(
      never,
      ready,
      reference,
      recoveryPaint(never, ready, reference, []),
    ),
    null,
  );
});

test("a schema sample taken again leaves its slot to the next, and the second reading counts what it reached", () => {
  const sample = (
    kind: SchemaResult["kind"],
    n: number,
    verdict: SchemaResult["verdict"],
    recoveryMs: number | null,
  ): SchemaResult => ({
    name:
      kind === "schema" ? `schema-${String(n)}` : `schema-${kind}-${String(n)}`,
    kind,
    n,
    verdict,
    invalid: [],
    recoveryMs,
    reachedMs: recoveryMs,
    engagementPassed: true,
    record: {},
  });
  const summary = schemaSummary(
    "rename-suspend",
    [
      sample("warmup", 0, "PASS", 3_000),
      sample("schema", 0, "PASS", 2_700),
      sample("schema", 1, "INVALID_RUN", null),
      sample("schema", 2, "PASS", 3_300),
      sample("schema", 3, "PASS", 2_900),
      sample("traced", 0, "PASS", 4_000),
    ],
    [
      {
        n: 1,
        slot: 1,
        reasons: ["a dropped-frame signal fired"],
        valueMs: 2_800,
      },
    ],
  );
  assert.deepEqual(summary.recovery_ms, [2_700, 3_300, 2_900]);
  assert.deepEqual(summary.secondReading?.recovery_ms, [2_700, 2_800, 2_900]);
  assert.equal(summary.allPassed, true);
  assert.equal(summary.verdicts["schema-1"], "INVALID_RUN");
  const none = schemaSummary(
    "config-add",
    [sample("schema", 0, "PASS", 1)],
    [],
  );
  assert.equal(none.secondReading, undefined);
});

test("the log follower reads only what calque writes after it starts", async () => {
  const directory = mkdtempSync(join(tmpdir(), "bench-schema-"));
  try {
    const path = join(directory, "calque.log");
    writeFileSync(path, "Schema generation 1: serving\n");
    const follower = new LogFollower(path);
    appendFileSync(path, "Schema generation 2: serv");
    follower.poll();
    assert.deepEqual(follower.lines, []);
    const waited = follower.waitFor(
      (text) => text === "Schema generation 2: serving",
      0,
      5_000,
    );
    appendFileSync(path, "ing\n");
    assert.equal((await waited)?.text, "Schema generation 2: serving");
    assert.equal(
      await follower.waitFor((text) => text.includes("never"), 0, 50),
      null,
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

import assert from "node:assert/strict";
import test from "node:test";

import { gridWindowSql } from "./bench-oracle.ts";
import {
  announcedPreparation,
  announcedSeed,
  announcedUrl,
  canonicalBuckets,
  parseIncidentsE2eArguments,
  sqlRows,
  writeOrderProblems,
  type TrackedWrite,
} from "./incidents-e2e.ts";
import {
  buildIncidentsBindingsCheckArguments,
  INCIDENTS_BINDINGS,
  incidentsComposeEnvironment,
  incidentsComposeProject,
  formatSeconds,
  cutSchemaMarker,
  holdsSeededDataset,
  issueTagsRekeyedSql,
  isSeededDataset,
  localStateDestroyArguments,
  metadataPresentSql,
  parseIncidentsArguments,
} from "./incidents.ts";
import {
  CONTENT_DATASET_VERSION,
  CONTENT_SEEDING_MARKER,
  schemaMarkerSql,
} from "./bench-content-fixture.ts";
import { CORRECTED_PROFILE_SHA256, DATASET_VERSION } from "./bench-fixture.ts";

test("the compose project is the checkout's own, never the default one", () => {
  assert.equal(
    incidentsComposeProject("/work/worktrees/some-feature"),
    "some-feature-incidents",
  );
  assert.equal(
    incidentsComposeProject("/work/My Calque.Checkout"),
    "my-calque-checkout-incidents",
  );
  assert.equal(incidentsComposeProject("/work/..."), "calque-incidents");
});

test("the compose environment names the project and the port and keeps the rest", () => {
  const environment = incidentsComposeEnvironment(
    { PATH: "/bin", COMPOSE_PROJECT_NAME: "someone-elses" },
    55481,
    "/work/calque",
  );
  assert.deepEqual(environment, {
    PATH: "/bin",
    COMPOSE_PROJECT_NAME: "calque-incidents",
    CALQUE_POSTGRES_PORT: "55481",
  });
});

test("only the exact fixture version and profile count as seeded", () => {
  assert.equal(
    isSeededDataset(`${DATASET_VERSION} ${CORRECTED_PROFILE_SHA256}\n`),
    true,
  );
  assert.equal(isSeededDataset(""), false);
  assert.equal(isSeededDataset(`${DATASET_VERSION} ${"0".repeat(64)}`), false);
  assert.equal(
    isSeededDataset(`dataset.other ${CORRECTED_PROFILE_SHA256}`),
    false,
  );
});

/**
 * A source that answers the probe and fails like PostgreSQL on any other
 * statement naming `metadata` while the table does not exist. `marker` is the
 * schema's comment.
 */
function sourceWith(
  metadata: string | undefined,
  rekeyed = false,
  marker = "",
) {
  const statements: string[] = [];
  const sql = (statement: string) => {
    statements.push(statement);
    if (statement === metadataPresentSql)
      return metadata === undefined ? "f" : "t";
    if (statement === issueTagsRekeyedSql) return rekeyed ? "1" : "0";
    if (statement === schemaMarkerSql) return marker;
    if (metadata === undefined && statement.includes("incidents.metadata"))
      throw new Error(
        'psql failed: ERROR:  relation "incidents.metadata" does not exist',
      );
    return metadata ?? "";
  };
  return { sql, statements };
}

test("a fresh database is unseeded, and its metadata is never read", () => {
  const source = sourceWith(undefined);
  assert.equal(holdsSeededDataset(source.sql), false);
  assert.deepEqual(source.statements, [metadataPresentSql]);
});

test("a seeded database is read for its version and profile", () => {
  const seeded = sourceWith(`${DATASET_VERSION} ${CORRECTED_PROFILE_SHA256}`);
  assert.equal(holdsSeededDataset(seeded.sql), true);
  assert.equal(seeded.statements.length, 4);
  const other = sourceWith(`${DATASET_VERSION} ${"0".repeat(64)}`);
  assert.equal(holdsSeededDataset(other.sql), false);
});

test("a source an earlier run re-keyed is reseeded", () => {
  const rekeyed = sourceWith(
    `${DATASET_VERSION} ${CORRECTED_PROFILE_SHA256}`,
    true,
  );
  assert.equal(holdsSeededDataset(rekeyed.sql), false);
  assert.equal(rekeyed.statements.at(-1), issueTagsRekeyedSql);
});

test("the schema's comment tells v1 from a finished content seed", () => {
  const seeded = `${DATASET_VERSION} ${CORRECTED_PROFILE_SHA256}`;
  const v1 = sourceWith(seeded);
  assert.equal(holdsSeededDataset(v1.sql, "v1"), true);
  assert.equal(holdsSeededDataset(v1.sql, "content-v1"), false);
  assert.equal(v1.statements.at(-1), schemaMarkerSql);
  const content = sourceWith(seeded, false, CONTENT_DATASET_VERSION);
  assert.equal(holdsSeededDataset(content.sql), false);
  assert.equal(holdsSeededDataset(content.sql, "content-v1"), true);
  // A content seed that stopped before its checks passed is neither.
  const partial = sourceWith(seeded, false, CONTENT_SEEDING_MARKER);
  assert.equal(holdsSeededDataset(partial.sql, "v1"), false);
  assert.equal(holdsSeededDataset(partial.sql, "content-v1"), false);
});

test("the 100k cut and the whole fixture are told apart by the schema's comment", () => {
  const seeded = `${DATASET_VERSION} ${CORRECTED_PROFILE_SHA256}`;
  const whole = sourceWith(seeded);
  assert.equal(holdsSeededDataset(whole.sql, "v1", "1m"), true);
  assert.equal(holdsSeededDataset(whole.sql, "v1", "100k"), false);
  const cut = sourceWith(seeded, false, cutSchemaMarker("v1"));
  assert.equal(holdsSeededDataset(cut.sql, "v1", "100k"), true);
  assert.equal(holdsSeededDataset(cut.sql, "v1", "1m"), false);
  assert.equal(holdsSeededDataset(cut.sql, "content-v1", "100k"), false);
  const contentCut = sourceWith(seeded, false, cutSchemaMarker("content-v1"));
  assert.equal(holdsSeededDataset(contentCut.sql, "content-v1", "100k"), true);
  assert.equal(holdsSeededDataset(contentCut.sql, "content-v1", "1m"), false);
  assert.equal(holdsSeededDataset(contentCut.sql, "v1", "100k"), false);
});

test("state from a recreated source is removed locally, and otherwise with its slot", () => {
  const state = (systemIdentifier: unknown) =>
    JSON.stringify({ sourceIdentity: { systemIdentifier } });
  const destroy = ["destroy", "--state-dir", "/state", "--yes"];
  assert.deepEqual(
    localStateDestroyArguments(
      "/state",
      state("7689553678032322603"),
      "7689553678032322603\n",
    ),
    destroy,
  );
  assert.deepEqual(
    localStateDestroyArguments(
      "/state",
      state("7689553678032322603"),
      "7689597241184026668",
    ),
    [...destroy, "--local-state-only"],
  );
  assert.deepEqual(
    localStateDestroyArguments("/state", "{}", "7689597241184026668"),
    destroy,
  );
  assert.deepEqual(
    localStateDestroyArguments("/state", state(42), "7689597241184026668"),
    destroy,
  );
});

test("the launcher takes --no-open, --fresh, --dataset and --rows and refuses anything else", () => {
  assert.deepEqual(parseIncidentsArguments([]), {
    open: true,
    fresh: false,
    dataset: "v1",
    rows: "1m",
  });
  assert.deepEqual(parseIncidentsArguments(["--", "--no-open", "--fresh"]), {
    open: false,
    fresh: true,
    dataset: "v1",
    rows: "1m",
  });
  assert.deepEqual(
    parseIncidentsArguments(["--", "--dataset", "content-v1", "--no-open"]),
    { open: false, fresh: false, dataset: "content-v1", rows: "1m" },
  );
  assert.deepEqual(parseIncidentsArguments(["--dataset", "v1"]), {
    open: true,
    fresh: false,
    dataset: "v1",
    rows: "1m",
  });
  assert.deepEqual(parseIncidentsArguments(["--rows", "100k"]), {
    open: true,
    fresh: false,
    dataset: "v1",
    rows: "100k",
  });
  assert.deepEqual(
    parseIncidentsArguments(["--rows", "100k", "--dataset", "content-v1"]),
    { open: true, fresh: false, dataset: "content-v1", rows: "100k" },
  );
  for (const refused of [
    ["--rows"],
    ["--rows", "10m"],
    ["--rows", "100k", "--rows", "1m"],
  ])
    assert.throws(
      () => parseIncidentsArguments(refused),
      /usage/u,
      `${refused}`,
    );
  assert.throws(() => parseIncidentsArguments(["--open"]), /usage/u);
  assert.throws(() => parseIncidentsArguments(["--dataset"]), /usage/u);
  assert.throws(
    () => parseIncidentsArguments(["--dataset", "content-v2"]),
    /usage/u,
  );
  assert.throws(
    () => parseIncidentsArguments(["--dataset", "--fresh"]),
    /usage/u,
  );
  assert.throws(
    () =>
      parseIncidentsArguments(["--dataset", "v1", "--dataset", "content-v1"]),
    /usage/u,
  );
});

test("the e2e passes --dataset through and refuses anything else", () => {
  assert.deepEqual(parseIncidentsE2eArguments([]), { dataset: "v1" });
  assert.deepEqual(
    parseIncidentsE2eArguments(["--", "--dataset", "content-v1"]),
    { dataset: "content-v1" },
  );
  assert.deepEqual(parseIncidentsE2eArguments(["--dataset", "v1"]), {
    dataset: "v1",
  });
  for (const arguments_ of [
    ["--dataset"],
    ["--dataset", "v2"],
    ["--fresh"],
    ["--dataset", "v1", "--no-open"],
  ])
    assert.throws(() => parseIncidentsE2eArguments(arguments_), /usage/u);
});

test("the bindings check renders the page's pinned config", () => {
  assert.deepEqual(buildIncidentsBindingsCheckArguments(), [
    "client/packages/config/bin/calque-bindings.js",
    "--config",
    INCIDENTS_BINDINGS.config,
    "--out",
    INCIDENTS_BINDINGS.output,
    "--check",
  ]);
  assert.equal(formatSeconds(77_640), "77.6");
});

test("the e2e reads the launcher's announcements and nothing like them", () => {
  const output = [
    "Seeded the fixture in 212.4 s.",
    "calque dev prepared the working set in 77.6 s (fresh state).",
    "Open: http://127.0.0.1:55488/",
  ].join("\n");
  assert.equal(announcedUrl(output), "http://127.0.0.1:55488/");
  assert.deepEqual(announcedPreparation(output), {
    seconds: 77.6,
    state: "fresh",
  });
  assert.equal(announcedSeed(output), 212.4);
  assert.equal(
    announcedUrl("[page]   ➜  Local:   http://127.0.0.1:55488/"),
    undefined,
  );
  assert.equal(announcedPreparation("calque dev prepared"), undefined);
  assert.equal(announcedSeed("Fixture already seeded"), undefined);
});

test("psql rows split on newlines and \\u0001", () => {
  assert.deepEqual(sqlRows("a\u0001b\nc\u0001\n\n"), [
    ["a", "b"],
    ["c", ""],
  ]);
  assert.deepEqual(sqlRows(""), []);
});

test("buckets compare in one order whatever order they came in", () => {
  assert.deepEqual(
    canonicalBuckets([
      { status: "unresolved", priority: "10", count: 1 },
      { status: "archived", priority: "2", count: 3 },
      { status: "unresolved", priority: "9", count: 2 },
    ]),
    [
      { status: "archived", priority: "2", count: 3 },
      { status: "unresolved", priority: "9", count: 2 },
      { status: "unresolved", priority: "10", count: 1 },
    ],
  );
});

const orderedWrite: TrackedWrite = {
  sequence: 1,
  collection: "comments",
  operation: "insert",
  key: "c7dad7f0-b416-4512-b67a-e755002431b0",
  expected: "body",
  calledAt: 100,
  submittedAt: 104,
  mutationId: "5f3c31cc-3684-4c14-aef5-c30e6236cf15",
  state: "canonical",
  canonicalAt: 160,
  optimisticShownAt: 110,
  confirmedShownAt: 162,
  failure: null,
};

test("a write shown optimistic, then canonical, then confirmed has no problem", () => {
  assert.deepEqual(writeOrderProblems(orderedWrite), []);
});

test("every way a write can miss the order is named", () => {
  const name = "insert comments c7dad7f0-b416-4512-b67a-e755002431b0";
  assert.deepEqual(
    writeOrderProblems({ ...orderedWrite, optimisticShownAt: 160 }),
    [`${name} was shown optimistic only after its echo`],
  );
  assert.deepEqual(
    writeOrderProblems({ ...orderedWrite, optimisticShownAt: null }),
    [`${name} was never on screen while optimistic`],
  );
  assert.deepEqual(
    writeOrderProblems({ ...orderedWrite, confirmedShownAt: null }),
    [`${name} was never on screen confirmed`],
  );
  assert.deepEqual(writeOrderProblems({ ...orderedWrite, submittedAt: null }), [
    `${name} was never submitted`,
  ]);
  assert.deepEqual(
    writeOrderProblems({
      ...orderedWrite,
      state: "rejected",
      failure: "denied",
      canonicalAt: null,
      confirmedShownAt: null,
    }),
    [
      `${name} ended rejected: denied`,
      `${name} never became canonical`,
      `${name} was never on screen confirmed`,
    ],
  );
});

test("a grid window is read back from PostgreSQL under the grid's total order", () => {
  assert.equal(
    gridWindowSql("all", "newest", 400_000, 20),
    "SELECT id FROM incidents.events ORDER BY occurred_at DESC, id DESC OFFSET 400000 LIMIT 20",
  );
  assert.equal(
    gridWindowSql("error", "oldest", 0, 11),
    "SELECT id FROM incidents.events WHERE level = 'error' ORDER BY occurred_at ASC, id ASC OFFSET 0 LIMIT 11",
  );
  assert.throws(() => gridWindowSql("error' OR true --", "newest", 0, 1));
});

import assert from "node:assert/strict";
import test from "node:test";

import {
  answerOf,
  NARROW_SETS,
  PROBE_REQUESTS,
  roundRequests,
} from "./bench-bootstrap-probe.ts";
import { parseBenchArguments } from "./bench-conditions.ts";

const EVERY_TABLE = [
  "incidents.comments",
  "incidents.events",
  "incidents.issue_tags",
  "incidents.issues",
  "incidents.memberships",
  "incidents.projects",
  "incidents.users",
];

test("a round posts the first n narrow sets and every table for the rest", () => {
  const round = roundRequests(3, EVERY_TABLE);
  assert.equal(round.length, PROBE_REQUESTS);
  assert.deepEqual(
    round.map((request) => request.set),
    [0, 1, 2, null, null, null, null, null],
  );
  assert.deepEqual(round[2]?.tables, ["incidents.events"]);
  assert.deepEqual(round[7]?.tables, EVERY_TABLE);
  assert.equal(
    roundRequests(8, EVERY_TABLE).every((request) => request.set !== null),
    true,
  );
  for (const set of NARROW_SETS)
    assert.ok(set.length > 0 && set.length < EVERY_TABLE.length);
  assert.throws(() => roundRequests(1, ["incidents.issues"]), /no table/u);
});

test("a plan is exact when it installs one artifact of its required tables", () => {
  const plan = (tables: string[][], requiredTables = ["table:e:g1"]) =>
    JSON.stringify({
      requiredTables,
      artifacts: tables.map((artifactTables) => ({
        tables: artifactTables,
        snapshotCursor: "cursor-1",
        decodedBytes: 10,
        selectedRepresentation: { encoding: "zstd", encodedBytes: 4 },
      })),
    });
  assert.deepEqual(answerOf(plan([["table:e:g1"]]), ["incidents.events"]), {
    failure: null,
    exact: true,
    tables: 1,
    installed: ["table:e:g1"],
    cursor: "cursor-1",
    encoding: "zstd",
    encodedBytes: 4,
    decodedBytes: 10,
  });
  assert.equal(
    answerOf(plan([["table:e:g1", "table:i:g1"]]), ["incidents.events"]).exact,
    false,
  );
  assert.equal(
    answerOf(
      plan([["table:e:g1"], ["table:i:g1"]], ["table:e:g1", "table:i:g1"]),
      ["incidents.events", "incidents.issues"],
    ).exact,
    false,
  );
  assert.throws(
    () =>
      answerOf(plan([["table:e:g1"]]), [
        "incidents.events",
        "incidents.issues",
      ]),
    /requires 1 tables, not the 2/u,
  );
});

test("--bootstrap-probe takes 0 to 8 narrow sets beside the realtime session", () => {
  assert.equal(
    parseBenchArguments(["--realtime-only", "--bootstrap-probe", "3"])
      .bootstrapProbe,
    3,
  );
  assert.equal(
    parseBenchArguments(["--realtime-only", "--bootstrap-probe", "0"])
      .bootstrapProbe,
    0,
  );
  assert.throws(
    () => parseBenchArguments(["--realtime-only", "--bootstrap-probe", "9"]),
    /0 to 8/u,
  );
  assert.throws(
    () => parseBenchArguments(["--bootstrap-probe", "3"]),
    /needs --realtime-only/u,
  );
});

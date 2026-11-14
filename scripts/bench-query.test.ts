import assert from "node:assert/strict";
import test from "node:test";

import { QUERY_VIEW } from "./bench-oracle.ts";
import { queryInvalidity, queryProblems } from "./bench-query.ts";
import { aboveWireMs, wireByPurpose } from "./bench-results.ts";

const expected = { rows: 5, ids: ["e9", "e7", "e5", "e3", "e1"] };

function grid(overrides: Record<string, unknown> = {}) {
  return {
    view: { level: QUERY_VIEW.level, sort: QUERY_VIEW.sort },
    rows: 5,
    top: 0,
    ids: ["e9", "e7", "e5"],
    ...overrides,
  } as Parameters<typeof queryProblems>[1];
}

test("a grid that shows PostgreSQL's filtered and sorted result has no problem", () => {
  assert.deepEqual(queryProblems(expected, grid()), []);
  assert.deepEqual(
    queryProblems(expected, grid({ ids: ["e9", "e7", "e5", "e3", "e1"] })),
    [],
  );
});

test("each way the grid can differ from PostgreSQL is a problem of its own", () => {
  assert.deepEqual(queryProblems(expected, null), ["the grid shows no result"]);
  const problems = (overrides: Record<string, unknown>) =>
    queryProblems(expected, grid(overrides));
  assert.match(
    problems({ view: { level: "all", sort: QUERY_VIEW.sort } })[0] ?? "",
    /controls read all and newest/u,
  );
  assert.match(
    problems({ view: { level: QUERY_VIEW.level, sort: "oldest" } })[0] ?? "",
    /controls read error and oldest/u,
  );
  assert.match(problems({ rows: 6 })[0] ?? "", /shows 6 matches.*holds 5/u);
  assert.match(problems({ top: 3 })[0] ?? "", /scrolled to row 3/u);
  assert.match(problems({ ids: [] })[0] ?? "", /shows 0 ids/u);
  assert.match(
    problems({ ids: ["e9", "e7", "e5", "e3", "e1", "e0"] })[0] ?? "",
    /shows 6 ids/u,
  );
  // Two newest errors swapped: right rows, wrong order.
  assert.deepEqual(problems({ ids: ["e7", "e9", "e5"] }), [
    "the ids in view differ from PostgreSQL's newest errors",
  ]);
});

const sound = {
  sessionClockPassed: true,
  frameClockPassed: true,
  wallDriftMs: 0,
  hostSleptMs: 0,
  inputAtMs: 1_000,
  calledAtMs: 990,
  returnedAtMs: 1_040,
};

test("the input event's stamp must lie within the calls that made it", () => {
  assert.deepEqual(queryInvalidity(null, sound), []);
  // Within the tolerance of either end.
  assert.deepEqual(queryInvalidity(null, { ...sound, inputAtMs: 986 }), []);
  assert.deepEqual(queryInvalidity(null, { ...sound, inputAtMs: 1_044 }), []);
  for (const inputAtMs of [980, 1_050, null])
    assert.deepEqual(
      queryInvalidity(null, { ...sound, inputAtMs }).map(
        (entry) => entry.reason,
      ),
      ["the input event's stamp lies outside the calls that made it"],
      String(inputAtMs),
    );
});

test("a query is invalid for a probe's reasons too", () => {
  const reasons = queryInvalidity(
    {
      contradiction: null,
      droppedFrameSignals: [{ kind: "screencast-hidden" }],
    },
    { ...sound, sessionClockPassed: false, wallDriftMs: 9 },
  ).map((entry) => entry.reason);
  assert.equal(reasons.length, 3);
  assert.ok(reasons.includes("a dropped-frame signal fired"));
  assert.ok(reasons.includes("the session's clock check failed"));
  assert.ok(reasons.some((reason) => /wall clock moved 9\.0 ms/u.test(reason)));
});

test("wire bytes are filed by the route a request asked", () => {
  const request = (
    url: string,
    encoded: number,
    state = "finished",
    chunkEncoded = 0,
    status: number | null = 200,
    contentLength: number | null = null,
  ) => ({ url, state, encoded, chunkEncoded, status, contentLength });
  const origin = "https://127.0.0.1:4443";
  const requests = [
    request(`${origin}/`, 1_200),
    request(`${origin}/assets/index.js`, 30_000),
    request(`${origin}/_calque/v1/bootstrap`, 300),
    request(`${origin}/_calque/v1/bootstrap`, 150),
    request(
      `${origin}/_calque/v1/artifact-objects/sha256:abc?<redacted>`,
      7_490_000,
    ),
    request(`${origin}/_calque/v1/changes?<redacted>`, 4_000),
    request(`${origin}/_calque/v1/changes?<redacted>`, 0, "pending", 900),
    request(`${origin}/_calque/v1/changes?<redacted>`, 0, "failed", 100, null),
  ];
  const bytes = wireByPurpose(requests);
  assert.deepEqual(bytes, {
    plan: 450,
    artifact: 7_490_000,
    catchUp: 5_000,
    page: 31_200,
    total: 7_526_650,
  });
  // A streamed body Chromium ended as canceled counts at its declared length.
  assert.equal(
    wireByPurpose([
      request(
        `${origin}/_calque/v1/artifact-objects/sha256:abc?<redacted>`,
        0,
        "failed",
        1_240_704,
        200,
        7_531_467,
      ),
    ]).artifact,
    7_531_467,
  );
  // A page with its plan rendered in asks no bootstrap.
  assert.equal(wireByPurpose([request(`${origin}/`, 9_000)]).plan, 0);
});

test("above the wire is the paint minus the time the bytes need on the link", () => {
  const sample = {
    verdict: "PASS",
    valueMs: 747.5,
    totalBytes: 7_725_000,
  } as const;
  // 7,725,000 bytes are 61,800,000 bits: 618 ms at 100 Mbit/s.
  assert.equal(aboveWireMs(sample, 100), 129.5);
  assert.equal(aboveWireMs(sample, 50), 747.5 - 1_236);
});

test("above the wire is null without a passed sample, a link rate or bytes", () => {
  const sample = {
    verdict: "PASS",
    valueMs: 747.5,
    totalBytes: 7_725_000,
  } as const;
  assert.equal(aboveWireMs({ ...sample, verdict: "DNC" }, 100), null);
  assert.equal(aboveWireMs({ ...sample, valueMs: null }, 100), null);
  assert.equal(aboveWireMs(sample, null), null);
  assert.equal(aboveWireMs({ ...sample, totalBytes: 0 }, 100), null);
});

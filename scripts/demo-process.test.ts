import assert from "node:assert/strict";
import test from "node:test";

import {
  exitWithin,
  restorePlan,
  type DemoExit,
  type FoundDemo,
} from "./demo-process.ts";

const LAUNCHER_STATE = ["catalog.sqlite3", "objects", "state.json"];

test("a source found running is left alone, with the state the run wrote", () => {
  const found: FoundDemo = { postgres: "running", stateEntries: null };
  assert.deepEqual(restorePlan(found, LAUNCHER_STATE), []);
});

test("a source found stopped is stopped again, with the state the run wrote", () => {
  const found: FoundDemo = { postgres: "stopped", stateEntries: null };
  assert.deepEqual(restorePlan(found, LAUNCHER_STATE), [
    { kind: "compose-stop" },
  ]);
});

test("a source the run created goes down with its volume and the .calque-dev it created", () => {
  const found: FoundDemo = { postgres: "absent", stateEntries: null };
  assert.deepEqual(restorePlan(found, LAUNCHER_STATE), [
    { kind: "compose-down-volumes" },
    { kind: "remove-state-directory" },
  ]);
});

test("a run that wrote no state takes only its source down", () => {
  const found: FoundDemo = { postgres: "absent", stateEntries: null };
  assert.deepEqual(restorePlan(found, null), [
    { kind: "compose-down-volumes" },
  ]);
});

test("a .calque-dev without state.json loses only the entries the run added", () => {
  const found: FoundDemo = {
    postgres: "absent",
    stateEntries: ["incidents"],
  };
  assert.deepEqual(restorePlan(found, ["state.json", "incidents", "objects"]), [
    { kind: "compose-down-volumes" },
    { kind: "remove-state-entry", entry: "objects" },
    { kind: "remove-state-entry", entry: "state.json" },
  ]);
});

test("an empty .calque-dev the run found is left in place, emptied of what it added", () => {
  const found: FoundDemo = { postgres: "absent", stateEntries: [] };
  assert.deepEqual(restorePlan(found, ["state.json"]), [
    { kind: "compose-down-volumes" },
    { kind: "remove-state-entry", entry: "state.json" },
  ]);
});

test("state the run found is kept, whatever the run added beside it", () => {
  const found: FoundDemo = {
    postgres: "absent",
    stateEntries: ["state.json", "catalog.sqlite3"],
  };
  assert.deepEqual(restorePlan(found, [...LAUNCHER_STATE, "builds"]), [
    { kind: "compose-down-volumes" },
  ]);
});

test("a plan over what the first restore left removes nothing more", () => {
  const found: FoundDemo = {
    postgres: "absent",
    stateEntries: ["incidents"],
  };
  assert.deepEqual(restorePlan(found, ["incidents"]), [
    { kind: "compose-down-volumes" },
  ]);
});

function armedTimers(): number {
  return process
    .getActiveResourcesInfo()
    .filter((resource) => resource === "Timeout").length;
}

const EXIT: DemoExit = { code: 0, signal: null };

test("a child that exits first leaves no timer armed", async () => {
  const before = armedTimers();
  // The instrument sees an armed timer, so its silence below means something.
  const control = setTimeout(() => undefined, 60_000);
  assert.equal(armedTimers(), before + 1);
  clearTimeout(control);

  assert.deepEqual(await exitWithin(Promise.resolve(EXIT), 60_000), EXIT);
  assert.equal(armedTimers(), before);
});

test("a child still running when the period ends reads as undefined", async () => {
  const before = armedTimers();
  assert.equal(
    await exitWithin(new Promise<DemoExit>(() => {}), 10),
    undefined,
  );
  assert.equal(armedTimers(), before);
});

test("a failed wait for the exit leaves no timer armed either", async () => {
  const before = armedTimers();
  await assert.rejects(
    exitWithin(Promise.reject(new Error("lost")), 60_000),
    /lost/u,
  );
  assert.equal(armedTimers(), before);
});

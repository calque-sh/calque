import assert from "node:assert/strict";
import test from "node:test";

import {
  workspaceBuildOrder,
  type BuildOrderPackage,
} from "./lib/workspace-build-order.ts";

// The client workspace's packages, less `@calque/quickstart`, each with some
// of the dependencies its package.json declares. `@calque/client` sorts first
// alphabetically and imports `@calque/worker`.
const workspace: readonly BuildOrderPackage[] = [
  {
    name: "@calque/client",
    dependencies: ["@calque/protocol", "@calque/worker"],
  },
  { name: "@calque/config", dependencies: [] },
  { name: "@calque/core", dependencies: [] },
  {
    name: "@calque/incidents",
    dependencies: ["@calque/client", "@calque/react", "react"],
  },
  { name: "@calque/protocol", dependencies: ["@bufbuild/protobuf"] },
  { name: "@calque/react", dependencies: ["@calque/client", "react"] },
  {
    name: "@calque/worker",
    dependencies: ["@calque/core", "@calque/protocol"],
  },
];

const position = (ordered: readonly BuildOrderPackage[], name: string) => {
  const index = ordered.findIndex((entry) => entry.name === name);
  assert.notEqual(index, -1, `${name} is missing from the build order`);
  return index;
};

test("every package is built after each workspace package it depends on", () => {
  const ordered = workspaceBuildOrder(workspace);
  assert.equal(ordered.length, workspace.length);
  const names = new Set(workspace.map((entry) => entry.name));
  for (const entry of workspace) {
    for (const dependency of entry.dependencies) {
      if (!names.has(dependency)) continue;
      assert.ok(
        position(ordered, dependency) < position(ordered, entry.name),
        `${dependency} must be built before ${entry.name}`,
      );
    }
  }
  assert.ok(
    position(ordered, "@calque/worker") < position(ordered, "@calque/client"),
    "alphabetical order builds @calque/client first; the build order must not",
  );
});

test("the order is deterministic and alphabetical where the graph leaves it free", () => {
  const ordered = workspaceBuildOrder(workspace).map((entry) => entry.name);
  assert.deepEqual(ordered, [
    "@calque/config",
    "@calque/core",
    "@calque/protocol",
    "@calque/worker",
    "@calque/client",
    "@calque/react",
    "@calque/incidents",
  ]);
  const reversed = workspaceBuildOrder([...workspace].reverse()).map(
    (entry) => entry.name,
  );
  assert.deepEqual(
    reversed,
    ordered,
    "input order must not leak into the result",
  );
});

test("dependencies outside the workspace do not block a package", () => {
  const ordered = workspaceBuildOrder([
    { name: "leaf", dependencies: ["react", "@bufbuild/protobuf"] },
  ]);
  assert.deepEqual(
    ordered.map((entry) => entry.name),
    ["leaf"],
  );
});

test("a workspace dependency cycle is refused, naming its members", () => {
  assert.throws(
    () =>
      workspaceBuildOrder([
        { name: "a", dependencies: ["b"] },
        { name: "b", dependencies: ["c"] },
        { name: "c", dependencies: ["a"] },
        { name: "root", dependencies: [] },
      ]),
    /workspace dependency cycle among: a, b, c$/u,
  );
  assert.throws(
    () => workspaceBuildOrder([{ name: "self", dependencies: ["self"] }]),
    /workspace dependency cycle among: self$/u,
  );
});

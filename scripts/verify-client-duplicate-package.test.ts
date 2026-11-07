import assert from "node:assert/strict";
import test from "node:test";

import { duplicatePackageFault } from "./verify-client-duplicate-package.ts";

const NONE = { auth: 0, fetch: 0, worker: 0 };
const MESSAGE =
  "SyncClientOptions schema is invalid: pass the `schema` that calque.generated.ts exports, from a bundle that holds one copy of @calque/client";
const REFUSAL = new TypeError(MESSAGE);

test("an exact refusal before any effect passes", () => {
  assert.equal(duplicatePackageFault(REFUSAL, NONE), undefined);
});

test("a foreign schema accepted, or refused for another reason, fails", () => {
  assert.match(
    duplicatePackageFault(undefined, NONE) ?? "",
    /was not rejected exactly: undefined/u,
  );
  assert.match(
    duplicatePackageFault(new Error(MESSAGE), NONE) ?? "",
    /was not rejected exactly/u,
  );
  assert.match(
    duplicatePackageFault(new TypeError("endpoint is invalid"), NONE) ?? "",
    /was not rejected exactly: TypeError: endpoint is invalid/u,
  );
});

test("a refusal after auth, fetch or a worker ran fails", () => {
  for (const effect of ["auth", "fetch", "worker"] as const)
    assert.equal(
      duplicatePackageFault(REFUSAL, { ...NONE, [effect]: 1 }),
      `duplicate package rejection caused effects: ${JSON.stringify({ ...NONE, [effect]: 1 })}`,
    );
});

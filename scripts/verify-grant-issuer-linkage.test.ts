import assert from "node:assert/strict";
import test from "node:test";

import type {
  CargoDependency,
  CargoMetadata,
  CargoPackage,
} from "./verify-grant-issuer-linkage.ts";
import {
  findForbiddenIssuerDependents,
  ISSUER_CRATE,
  readWorkspaceMetadata,
} from "./verify-grant-issuer-linkage.ts";

const metadata = (packages: readonly CargoPackage[]): CargoMetadata => ({
  packages,
});
const dependency = (
  name: string,
  kind: string | null = null,
): CargoDependency => ({ name, kind });

test("a normal dependency on the issuer is a violation", () => {
  assert.deepEqual(
    findForbiddenIssuerDependents(
      metadata([
        {
          name: "calque",
          dependencies: [dependency("authz"), dependency(ISSUER_CRATE)],
        },
      ]),
    ),
    ["calque"],
  );
});

test("dev and build dependencies on the issuer are allowed", () => {
  assert.deepEqual(
    findForbiddenIssuerDependents(
      metadata([
        {
          name: "dataset-grant",
          dependencies: [dependency(ISSUER_CRATE, "dev")],
        },
        {
          name: "some-build-script-user",
          dependencies: [dependency(ISSUER_CRATE, "build")],
        },
      ]),
    ),
    [],
  );
});

test("the issuer's own dependencies are not reported", () => {
  assert.deepEqual(
    findForbiddenIssuerDependents(
      metadata([{ name: ISSUER_CRATE, dependencies: [dependency("ring")] }]),
    ),
    [],
  );
});

test("violations are reported sorted", () => {
  assert.deepEqual(
    findForbiddenIssuerDependents(
      metadata([
        { name: "zeta", dependencies: [dependency(ISSUER_CRATE)] },
        { name: "alpha", dependencies: [dependency(ISSUER_CRATE)] },
      ]),
    ),
    ["alpha", "zeta"],
  );
});

test("the real workspace has no forbidden issuer dependent", () => {
  const workspace = readWorkspaceMetadata();
  assert.ok(
    workspace.packages.some((pkg) => pkg.name === ISSUER_CRATE),
    `${ISSUER_CRATE} must be a workspace member for this gate to mean anything`,
  );
  assert.deepEqual(findForbiddenIssuerDependents(workspace), []);
});

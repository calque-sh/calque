import { cpSync, mkdirSync, readFileSync, rmSync, symlinkSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";

/**
 * The two built copies of `@calque/client/internal`, as this gate calls them.
 * `import()` on a computed specifier is typed `any`.
 */
interface DuplicatePackageInternal {
  createGeneratedClientSchema(descriptor: {
    collections: readonly unknown[];
  }): object;
  createInternalColumnarClient(
    options: {
      endpoint: string;
      auth: () => { bearerToken: string };
      schema: object;
      memoryBudgetBytes: number;
      deviceId: string;
      fetch: () => never;
      clientHeapReserveBytes: number;
    },
    workerFactory: () => never,
  ): unknown;
}

/** What each injected effect was called; a refusal before the client starts calls none. */
export interface DuplicatePackageEffects {
  readonly auth: number;
  readonly fetch: number;
  readonly worker: number;
}

/**
 * Why copy B's handling of copy A's schema fails the gate, or undefined when
 * it refused the schema exactly, before calling auth, fetch or the worker
 * factory.
 */
export function duplicatePackageFault(
  rejection: unknown,
  effects: DuplicatePackageEffects,
): string | undefined {
  if (
    !(rejection instanceof TypeError) ||
    rejection.message !==
      "SyncClientOptions schema is invalid: pass the `schema` that calque.generated.ts exports, from a bundle that holds one copy of @calque/client"
  )
    return `duplicate package schema was not rejected exactly: ${String(rejection)}`;
  if (effects.auth !== 0 || effects.fetch !== 0 || effects.worker !== 0)
    return `duplicate package rejection caused effects: ${JSON.stringify(effects)}`;
  return undefined;
}

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const clientRoot = join(root, "client/packages/client");
const workspace = join(root, "target/client-duplicate-package");
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  rmSync(workspace, { force: true, recursive: true });
  mkdirSync(workspace, { recursive: true });

  try {
    const copyA = makePackageCopy("copy-a");
    const copyB = makePackageCopy("copy-b");
    const packageA: DuplicatePackageInternal = await import(
      `${pathToFileURL(join(copyA, "dist/internal.js")).href}?copy=a`
    );
    const packageB: DuplicatePackageInternal = await import(
      `${pathToFileURL(join(copyB, "dist/internal.js")).href}?copy=b`
    );
    const schemaFromA = packageA.createGeneratedClientSchema({
      collections: [],
    });
    const effects = { auth: 0, fetch: 0, worker: 0 };
    let rejection: unknown;
    try {
      packageB.createInternalColumnarClient(
        {
          endpoint: "https://sync.example.test",
          auth: () => {
            effects.auth += 1;
            return { bearerToken: "secret" };
          },
          schema: schemaFromA,
          memoryBudgetBytes: 64 * 1024 * 1024,
          deviceId: "duplicate-package",
          fetch: () => {
            effects.fetch += 1;
            throw new Error("fetch must not run");
          },
          clientHeapReserveBytes: 8 * 1024 * 1024,
        },
        () => {
          effects.worker += 1;
          throw new Error("worker factory must not run");
        },
      );
    } catch (error) {
      rejection = error;
    }
    const fault = duplicatePackageFault(rejection, effects);
    if (fault !== undefined) throw new Error(fault);
    process.stdout.write(
      "Client duplicate-package verification: PASS (copy A capability rejected by copy B before worker/auth/fetch)\n",
    );
  } finally {
    rmSync(workspace, { force: true, recursive: true });
  }
}

function makePackageCopy(name: string): string {
  const destination = join(workspace, name);
  mkdirSync(destination, { recursive: true });
  cpSync(join(clientRoot, "dist"), join(destination, "dist"), {
    recursive: true,
  });
  const packageJson = readFileSync(join(clientRoot, "package.json"), "utf8");
  // Node reads the copy's own package.json (`"type": "module"`) to load its
  // `dist/` as ES modules.
  cpSync(join(clientRoot, "package.json"), join(destination, "package.json"));
  if (packageJson.length === 0)
    throw new Error("client package manifest is empty");
  symlinkSync(
    join(clientRoot, "node_modules"),
    join(destination, "node_modules"),
  );
  return destination;
}

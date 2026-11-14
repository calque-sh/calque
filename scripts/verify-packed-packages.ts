/**
 * `pnpm verify:packed`: packs every published `@calque` package as the
 * registry would receive it, installs the tarballs into an empty project
 * outside the workspace, and builds a React page on them with the packed
 * Vite plugin, which renders the page's bindings, then typechecks it with
 * the packed declarations and checks the render with the packed
 * `calque-bindings`. The packed `@calque/grant` creates a key, mints a grant
 * and a tenant order in a Node script, and its `calque-grant` command does the
 * same from the shell. A file a consumer needs that a tarball lacks fails here
 * rather than after a release.
 */
import { spawnSync } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

interface Manifest {
  readonly name: string;
  readonly version: string;
  readonly private?: boolean;
  readonly dependencies?: Record<string, string>;
  readonly devDependencies?: Record<string, string>;
}

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packagesDirectory = join(root, "client/packages");
const quickstart = join(root, "client/examples/quickstart");

const manifest = (directory: string): Manifest =>
  JSON.parse(readFileSync(join(directory, "package.json"), "utf8")) as Manifest;

function run(command: string, arguments_: readonly string[], cwd: string) {
  const result = spawnSync(command, arguments_, { cwd, stdio: "inherit" });
  if (result.status !== 0)
    throw new Error(
      `${[command, ...arguments_].join(" ")} in ${cwd} exited ${String(result.status ?? result.signal)}`,
    );
}

function pin(source: Manifest, name: string): string {
  const version = source.devDependencies?.[name] ?? source.dependencies?.[name];
  if (version === undefined)
    throw new Error(`${source.name} does not pin ${name}`);
  return version;
}

const published = readdirSync(packagesDirectory)
  .map((name) => join(packagesDirectory, name))
  .filter((directory) => manifest(directory).private !== true);

const scratch = mkdtempSync(join(tmpdir(), "calque-packed-"));
const tarballs = join(scratch, "tarballs");
const app = join(scratch, "app");
mkdirSync(app);

const tarball = new Map<string, string>();
for (const directory of published) {
  const { name, version } = manifest(directory);
  run("pnpm", ["pack", "--pack-destination", tarballs], directory);
  tarball.set(
    name,
    `file:${join(tarballs, `${name.slice(1).replace("/", "-")}-${version}.tgz`)}`,
  );
}

// `./internal` is the workspace's test seam. It stays out of the published
// package: no file and no export of it.
{
  const archive = tarball.get("@calque/client")?.slice("file:".length);
  if (archive === undefined) throw new Error("@calque/client is not published");
  const tar = (...arguments_: readonly string[]): string => {
    const result = spawnSync("tar", arguments_, { encoding: "utf8" });
    if (result.status !== 0)
      throw new Error(
        `tar ${arguments_.join(" ")} exited ${String(result.status)}`,
      );
    return result.stdout;
  };
  const internalFiles = tar("-tzf", archive)
    .split("\n")
    .filter((entry) => /(^|\/)internal\./u.test(entry));
  const packed = JSON.parse(tar("-xzOf", archive, "package/package.json")) as {
    readonly exports?: Record<string, unknown>;
  };
  if (internalFiles.length > 0 || packed.exports?.["./internal"] !== undefined)
    throw new Error(
      `the packed @calque/client ships its test-only ./internal: ${internalFiles.join(", ") || "an export"}`,
    );
}

const tarballOf = (name: string): string => {
  const path = tarball.get(name);
  if (path === undefined) throw new Error(`${name} is not published`);
  return path;
};
const react = manifest(join(packagesDirectory, "react"));
const page = manifest(quickstart);

writeFileSync(
  join(app, "package.json"),
  JSON.stringify(
    {
      name: "calque-packed-consumer",
      private: true,
      type: "module",
      dependencies: {
        "@calque/client": tarballOf("@calque/client"),
        "@calque/react": tarballOf("@calque/react"),
        react: pin(react, "react"),
        "react-dom": pin(react, "react-dom"),
      },
      devDependencies: {
        "@calque/config": tarballOf("@calque/config"),
        "@calque/grant": tarballOf("@calque/grant"),
        "@types/node": pin(page, "@types/node"),
        "@types/react": pin(react, "@types/react"),
        "@types/react-dom": pin(react, "@types/react-dom"),
        typescript: pin(page, "typescript"),
        vite: pin(page, "vite"),
      },
    },
    null,
    2,
  ),
);
// A tarball's `@calque/*` dependencies name versions the registry does not
// hold yet.
writeFileSync(
  join(app, "pnpm-workspace.yaml"),
  `overrides:\n${[...tarball]
    .map(([name, path]) => `  "${name}": "${path}"\n`)
    .join("")}`,
);
writeFileSync(
  join(app, "tsconfig.json"),
  JSON.stringify({
    compilerOptions: {
      target: "ESNext",
      module: "ESNext",
      moduleResolution: "Bundler",
      lib: ["ESNext", "DOM", "DOM.Iterable"],
      jsx: "react-jsx",
      strict: true,
      exactOptionalPropertyTypes: true,
      verbatimModuleSyntax: true,
      noEmit: true,
      types: ["vite/client", "node"],
    },
    include: ["*.ts", "*.tsx"],
  }),
);
writeFileSync(
  join(app, "vite.config.ts"),
  `import { calque } from "@calque/client/vite";
import { defineConfig } from "vite";

export default defineConfig({ plugins: [calque()] });
`,
);
writeFileSync(
  join(app, "index.html"),
  `<!doctype html>
<html lang="en">
  <body>
    <div id="root"></div>
    <script type="module" src="/main.tsx"></script>
  </body>
</html>
`,
);
writeFileSync(
  join(app, "main.tsx"),
  `import { createSyncClient, eq } from "@calque/client";
import { SyncProvider, useLiveQuery, useSyncClient } from "@calque/react";
import { createRoot } from "react-dom/client";
import { schema } from "./calque.generated.js";

const client = createSyncClient({
  endpoint: "https://localhost:8443/",
  auth: async () => ({ bearerToken: "token" }),
  schema,
});

function Tasks() {
  const { db } = useSyncClient();
  // @ts-expect-error The packed declarations know \`tasks\` has no \`missing\` column.
  db.tasks.query().orderBy("missing");
  const tasks = useLiveQuery(
    db.tasks.query().where(eq("done", false)).select("id", "title"),
  );
  if (tasks.status !== "ready") return <p>{tasks.status}</p>;
  return (
    <ul>
      {tasks.result.map((task) => (
        <li key={String(task.id)}>{task.title}</li>
      ))}
    </ul>
  );
}

const element = document.getElementById("root");
if (element)
  createRoot(element).render(
    <SyncProvider client={client}>
      <Tasks />
    </SyncProvider>,
  );
`,
);
writeFileSync(
  join(app, "grant.ts"),
  `import {
  generateSigningKey,
  GrantRefused,
  loadSigningKey,
  mintGrant,
  mintTenantOrder,
  verificationKeyEnvironment,
} from "@calque/grant";

const { pem, privateKey } = generateSigningKey();
const key = loadSigningKey(pem);
const now = Math.floor(Date.now() / 1000);

export const token: string = mintGrant(
  {
    keyId: "packed",
    audience: "packed.example",
    subject: "me",
    attributes: { tenant_id: { int8: "42" } },
    authorizationGeneration: 1,
    issuedAtUnixSeconds: now,
    expiresAtUnixSeconds: now + 900,
    commands: [{ collection: "tasks", operations: ["update"] }],
  },
  key,
);
export const order: string = mintTenantOrder(
  {
    keyId: "packed",
    audience: "packed.example",
    action: "warm",
    tenant: { int8: "42" },
    issuedAtUnixSeconds: now,
    expiresAtUnixSeconds: now + 60,
  },
  key,
);
export const environment: string = verificationKeyEnvironment("packed", privateKey);
export const refused = GrantRefused;

mintGrant(
  {
    keyId: "packed",
    audience: "packed.example",
    subject: "me",
    attributes: {},
    authorizationGeneration: 1,
    issuedAtUnixSeconds: now,
    expiresAtUnixSeconds: now + 900,
    // @ts-expect-error The packed declarations know the three operations.
    commands: [{ collection: "tasks", operations: ["truncate"] }],
  },
  key,
);
`,
);
writeFileSync(
  join(app, "grant-check.mjs"),
  `import assert from "node:assert/strict";
import { generateSigningKey, loadSigningKey, mintGrant } from "@calque/grant";

const { pem } = generateSigningKey();
const now = Math.floor(Date.now() / 1000);
const claims = {
  keyId: "packed",
  audience: "packed.example",
  subject: "me",
  attributes: {},
  authorizationGeneration: 1,
  issuedAtUnixSeconds: now,
  expiresAtUnixSeconds: now + 900,
};
assert.match(mintGrant(claims, loadSigningKey(pem)), /^[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]{86}$/u);
assert.throws(
  () => mintGrant({ ...claims, expiresAtUnixSeconds: now + 901 }, loadSigningKey(pem)),
  { name: "GrantRefused" },
);
`,
);
copyFileSync(
  join(quickstart, "calque.config.json"),
  join(app, "calque.config.json"),
);

run("pnpm", ["install", "--prefer-offline"], app);
run("pnpm", ["exec", "vite", "build", "--logLevel", "warn"], app);
for (const name of ["calque.generated.ts", "calque.start.generated.ts"])
  if (
    !existsSync(join(app, name)) ||
    !readFileSync(join(app, name)).equals(readFileSync(join(quickstart, name)))
  )
    throw new Error(
      `vite build did not render ${name} as calque-bindings renders the quickstart's`,
    );
run("pnpm", ["exec", "calque-bindings", "--check"], app);
run("node", ["grant-check.mjs"], app);
{
  const calqueGrant = (...arguments_: readonly string[]): string => {
    const result = spawnSync("pnpm", ["exec", "calque-grant", ...arguments_], {
      cwd: app,
      encoding: "utf8",
    });
    if (result.status !== 0)
      throw new Error(
        `calque-grant ${arguments_[0] ?? ""} exited ${String(result.status ?? result.signal)}: ${result.stderr}`,
      );
    return result.stdout;
  };
  const printed = JSON.parse(
    calqueGrant("keygen", "--key-file", "grant-key.pem", "--key-id", "packed"),
  ) as { keyId: string; publicKey: string };
  const minted = JSON.parse(
    calqueGrant(
      "mint",
      "--key-file",
      "grant-key.pem",
      "--key-id",
      "packed",
      "--audience",
      "packed.example",
      "--subject",
      "me",
      "--generation",
      "1",
      "--lifetime-seconds",
      "900",
      "--command",
      "tasks=update",
    ),
  ) as { token: string; verificationKeyEnvironment: unknown };
  if (
    !/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{86}$/u.test(minted.token) ||
    JSON.stringify(minted.verificationKeyEnvironment) !==
      JSON.stringify(printed)
  )
    throw new Error(
      "the packed calque-grant did not mint under the key it created",
    );
}
run("pnpm", ["exec", "tsc", "-p", "tsconfig.json"], app);

const assets = readdirSync(join(app, "dist/assets"));
for (const expected of [/^columnar-browser-worker-entry-.*\.js$/u, /\.wasm$/u])
  if (!assets.some((asset) => expected.test(asset)))
    throw new Error(
      `the packed page's build has no asset matching ${String(expected)}: ${assets.join(", ")}`,
    );

rmSync(scratch, { recursive: true, force: true });
process.stdout.write(
  `Packed packages: PASS (${[...tarball.keys()].join(", ")})\n`,
);

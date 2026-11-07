/**
 * `pnpm verify:packed`: packs every published `@calque` package as the
 * registry would receive it, installs the tarballs into an empty project
 * outside the workspace, and builds a React page on them with the packed
 * Vite plugin, which renders the page's bindings, then typechecks it with
 * the packed declarations and checks the render with the packed
 * `calque-bindings`. A file a consumer needs that a tarball lacks fails here
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

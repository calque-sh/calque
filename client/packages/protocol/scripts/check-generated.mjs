import { spawnSync } from "node:child_process";
import {
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, relative } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const temporaryRoot = mkdtempSync(join(packageRoot, ".generated-check-"));
const outputDirectory = join(temporaryRoot, "gen");
const templatePath = join(temporaryRoot, "buf.gen.yaml");
const relativeOutput = relative(packageRoot, outputDirectory);

function run(command, arguments_) {
  const result = spawnSync(command, arguments_, {
    cwd: packageRoot,
    encoding: "utf8",
    stdio: "pipe",
  });
  if (result.status !== 0) {
    process.stderr.write(result.stdout);
    process.stderr.write(result.stderr);
    throw new Error(`${command} exited with status ${String(result.status)}`);
  }
}

function files(directory, root = directory) {
  return readdirSync(directory, { withFileTypes: true })
    .flatMap((entry) => {
      const path = join(directory, entry.name);
      return entry.isDirectory() ? files(path, root) : [relative(root, path)];
    })
    .sort();
}

try {
  writeFileSync(
    templatePath,
    `version: v2
inputs:
  - directory: ../../../spec
plugins:
  - local: protoc-gen-es
    out: ${relativeOutput}
    opt:
      - target=ts
      - import_extension=js
`,
  );
  run("buf", ["generate", "--template", templatePath]);
  run("prettier", ["--write", outputDirectory]);

  const expectedDirectory = join(packageRoot, "src", "gen");
  const expectedFiles = files(expectedDirectory);
  const actualFiles = files(outputDirectory);
  if (JSON.stringify(actualFiles) !== JSON.stringify(expectedFiles)) {
    throw new Error(
      `generated file set differs: expected ${expectedFiles.join(", ")}; received ${actualFiles.join(", ")}`,
    );
  }
  for (const path of expectedFiles) {
    const expected = readFileSync(join(expectedDirectory, path));
    const actual = readFileSync(join(outputDirectory, path));
    if (!expected.equals(actual)) {
      throw new Error(`generated binding is stale: src/gen/${path}`);
    }
  }
} finally {
  rmSync(temporaryRoot, { recursive: true, force: true });
}

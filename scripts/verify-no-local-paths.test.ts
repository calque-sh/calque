/**
 * `scripts/verify-no-local-paths.ts`: which shapes it finds, which lookalikes
 * it leaves, and that its entry point never prints what it matched.
 *
 * Every path here is assembled when the test runs, so the test file carries
 * none of the shapes it checks.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { treePaths } from "./lib/tree-paths.ts";
import { localPathsIn, withoutLocalPaths } from "./verify-no-local-paths.ts";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

// Each shape is split around one of these, so this file holds none of them.
const USERS = "Users";
const PRIVATE = "private";
const HOME = "home";
const TMP = "tmp";
const VAR = "var";
/** A user name that appears nowhere until the test writes it. */
const NAME = `someone${randomBytes(4).toString("hex")}`;

function shapes(text: string): string[] {
  return localPathsIn(text).map(({ shape }) => shape);
}

test("a home directory is found plainly, in a file URL, percent-encoded and in JSON", () => {
  assert.deepEqual(
    shapes(
      [
        `cwd /${USERS}/${NAME}/work/calque`,
        `file:///${USERS}/${NAME}/work/calque/index.html`,
        `http://localhost/?root=%2F${USERS}%2F${NAME}%2Fwork`,
        `http://localhost/?root=%2f${USERS}%2f${NAME}%2fwork`,
        JSON.stringify({ cwd: `/${HOME}/${NAME}/work` }),
        `/${HOME}/runner${NAME}/work`,
        `/${HOME}/calque${NAME}/work`,
        `?root=%2F${USERS.toLowerCase()}%2F${NAME}`,
        `HOME=/${USERS}/${NAME}`,
        `a process listing cut at its column: /${USERS}/${NAME.slice(0, 1)}`,
        `file%3A%2F%2F%2F${USERS}%2F${NAME}%2Fwork`,
        `at http://localhost:63315/${USERS}/${NAME}/work/client/src/a.ts:1:2`,
        `at http://localhost:63315/@fs/${USERS}/${NAME}/work/node_modules/x.js`,
        `at http://localhost:63315/@fs/${HOME}/${NAME}/work/node_modules/x.js`,
        `https://example.com/${HOME}/${NAME}/`,
      ].join("\n"),
    ),
    Array(15).fill("home directory"),
  );
});

test("each temporary directory is found, and a longer shape counts once", () => {
  assert.deepEqual(
    shapes(
      [
        `/${PRIVATE}/${TMP}/calque-x/out.json`,
        `/${PRIVATE}/${VAR}/folders/ab/cd/T/calque-x`,
        `/${VAR}/folders/ab/cd/T/calque-x`,
        `/${TMP}/tool-501/x`,
        `/${PRIVATE}/${TMP}/tool-501/x`,
        `http://localhost:5173/@fs/${PRIVATE}/${VAR}/folders/ab/cd/T/x.js`,
        `%2F${TMP}%2Ftool-501`,
      ].join("\n"),
    ),
    Array(7).fill("temporary directory"),
  );
});

test("a scratch directory named after a project finds both its shapes", () => {
  assert.deepEqual(
    shapes(
      [
        `/${PRIVATE}/${TMP}/tool-501/-${USERS}-${NAME}-work-calque/x`,
        `projects/-${USERS}-${NAME}`,
        `ALL-${USERS}-${NAME}-x`,
      ].join("\n"),
    ),
    [
      "temporary directory",
      "dash-encoded home directory",
      "dash-encoded home directory",
      "dash-encoded home directory",
    ],
  );
});

test("a Windows home directory is found in any case, with either separator, JSON's escaping or a letter before it", () => {
  assert.deepEqual(
    shapes(
      [
        `C:\\${USERS}\\${NAME}\\work`,
        JSON.stringify({ cwd: `D:\\${USERS}\\${NAME}` }),
        `c:/${USERS}/${NAME}/work`,
        `cwd=C:\\${USERS.toUpperCase()}\\${NAME}`,
        `pathC:\\${USERS}\\${NAME}`,
      ].join("\n"),
    ),
    Array(5).fill("Windows home directory"),
  );
});

test("placeholders, hosted CI's runner, the image's user and lookalikes are left alone", () => {
  assert.deepEqual(
    shapes(
      [
        "<home>/work, <checkout>/scripts, <scratch>/run, <tmp>/x",
        `/${USERS}/<name>/work`,
        `/${HOME}/runner/work/calque/calque`,
        `%2F${HOME}%2Frunner%2Fwork`,
        `WORKDIR /${HOME}/calque`,
        `/${HOME}/calque/.cache and %2F${HOME}%2Fcalque`,
        `https://api.github.com/users/${NAME}/repos`,
        `/${TMP}/calque-bench/out.json`,
        `cd /${PRIVATE}/${TMP}`,
      ].join("\n"),
    ),
    [],
  );
});

test("a writer's text keeps its own checkout relative and writes every other place as a placeholder", () => {
  const home = `/${USERS}/${NAME}`;
  const checkout = `${home}/work/calque`;
  const places = {
    checkout,
    checkouts: [
      checkout,
      `${home}/work/other/one`,
      `${checkout}/worktrees/nested`,
    ],
    home,
    temporary: [
      `/${VAR}/folders/ab/cd/T`,
      `/${PRIVATE}/${VAR}/folders/ab/cd/T`,
    ],
  };
  assert.equal(
    withoutLocalPaths(
      [
        `${checkout}/target/run.log`,
        `${checkout}`,
        `${checkout}2/x`,
        `file://${checkout}/scripts/a.ts:1:2`,
        `${home}/work/other/one/server/x.rs`,
        `${checkout}/worktrees/nested/y`,
        `/${PRIVATE}/${VAR}/folders/ab/cd/T/chrome-1/x`,
        `/${VAR}/folders/ab/cd/T/chrome-2`,
        `file://${home}/.cache/calque/chrome`,
        `${home}/.cache/calque`,
        `${home}2/x and /${HOME}/${NAME}/y`,
        `?root=%2F${USERS}%2F${NAME}%2Fwork&n=1`,
        `/${PRIVATE}/${TMP}/tool-501/-${USERS}-${NAME}-work/x`,
        `C:\\${USERS}\\${NAME}\\work`,
      ].join("\n"),
      places,
    ),
    [
      "target/run.log",
      ".",
      "<home>/work/calque2/x",
      "scripts/a.ts:1:2",
      "<checkout>/server/x.rs",
      "<checkout>/y",
      "<tmp>/chrome-1/x",
      "<tmp>/chrome-2",
      "file://<home>/.cache/calque/chrome",
      "<home>/.cache/calque",
      "<withheld> and <withheld>",
      "?root=<withheld>",
      "<withheld>",
      "<withheld>",
    ].join("\n"),
  );
});

test("a writer's text that names no local place is left as it was", () => {
  const text = [
    "<home>/work, <checkout>/scripts, <tmp>/x",
    `/${HOME}/runner/work/calque/calque`,
    `WORKDIR /${HOME}/calque`,
    `https://api.github.com/users/${NAME}/repos`,
    `/${TMP}/calque-bench/out.json`,
  ].join("\n");
  assert.equal(
    withoutLocalPaths(text, {
      checkout: `/${USERS}/${NAME}/work/calque`,
      checkouts: [],
      home: `/${USERS}/${NAME}`,
      temporary: [`/${VAR}/folders/ab/cd/T`],
    }),
    text,
  );
});

test("the entry point prints place and shape, never the matched value or a matching file name", () => {
  const root = mkdtempSync(join(tmpdir(), "calque-local-paths-"));
  try {
    spawnSync("git", ["init", "-q"], { cwd: root });
    mkdirSync(join(root, "docs"));
    writeFileSync(join(root, "docs", "clean.md"), "<home>/work\n");
    writeFileSync(
      join(root, "docs", "run.log"),
      `start\ncwd /${USERS}/${NAME}/work\n/${PRIVATE}/${TMP}/x\n`,
    );
    writeFileSync(join(root, `-${USERS}-${NAME}-work.txt`), "nothing\n");
    const result = spawnSync(
      process.execPath,
      [
        "--import",
        "./scripts/register.mjs",
        "scripts/verify-no-local-paths.ts",
        root,
      ],
      { cwd: repositoryRoot, encoding: "utf8" },
    );
    assert.equal(result.status, 1, result.stderr);
    assert.match(
      result.stderr,
      /^ {2}docs\/run\.log:2 — 2 on 2 line\(s\): 1 home directory, 1 temporary directory$/mu,
    );
    assert.match(
      result.stderr,
      /^ {2}1 file\(s\) whose own path is machine-local, withheld/mu,
    );
    assert.doesNotMatch(result.stderr, /clean\.md/u);
    assert.ok(!`${result.stdout}${result.stderr}`.includes(NAME));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("this repository carries no machine-local path", () => {
  const found = treePaths(repositoryRoot).flatMap((file) => {
    if (localPathsIn(file).length > 0) return ["<path withheld>"];
    let text: string;
    try {
      text = readFileSync(join(repositoryRoot, file)).toString("latin1");
    } catch {
      return []; // a gitlink, or deleted from the working tree
    }
    return localPathsIn(text).length > 0 ? [file] : [];
  });
  assert.deepEqual(found, []);
});

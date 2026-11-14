import assert from "node:assert/strict";
import { test } from "node:test";

import { docFiles, docShellBlocks } from "./lib/doc-walk.ts";

const STOP = "## Tearing it down";

function doc(...walk: readonly string[]): string {
  return [
    "# Self-host",
    "",
    ...walk,
    "",
    STOP,
    "",
    "```sh",
    "echo after the walk",
    "```",
    "",
  ].join("\n");
}

test("takes every sh block before the stop heading, byte for byte and in order", () => {
  const markdown = doc(
    "## A grant",
    "",
    "```sh",
    "cargo run -q -p dataset-grant-issuer -- \\",
    "  --key-file grant-key.pk8  ",
    "```",
    "",
    "```ts",
    "const ignored = 1;",
    "```",
    "",
    "## The database",
    "",
    "```sh",
    "psql <<'SQL'",
    "## Tearing it down inside a fence is not the stop",
    "",
    "SQL",
    "```",
  );
  assert.deepEqual(docShellBlocks(markdown, STOP), [
    "cargo run -q -p dataset-grant-issuer -- \\\n  --key-file grant-key.pk8  ",
    "psql <<'SQL'\n## Tearing it down inside a fence is not the stop\n\nSQL",
  ]);
});

test("refuses a document the walk cannot be read from", () => {
  const refusals: readonly (readonly [string, string, RegExp])[] = [
    [
      "no stop heading",
      "# Self-host\n\n```sh\ntrue\n```\n",
      /0 lines reading/u,
    ],
    [
      "the stop heading twice",
      `${doc("```sh", "true", "```")}${STOP}\n`,
      /2 lines reading/u,
    ],
    ["no sh block", doc("Prose only."), /no ```sh block before/u],
    [
      "an unclosed fence",
      `# Self-host\n\n\`\`\`sh\ntrue\n${STOP}\n`,
      /never closes/u,
    ],
    [
      "the stop heading only inside a fence",
      `# Self-host\n\n\`\`\`sh\ntrue\n\`\`\`\n\n\`\`\`text\n${STOP}\n\`\`\`\n`,
      /is inside a fence/u,
    ],
    [
      "shell under another fence name",
      doc("```bash", "true", "```"),
      /opens a shell block as "```bash"/u,
    ],
  ];
  for (const [what, markdown, refusal] of refusals)
    assert.throws(() => docShellBlocks(markdown, STOP), refusal, what);
});

test("leaves out the sections a document keeps alternatives in, up to the next heading of that level", () => {
  const markdown = doc(
    "## Run it",
    "",
    "```sh",
    "first",
    "```",
    "",
    "### With the container",
    "",
    "```sh",
    "docker run",
    "```",
    "",
    "#### A detail inside it",
    "",
    "```sh",
    "also skipped",
    "```",
    "",
    "### With the binary",
    "",
    "```sh",
    "calque dev",
    "```",
    "",
    "## Open the page",
    "",
    "```sh",
    "last",
    "```",
  );
  assert.deepEqual(
    docShellBlocks(markdown, STOP, {
      skipSections: ["### With the container"],
    }),
    ["first", "calque dev", "last"],
  );
  assert.throws(
    () => docShellBlocks(markdown, STOP, { skipSections: ["### Missing"] }),
    /0 lines reading "### Missing"/u,
  );
});

test("reads the files a document titles, whole and keyed by path", () => {
  const markdown = doc(
    '```ts title="vite.config.ts"',
    "export default {};",
    "```",
    "",
    '```html title="index.html"',
    "<ul></ul>",
    "```",
    "",
    "```ts",
    "const untitled = 1;",
    "```",
  );
  assert.deepEqual(
    [...docFiles(markdown, STOP)],
    [
      ["vite.config.ts", "export default {};\n"],
      ["index.html", "<ul></ul>\n"],
    ],
  );
  assert.throws(
    () =>
      docFiles(
        doc('```ts title="a.ts"', "1", "```", '```ts title="a.ts"', "2", "```"),
        STOP,
      ),
    /titles two blocks "a.ts"/u,
  );
});

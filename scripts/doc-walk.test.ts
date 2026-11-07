import assert from "node:assert/strict";
import { test } from "node:test";

import { docShellBlocks } from "./lib/doc-walk.ts";

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

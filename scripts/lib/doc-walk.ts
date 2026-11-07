// quickstart-e2e runs the self-host walk from these blocks, so anything that
// would run less than a reader runs is refused.

const FENCE = /^```/u;
const SHELL_FENCE = "```sh";
const OTHER_SHELL_FENCE = /^```(?:bash|zsh|shell|console|shell-session)\b/u;
const CLOSING_FENCE = "```";

/**
 * Every ```sh block before the level-2 heading `stop`, in order, each the
 * exact text between its fences with no trailing newline. `stop` must appear
 * exactly once as a whole line outside a fence.
 */
export function docShellBlocks(markdown: string, stop: string): string[] {
  const lines = markdown.split("\n");
  const stops = lines.filter((line) => line === stop).length;
  if (stops !== 1)
    throw new Error(
      `the document has ${String(stops)} lines reading "${stop}"; the walk needs exactly one`,
    );
  const blocks: string[] = [];
  let open: { readonly line: number; readonly shell: boolean } | undefined;
  let body: string[] = [];
  for (const [index, line] of lines.entries()) {
    if (open === undefined) {
      if (line === stop) return walk(blocks, stop);
      if (OTHER_SHELL_FENCE.test(line))
        throw new Error(
          `line ${String(index + 1)} opens a shell block as "${line}"; the walk runs only \`\`\`sh blocks`,
        );
      if (FENCE.test(line)) {
        open = { line: index + 1, shell: line === SHELL_FENCE };
        body = [];
      }
    } else if (line === CLOSING_FENCE) {
      if (open.shell) blocks.push(body.join("\n"));
      open = undefined;
    } else body.push(line);
  }
  throw new Error(
    open === undefined
      ? `"${stop}" is inside a fence; the walk needs it as a heading`
      : `the fence opened at line ${String(open.line)} never closes`,
  );
}

function walk(blocks: string[], stop: string): string[] {
  if (blocks.length === 0)
    throw new Error(`the document has no \`\`\`sh block before "${stop}"`);
  return blocks;
}

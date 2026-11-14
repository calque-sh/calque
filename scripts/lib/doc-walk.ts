// The doc walks (quickstart-e2e, getting-started-e2e) run a document from
// these blocks, so anything that would run less than a reader runs is refused.

const FENCE = /^```/u;
const SHELL_FENCE = "```sh";
const OTHER_SHELL_FENCE = /^```(?:bash|zsh|shell|console|shell-session)\b/u;
const CLOSING_FENCE = "```";

export interface WalkOptions {
  /**
   * Headings whose sections the walk leaves out, each appearing exactly once
   * as a whole line outside a fence. A section runs to the next heading at
   * its own level or above. The document keeps alternatives there that a
   * reader chooses between and one bash cannot run together.
   */
  readonly skipSections?: readonly string[];
}

const HEADING = /^(#{1,6}) /u;

/**
 * Every ```sh block before the level-2 heading `stop`, in order, each the
 * exact text between its fences with no trailing newline. `stop` must appear
 * exactly once as a whole line outside a fence.
 */
export function docShellBlocks(
  markdown: string,
  stop: string,
  options: WalkOptions = {},
): string[] {
  const lines = markdown.split("\n");
  const skipSections = options.skipSections ?? [];
  for (const heading of [stop, ...skipSections]) {
    const count = lines.filter((line) => line === heading).length;
    if (count !== 1)
      throw new Error(
        `the document has ${String(count)} lines reading "${heading}"; the walk needs exactly one`,
      );
  }
  const blocks: string[] = [];
  let open: { readonly line: number; readonly shell: boolean } | undefined;
  let body: string[] = [];
  let skipping: number | undefined;
  for (const [index, line] of lines.entries()) {
    if (open === undefined) {
      if (line === stop) return walk(blocks, stop);
      const level = HEADING.exec(line)?.[1]?.length;
      if (level !== undefined && skipping !== undefined && level <= skipping)
        skipping = undefined;
      if (level !== undefined && skipSections.includes(line)) skipping = level;
      if (OTHER_SHELL_FENCE.test(line))
        throw new Error(
          `line ${String(index + 1)} opens a shell block as "${line}"; the walk runs only \`\`\`sh blocks`,
        );
      if (FENCE.test(line)) {
        open = { line: index + 1, shell: line === SHELL_FENCE };
        body = [];
      }
    } else if (line === CLOSING_FENCE) {
      if (open.shell && skipping === undefined) blocks.push(body.join("\n"));
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

const TITLED_FENCE = /^```[a-z]+ title="([^"]+)"$/u;

/**
 * The files a document writes out in full: each fenced block whose opening
 * line is a language and `title="<path>"`, before the heading `stop`, keyed
 * by that path with the text between its fences. A path may be titled once.
 */
export function docFiles(
  markdown: string,
  stop: string,
): ReadonlyMap<string, string> {
  const files = new Map<string, string>();
  let title: string | undefined;
  let body: string[] = [];
  let inFence = false;
  for (const line of markdown.split("\n")) {
    if (!inFence) {
      if (line === stop) return files;
      if (FENCE.test(line)) {
        inFence = true;
        title = TITLED_FENCE.exec(line)?.[1];
        body = [];
      }
    } else if (line === CLOSING_FENCE) {
      if (title !== undefined) {
        if (files.has(title))
          throw new Error(`the document titles two blocks "${title}"`);
        files.set(title, `${body.join("\n")}\n`);
      }
      inFence = false;
      title = undefined;
    } else body.push(line);
  }
  throw new Error(`"${stop}" was not found outside a fence`);
}

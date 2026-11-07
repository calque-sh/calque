/**
 * Finds, in a test file's source, the tests a run can pass without
 * executing: Rust `#[ignore]` functions and TypeScript tests that are
 * skipped, todo, or gated on a condition. `pnpm verify` prints what these
 * return beside its passes; it never asserts on them.
 */

/** One test a run can pass without executing it. */
export interface TestExclusion {
  readonly kind: string;
  /** `path:line` of the match. */
  readonly path: string;
  readonly name: string;
  readonly reason: string;
}

const RUST_IGNORE =
  /#\s*\[\s*ignore(?:\s*=\s*"([^"]*)")?\s*\]\s*(?:async\s+)?fn\s+([A-Za-z0-9_]+)/gu;

// Three TypeScript shapes: a declaration form (`test.skip`, `xdescribe`),
// whose first string is the test's name; the test context's own skip method
// called inside the body, whose first string is the reason; and node:test's
// `skip` or `todo` option, whose expression is the reason. Each pattern
// requires the call's parenthesis or the brace or comma before a key, so none
// matches these names in prose.
const DECLARATION =
  /\b((?:it|test|describe)\.(?:skip|todo|skipIf|runIf)|xit|xtest|xdescribe)\s*\(/gu;
const SELF_GATED =
  /\b((?:t|ctx|context)\.skip)\s*\(\s*(?:"([^"]*)"|'([^']*)')?/gu;
const OPTIONS = /[{,]\s*(skip|todo)\s*:\s*([^,}\n]+)/gu;

function lineOf(source: string, index: number): number {
  return source.slice(0, index).split("\n").length;
}

/** The text of a lone string literal, or the expression as written. */
function expressionReason(expression: string): string {
  const trimmed = expression.trim();
  const literal = /^"([^"]*)"$|^'([^']*)'$/u.exec(trimmed);
  return literal ? (literal[1] ?? literal[2] ?? "") : trimmed;
}

export function rustIgnored(source: string, path: string): TestExclusion[] {
  return [...source.matchAll(RUST_IGNORE)].map((match) => ({
    kind: "Rust ignored",
    path: `${path}:${String(lineOf(source, match.index))}`,
    // Group 2 is the function name; the pattern cannot match without it.
    name: match[2] ?? "<unnamed>",
    reason: match[1] ?? "no reason recorded",
  }));
}

export function typeScriptSkipped(
  source: string,
  path: string,
): TestExclusion[] {
  const found: { index: number; exclusion: TestExclusion }[] = [];
  const record = (index: number, name: string, reason: string): void => {
    found.push({
      index,
      exclusion: {
        kind: "TypeScript skipped/conditional",
        path: `${path}:${String(lineOf(source, index))}`,
        name,
        reason,
      },
    });
  };
  for (const match of source.matchAll(DECLARATION))
    record(
      match.index,
      // Group 1 is the matched token; the pattern cannot match without it.
      match[1] ?? "<unnamed>",
      "source contains an explicit skipped/conditional test declaration",
    );
  for (const match of source.matchAll(SELF_GATED))
    record(
      match.index,
      match[1] ?? "<unnamed>",
      match[2] ?? match[3] ?? "no reason recorded",
    );
  for (const match of source.matchAll(OPTIONS))
    record(
      match.index,
      `options.${match[1] ?? "skip"}`,
      expressionReason(match[2] ?? ""),
    );
  return found
    .sort((left, right) => left.index - right.index)
    .map(({ exclusion }) => exclusion);
}

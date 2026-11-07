import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * This checkout's own Cargo target directory. A `CARGO_TARGET_DIR` shared with
 * other worktrees would let another worktree's build pass this one's freshness
 * check, and the harness would run that worktree's binary.
 */
export const CARGO_TARGET_DIRECTORY = join(
  resolve(dirname(fileURLToPath(import.meta.url)), "../.."),
  "target",
);

/** `--target-dir` with this checkout's target directory, for cargo's arguments. */
export const CARGO_TARGET_ARGUMENTS: readonly string[] = [
  "--target-dir",
  CARGO_TARGET_DIRECTORY,
];

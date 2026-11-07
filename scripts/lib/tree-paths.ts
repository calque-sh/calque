import { spawnSync } from "node:child_process";

/** Tracked and untracked-but-not-ignored files under `root`. */
export function treePaths(root: string): string[] {
  const listed = spawnSync(
    "git",
    ["ls-files", "-z", "--cached", "--others", "--exclude-standard"],
    { cwd: root, encoding: "utf8" },
  );
  if (listed.status !== 0) {
    throw new Error(`git ls-files failed in ${root}: ${listed.stderr}`);
  }
  return listed.stdout.split("\0").filter((file) => file.length > 0);
}

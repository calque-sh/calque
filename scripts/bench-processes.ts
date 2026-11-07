/**
 * The process table the benchmark samples for the browser's resident memory,
 * and the process tree under one pid.
 */
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

interface ProcessRssRow {
  readonly pid: number;
  readonly parentPid: number;
  readonly rssBytes: number;
  readonly command: string;
}

/** `ps -axo pid=,ppid=,rss=,command=` output; an unreadable line is skipped. */
export function parseRssTable(stdout: string): ProcessRssRow[] {
  const rows: ProcessRssRow[] = [];
  for (const line of stdout.split("\n")) {
    const match = /^\s*([0-9]+)\s+([0-9]+)\s+([0-9]+)\s+(.*)$/u.exec(line);
    if (!match) continue;
    const [, pid = "", parentPid = "", rssKiB = "", command = ""] = match;
    const row = {
      pid: Number(pid),
      parentPid: Number(parentPid),
      rssBytes: Number(rssKiB) * 1024,
      command,
    };
    if (
      Number.isSafeInteger(row.pid) &&
      Number.isSafeInteger(row.parentPid) &&
      Number.isSafeInteger(row.rssBytes)
    )
      rows.push(row);
  }
  return rows;
}

export async function readProcessTable({
  timeoutMilliseconds,
}: {
  readonly timeoutMilliseconds: number;
}): Promise<ProcessRssRow[]> {
  let stdout: string;
  try {
    ({ stdout } = await execFileAsync(
      "ps",
      ["-axo", "pid=,ppid=,rss=,command="],
      { timeout: timeoutMilliseconds },
    ));
  } catch (error) {
    if ((error as { killed?: unknown } | null)?.killed === true)
      throw new Error("the process-table read timed out", { cause: error });
    throw error;
  }
  return parseRssTable(stdout);
}

/** `rootPid` and every process descended from it, in `processes`' order. */
export function selectProcessTree<
  T extends { readonly pid: number; readonly parentPid: number },
>(processes: readonly T[], rootPid: number): T[] {
  if (!Number.isSafeInteger(rootPid) || rootPid <= 0) return [];
  const descendants = new Set([rootPid]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const row of processes) {
      if (descendants.has(row.parentPid) && !descendants.has(row.pid)) {
        descendants.add(row.pid);
        changed = true;
      }
    }
  }
  return processes.filter((row) => descendants.has(row.pid));
}

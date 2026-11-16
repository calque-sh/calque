import { useLayoutEffect, useState } from "react";

/**
 * The panels the load clock waits for. The page is complete once each shows
 * its first result; the clock in `showcase/index.html` stops on the next
 * frame.
 */
const waiting = new Set([
  "rows",
  "levels",
  "services",
  "events",
  "issue",
  "statuses",
]);

export function useReady(panel: string, ready: boolean): void {
  useLayoutEffect(() => {
    if (ready && waiting.delete(panel) && waiting.size === 0)
      document.documentElement.dataset.ready = "";
  }, [panel, ready]);
}

/**
 * Milliseconds from `since`, an event's `timeStamp`, to the first frame after
 * `settled` holds for `key`; undefined until then, and again once `key`
 * changes.
 */
export function useElapsed(
  key: string,
  since: number | undefined,
  settled: boolean,
): number | undefined {
  const [elapsed, setElapsed] = useState<{ key: string; ms: number }>();
  useLayoutEffect(() => {
    if (since === undefined || !settled || elapsed?.key === key) return;
    const frame = requestAnimationFrame((now) =>
      setElapsed({ key, ms: now - since }),
    );
    return () => cancelAnimationFrame(frame);
  }, [key, since, settled, elapsed?.key]);
  return elapsed?.key === key ? elapsed.ms : undefined;
}

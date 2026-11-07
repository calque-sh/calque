import { once } from "../observer.js";

/** The longest a confirmation waits for a page that stopped rendering. */
const MAX_PAINT_WAIT_MS = 1_000;

/**
 * Calls `then` once the browser has painted what the page holds now, and
 * returns false, calling nothing, where the page does not paint: outside a
 * document, or hidden. Hiding the page while it waits calls `then` at once,
 * and so does `MAX_PAINT_WAIT_MS` passing without a paint, as in an iframe
 * out of view.
 */
export function afterNextPaint(then: () => void): boolean {
  if (
    typeof document === "undefined" ||
    typeof requestAnimationFrame !== "function" ||
    document.visibilityState !== "visible"
  )
    return false;
  const done = once(() => {
    clearTimeout(limit);
    document.removeEventListener("visibilitychange", done);
    then();
  });
  const limit = setTimeout(done, MAX_PAINT_WAIT_MS);
  document.addEventListener("visibilitychange", done);
  // A task queued in an animation frame runs after the paint that frame ends in.
  requestAnimationFrame(() => setTimeout(done, 0));
  return true;
}

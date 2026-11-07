/** A user-timing mark on the cold path, for a trace or a harness to read. */
export function mark(name: string): void {
  globalThis.performance?.mark?.(`calque:${name}`);
}

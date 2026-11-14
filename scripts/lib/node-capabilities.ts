import process from "node:process";
import zlib from "node:zlib";

/**
 * What the probes read from a Node runtime. `currentNodeRuntime` reads the one
 * running; a test hands in a runtime that lacks a capability.
 */
export interface NodeRuntime {
  readonly zstdCompressSync: unknown;
}

export const currentNodeRuntime = (): NodeRuntime => ({
  zstdCompressSync: zlib.zstdCompressSync,
});

/**
 * One line per capability the workspace needs and the runtime lacks, naming
 * the Node releases that bring it; empty when it has all of them. Capabilities
 * are probed rather than a version, because the releases that bring them do
 * not line up.
 */
export function nodeCapabilityRefusals(runtime: NodeRuntime): string[] {
  const refusals: string[] = [];
  if (typeof runtime.zstdCompressSync !== "function") {
    refusals.push(
      "`zlib.zstdCompressSync` is not a function: Node's built-in zstd arrived in 22.15 and 23.8, " +
        "and the client browser fixture compresses its artifacts with it.",
    );
  }
  return refusals;
}

/** Throws, listing every missing capability, unless the running Node has all of them. */
export function requireNodeCapabilities(): void {
  const refusals = nodeCapabilityRefusals(currentNodeRuntime());
  if (refusals.length > 0) {
    throw new Error(
      `Node ${process.versions.node} (${process.execPath}) cannot run this workspace:\n` +
        refusals.map((refusal) => `  - ${refusal}`).join("\n") +
        "\npackage.json's engines.node is the supported range and .nvmrc the pinned version.",
    );
  }
}

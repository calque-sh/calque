import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import {
  createColumnarBrowserHasher,
  createColumnarBrowserWorker,
} from "./worker.js";

const workers = [
  {
    create: createColumnarBrowserWorker,
    entry: "columnar-browser-worker-entry.js",
    name: "calque-columnar",
  },
  {
    create: createColumnarBrowserHasher,
    entry: "columnar-browser-hasher-entry.js",
    name: "calque-hasher",
  },
] as const;

describe.each(workers)(
  "columnar browser worker ownership: $name",
  ({ create, entry, name }) => {
    it("captures the platform Worker constructor exactly once before construction", () => {
      const original = Object.getOwnPropertyDescriptor(globalThis, "Worker");
      let reads = 0;
      const constructed: Array<{
        readonly url: string | URL;
        readonly options: WorkerOptions | undefined;
      }> = [];
      const CapturedWorker = function (
        url: string | URL,
        options?: WorkerOptions,
      ) {
        constructed.push({ url, options });
      } as unknown as typeof globalThis.Worker;
      try {
        Object.defineProperty(globalThis, "Worker", {
          configurable: true,
          get() {
            reads += 1;
            return CapturedWorker;
          },
        });

        const worker = create();

        expect(worker).toBeInstanceOf(CapturedWorker);
        expect(reads).toBe(1);
        expect(constructed).toHaveLength(1);
        expect(String(constructed[0]?.url)).toContain(entry);
        expect(constructed[0]?.options).toEqual({ type: "module", name });
      } finally {
        if (original) Object.defineProperty(globalThis, "Worker", original);
        else Reflect.deleteProperty(globalThis, "Worker");
      }
    });

    it("spells the construction in the one shape bundlers emit as a worker chunk", () => {
      // A bundler recognises a worker entry only by the literal `new
      // Worker(new URL("…", import.meta.url), …)`. Any other spelling inlines
      // the unbundled entry, whose bare `@calque/worker` import cannot
      // resolve, and `createSyncClient` fails with "engine worker failed".
      const source = readFileSync(
        new URL("./worker.ts", import.meta.url),
        "utf8",
      );
      expect(source).toMatch(
        new RegExp(
          String.raw`new\s+Worker\s*\(\s*new\s+URL\s*\(\s*"\.\./` +
            entry.replaceAll(".", String.raw`\.`) +
            String.raw`"\s*,\s*import\.meta\.url\s*\)`,
          "u",
        ),
      );
    });

    it("refuses a runtime without a Worker constructor", () => {
      const original = Object.getOwnPropertyDescriptor(globalThis, "Worker");
      try {
        Reflect.deleteProperty(globalThis, "Worker");
        expect(create).toThrow("this runtime has no Worker constructor");
      } finally {
        if (original) Object.defineProperty(globalThis, "Worker", original);
      }
    });
  },
);

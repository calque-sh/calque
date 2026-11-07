import { afterEach, describe, expect, test, vi } from "vitest";

import type {
  HasherFailure,
  HasherRequest,
} from "./columnar-hasher-protocol.js";
import {
  ArtifactHashers,
  CHUNKS_PER_HASHER,
  HASHER_CREDIT_MS,
  hasherCount,
  type HasherWorker,
} from "./columnar-hashers.js";

/** A hasher worker that records what the page sends it and never answers. */
class FakeHasher extends EventTarget implements HasherWorker {
  terminated = false;
  port: MessagePort | undefined;
  readonly chunks: { readonly index: number; readonly bytes: number }[] = [];
  /** The buffers each message transferred. */
  readonly transferred: Transferable[][] = [];

  postMessage(message: HasherRequest, transfer: Transferable[]): void {
    this.transferred.push(transfer);
    if (message.type === "port") this.port = message.port;
    else
      this.chunks.push({
        index: message.index,
        bytes: message.bytes.byteLength,
      });
  }

  terminate(): void {
    this.terminated = true;
    this.port?.close();
  }

  fail(data: HasherFailure): void {
    this.dispatchEvent(new MessageEvent("message", { data }));
  }
}

let spawned: FakeHasher[] = [];
const pools: ArtifactHashers[] = [];

function pool(count = 3): ArtifactHashers {
  const hashers = new ArtifactHashers(() => {
    const hasher = new FakeHasher();
    spawned.push(hasher);
    return hasher;
  }, count);
  pools.push(hashers);
  return hashers;
}

/** Where each chunk went, as `[hasher, index]`. */
function placements(): [number, number][] {
  return spawned.flatMap((hasher, which) =>
    hasher.chunks.map(({ index }): [number, number] => [which, index]),
  );
}

/** Whether `promise` has settled after the queued microtasks run. */
async function settled(promise: Promise<unknown>): Promise<boolean> {
  let done = false;
  promise.then(
    () => (done = true),
    () => (done = true),
  );
  for (let turn = 0; turn < 10; turn++) await Promise.resolve();
  return done;
}

afterEach(() => {
  for (const hashers of pools.splice(0)) {
    for (const port of hashers.takePorts()) port.close();
    hashers.close();
  }
  spawned = [];
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("the page's hasher workers", () => {
  test("spawns three hashers on this machine's thread count and fewer on a small one", () => {
    expect(
      [18, 8, 7, 6, 5, 4, 1].map((threads) => hasherCount(threads)),
    ).toEqual([3, 3, 3, 2, 1, 1, 1]);
    // The browser's own count by default, and one hasher where it has none.
    vi.stubGlobal("navigator", { hardwareConcurrency: 6 });
    expect(hasherCount()).toBe(2);
    vi.stubGlobal("navigator", {});
    expect(hasherCount()).toBe(1);
  });

  test("gives each hasher its port to the engine worker first, and the engine worker the other ends, once", () => {
    const hashers = pool(3);
    expect(spawned).toHaveLength(3);
    for (const hasher of spawned) {
      expect(hasher.port).toBeInstanceOf(MessagePort);
      expect(hasher.transferred).toEqual([[hasher.port]]);
    }
    const ports = hashers.takePorts();
    expect(ports).toHaveLength(3);
    for (const port of ports) expect(port).toBeInstanceOf(MessagePort);
    expect(hashers.takePorts()).toEqual([]);
    for (const port of ports) port.close();
  });

  test("sends each chunk, transferred, to the hasher holding the fewest, and holds a send while every hasher holds two", async () => {
    const hashers = pool(3);
    const buffers = Array.from(
      { length: 7 },
      (_, index) => new ArrayBuffer(index + 1),
    );
    for (let index = 0; index < 3 * CHUNKS_PER_HASHER; index++)
      await hashers.send(index, buffers[index] ?? new ArrayBuffer(0));
    expect(placements()).toEqual([
      [0, 0],
      [0, 3],
      [1, 1],
      [1, 4],
      [2, 2],
      [2, 5],
    ]);
    for (const hasher of spawned)
      for (const [position, { index }] of hasher.chunks.entries())
        expect(hasher.transferred[position + 1]).toEqual([buffers[index]]);
    // The seventh waits: the page reads no further.
    const seventh = hashers.send(6, buffers[6] ?? new ArrayBuffer(0));
    expect(await settled(seventh)).toBe(false);
    expect(spawned.map((hasher) => hasher.chunks.length)).toEqual([2, 2, 2]);
    // A credit for a chunk the page never sent frees nothing.
    hashers.credit(99);
    expect(await settled(seventh)).toBe(false);
    // The credit for chunk 4 frees the hasher that held it.
    hashers.credit(4);
    await seventh;
    expect(spawned[1]?.chunks.map(({ index }) => index)).toEqual([1, 4, 6]);
  });

  test("a constructor that throws terminates the hashers already spawned and throws", () => {
    let constructed = 0;
    const refused = new TypeError(
      "this runtime has no Worker constructor; Calque runs only in Chromium-based browsers",
    );
    expect(
      () =>
        new ArtifactHashers(() => {
          if (constructed === 2) throw refused;
          constructed += 1;
          const hasher = new FakeHasher();
          spawned.push(hasher);
          return hasher;
        }, 3),
    ).toThrow(refused);
    expect(spawned.map((hasher) => hasher.terminated)).toEqual([true, true]);
  });

  const failures: ReadonlyArray<{
    readonly name: string;
    readonly fail: (hasher: FakeHasher) => void;
    readonly error: string;
  }> = [
    {
      name: "an error event",
      fail: (hasher) => {
        hasher.dispatchEvent(
          Object.assign(new Event("error"), { message: "hasher crashed" }),
        );
      },
      error: "hasher crashed",
    },
    {
      name: "an error event without a message",
      fail: (hasher) => {
        hasher.dispatchEvent(new Event("error"));
      },
      error: "a hasher worker failed",
    },
    {
      name: "a messageerror event",
      fail: (hasher) => {
        hasher.dispatchEvent(new MessageEvent("messageerror"));
      },
      error: "a hasher worker message could not be decoded",
    },
    {
      name: "a digest the hasher could not take",
      fail: (hasher) => {
        hasher.fail({ type: "failed", index: 4, message: "digest failed" });
      },
      error: "a hasher could not digest chunk 4: digest failed",
    },
  ];

  for (const { name, fail, error } of failures)
    test(`${name} refuses the load, terminates every hasher and releases a waiting send`, async () => {
      const hashers = pool(3);
      for (let index = 0; index < 6; index++)
        await hashers.send(index, new ArrayBuffer(1));
      const waiting = hashers.send(6, new ArrayBuffer(1));
      const hasher = spawned[1];
      if (!hasher) throw new Error("no hasher");
      fail(hasher);
      // Released at once, not at the deadline.
      expect(await settled(waiting)).toBe(true);
      await expect(waiting).rejects.toThrow(error);
      await expect(hashers.failed).rejects.toThrow(error);
      expect(spawned.map(({ terminated }) => terminated)).toEqual([
        true,
        true,
        true,
      ]);
      await expect(hashers.send(7, new ArrayBuffer(1))).rejects.toThrow(error);
      // Only the first failure counts.
      spawned[0]?.dispatchEvent(new Event("error"));
      await expect(hashers.failed).rejects.toThrow(error);
    });

  test("a chunk not credited within the deadline of its own send refuses the load", async () => {
    vi.useFakeTimers();
    const hashers = pool(3);
    await hashers.send(0, new ArrayBuffer(1));
    vi.advanceTimersByTime(10_000);
    await hashers.send(1, new ArrayBuffer(1));
    // Chunk 0 is credited just in time; chunk 1 has 10 s left.
    vi.advanceTimersByTime(HASHER_CREDIT_MS - 10_000 - 1);
    hashers.credit(0);
    vi.advanceTimersByTime(10_000);
    expect(await settled(hashers.failed)).toBe(false);
    expect(spawned.map(({ terminated }) => terminated)).toEqual([
      false,
      false,
      false,
    ]);
    vi.advanceTimersByTime(1);
    expect(await settled(hashers.failed)).toBe(true);
    await expect(hashers.failed).rejects.toThrow(
      `chunk 1 was not accepted within ${String(HASHER_CREDIT_MS)} ms`,
    );
    expect(spawned.map(({ terminated }) => terminated)).toEqual([
      true,
      true,
      true,
    ]);
  });

  test("closing terminates every hasher, closes the engine worker's ports and releases a waiting send", async () => {
    const hashers = pool(1);
    await hashers.send(0, new ArrayBuffer(1));
    await hashers.send(1, new ArrayBuffer(1));
    const waiting = hashers.send(2, new ArrayBuffer(1));
    hashers.close();
    expect(await settled(waiting)).toBe(true);
    await expect(waiting).rejects.toThrow("the hashers are closed");
    expect(spawned[0]?.terminated).toBe(true);
    expect(hashers.takePorts()).toEqual([]);
    // Closing is not a failure.
    expect(await settled(hashers.failed)).toBe(false);
  });

  test("refuses a pool of no hashers", () => {
    expect(() => new ArtifactHashers(() => new FakeHasher(), 0)).toThrow(
      "at least one hasher",
    );
  });
});

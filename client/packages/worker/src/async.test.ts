import { describe, expect, test } from "vitest";

import { awaitedLater, BoundedQueue, stopReading } from "./async.js";

/** Whether `promise` has settled once the microtasks queued so far ran. */
async function settled(promise: Promise<unknown>): Promise<boolean> {
  let done = false;
  promise.then(
    () => (done = true),
    () => (done = true),
  );
  await new Promise((resolve) => setTimeout(resolve, 0));
  return done;
}

describe("BoundedQueue", () => {
  test("holds the producer back while it holds more than its high-water mark", async () => {
    const queue = new BoundedQueue<Uint8Array>(4, (item) => item.byteLength);
    queue.push(new Uint8Array(3));
    expect(await settled(queue.room())).toBe(true);
    queue.push(new Uint8Array(2));
    const room = queue.room();
    expect(await settled(room)).toBe(false);
    const taken = await queue.take();
    expect(taken?.map((item) => item.byteLength)).toEqual([3, 2]);
    expect(await settled(room)).toBe(true);
  });

  test("hands a waiting consumer the first item pushed", async () => {
    const queue = new BoundedQueue<string>(8);
    const taking = queue.take();
    expect(await settled(taking)).toBe(false);
    queue.push("a");
    expect(await taking).toEqual(["a"]);
  });

  test("hands items pushed together to a waiting consumer in one call", async () => {
    const queue = new BoundedQueue<string>(8);
    const taking = queue.take();
    queue.push("a", "b");
    queue.push("c");
    expect(await taking).toEqual(["a", "b"]);
    expect(await queue.take()).toEqual(["c"]);
  });

  test("returns what was queued before a close, then the end", async () => {
    const queue = new BoundedQueue<string>(8);
    queue.push("a");
    queue.push("b");
    queue.close();
    expect(await queue.take()).toEqual(["a", "b"]);
    expect(await queue.take()).toBeUndefined();
    expect(() => queue.push("c")).toThrow("the queue is closed");
  });

  test("returns what was queued before a failure, then the failure", async () => {
    const queue = new BoundedQueue<string>(8);
    const failure = new Error("the body was cut");
    queue.push("a");
    queue.fail(failure);
    expect(await queue.take()).toEqual(["a"]);
    await expect(queue.take()).rejects.toBe(failure);
  });

  test("a consumer that stops early releases a waiting producer and drops the rest", async () => {
    const queue = new BoundedQueue<string>(1);
    queue.push("a");
    queue.push("b");
    const room = queue.room();
    const reason = new Error("stopped");
    queue.cancel(reason);
    await expect(room).rejects.toBe(reason);
    await expect(queue.room()).rejects.toBe(reason);
    queue.push("c");
    await expect(queue.take()).rejects.toBe(reason);
  });

  test("a consumer that stops while it waits is answered with its reason", async () => {
    const queue = new BoundedQueue<string>(1);
    const taking = queue.take();
    const reason = new Error("stopped");
    queue.cancel(reason);
    await expect(taking).rejects.toBe(reason);
  });

  test("takes from one consumer at a time", async () => {
    const queue = new BoundedQueue<string>(1);
    const first = queue.take();
    await expect(queue.take()).rejects.toThrow("already being taken from");
    queue.push("a");
    expect(await first).toEqual(["a"]);
  });
});

describe("awaitedLater", () => {
  test("keeps a rejection for whoever awaits the promise", async () => {
    const failure = new Error("refused");
    const promise = awaitedLater(Promise.reject(failure));
    await new Promise((resolve) => setTimeout(resolve, 0));
    await expect(promise).rejects.toBe(failure);
  });
});

describe("stopReading", () => {
  test("cancels a body that is still being read", async () => {
    let cancelled: unknown;
    const reader = new ReadableStream({
      cancel: (reason) => {
        cancelled = reason ?? "cancelled";
      },
    }).getReader();
    await stopReading(reader);
    expect(cancelled).toBe("cancelled");
  });

  test("resolves for a body that already failed", async () => {
    const reader = new ReadableStream({
      start: (controller) => controller.error(new Error("cut")),
    }).getReader();
    await expect(reader.read()).rejects.toThrow("cut");
    await expect(stopReading(reader)).resolves.toBeUndefined();
  });
});

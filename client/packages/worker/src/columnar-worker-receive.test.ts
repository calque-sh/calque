/**
 * The engine worker opening an artifact whose chunks reach it from hasher
 * workers while the artifact downloads. The module under test is
 * `columnar-worker-entry.ts`, driven through the `WorkerScope` a
 * `DedicatedWorker` gives it, over a real artifact, with the hashers' ports
 * real `MessageChannel`s; each test loads a fresh copy, since a worker mounts
 * once.
 */
import { createHash } from "node:crypto";
import {
  FIXTURE_ARTIFACT_IDENTITY,
  WORKSPACE_CONTRACT,
  encodeWorkspace,
  hashedArtifactChunks,
} from "../../core/src/artifact.fixture.js";
import type { TrustedArtifact } from "@calque/core";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import type {
  WorkerArtifactChunk,
  WorkerArtifactCredit,
  WorkerArtifactEnd,
  WorkerRequest,
  WorkerResponse,
} from "./columnar-worker-protocol.js";

interface Scope {
  onmessage:
    ((event: { data: WorkerRequest | WorkerArtifactEnd }) => void) | null;
  postMessage(message: WorkerResponse | WorkerArtifactCredit): void;
}

const scope = globalThis as unknown as Scope;
const USERS = Array.from({ length: 40 }, (_unused, index) => ({
  id: `user-${String(index)}`,
  display_name: `User ${String(index)}`,
  email: `user${String(index)}@example.test`,
  active: index % 2 === 0,
}));
const CHUNK_BYTES = 128;

const encoded = encodeWorkspace(
  new Map<string, Record<string, unknown>[]>(
    WORKSPACE_CONTRACT.tables.map((table) => [
      table.name,
      table.name === "users" ? USERS.map((user) => ({ ...user })) : [],
    ]),
  ),
);

function sha256(bytes: Uint8Array): `sha256:${string}` {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

/** The artifact's chunks as hashers send them, each in a fresh buffer. */
const chunks = () => hashedArtifactChunks(encoded.bytes, CHUNK_BYTES);

const trusted: TrustedArtifact = {
  byteLength: encoded.byteLength,
  digest: encoded.digest,
  chunks: {
    bytes: CHUNK_BYTES,
    sha256: chunks().map(({ sha256 }) => sha256),
  },
  identity: FIXTURE_ARTIFACT_IDENTITY,
  schema: WORKSPACE_CONTRACT,
};

let answers: WorkerResponse[] = [];
let credits: number[] = [];
let hashers: MessageChannel[] = [];

function send(message: WorkerRequest | WorkerArtifactEnd): void {
  scope.onmessage?.({ data: message });
}

/** An `open` of `trusted` whose chunks come from three hashers' ports. */
function open(id: number): void {
  hashers = Array.from({ length: 3 }, () => new MessageChannel());
  send({
    id,
    type: "open",
    trusted,
    hashers: hashers.map(({ port2 }) => port2),
    queryCapacityBytes: 1 << 20,
  });
}

/** Sends `chunk` the way a hasher does, transferring its buffer. */
function hash(chunk: WorkerArtifactChunk): void {
  const channel = hashers[chunk.index % hashers.length];
  if (!channel) throw new Error("no hasher");
  channel.port1.postMessage(chunk, [chunk.bytes]);
}

async function answer(id: number): Promise<WorkerResponse> {
  await vi.waitFor(() => {
    expect(answers.some((response) => response.id === id)).toBe(true);
  });
  const response = answers.find((candidate) => candidate.id === id);
  if (!response) throw new Error("no answer");
  return response;
}

/** Lets the worker run everything it has queued. */
async function settle(): Promise<void> {
  for (let turn = 0; turn < 20; turn++)
    await new Promise((resolve) => setTimeout(resolve, 0));
}

function closeHashers(): void {
  for (const { port1, port2 } of hashers) {
    port1.close();
    port2.close();
  }
  hashers = [];
}

async function freshWorker(): Promise<void> {
  closeHashers();
  answers = [];
  credits = [];
  scope.postMessage = (message) => {
    if ("type" in message && message.type === "artifact-credit")
      credits.push(message.index);
    else answers.push(message as WorkerResponse);
  };
  vi.resetModules();
  await import("./columnar-worker-entry.js");
}

describe("the engine worker opens an artifact while it arrives", () => {
  beforeEach(freshWorker);

  afterEach(closeHashers);

  test("opens once every chunk has been accepted and the end is sent, in any order", async () => {
    const all = chunks();
    expect(all.length).toBeGreaterThan(4);
    expect(encoded.byteLength % CHUNK_BYTES).not.toBe(0);
    open(1);
    // The last chunk first, then the rest backwards, across three hashers.
    for (const chunk of [...all].reverse()) hash(chunk);
    await vi.waitFor(() => {
      expect(credits).toHaveLength(all.length);
    });
    await settle();
    expect(answers).toEqual([]);
    // One credit per chunk, as each lands.
    expect([...credits].sort((a, b) => a - b)).toEqual(
      all.map(({ index }) => index),
    );
    send({ type: "artifact-end", chunks: all.length });
    const opened = await answer(1);
    expect(opened).toMatchObject({
      ok: true,
      type: "open",
      rowCount: USERS.length,
      artifactByteLength: encoded.byteLength,
    });
    send({
      id: 2,
      type: "rows",
      table: "users",
      start: 39,
      count: 1,
      fields: ["id", "display_name"],
    });
    expect(await answer(2)).toMatchObject({
      ok: true,
      rows: [["user-39", "User 39"]],
    });
  });

  test("opens when the end arrives before the hashers' last chunk", async () => {
    const all = chunks();
    open(1);
    send({ type: "artifact-end", chunks: all.length });
    for (const chunk of all.slice(0, -1)) hash(chunk);
    await vi.waitFor(() => {
      expect(credits).toHaveLength(all.length - 1);
    });
    await settle();
    expect(answers).toEqual([]);
    const last = all.at(-1);
    if (!last) throw new Error("no chunk");
    hash(last);
    expect(await answer(1)).toMatchObject({
      ok: true,
      rowCount: USERS.length,
    });
  });

  test("refuses a chunk whose digest is not the plan's at once, credits nothing after it, and mounts nothing", async () => {
    const all = chunks();
    const middle = Math.floor(all.length / 2);
    open(1);
    for (const chunk of all.slice(0, middle)) hash(chunk);
    await vi.waitFor(() => {
      expect(credits).toHaveLength(middle);
    });
    const refused = all[middle];
    if (!refused) throw new Error("no chunk");
    const tampered = new Uint8Array(refused.bytes.slice(0));
    tampered[0] = (tampered[0] ?? 0) ^ 0x80;
    // The hasher's digest is honest: it is of the bytes it was sent.
    hash({ index: middle, bytes: tampered.buffer, sha256: sha256(tampered) });
    // The open answers without waiting for the end or the rest.
    expect(await answer(1)).toMatchObject({
      ok: false,
      error: { message: "artifact digest binding mismatch" },
    });
    for (const chunk of all.slice(middle + 1)) hash(chunk);
    send({ type: "artifact-end", chunks: all.length });
    await settle();
    expect(credits).toHaveLength(middle);
    send({
      id: 2,
      type: "rows",
      table: "users",
      start: 0,
      count: 1,
      fields: ["id"],
    });
    expect(await answer(2)).toMatchObject({
      ok: false,
      error: { message: "worker workspace is not open" },
    });
  });

  test("refuses an end that names another chunk count", async () => {
    const all = chunks();
    for (const count of [all.length - 1, all.length + 1]) {
      await freshWorker();
      open(1);
      for (const chunk of chunks().slice(0, count)) hash(chunk);
      send({ type: "artifact-end", chunks: count });
      expect(await answer(1), String(count)).toMatchObject({
        ok: false,
        error: { message: "artifact length binding mismatch" },
      });
    }
  });

  test("refuses a chunk that arrives without its bytes", async () => {
    open(1);
    const [first] = chunks();
    if (!first) throw new Error("no chunk");
    hashers[0]?.port1.postMessage({ index: 0, sha256: first.sha256 });
    expect(await answer(1)).toMatchObject({
      ok: false,
      error: { message: "an artifact chunk arrived without its bytes" },
    });
  });

  test("refuses an open that names no hasher ports", async () => {
    send({ id: 1, type: "open", trusted, hashers: [] });
    expect(await answer(1)).toMatchObject({
      ok: false,
      error: {
        message: "the open names no hasher ports to receive chunks on",
      },
    });
  });

  test("refuses a binding it cannot hold before any chunk arrives", async () => {
    hashers = [new MessageChannel()];
    send({
      id: 1,
      type: "open",
      trusted,
      hashers: hashers.map(({ port2 }) => port2),
      capacity: {
        capacityBytes: encoded.byteLength - 1,
        residentBytes: 0,
        standbyBytes: 0,
        safetyBytes: 0,
      },
    });
    expect(await answer(1)).toMatchObject({
      ok: false,
      error: { name: "CapacityError" },
    });
  });

  test("digests no chunk itself: its one digest is the schema's", async () => {
    const digest = vi.spyOn(crypto.subtle, "digest");
    try {
      const all = chunks();
      open(1);
      for (const chunk of all) hash(chunk);
      send({ type: "artifact-end", chunks: all.length });
      expect(await answer(1)).toMatchObject({ ok: true });
      expect(digest).toHaveBeenCalledTimes(1);
    } finally {
      digest.mockRestore();
    }
  });
});

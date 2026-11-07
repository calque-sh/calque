/**
 * An artifact reaching the engine worker through hasher workers, in Chromium,
 * with every worker the real one: the hashers run `columnar-hasher-entry.ts`
 * and the engine worker `columnar-worker-entry.ts`.
 *
 * The page's part is reproduced here because the session lives in the
 * client package: it spawns the hashers, sends each chunk to one, ends the
 * artifact, and refuses the load when the engine worker refuses a chunk or a
 * hasher fails. Real workers, with real digests, refuse a chunk whose bytes
 * changed between the page and its hasher, whichever chunk it is.
 */
import type { TrustedArtifact } from "@calque/core";
import { afterEach, describe, expect, it } from "vitest";
import { commands } from "vitest/browser";

import {
  ArtifactHashers,
  EngineWorkerClient,
  type OpenedWorkspace,
} from "../src/columnar-worker-client.js";
import type { ColumnarFixture } from "./columnar-fixture.js";

/** Small enough that the four-user fixture is many chunks. */
const CHUNK_BYTES = 256;
const HASHERS = 3;

interface Spawned {
  readonly name: string;
  terminated: boolean;
}

let spawned: Spawned[] = [];
const clients: EngineWorkerClient[] = [];

afterEach(() => {
  for (const client of clients.splice(0)) client.close();
  spawned = [];
});

/** `worker`, with its termination recorded under `name`. */
function traced(worker: Worker, name: string): Worker {
  const record: Spawned = { name, terminated: false };
  spawned.push(record);
  const terminate = worker.terminate.bind(worker);
  worker.terminate = () => {
    record.terminated = true;
    terminate();
  };
  return worker;
}

function bytesOf(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1)
    bytes[index] = binary.charCodeAt(index);
  return bytes;
}

/**
 * Loads `fixture` the way the session does, flipping one bit of chunk
 * `flipped` in the page's buffer before it goes to a hasher.
 */
async function load(
  fixture: ColumnarFixture,
  flipped?: number,
): Promise<{
  readonly client: EngineWorkerClient;
  readonly opened: Promise<OpenedWorkspace>;
}> {
  const trusted: TrustedArtifact = {
    byteLength: fixture.byteLength,
    digest: fixture.digest,
    chunks: fixture.chunks,
    identity: fixture.identity,
    schema: fixture.schema,
  };
  const client = new EngineWorkerClient(
    traced(
      new Worker(new URL("../src/columnar-worker-entry.ts", import.meta.url), {
        type: "module",
      }),
      "engine",
    ),
  );
  clients.push(client);
  const hashers = new ArtifactHashers(
    () =>
      traced(
        new Worker(
          new URL("../src/columnar-hasher-entry.ts", import.meta.url),
          { type: "module", name: "calque-hasher" },
        ),
        "hasher",
      ),
    HASHERS,
  );
  const receiving = client.receive(trusted, hashers);
  receiving.opened.catch((error: unknown) => {
    hashers.fail(error);
  });
  const opened = (async () => {
    try {
      const bytes = bytesOf(fixture.bytes);
      let index = 0;
      for (let start = 0; start < bytes.byteLength; start += CHUNK_BYTES) {
        const chunk = bytes.slice(start, start + CHUNK_BYTES);
        if (index === flipped) chunk[0] = (chunk[0] ?? 0) ^ 0x01;
        await hashers.send(index, chunk.buffer);
        index += 1;
      }
      receiving.end(index);
      return await Promise.race([receiving.opened, hashers.failed]);
    } catch (error) {
      // The engine worker's refusal is the reason, not the send it released.
      throw await receiving.opened.then(
        () => error,
        (refusal: unknown) => refusal,
      );
    } finally {
      hashers.close();
    }
  })();
  return { client, opened };
}

describe("hashers between the page and the engine worker", () => {
  it("open the artifact from many chunks across three hashers", async () => {
    const fixture = await commands.encodeColumnarFixture(
      "cursor-0",
      CHUNK_BYTES,
    );
    expect(fixture.chunks.sha256.length).toBeGreaterThan(2 * HASHERS);
    const { client, opened } = await load(fixture);
    expect(await opened).toMatchObject({
      rowCount: fixture.users.length,
      artifactByteLength: fixture.byteLength,
    });
    const { rows } = await client.rows("users", 0, fixture.users.length, [
      "id",
    ]);
    expect(rows.map(([id]) => id).sort()).toEqual(
      fixture.users.map(({ id }) => id).sort(),
    );
    expect(spawned.filter(({ name }) => name === "hasher")).toEqual(
      Array.from({ length: HASHERS }, () => ({
        name: "hasher",
        terminated: true,
      })),
    );
  });

  for (const which of ["first", "middle", "last"] as const)
    it(`refuse the ${which} chunk changed on its way to a hasher, open nothing and answer no query`, async () => {
      const fixture = await commands.encodeColumnarFixture(
        "cursor-0",
        CHUNK_BYTES,
      );
      const count = fixture.chunks.sha256.length;
      const flipped =
        which === "first" ? 0 : which === "last" ? count - 1 : count >> 1;
      const { client, opened } = await load(fixture, flipped);
      await expect(opened).rejects.toThrow("artifact digest binding mismatch");
      expect(
        spawned
          .filter(({ name }) => name === "hasher")
          .map(({ terminated }) => terminated),
      ).toEqual(Array.from({ length: HASHERS }, () => true));
      await expect(client.rows("users", 0, 1, ["id"])).rejects.toThrow(
        "worker workspace is not open",
      );
      client.close();
      expect(spawned.every(({ terminated }) => terminated)).toBe(true);
    });
});

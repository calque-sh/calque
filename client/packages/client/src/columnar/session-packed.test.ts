import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { brotliCompressSync, constants as zlib } from "node:zlib";

import { decoderWindowBytes } from "@calque/worker/columnar-decoder-worker";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { until, CHUNK_BYTES, stubThreads, settled } from "./session.fixture.js";
import {
  brotliPlan,
  serveBody,
  makeDecodingClient,
  joined,
  marks,
  refused,
} from "./session-brotli.fixture.js";

describe("a brotli-large-window artifact", () => {
  beforeEach(() => {
    stubThreads(8);
    performance.clearMarks();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  describe("packed, as a packed-brotli-large-window plan says,", () => {
    // The body is brotli of a packed body, which the decoder worker unpacks
    // into the artifact as it decodes it. All but the first test build the
    // packed body here from named pieces, so each can damage the piece it
    // names.

    const PACKED = "packed-brotli-large-window";
    /** How much of the body a read of the response gives. */
    const PIECE_BYTES = 65_536;
    const ROWS = 170_000;
    /** The byte each owner's key is sixteen of. */
    const OWNERS = [0x11, 0x22, 0x33, 0x44, 0x55];
    const text = new TextEncoder();

    /** `body` as a large-window brotli stream of the artifact's window. */
    function brotliOf(body: Uint8Array, decodedBytes: number): Uint8Array {
      return new Uint8Array(
        brotliCompressSync(body, {
          params: {
            [zlib.BROTLI_PARAM_QUALITY]: 0,
            [zlib.BROTLI_PARAM_LARGE_WINDOW]: 1,
            [zlib.BROTLI_PARAM_LGWIN]: Math.log2(
              decoderWindowBytes(decodedBytes),
            ),
            [zlib.BROTLI_PARAM_SIZE_HINT]: body.byteLength,
          },
        }),
      );
    }

    /** Bytes brotli cannot shorten, the same on every run. */
    function noise(length: number): Uint8Array {
      const bytes = new Uint8Array(length);
      let state = 0x9e3779b9;
      for (let index = 0; index < length; index += 1) {
        state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
        bytes[index] = state >>> 24;
      }
      return bytes;
    }

    /** An artifact's bytes up to its payload, and all of them. */
    function artifactOf(
      tables: readonly {
        readonly name: string;
        readonly rows: number;
        readonly primaryKey: readonly string[];
        readonly columns: readonly {
          readonly name: string;
          readonly type: string;
          readonly parts: readonly Uint8Array[];
        }[];
      }[],
    ): { head: Uint8Array; artifact: Uint8Array } {
      const payload: Uint8Array[] = [];
      let end = 0;
      const json = text.encode(
        JSON.stringify({
          format: "calque-direct-columnar",
          version: 4,
          tables: tables.map((table) => ({
            ...table,
            columns: table.columns.map((column) => ({
              ...column,
              nullable: false,
              // Each part lies at the next multiple of 8, in this order.
              parts: column.parts.map((part) => {
                const offset = Math.ceil(end / 8) * 8;
                payload.push(new Uint8Array(offset - end), part);
                end = offset + part.byteLength;
                return { offset, length: part.byteLength };
              }),
            })),
          })),
        }),
      );
      const head = new Uint8Array(Math.ceil((12 + json.byteLength) / 8) * 8);
      head.set(text.encode("CALQART1"));
      new DataView(head.buffer).setUint32(8, json.byteLength, true);
      head.set(json, 12);
      return { head, artifact: joined([head, ...payload]) };
    }

    /**
     * What the packed body holds after its magic and its preamble's length,
     * in the body's order.
     */
    interface Pieces {
      readonly preamble: Uint8Array;
      /** The artifact up to its payload. */
      readonly head: Uint8Array;
      /** The owners' keys, hoisted ahead of the payload. */
      readonly keys: Uint8Array;
      /** A column left as the artifact stores it. */
      readonly weights: Uint8Array;
      /** The `fk` stream: for each issue, its owner's row. */
      readonly codes: Uint8Array;
      /** The `int` stream: each number's zigzag delta from the one before. */
      readonly deltas: Uint8Array;
      /** The `sep` stream: each label and a separator. */
      readonly labels: Uint8Array;
    }

    /**
     * Three chunks. `bulk` is 4.6 MB brotli cannot shorten, so the first
     * chunk ends inside it and a place in the encoded body is, within a few
     * bytes, that place in the packed one. `issues` has a column for each
     * transform, all of them in the second chunk and the third, and its
     * `owner_id` names a row of `owners`, the artifact's last 80 bytes.
     */
    const { ARTIFACT, PIECES } = (() => {
      const weights = noise(8 * 575_000);
      const keys = Uint8Array.from(
        OWNERS.flatMap((owner) => Array<number>(16).fill(owner)),
      );
      const ownerIds = new Uint8Array(16 * ROWS);
      const numbers = new DataView(new ArrayBuffer(8 * ROWS));
      const lengths = new DataView(new ArrayBuffer(4 * (ROWS + 1)));
      const values = new Uint8Array(2 * ROWS);
      const codes = new Uint8Array(ROWS);
      const deltas = new Uint8Array(ROWS);
      const labels = new Uint8Array(3 * ROWS);
      for (let row = 0; row < ROWS; row += 1) {
        const owner = row % OWNERS.length;
        ownerIds.fill(OWNERS[owner] ?? 0, 16 * row, 16 * (row + 1));
        codes[row] = owner;
        // Each number is its row's, so each delta but the first is 1.
        numbers.setBigInt64(8 * row, BigInt(row), true);
        deltas[row] = row === 0 ? 0 : 2;
        const label = [0x72, 0x30 + (row % 7)];
        lengths.setUint32(4 * (row + 1), label.length, true);
        values.set(label, 2 * row);
        labels.set([...label, 0xff], 3 * row);
      }
      const { head, artifact } = artifactOf([
        {
          name: "bulk",
          rows: weights.byteLength / 8,
          primaryKey: ["weight"],
          columns: [{ name: "weight", type: "f64", parts: [weights] }],
        },
        {
          name: "issues",
          rows: ROWS,
          primaryKey: ["n"],
          columns: [
            { name: "owner_id", type: "uuid", parts: [ownerIds] },
            {
              name: "n",
              type: "int64",
              parts: [new Uint8Array(numbers.buffer)],
            },
            {
              name: "label",
              type: "utf8",
              parts: [new Uint8Array(lengths.buffer), values],
            },
          ],
        },
        {
          name: "owners",
          rows: OWNERS.length,
          primaryKey: ["id"],
          columns: [{ name: "id", type: "uuid", parts: [keys] }],
        },
      ]);
      const stream = (
        column: number,
        transform: "fk" | "int" | "sep",
        streamBytes: number,
      ) => ({
        table: 1,
        column,
        transform,
        references: transform === "fk" ? 2 : null,
        width: transform === "sep" ? 0 : 1,
        escapes: 0,
        streamBytes,
      });
      const preamble = text.encode(
        JSON.stringify({
          version: 1,
          hoisted: [{ table: 2, column: 0, part: 0 }],
          columns: [
            stream(0, "fk", codes.byteLength),
            stream(1, "int", deltas.byteLength),
            stream(2, "sep", labels.byteLength),
          ],
        }),
      );
      const pieces: Pieces = {
        preamble,
        head,
        keys,
        weights,
        codes,
        deltas,
        labels,
      };
      return { ARTIFACT: artifact, PIECES: pieces };
    })();
    const CHUNKS = [0, 1, 2];

    /** The packed body, with `changed` in place of the pieces it names. */
    function packedBody(changed: Partial<Pieces> = {}): Uint8Array {
      const { preamble, head, keys, weights, codes, deltas, labels } = {
        ...PIECES,
        ...changed,
      };
      const length = new Uint8Array(4);
      new DataView(length.buffer).setUint32(0, preamble.byteLength, true);
      return joined([
        text.encode("CALQPAK1"),
        length,
        preamble,
        head,
        keys,
        weights,
        codes,
        deltas,
        labels,
      ]);
    }

    /** `bytes` with the byte at `at` set to `value`. */
    function withByte(
      bytes: Uint8Array,
      at: number,
      value: number,
    ): Uint8Array {
      const copy = bytes.slice();
      copy[at] = value;
      return copy;
    }

    /** `bytes` with one bit of the byte at `at` flipped. */
    const withBit = (bytes: Uint8Array, at: number, bit: number): Uint8Array =>
      withByte(bytes, at, (bytes[at] ?? 0) ^ (1 << bit));

    /** The plan for an artifact of `decodedBytes` whose body is `body`. */
    const packedPlan = (
      body: Uint8Array,
      decodedBytes = ARTIFACT.byteLength,
    ): Record<string, unknown> =>
      brotliPlan(decodedBytes, body.byteLength, PACKED);

    const digest = (bytes: Uint8Array): string =>
      createHash("sha256").update(bytes).digest("hex");

    /** The digest of each chunk of `bytes`. */
    const digests = (bytes: Uint8Array): string[] =>
      Array.from(
        { length: Math.ceil(bytes.byteLength / CHUNK_BYTES) },
        (_unused, index) =>
          digest(
            bytes.subarray(index * CHUNK_BYTES, (index + 1) * CHUNK_BYTES),
          ),
      );

    /** A file the server's tests read too. */
    const serverFixture = (path: string): Uint8Array =>
      new Uint8Array(
        readFileSync(
          new URL(`../../../../../spec/fixtures/${path}`, import.meta.url),
        ),
      );

    it("loads a body the packer wrote", async () => {
      const artifact = serverFixture("packed-artifact/every-kind/artifact.bin");
      const packed = serverFixture("packed-artifact/every-kind/packed.bin");
      const body = brotliOf(packed, artifact.byteLength);
      const { client, worker, decoders } = makeDecodingClient(
        serveBody(body, 1024),
        packedPlan(body, artifact.byteLength),
      );
      expect(await settled(client)).toEqual({ state: "ready" });
      expect(Buffer.compare(joined(worker.received), artifact)).toBe(0);
      expect(decoders.map(({ sent }) => sent)).toEqual([[0]]);
      await client.close();
    });

    it("loads the object the server publishes for that artifact", async () => {
      // The server's tests hold this object to what a publication writes.
      const artifact = serverFixture("packed-artifact/every-kind/artifact.bin");
      const object = serverFixture("packed-brotli-large-window/object.br");
      const { client, worker, decoders, requests } = makeDecodingClient(
        serveBody(object, 1024),
        packedPlan(object, artifact.byteLength),
      );
      expect(await settled(client)).toEqual({ state: "ready" });
      expect(requests("/artifact-1")).toBe(1);
      expect(Buffer.compare(joined(worker.received), artifact)).toBe(0);
      expect(decoders.map(({ sent }) => sent)).toEqual([[0]]);
      expect(decoders[0]?.bodyBytes).toBe(object.byteLength);
      await client.close();
    });

    it("loads the artifact its decoder worker unpacks, in the plan's chunks", async () => {
      expect(ARTIFACT.byteLength).toBeGreaterThan(2 * CHUNK_BYTES);
      const packed = packedBody();
      // Packing leaves it shorter than its artifact, before brotli.
      expect(packed.byteLength).toBeLessThan(0.6 * ARTIFACT.byteLength);
      const body = brotliOf(packed, ARTIFACT.byteLength);
      const { client, worker, hashers, decoders, requests } =
        makeDecodingClient(serveBody(body, PIECE_BYTES), packedPlan(body));
      expect(await settled(client)).toEqual({ state: "ready" });
      expect(requests("/artifact-1")).toBe(1);
      expect(worker.received.map((bytes) => bytes.byteLength)).toEqual([
        CHUNK_BYTES,
        CHUNK_BYTES,
        ARTIFACT.byteLength - 2 * CHUNK_BYTES,
      ]);
      expect(Buffer.compare(joined(worker.received), ARTIFACT)).toBe(0);
      expect(
        hashers.flatMap(({ chunks }) => chunks).sort((a, b) => a - b),
      ).toEqual(CHUNKS);
      expect(
        worker.requests.filter(({ type }) => type === "artifact-end"),
      ).toEqual([expect.objectContaining({ chunks: 3 })]);
      // The one decoder worker, told that what the body decodes to is
      // packed, and that the artifact is the plan's length.
      expect(decoders).toHaveLength(1);
      expect(decoders[0]?.start).toEqual({
        type: "start",
        decodedBytes: ARTIFACT.byteLength,
        chunkBytes: CHUNK_BYTES,
        skipChunks: 0,
        packed: true,
      });
      expect(decoders[0]?.bodyBytes).toBe(body.byteLength);
      expect(decoders[0]?.sent).toEqual(CHUNKS);
      expect(decoders[0]?.terminated).toBe(true);
      expect(await client.db.events.query().run()).toEqual([
        { id: 1n, level: "error" },
      ]);
      await client.close();
    });

    describe("is refused by the decoder worker, with no row shown and nothing asked for again,", () => {
      const column = (index: number, name: string): string =>
        `the body's column ${index} of table 1 has the defect ${name}`;
      const refusals: ReadonlyArray<{
        readonly name: string;
        readonly packed: () => Uint8Array;
        readonly decodedBytes?: number;
        readonly reason: string;
      }> = [
        {
          name: "when it is cut off a byte before its end",
          packed: () => packedBody().subarray(0, -1),
          reason: "the body ends before its last part",
        },
        {
          name: "when a byte follows its last part",
          packed: () => joined([packedBody(), Uint8Array.of(0)]),
          reason: "bytes follow the body's last part",
        },
        {
          // The preamble's first byte, its opening brace.
          name: "when one flipped bit leaves a preamble that is not JSON",
          packed: () =>
            packedBody({ preamble: withBit(PIECES.preamble, 0, 0) }),
          reason: "the body's preamble cannot be read",
        },
        {
          // The table `owner_id` refers to: 2 becomes 3, which is no table.
          name: "when one flipped bit of its preamble names another table",
          packed: () =>
            packedBody({
              preamble: withBit(
                PIECES.preamble,
                Buffer.from(PIECES.preamble).indexOf('"references":2') + 13,
                0,
              ),
            }),
          reason: column(0, "References"),
        },
        {
          // A separator's: the label it ended runs on into the next, and
          // the labels are a byte longer than the artifact's.
          name: "when one flipped bit of a stream makes a value of a separator",
          packed: () =>
            packedBody({ labels: withBit(PIECES.labels, 3 * 1000 + 2, 7) }),
          reason: column(2, "PartLength"),
        },
        {
          // The first third of the labels reads as numbers. The rest and
          // the numbers read as labels, and the numbers hold no separator,
          // so the labels come to more bytes than the artifact's.
          name: "when two parts of different lengths are swapped",
          packed: () =>
            packedBody({ deltas: PIECES.labels, labels: PIECES.deltas }),
          reason: column(2, "PartLength"),
        },
        {
          // Five owners: rows 0 to 4, and 5 would be an escape.
          name: "when a row index is past its table",
          packed: () => packedBody({ codes: withByte(PIECES.codes, 7, 6) }),
          reason: column(0, "RowIndex"),
        },
        {
          name: "when it restores more bytes than the plan's",
          packed: packedBody,
          decodedBytes: ARTIFACT.byteLength - 5,
          reason: `the body restores ${ARTIFACT.byteLength} bytes, not ${ARTIFACT.byteLength - 5}`,
        },
        {
          name: "when it restores fewer bytes than the plan's",
          packed: packedBody,
          decodedBytes: ARTIFACT.byteLength + 5,
          reason: `the body restores ${ARTIFACT.byteLength} bytes, not ${ARTIFACT.byteLength + 5}`,
        },
      ];

      for (const { name, packed, decodedBytes, reason } of refusals)
        it(name, async () => {
          const body = brotliOf(packed(), ARTIFACT.byteLength);
          await refused(
            body,
            PIECE_BYTES,
            packedPlan(body, decodedBytes),
            PACKED,
            reason,
          );
        });
    });

    describe("is refused by a chunk's digest, with no row shown, when the decoder worker cannot tell,", () => {
      // Each of these bodies is well formed, and unpacks to the artifact's
      // length and other bytes. The engine worker's comparison of each
      // chunk's digest with the plan's is all that refuses it, and the fake
      // is given that comparison here.
      const unseen: ReadonlyArray<{
        readonly name: string;
        readonly packed: () => Uint8Array;
        /** The chunks that differ from the artifact's. */
        readonly chunks: readonly number[];
      }> = [
        {
          // Every issue of the second owner, and that owner's key.
          name: "one flipped bit in a hoisted part",
          packed: () => packedBody({ keys: withBit(PIECES.keys, 16, 0) }),
          chunks: [1, 2],
        },
        {
          // The last number's delta: 1 becomes -2.
          name: "one flipped bit in a stream",
          packed: () =>
            packedBody({ deltas: withBit(PIECES.deltas, ROWS - 1, 0) }),
          chunks: [2],
        },
        {
          // Each is as long as the other and reads as the other's kind.
          name: "two parts of one length swapped",
          packed: () =>
            packedBody({ codes: PIECES.deltas, deltas: PIECES.codes }),
          chunks: [1, 2],
        },
        {
          // The eighth issue's owner: row 2 of `owners` becomes row 3.
          name: "one row index moved to another row of its table",
          packed: () => packedBody({ codes: withByte(PIECES.codes, 7, 3) }),
          chunks: [1],
        },
      ];
      const expected = digests(ARTIFACT);

      for (const { name, packed, chunks } of unseen)
        it(name, async () => {
          const body = brotliOf(packed(), ARTIFACT.byteLength);
          // Without the comparison the body loads, as the wrong artifact.
          const loose = makeDecodingClient(
            serveBody(body, PIECE_BYTES),
            packedPlan(body),
          );
          expect(await settled(loose.client)).toEqual({ state: "ready" });
          const restored = joined(loose.worker.received);
          expect(restored.byteLength).toBe(ARTIFACT.byteLength);
          expect(
            digests(restored).flatMap((value, index) =>
              value === expected[index] ? [] : [index],
            ),
          ).toEqual(chunks);
          await loose.client.close();

          const { client, worker, decoders, requests } = makeDecodingClient(
            serveBody(body, PIECE_BYTES),
            packedPlan(body),
          );
          const failed: number[] = [];
          worker.verifyChunk = (index, bytes) => {
            if (digest(bytes) === expected[index]) return true;
            failed.push(index);
            return false;
          };
          expect(await settled(client)).toMatchObject({
            state: "error",
            error: { message: "artifact digest binding mismatch" },
          });
          // The first chunk that differs is refused, and the engine worker
          // kept nothing of it.
          expect(failed).toEqual(chunks.slice(0, 1));
          expect(
            worker.received.flatMap((_bytes, index) => [index]),
          ).not.toContain(failed[0]);
          expect(worker.terminated).toBe(true);
          expect(decoders.map(({ terminated }) => terminated)).toEqual([true]);
          await expect(client.db.events.query().run()).rejects.toMatchObject({
            message: "artifact digest binding mismatch",
          });
          expect(requests("/artifact-1")).toBe(1);
          await client.close();
        });
    });

    it("asks again for a body the network cut, and its new decoder skips the chunk already handed over", async () => {
      const body = brotliOf(packedBody(), ARTIFACT.byteLength);
      // Inside `bulk`, and past the first chunk's end by 200,000 bytes.
      const at = 4_400_000;
      let drop!: () => void;
      const when = new Promise<void>((resolve) => {
        drop = resolve;
      });
      const { client, worker, hashers, decoders, requests } =
        makeDecodingClient(
          serveBody(body, PIECE_BYTES, (response) =>
            response === 1 ? { at, when } : undefined,
          ),
          packedPlan(body),
        );
      await until(
        () =>
          decoders[0]?.bodyBytes === at &&
          hashers.some(({ chunks }) => chunks.includes(0)),
        "the first response's chunk to reach a hasher",
      );
      drop();
      expect(await settled(client)).toEqual({ state: "ready" });
      expect(requests("/v1/bootstrap")).toBe(1);
      expect(requests("/artifact-1")).toBe(2);
      expect(marks()).toContain("calque:artifact:retry:1");
      // The second decoder unpacks the body from its first byte, which is
      // where the hoisted keys and the preamble are, and sends nothing of
      // the chunk the first response delivered.
      expect(
        decoders.map(({ start, bodyBytes, sent, terminated }) => ({
          packed: start?.packed,
          skipChunks: start?.skipChunks,
          bodyBytes,
          sent,
          terminated,
        })),
      ).toEqual([
        {
          packed: true,
          skipChunks: 0,
          bodyBytes: at,
          sent: [0],
          terminated: true,
        },
        {
          packed: true,
          skipChunks: 1,
          bodyBytes: body.byteLength,
          sent: [1, 2],
          terminated: true,
        },
      ]);
      expect(
        hashers.flatMap(({ chunks }) => chunks).sort((a, b) => a - b),
      ).toEqual(CHUNKS);
      expect(Buffer.compare(joined(worker.received), ARTIFACT)).toBe(0);
      expect(
        worker.requests.filter(({ type }) => type === "artifact-end"),
      ).toEqual([expect.objectContaining({ chunks: 3 })]);
      await client.close();
    });
  });
});

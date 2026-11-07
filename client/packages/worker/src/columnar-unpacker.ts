/**
 * Restores an artifact from the body a brotli stream decodes to, as the body
 * arrives: the artifact itself, or the packed body the server makes of it.
 * `spec/packed-artifact-v1.md` defines the packed format and its refusals.
 *
 * The artifact's bytes are written into the plan's chunks as the body's bytes
 * arrive. A row of byte planes is written once its last plane's byte is in, so
 * the earlier planes wait in a copy of the stream. `ready` is the offset below
 * which nothing changes any more.
 *
 * No buffer is sized by a number the body gives before that number has been
 * held to the artifact's header or to what is left of `decodedBytes`. Chunks
 * below `ready` may have gone out before a refusal, and a body that passes
 * here can still restore the wrong bytes: the plan's chunk digests refuse
 * those.
 */

/** An artifact's first bytes, and so a stored body's. */
const ARTIFACT_MAGIC = "CALQART1";
const PACKED_MAGIC = "CALQPAK1";
const FORMAT_NAME = "calque-direct-columnar";
const FORMAT_VERSION = 4;
/** The rows of a frame-of-reference block, and the bytes of its header. */
const BLOCK_ROWS = 1024;
const BLOCK_HEADER_BYTES = 16;
const UUID_BYTES = 16;
const SEPARATOR = 0xff;
/** An `int` column's values lie strictly between `-INT_LIMIT` and it. */
const INT_LIMIT = 2 ** 51;
/**
 * The longest header the artifact's reader takes (`MAX_HEADER_BYTES` in
 * `@calque/core`), and so the longest a body may carry.
 */
const MAX_HEADER_BYTES = 1_048_576;
/**
 * A preamble names each column and each part of the header at most once, in
 * under twice the bytes the header spends on it.
 */
const MAX_PREAMBLE_BYTES = 2 * MAX_HEADER_BYTES;
/**
 * A header of `MAX_HEADER_BYTES` has fewer tables, columns in a table and
 * parts in a column than this, so an entry's indexes order as one number.
 */
const FEW = 2 ** 17;
/**
 * One block's rows at their widest: rebuilt rows and a text column's values
 * are put this many bytes at a time.
 */
const SCRATCH_BYTES = BLOCK_ROWS * 8;
/** The most of a packed body `room` asks for at once. */
const PIECE_BYTES = 65_536;

const PREAMBLE_UNREAD = "the body's preamble cannot be read";
const HEADER_UNREAD = "the body's artifact header cannot be read";

const INTEGER = "(?:0|[1-9]\\d{0,14})";
const listOf = (entry: string): string => `\\[(?:${entry}(?:,${entry})*)?\\]`;
/**
 * The preamble as the packer spells it: no whitespace, its members in this
 * order, its integers in decimal and short enough to be exact as doubles.
 */
const PREAMBLE_SPELLING = new RegExp(
  `^\\{"version":1,"hoisted":${listOf(
    `\\{"table":${INTEGER},"column":${INTEGER},"part":${INTEGER}\\}`,
  )},"columns":${listOf(
    `\\{"table":${INTEGER},"column":${INTEGER},"transform":"(?:fk|int|sep)","references":(?:${INTEGER}|null),"width":${INTEGER},"escapes":${INTEGER},"streamBytes":${INTEGER}\\}`,
  )}\\}$`,
);

/** One transformed column, as the preamble lists it. */
interface Transformed {
  readonly table: number;
  readonly column: number;
  readonly transform: "fk" | "int" | "sep";
  readonly references: number | null;
  readonly width: number;
  readonly escapes: number;
  readonly streamBytes: number;
}

interface Preamble {
  readonly hoisted: readonly {
    readonly table: number;
    readonly column: number;
    readonly part: number;
  }[];
  readonly columns: readonly Transformed[];
}

/** The artifact header's members that unpacking reads, as JSON gives them. */
interface Header {
  readonly format: unknown;
  readonly version: unknown;
  readonly tables: readonly {
    readonly rows: unknown;
    readonly primaryKey: readonly unknown[];
    readonly columns: readonly {
      readonly name: unknown;
      readonly type: unknown;
      readonly nullable: unknown;
      readonly encoding?: {
        readonly kind: unknown;
        readonly blockRows: unknown;
      } | null;
      readonly parts: readonly {
        readonly offset: unknown;
        readonly length: unknown;
      }[];
    }[];
  }[];
}

/** One part of the artifact. */
interface Part {
  /** Where it begins in the artifact. */
  readonly at: number;
  readonly length: number;
  /** Its place in payload order. */
  place: number;
  /** Whether a stream reads it, so that its bytes are kept as they pass. */
  keep?: true;
  /** Its bytes: a hoisted part's, and a part's that `keep` had kept. */
  kept?: Uint8Array;
  /**
   * Set when a stream replaces the part. Takes the stream where it stands,
   * and nothing for a later part the same stream replaces.
   */
  begin?: () => void;
}

/** Refuses the body for what is wrong with one transformed column. */
type Defect = (name: string) => never;

const natural = (value: unknown): value is number =>
  Number.isSafeInteger(value) && (value as number) >= 0;

/** Whether `row` is present under the validity bitmap `bits`, as 1 or 0. */
const present = (bits: Uint8Array | undefined, row: number): number =>
  bits ? ((bits[row >> 3] ?? 0) >> (row & 7)) & 1 : 1;

/** A body being restored. */
export interface ArtifactUnpacker {
  /**
   * How many of the body's next bytes to bring. Of the artifact itself, as
   * many as fill the chunk `ready` is in, so that no byte of a later chunk
   * is taken before that one can go. Of a packed body, whose chunks do not
   * fill in order, no more than its current piece takes or than
   * `PIECE_BYTES`. Zero once the body has ended.
   */
  room(): number;
  /**
   * Takes the body's next bytes, any number of them, and copies what it
   * keeps. Returns why the body is refused, from then on, or `undefined`.
   */
  write(bytes: Uint8Array): string | undefined;
  /**
   * The body has no more bytes. Returns why it is refused, which is also
   * what an earlier `write` returned and what a later one will, or
   * `undefined` when it ended exactly after its last part.
   */
  end(): string | undefined;
  /** The offset in the artifact below which every byte is final. */
  ready(): number;
  /**
   * Chunk `index` of the artifact, which the unpacker then forgets: zeros
   * where nothing was written, and where it is one of the chunks to skip.
   */
  take(index: number): Uint8Array<ArrayBuffer>;
}

/**
 * Restores an artifact of `decodedBytes` into chunks of `chunkBytes`, the
 * first `skipChunks` of which nobody takes and so are not written. `packed`
 * says the body may be a packed one; otherwise it is the artifact itself.
 */
export function artifactUnpacker(
  decodedBytes: number,
  chunkBytes: number,
  skipChunks: number,
  packed: boolean,
): ArtifactUnpacker {
  const chunks: (Uint8Array<ArrayBuffer> | undefined)[] = [];
  let refusal: string | undefined;
  /** Whether the body is the artifact itself. */
  let stored = !packed;
  /** How much of the body has been taken. */
  let taken = 0;
  let ready = 0;
  /** What the body's current piece still takes, and where its bytes go. */
  let need = 0;
  let eat: (bytes: Uint8Array) => void = () => undefined;
  /** Lays out the piece after the current one, once that has its bytes. */
  let then: (() => void) | undefined;
  /** A packed body's parts in payload order, and how many have been taken. */
  let order: Part[] = [];
  let place = 0;
  /** Where rebuilt rows are gathered before they are put, and as words. */
  let scratch = new Uint8Array(0);
  let scratchWords = new Uint32Array(0);
  /** Where a text column's lengths are, before they are put. */
  let noted = scratch;

  const refuse = (message: string): never => {
    refusal = message;
    throw new Error(message);
  };

  const chunk = (index: number): Uint8Array<ArrayBuffer> =>
    (chunks[index] ??= new Uint8Array(
      Math.min(chunkBytes, decodedBytes - index * chunkBytes),
    ));

  /** Writes `bytes` at `at` in the artifact, across the chunks they span. */
  const put = (at: number, bytes: Uint8Array): void => {
    for (let done = 0; done < bytes.length;) {
      const index = Math.floor((at + done) / chunkBytes);
      const from = at + done - index * chunkBytes;
      const length = Math.min(bytes.length - done, chunkBytes - from);
      if (index >= skipChunks)
        chunk(index).set(bytes.subarray(done, done + length), from);
      done += length;
    }
  };

  /** `length`, which the body may hold besides what has been taken. */
  const fits = (length: number): number =>
    length <= decodedBytes - taken
      ? length
      : refuse("the body is longer than its artifact");

  /** The body's next `length` bytes go to `take`, and `next` follows them. */
  const piece = (
    length: number,
    take: (bytes: Uint8Array) => void,
    next?: () => void,
  ): void => {
    need = fits(length);
    eat = take;
    then = next;
  };

  /** Gives `done` the body's next `length` bytes, in a buffer of its own. */
  const collect = (length: number, done: (bytes: Uint8Array) => void): void => {
    const bytes = new Uint8Array(fits(length));
    let got = 0;
    piece(
      length,
      (next) => {
        bytes.set(next, got);
        got += next.length;
      },
      () => {
        done(bytes);
      },
    );
  };

  /** The body's next `length` bytes are the artifact's from `at`. */
  const through = (
    at: number,
    length: number,
    kept?: Uint8Array,
    next?: () => void,
  ): void => {
    const start = at;
    ready = at;
    piece(
      length,
      (bytes) => {
        put(at, bytes);
        kept?.set(bytes, at - start);
        at += bytes.length;
        ready = at;
      },
      next,
    );
  };

  /** Takes the next part in payload order, or ends the body. */
  const walk = (): void => {
    const part = order[place++];
    if (!part) ready = decodedBytes;
    else if (part.begin) {
      ready = part.at;
      part.begin();
    } else if (part.kept) {
      // Hoisted: nothing else is kept before its place comes.
      put(part.at, part.kept);
      then = walk;
    } else
      through(
        part.at,
        part.length,
        part.keep ? (part.kept = new Uint8Array(fits(part.length))) : undefined,
        walk,
      );
  };

  /**
   * Takes an `fk` stream, which stands for the uuid values of `values`: for
   * each of `rows`, a row of the key part `key`, of `keyRows` rows, in
   * `width` byte planes, and then the escapes.
   */
  const takeKeys = (
    { width, escapes, streamBytes }: Transformed,
    defect: Defect,
    rows: number,
    validity: Part | undefined,
    values: Part,
    key: Part,
    keyRows: number,
  ): void => {
    const bits = validity?.kept;
    // Hoisted, or stored ahead of the stream: otherwise it is not here yet.
    const keys = key.kept ?? defect("References");
    // A uuid goes from the key part to the scratch as four words, as they
    // are: both buffers are the unpacker's own, and so begin on a word.
    const keyWords = new Uint32Array(keys.buffer, 0, keys.length >> 2);
    const words = scratchWords;
    const stream = new Uint8Array(fits(streamBytes));
    /** Where the stream's planes end: `width` of them, of `rows` bytes. */
    const planes = rows * width;
    /** The rows that take an escape, which the stream holds last. */
    const escaped: number[] = [];
    let got = 0;
    let row = 0;
    piece(
      streamBytes,
      (bytes) => {
        stream.set(bytes, got);
        got += bytes.length;
        // The rows whose every plane has arrived.
        const most = Math.min(rows, got - rows * (width - 1));
        while (row < most) {
          const count = Math.min(most - row, SCRATCH_BYTES / UUID_BYTES);
          for (let out = 0; out < count * 4; out += 4, row++) {
            let code = stream[row] ?? 0;
            for (
              let at = rows + row, weight = 256;
              at < planes;
              at += rows, weight *= 256
            )
              code += (stream[at] ?? 0) * weight;
            const live = bits ? ((bits[row >> 3] ?? 0) >> (row & 7)) & 1 : 1;
            if (live && code < keyRows) {
              const from = code * 4;
              words[out] = keyWords[from] ?? 0;
              words[out + 1] = keyWords[from + 1] ?? 0;
              words[out + 2] = keyWords[from + 2] ?? 0;
              words[out + 3] = keyWords[from + 3] ?? 0;
              continue;
            }
            // A null row and an escape are zeros, the escape for now.
            if (!live) {
              if (code) defect("NullRow");
            } else if (code > keyRows) defect("RowIndex");
            else if (escaped.push(row) > escapes) defect("EscapePastEscapes");
            words.fill(0, out, out + 4);
          }
          put(
            values.at + UUID_BYTES * (row - count),
            scratch.subarray(0, count * UUID_BYTES),
          );
        }
        ready = values.at + UUID_BYTES * Math.min(row, escaped[0] ?? row);
      },
      () => {
        if (escaped.length < escapes) defect("UnusedEscapes");
        escaped.forEach((at, index) => {
          const from = rows * width + UUID_BYTES * index;
          put(
            values.at + UUID_BYTES * at,
            stream.subarray(from, from + UUID_BYTES),
          );
        });
        walk();
      },
    );
  };

  /**
   * Takes an `int` stream, which stands for `replaced`: the zigzag deltas of
   * the present rows in `width` byte planes. `replaced` is the packed part
   * of a frame-of-reference column, whose block headers are `headers`, and
   * without them the column's `i64` values, as a block of width 64 holds
   * them.
   */
  const takeDeltas = (
    { width, streamBytes }: Transformed,
    defect: Defect,
    rows: number,
    validity: Part | undefined,
    replaced: Part,
    headers: Part | undefined,
  ): void => {
    const bits = validity?.kept;
    const blocks = headers?.kept && new DataView(headers.kept.buffer);
    let count = rows;
    if (bits) {
      count = 0;
      for (let row = 0; row < rows; row++) count += present(bits, row);
    }
    if (count * width !== streamBytes) defect("Rows");
    const stream = new Uint8Array(fits(streamBytes));
    /** Where the stream's planes end: `width` of them, of `count` bytes. */
    const planes = count * width;
    const rebuilt = scratch;
    let got = 0;
    let block = 0;
    /** The present rows of `block`, once they are counted. */
    let live = -1;
    /** How many deltas the blocks so far have read. */
    let index = 0;
    let previous = 0;
    let written = 0;
    /** Writes every block whose rows' last plane has arrived. */
    const rebuild = (): void => {
      const most = width ? got - count * (width - 1) : count;
      for (; block * BLOCK_ROWS < rows; block++, live = -1) {
        const start = block * BLOCK_ROWS;
        const size = Math.min(BLOCK_ROWS, rows - start);
        if (live < 0) {
          live = size;
          if (bits) {
            live = 0;
            for (let row = start; row < start + size; row++)
              live += present(bits, row);
          }
        }
        if (index + live > most) return;
        let rowBits = 64;
        let base = 0;
        let unit = 1;
        if (blocks) {
          const header = block * BLOCK_HEADER_BYTES;
          if (blocks.getUint32(header, true) !== written) defect("Block");
          rowBits = blocks.getUint8(header + 12);
          if (rowBits !== 64) {
            const scale = blocks.getUint8(header + 13);
            base =
              blocks.getInt32(header + 8, true) * 2 ** 32 +
              blocks.getUint32(header + 4, true);
            unit = scale ? 1000 : 1;
            if (
              (scale && scale !== 3) ||
              rowBits > 64 ||
              !(Math.abs(base) < INT_LIMIT)
            )
              defect("Block");
          }
        }
        const length = Math.ceil((size * rowBits) / 8);
        if (written + length > replaced.length) defect("PartLength");
        if (!width && !rowBits) {
          // No planes and no bits: each present row is 0 and only the base
          // needs checking. Skipping the row loop stops 16 header bytes from
          // buying 1,024 turns of it.
          if (live && base) defect("Block");
          continue;
        }
        const limit = 2 ** rowBits;
        let out = 0;
        let pending = 0;
        let filled = 0;
        for (let row = start; row < start + size; row++) {
          let value = 0;
          if (!bits || ((bits[row >> 3] ?? 0) >> (row & 7)) & 1) {
            const first = stream[index] ?? 0;
            let zigzag = first;
            for (
              let at = count + index, weight = 256;
              at < planes;
              at += count, weight *= 256
            )
              zigzag += (stream[at] ?? 0) * weight;
            index++;
            // The sign is the lowest bit, which the first plane holds.
            previous += first & 1 ? -(zigzag + 1) / 2 : zigzag / 2;
            if (!(Math.abs(previous) < INT_LIMIT)) defect("Value");
            value = previous;
            if (rowBits !== 64) {
              // A quotient this small is whole only when the division is.
              value = previous / unit;
              if (value !== Math.floor(value)) defect("Block");
              value -= base;
              if (value < 0 || value >= limit) defect("Block");
            }
          }
          const high = Math.floor(value / 2 ** 32);
          if (rowBits === 64) {
            const low = value >>> 0;
            rebuilt[out] = low;
            rebuilt[out + 1] = low >>> 8;
            rebuilt[out + 2] = low >>> 16;
            rebuilt[out + 3] = low >>> 24;
            rebuilt[out + 4] = high;
            rebuilt[out + 5] = high >> 8;
            rebuilt[out + 6] = high >> 16;
            rebuilt[out + 7] = high >> 24;
            out += 8;
          } else
            // From the lowest bit up, 16 bits at a time: with the bits
            // pending, fewer than 8, they stay within 32.
            for (
              let left = rowBits, low = value >>> 0, rest = high;
              left > 0;
              left -= 16
            ) {
              pending |= (low & 0xffff) << filled;
              low = (low >>> 16) | (rest << 16);
              rest >>>= 16;
              for (filled += Math.min(left, 16); filled >= 8; filled -= 8) {
                rebuilt[out++] = pending;
                pending >>>= 8;
              }
            }
        }
        if (filled) rebuilt[out] = pending;
        put(replaced.at + written, rebuilt.subarray(0, length));
        written += length;
        ready = replaced.at + written;
      }
      if (written !== replaced.length) defect("PartLength");
    };
    piece(
      streamBytes,
      (bytes) => {
        stream.set(bytes, got);
        got += bytes.length;
        rebuild();
      },
      () => {
        rebuild();
        walk();
      },
    );
  };

  /**
   * Takes a `sep` stream, which stands for the `lengths` and the `values` of
   * a text column: each present row's value and then a separator.
   */
  const takeValues = (
    { streamBytes }: Transformed,
    defect: Defect,
    rows: number,
    validity: Part | undefined,
    lengths: Part,
    values: Part,
  ): void => {
    const bits = validity?.kept;
    const gathered = scratch;
    const notes = noted;
    /** How many bytes of lengths `notes` holds: the rows' since `flush`. */
    let held = 0;
    let row = 0;
    /** The bytes so far of the value the next separator ends. */
    let size = 0;
    let written = 0;
    // The first length is the 0 the chunk already holds.
    let at = lengths.at + 4;
    const flush = (): void => {
      put(at, notes.subarray(0, held));
      at += held;
      held = 0;
    };
    piece(
      streamBytes,
      (bytes) => {
        for (let base = 0; base < bytes.length; base += SCRATCH_BYTES) {
          // The slice's values, moved together over its separators.
          const slice = bytes.subarray(base, base + SCRATCH_BYTES);
          gathered.set(slice);
          let kept = 0;
          for (let from = 0; ;) {
            const separator = slice.indexOf(SEPARATOR, from);
            const stop = separator < 0 ? slice.length : separator;
            size += stop - from;
            if (written + kept + stop - from > values.length)
              defect("PartLength");
            if (kept !== from) gathered.copyWithin(kept, from, stop);
            kept += stop - from;
            if (separator < 0) break;
            from = separator + 1;
            // The value is the next present row's, and the null rows before
            // that one have none.
            for (let live = 0; !live; row++) {
              if (row === rows) defect("Separators");
              live = present(bits, row);
              if (held === SCRATCH_BYTES) flush();
              const length = live ? size : 0;
              notes[held] = length;
              notes[held + 1] = length >>> 8;
              notes[held + 2] = length >>> 16;
              notes[held + 3] = length >>> 24;
              held += 4;
            }
            size = 0;
          }
          put(values.at + written, gathered.subarray(0, kept));
          written += kept;
        }
        flush();
        ready = at;
      },
      () => {
        // The null rows left have the length 0 the chunk holds.
        for (; row < rows; row++) if (present(bits, row)) defect("Separators");
        if (size || written !== values.length) defect("PartLength");
        walk();
      },
    );
  };

  /**
   * Checks the preamble against the header in the artifact's first
   * `payloadStart` bytes, and lays out the rest of the body.
   */
  const lay = (
    preamble: Uint8Array,
    json: Uint8Array,
    payloadStart: number,
  ): void => {
    let unread = PREAMBLE_UNREAD;
    const must = (held: unknown): void => {
      if (!held) refuse(unread);
    };
    try {
      const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
      const spelled = text.decode(preamble);
      if (!PREAMBLE_SPELLING.test(spelled)) {
        const version = (JSON.parse(spelled) as { version?: unknown } | null)
          ?.version;
        refuse(
          natural(version) && version !== 1
            ? `the body is version ${version}, not 1`
            : unread,
        );
      }
      const { hoisted, columns } = JSON.parse(spelled) as Preamble;

      unread = HEADER_UNREAD;
      const header = JSON.parse(text.decode(json)) as Header;
      must(header.format === FORMAT_NAME && header.version === FORMAT_VERSION);
      /** Every part, by table, column and its place among the column's. */
      const parts = header.tables.map((table) => {
        must(natural(table.rows) && table.rows < 2 ** 32);
        must(Array.isArray(table.primaryKey));
        return table.columns.map((column) => {
          must(typeof column.nullable === "boolean");
          return column.parts.map(({ offset, length }): Part => {
            must(natural(offset) && natural(length));
            return {
              at: payloadStart + (offset as number),
              length: length as number,
              place: 0,
            };
          });
        });
      });
      // Header order, which a stable sort keeps among equal extents.
      order = parts.flat(2).sort((a, b) => a.at - b.at || a.length - b.length);
      let end = payloadStart;
      order.forEach((part, index) => {
        must(part.at === Math.ceil(end / 8) * 8);
        part.place = index;
        end = part.at + part.length;
      });
      if (end !== decodedBytes)
        refuse(`the body restores ${end} bytes, not ${decodedBytes}`);

      unread = PREAMBLE_UNREAD;
      let before = -1;
      for (const entry of columns) {
        const { transform } = entry;
        const table = header.tables[entry.table];
        const column = table?.columns[entry.column];
        const own = parts[entry.table]?.[entry.column];
        const listed = entry.table * FEW + entry.column;
        must(table && column && own && listed > before);
        if (!table || !column || !own) continue;
        before = listed;
        must(
          transform === "fk" ||
            (entry.references === null &&
              !entry.escapes &&
              (transform === "int" || !entry.width)),
        );
        const defect: Defect = (name) =>
          refuse(
            `the body's column ${entry.column} of table ${entry.table} has the defect ${name}`,
          );
        const rows = table.rows as number;
        const { type, nullable, encoding } = column;
        const validity = nullable ? own[0] : undefined;
        const data = own.slice(nullable ? 1 : 0);
        const [first, second] = data;
        const shaped =
          own.every(
            (part, index) => (own[index - 1]?.place ?? -1) < part.place,
          ) &&
          (!nullable || validity?.length === Math.ceil(rows / 8));
        if (validity) validity.keep = true;

        if (transform === "fk") {
          if (type !== "uuid") defect("Type");
          if (!shaped || !first || second || first.length !== UUID_BYTES * rows)
            defect("Parts");
          const referenced = entry.references ?? entry.table;
          const target =
            referenced === entry.table ? undefined : header.tables[referenced];
          // The first column the primary key names, when it names one.
          const keyColumn =
            target?.primaryKey.length === 1
              ? target.columns.findIndex(
                  ({ name }) => name === target.primaryKey[0],
                )
              : -1;
          const keyHeader = target?.columns[keyColumn];
          const [key, more] = parts[referenced]?.[keyColumn] ?? [];
          const keyRows = (target?.rows ?? 0) as number;
          if (
            !key ||
            more ||
            key.length !== UUID_BYTES * keyRows ||
            keyHeader?.type !== "uuid" ||
            keyHeader.nullable
          )
            defect("References");
          if (
            entry.width !==
            (keyRows > 0xffffff
              ? 4
              : keyRows > 0xffff
                ? 3
                : keyRows > 0xff
                  ? 2
                  : 1)
          )
            defect("Width");
          if (
            entry.streamBytes !==
            rows * entry.width + UUID_BYTES * entry.escapes
          )
            defect("Rows");
          if (!first || !key) continue;
          key.keep = true;
          first.begin = () => {
            takeKeys(entry, defect, rows, validity, first, key, keyRows);
          };
        } else if (transform === "int") {
          if (type !== "int64" && type !== "timestamp") defect("Type");
          const replaced = encoding ? second : first;
          if (
            !shaped ||
            !first ||
            !replaced ||
            data.length !== (encoding ? 2 : 1) ||
            (encoding
              ? encoding.kind !== "for" ||
                encoding.blockRows !== BLOCK_ROWS ||
                first.length !==
                  BLOCK_HEADER_BYTES * Math.ceil(rows / BLOCK_ROWS)
              : first.length !== 8 * rows)
          )
            defect("Parts");
          if (entry.width > 7) defect("Width");
          if (!first || !replaced) continue;
          const headers = encoding ? first : undefined;
          if (headers) headers.keep = true;
          replaced.begin = () => {
            takeDeltas(entry, defect, rows, validity, replaced, headers);
          };
        } else {
          if (type !== "utf8" && type !== "json") defect("Type");
          if (
            !shaped ||
            !first ||
            !second ||
            data.length !== 2 ||
            first.length !== 4 * (rows + 1)
          )
            defect("Parts");
          if (!first || !second) continue;
          first.begin = () => {
            takeValues(entry, defect, rows, validity, first, second);
          };
          second.begin = () => {
            then = walk;
          };
        }
      }

      before = -1;
      const lifted = hoisted.map((entry) => {
        const part = parts[entry.table]?.[entry.column]?.[entry.part];
        const listed = (entry.table * FEW + entry.column) * FEW + entry.part;
        must(part && !part.begin && listed > before);
        before = listed;
        return part;
      });
      scratch = new Uint8Array(SCRATCH_BYTES);
      scratchWords = new Uint32Array(scratch.buffer);
      noted = new Uint8Array(SCRATCH_BYTES);
      let next = 0;
      /** Keeps the hoisted parts, then takes the payload's. */
      const hoist = (): void => {
        const part = lifted[next++];
        if (part)
          collect(part.length, (bytes) => {
            part.kept = bytes;
            then = hoist;
          });
        else walk();
      };
      then = hoist;
    } catch {
      refuse(refusal ?? unread);
    }
  };

  /** Reads a packed body up to its payload, or passes a stored one through. */
  const begin = (): void => {
    collect(8, (magic) => {
      const name = String.fromCharCode(...magic);
      if (name === ARTIFACT_MAGIC) {
        stored = true;
        put(0, magic);
        through(8, decodedBytes - 8);
      } else if (name !== PACKED_MAGIC)
        refuse(
          "the body begins with neither a packed nor a stored artifact's magic",
        );
      else
        collect(4, (length) => {
          const preambleBytes = new DataView(length.buffer).getUint32(0, true);
          if (preambleBytes > MAX_PREAMBLE_BYTES) refuse(PREAMBLE_UNREAD);
          collect(preambleBytes, (preamble) => {
            collect(12, (head) => {
              const headerBytes = new DataView(head.buffer).getUint32(8, true);
              if (
                String.fromCharCode(...head.subarray(0, 8)) !==
                  ARTIFACT_MAGIC ||
                headerBytes > MAX_HEADER_BYTES
              )
                refuse(HEADER_UNREAD);
              const payloadStart = Math.ceil((12 + headerBytes) / 8) * 8;
              collect(payloadStart - 12, (rest) => {
                // No chunk is made before the header has been held to
                // `decodedBytes`.
                lay(preamble, rest.subarray(0, headerBytes), payloadStart);
                put(0, head);
                put(12, rest);
              });
            });
          });
        });
    });
  };

  then = packed
    ? begin
    : () => {
        through(0, decodedBytes);
      };

  const unpacker: ArtifactUnpacker = {
    room: () =>
      Math.min(need, stored ? chunkBytes - (ready % chunkBytes) : PIECE_BYTES),
    write: (bytes) => {
      try {
        for (let at = 0; refusal === undefined;) {
          while (!need && then) {
            const next = then;
            then = undefined;
            next();
          }
          if (at === bytes.length) break;
          if (!need)
            refuse(
              stored
                ? `the body decodes to more than ${decodedBytes} bytes`
                : "bytes follow the body's last part",
            );
          const length = Math.min(need, bytes.length - at);
          taken += length;
          need -= length;
          eat(bytes.subarray(at, at + length));
          at += length;
        }
      } catch (error) {
        if (refusal === undefined) throw error;
      }
      return refusal;
    },
    end: () => {
      if (refusal === undefined && need)
        refusal = stored
          ? `the body decodes to ${taken} bytes, not ${decodedBytes}`
          : "the body ends before its last part";
      return refusal;
    },
    ready: () => ready,
    take: (index) => {
      const bytes = chunk(index);
      chunks[index] = undefined;
      return bytes;
    },
  };
  // Lays out the body's first piece, or refuses an artifact too short for it.
  unpacker.write(new Uint8Array(0));
  return unpacker;
}

/**
 * Restores the artifact of `decodedBytes` that the whole `body` stands for.
 * Throws an `Error` whose message says why a body is refused.
 */
export function unpackArtifact(
  body: Uint8Array,
  decodedBytes: number,
): Uint8Array<ArrayBuffer> {
  const unpacker = artifactUnpacker(
    decodedBytes,
    Math.max(decodedBytes, 1),
    0,
    true,
  );
  const refusal = unpacker.write(body) ?? unpacker.end();
  if (refusal !== undefined) throw new Error(refusal);
  return unpacker.take(0);
}

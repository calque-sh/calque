# Packed artifact v1 — wire contract

A packed body is a direct-columnar v4 artifact rewritten so that large-window
brotli encodes it in fewer bytes. The server packs and brotli-encodes it; the
browser decodes the brotli stream and unpacks the body back to the artifact,
byte for byte. `server/crates/direct-columnar/src/pack.rs` defines the format
(`pack`, `unpack`), and `client/packages/worker/src/columnar-unpacker.ts`
restores it in the browser. Both are tested against the artifacts and bodies in
`spec/fixtures/packed-artifact/`.

This document is written for an unpacker in any language. Where it and
`unpack` disagree, `unpack` decides what Calque accepts, and the document is the
defect. Refusal names below are identifiers in `pack.rs`.

All integers are little-endian. `align8(n)` is `n` rounded up to a multiple of
8. A column's _rows_ is the row count the artifact's header gives its table.

## The artifact, as packing reads it

An artifact is the 8 bytes `CALQART1`, a `u32` header length `H`, `H` bytes of
JSON, zeros up to `payloadStart = align8(12 + H)`, then the payload. The
header's `format` is `"calque-direct-columnar"` and its `version` is 4. Each
entry of `tables` has `name`, `rows`, `primaryKey` (column names) and
`columns`; each column has `name`, `type`, `nullable`, an optional `encoding`
and `parts`, a list of `{offset, length}` counted from `payloadStart`.

The parts have two orders:

- **Header order** is table, column and part order, as the header lists them.
- **Payload order** is ascending offset, then ascending length, then header
  order.

In payload order the first part is at offset 0, each next one at `align8` of
the end of the one before, the bytes between two parts are zeros, and the
artifact ends where its last part ends. The two orders differ when the payload
holds the tables in another order than the header lists them.

A nullable column's first part is its validity bitmap, `ceil(rows / 8)` bytes:
row `r` is present when bit `r % 8` of byte `r / 8` is set, and null otherwise.
A column that is not nullable has no such part and every row is present. The
column's data parts follow, in payload order too:

| Type | `encoding` | Data parts |
| --- | --- | --- |
| `uuid` | none | One part of `16 * rows` bytes; a null row's 16 bytes are zeros. |
| `int64`, `timestamp` | none | One part of `8 * rows` bytes, each row an `i64`; a null row's is 0. |
| `int64`, `timestamp` | `{"kind":"for","blockRows":1024}` | A block-headers part of `16 * ceil(rows / 1024)` bytes, then a packed part. |
| `utf8`, `json` | none | A lengths part of `4 * (rows + 1)` bytes, a `u32` 0 and then each row's value length (a null row's is 0); then a values part, the values end to end. |

In a frame-of-reference column, block `b` covers rows `1024 * b` up to the next
block or the last row. Its header is a `u32` offset into the packed part, an
`i64` base, a `u8` width in bits, a `u8` scale and two more bytes. At width 64
the block is its rows' `i64` values, a null row's being 0. Below 64 it is one
unsigned `width`-bit number per row, packed from the least significant bit of
the first byte upward into `ceil(count * width / 8)` bytes whose unused high
bits are 0. A present row's number is `value / unit - base`, with `unit` 1 at
scale 0 and 1000 at scale 3; a null row's is 0.

## The body

A body is one of two things, told apart by its first 8 bytes.

A **stored body** is the artifact itself, so it begins `CALQART1`. The packer
writes one when packing would not make the artifact shorter, so no body is
longer than its artifact.

A **packed body** is, with nothing between its pieces:

1. the 8 bytes `CALQPAK1`;
2. a `u32`, the preamble's length in bytes;
3. the preamble;
4. the artifact's first `payloadStart` bytes, as stored;
5. the hoisted parts, in the order the preamble lists them, each as stored;
6. for every part in payload order: nothing if it is hoisted; its column's
   stream if it is the first part that stream replaces; nothing if it is a
   later part the stream replaces; otherwise the part as stored.

The body ends with the last part and carries none of the padding between
parts.

## The preamble

UTF-8 JSON without whitespace, its members in the order given here, its
integers in decimal:

```text
{"version":1,"hoisted":[...],"columns":[...]}
```

`version` is 1. Each entry of `hoisted` names one part:

```text
{"table":T,"column":C,"part":K}
```

`T` indexes the header's `tables`, `C` that table's `columns` and `K` that
column's `parts`, all from 0. The entries are in header order and none repeats.
A part is hoisted when it is the key part of a table an `fk` column references
and it comes after that column's data part in payload order.

Each entry of `columns` is one transformed column:

```text
{"table":T,"column":C,"transform":X,"references":R,"width":W,"escapes":E,"streamBytes":N}
```

| Member | JSON type | Rule |
| --- | --- | --- |
| `table` | integer | The column's table, an index into `tables`. |
| `column` | integer | The column, an index into that table's `columns`. |
| `transform` | string | `"fk"`, `"int"` or `"sep"`. |
| `references` | integer or null | `fk`: the table whose key the column is coded against, an index into `tables`. Otherwise `null`. |
| `width` | integer | `fk`: bytes per row code, 1 to 4. `int`: bytes per delta, 0 to 7. `sep`: 0. |
| `escapes` | integer | `fk`: how many present values the key part does not hold. Otherwise 0. |
| `streamBytes` | integer | The length of the column's stream. |

The entries are in header order and no column appears twice. A column that is
not listed is stored: each of its parts is in the body unchanged. Which
columns are transformed is the packer's choice; an unpacker accepts any body
these rules allow.

## Streams

### `fk`

The column is a `uuid` column, and `references` names another table whose
`primaryKey` is one name, that of a `uuid` column that is not nullable (the
first column of that name). That column's one part is the _key part_, and
`keyRows` is that table's row count. `width` is the fewest bytes, at least one,
that hold `keyRows`: 1 below 2^8, 2 below 2^16, 3 below 2^24, else 4.

Every row has a code. A null row's is 0. A present row's is the lowest row of
the key part that holds the row's 16 bytes, or `keyRows` when none does: that
is the escape code, and the row's 16 bytes are then an escape.

The stream is `width` byte planes of `rows` bytes each, then the escapes, 16
bytes each, in row order: `rows * width + 16 * escapes` bytes. Plane `p`, from
0, holds byte `p` of every row's code, least significant byte first, so row
`r`'s code is the sum over `p` of `stream[p * rows + r] << (8 * p)`.

The stream replaces the column's data part. A validity part is stored.

### `int`

The column is an `int64` or `timestamp` column. Let `v[0..n]` be the values of
its present rows in row order, null rows left out. Then `d[i] = v[i] - v[i - 1]`
with `v[-1] = 0`, and `z[i]` is `d[i]` zigzag coded: `2 * d` when `d >= 0`,
`-2 * d - 1` when `d < 0`. `width` is the fewest bytes that hold the largest
`z`, and 0 when every `z` is 0 or `n` is 0.

The stream is `width` byte planes of `n` bytes each, `n * width` bytes: `z[i]`
is the sum over `p` of `stream[p * n + i] << (8 * p)`.

Every value is strictly between -2^51 and 2^51, so every `z` is below 2^53 and
an unpacker may hold them all in doubles.

The stream replaces the column's last part: the values of a column without
`encoding`, the packed part of a frame-of-reference column. A validity part and
the block headers are stored.

### `sep`

The column is a `utf8` or `json` column. The stream is, for each present row in
row order, the value's bytes and then one byte `0xFF`; a null row adds nothing,
and a present empty value adds a lone `0xFF`. No value holds `0xFF`.

The stream replaces the lengths part and the values part, and stands where the
lengths part stood. A validity part is stored.

## Restoring the artifact

Read the preamble, then the artifact's header from piece 4. The output is
`decodedBytes` long, the length the caller expects, and starts as zeros; the
padding is whatever no step below writes. Copy piece 4 to its start. Then every
part goes to `payloadStart + offset`:

- A stored or hoisted part is copied.
- `fk`: for each row, read its code. A null row writes 16 zeros. A code below
  `keyRows` writes that row of the key part, which is hoisted or was stored
  earlier in the body. A code of `keyRows` writes the next escape.
- `int`: add each `d[i]` to the value before it to get the present rows'
  values. Without `encoding`, write each row's value as an `i64`, a null row's
  as 0. With it, walk the stored block headers: each block's offset must be the
  number of bytes written to the packed part so far, and the block is written
  again at its own width, base and scale.
- `sep`: each present row's value is the stream up to the next `0xFF`. Write the
  lengths part, then the values end to end.

## What `unpack` refuses

`unpack` refuses a body as corrupt when:

- it begins with neither magic (`UnpackError::Magic`);
- the preamble runs past the body, is not JSON with a `version`, lacks a member
  or holds one of the wrong type, or is spelled otherwise than the preamble
  section says; its `columns` or `hoisted` are out of order, repeat, or name a
  column or part the header does not have; a hoisted part is one a stream
  replaces; or `references`, `escapes` or `width` is set on a transform it does
  not belong to (`UnpackError::Preamble`);
- `version` is not 1 (`UnpackError::Version`);
- piece 4 does not begin `CALQART1`, runs past the body, is not a v4 header, or
  its parts in payload order are not each at `align8` of the end of the one
  before (`UnpackError::Header`);
- the header's last part does not end at `decodedBytes`, or a stored body is not
  `decodedBytes` long (`UnpackError::DecodedLength`);
- the pieces, at the header's and the preamble's lengths, run past the body
  (`UnpackError::Truncated`) or end before it does
  (`UnpackError::TrailingBytes`);
- a transformed column has one of the defects below (`UnpackError::Column`,
  which names the column and the `Defect`).

| `Defect` | What the body holds |
| --- | --- |
| `Type` | A transform on a column of a type it does not apply to. |
| `Parts` | A column whose parts are not the ones its type, `nullable`, `encoding` and rows give it, or do not follow each other in payload order. |
| `Rows` | `streamBytes` other than `rows * width + 16 * escapes` for `fk`, or than the present rows times `width` for `int`. |
| `Width` | `fk`: a `width` other than the one `keyRows` gives. `int`: a `width` above 7. |
| `References` | `fk`: `references` that is null, the column's own table, no table, or a table without a key part; or a key part that is neither hoisted nor stored ahead of the stream in payload order. |
| `RowIndex` | A present row's code above `keyRows`. |
| `NullRow` | A null row's code other than 0. |
| `EscapePastEscapes` | More rows with the escape code than `escapes`. |
| `UnusedEscapes` | Fewer rows with the escape code than `escapes`. |
| `Value` | An `int` value 2^51 or more from zero. |
| `Block` | A block header whose offset is not the bytes written to the packed part so far, or whose width is above 64; below width 64, a scale other than 0 and 3, a base 2^51 or more from zero, or a present row's value that is not a multiple of `unit`, is below the base, or does not fit the width. |
| `Separators` | A `sep` stream whose count of `0xFF` is not the present rows. |
| `PartLength` | Blocks that run past the packed part or end before it; `sep` values that do not add up to the values part's length; bytes after a `sep` stream's last `0xFF`. |

A body that passes can still restore the wrong bytes, for instance with one row
code changed for another in range. Unpacking does not refuse those; the plan's
chunk digests over the restored artifact do.

const UUID_BYTES = 16;
const HEX_DIGITS = "0123456789abcdef";
const HIGH_DIGIT = Uint16Array.from({ length: 256 }, (_unused, byte) =>
  HEX_DIGITS.charCodeAt(byte >> 4),
);
const LOW_DIGIT = Uint16Array.from({ length: 256 }, (_unused, byte) =>
  HEX_DIGITS.charCodeAt(byte & 15),
);
/** Where each byte's two digits start in `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`. */
const DIGIT_OFFSETS = [
  0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34,
];
const textCodes = new Uint16Array(36).fill("-".charCodeAt(0));
const UUID_TEXT =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

/** The canonical lowercase text of a uuid's 16 bytes. */
export function uuidText(bytes: Uint8Array): string {
  if (bytes.length !== UUID_BYTES)
    throw new TypeError(`a uuid is 16 bytes, not ${bytes.length}`);
  for (let index = 0; index < UUID_BYTES; index += 1) {
    const byte = bytes[index] ?? 0;
    const at = DIGIT_OFFSETS[index] ?? 0;
    textCodes[at] = HIGH_DIGIT[byte] ?? 0;
    textCodes[at + 1] = LOW_DIGIT[byte] ?? 0;
  }
  // One flat string. Concatenating the digits instead leaves a rope of about
  // ten nodes per uuid, which retains several times the text's own size.
  return String.fromCharCode.apply(null, textCodes as unknown as number[]);
}

/** The 16 bytes of uuid text in either case; `name` labels the refusal. */
export function uuidBytes(value: unknown, name: string): Uint8Array {
  if (typeof value !== "string" || !UUID_TEXT.test(value))
    throw new TypeError(
      `${name} must be a uuid string, xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
    );
  const bytes = new Uint8Array(UUID_BYTES);
  for (let index = 0; index < UUID_BYTES; index += 1) {
    const at = DIGIT_OFFSETS[index] ?? 0;
    bytes[index] = Number.parseInt(value.slice(at, at + 2), 16);
  }
  return bytes;
}

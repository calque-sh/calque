import { Buffer } from "node:buffer";

/** The only payload version Calque accepts. */
export const GRANT_VERSION = 1;

/**
 * Prefixed to the payload bytes before signing, NUL byte included. A
 * signature over the bare payload is refused.
 */
export const GRANT_SIGNING_DOMAIN = "calque.dataset-grant.v1\0";

/** The longest validity window Calque accepts: 15 minutes. */
export const MAX_GRANT_LIFETIME_SECONDS = 15 * 60;

const MAX_PAYLOAD_BYTES = 4 * 1024;
const MAX_KEY_ID_BYTES = 128;
const MAX_AUDIENCE_BYTES = 256;
const MAX_NAME_BYTES = 512;
const MAX_ATTRIBUTES = 64;
const MAX_COMMAND_CAPABILITIES = 64;
const MAX_COLLECTION_BYTES = 128;
const OPERATIONS = ["insert", "update", "delete"] as const;
const CLAIMS = new Set([
  "keyId",
  "audience",
  "subject",
  "attributes",
  "authorizationGeneration",
  "issuedAtUnixSeconds",
  "expiresAtUnixSeconds",
  "commands",
]);

/**
 * One principal attribute in its wire form. `int8`, `bytes` and `uuid` travel
 * as strings, because a JavaScript number cannot hold every 64-bit integer,
 * and a tenant key that lost its low bits would authorize the wrong rows.
 * `int8` is a decimal integer, `bytes` is hex, and `uuid` is exactly 32 hex
 * digits with no hyphens.
 */
export type GrantAttributeValue =
  | { readonly bool: boolean }
  | { readonly int2: number }
  | { readonly int4: number }
  | { readonly int8: string }
  | { readonly bytes: string }
  | { readonly uuid: string };

export type GrantCommandOperation = (typeof OPERATIONS)[number];

/** One collection's write capability: exact names, no wildcard. */
export interface GrantCommandCapability {
  readonly collection: string;
  readonly operations: readonly GrantCommandOperation[];
}

/** Everything a grant states except its version, which is always 1. */
export interface DatasetGrantClaims {
  readonly keyId: string;
  readonly audience: string;
  readonly subject: string;
  readonly attributes: Readonly<Record<string, GrantAttributeValue>>;
  readonly authorizationGeneration: number;
  readonly issuedAtUnixSeconds: number;
  readonly expiresAtUnixSeconds: number;
  /** Absent or empty is a read-only grant, which authorizes no write. */
  readonly commands?: readonly GrantCommandCapability[];
}

/** A claim Calque's verifier would refuse, refused before it is signed. */
export class GrantRefused extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GrantRefused";
  }
}

export function refuse(message: string): never {
  throw new GrantRefused(message);
}

export const utf8Bytes = (text: string): number =>
  Buffer.byteLength(text, "utf8");

/** Rust's `u8::is_ascii_graphic`: `!` through `~`. */
const ASCII_GRAPHIC = /^[\x21-\x7e]+$/;
/** The same, without `.`, which separates a token's two halves. */
const KEY_ID = /^[\x21-\x2d\x2f-\x7e]+$/;
/** Rust's `char::is_control`, general category Cc. */
const CONTROL = /\p{Cc}/u;

export function wellFormed(field: string, value: unknown): string {
  if (typeof value !== "string" || !value.isWellFormed())
    refuse(`${field} must be a string of well-formed Unicode`);
  return value;
}

export function keyId(value: unknown): string {
  const text = wellFormed("keyId", value);
  if (utf8Bytes(text) > MAX_KEY_ID_BYTES || !KEY_ID.test(text))
    refuse(
      `keyId must be 1 to ${String(MAX_KEY_ID_BYTES)} printable ASCII characters other than "."`,
    );
  return text;
}

export function audience(value: unknown): string {
  const text = wellFormed("audience", value);
  if (text === "" || utf8Bytes(text) > MAX_AUDIENCE_BYTES)
    refuse(`audience must be 1 to ${String(MAX_AUDIENCE_BYTES)} bytes`);
  return text;
}

/** A subject or an attribute name, as `authz::Principal::new` checks it. */
function name(field: string, value: unknown): string {
  const text = wellFormed(field, value);
  if (text === "" || utf8Bytes(text) > MAX_NAME_BYTES || CONTROL.test(text))
    refuse(
      `${field} must be 1 to ${String(MAX_NAME_BYTES)} bytes of UTF-8 with no control character`,
    );
  return text;
}

export function unsigned(field: string, value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    refuse(
      `${field} must be a whole number from 0 to ${String(Number.MAX_SAFE_INTEGER)}`,
    );
  return value;
}

function integer(
  field: string,
  value: unknown,
  minimum: number,
  maximum: number,
): number {
  if (
    typeof value !== "number" ||
    !Number.isInteger(value) ||
    value < minimum ||
    value > maximum
  )
    refuse(
      `${field} must be a whole number from ${String(minimum)} to ${String(maximum)}`,
    );
  return value;
}

/** The attribute value's JSON, exactly as the Rust issuer writes it. */
export function attributeValue(attribute: string, value: unknown): string {
  const field = `attribute ${attribute}`;
  if (typeof value !== "object" || value === null || Array.isArray(value))
    refuse(`${field} must be an object naming one type`);
  const entries = Object.entries(value);
  const [entry] = entries;
  if (entry === undefined || entries.length !== 1)
    refuse(`${field} must name exactly one type`);
  const [type, raw] = entry;
  switch (type) {
    case "bool":
      if (typeof raw !== "boolean") refuse(`${field} must be true or false`);
      return `{"bool":${String(raw)}}`;
    case "int2":
      return `{"int2":${String(integer(field, raw, -32_768, 32_767))}}`;
    case "int4":
      return `{"int4":${String(integer(field, raw, -2_147_483_648, 2_147_483_647))}}`;
    case "int8":
      if (
        typeof raw !== "string" ||
        !/^[+-]?[0-9]+$/.test(raw) ||
        BigInt(raw) < -(2n ** 63n) ||
        BigInt(raw) >= 2n ** 63n
      )
        refuse(
          `${field} must be a decimal string within a signed 64-bit integer`,
        );
      return `{"int8":${JSON.stringify(raw)}}`;
    case "bytes":
      if (typeof raw !== "string" || !/^(?:[0-9a-fA-F]{2})*$/.test(raw))
        refuse(`${field} must be hex with an even number of digits`);
      return `{"bytes":${JSON.stringify(raw)}}`;
    case "uuid":
      if (typeof raw !== "string" || !/^[0-9a-fA-F]{32}$/.test(raw))
        refuse(`${field} must be 32 hex digits with no hyphens`);
      return `{"uuid":${JSON.stringify(raw)}}`;
    default:
      return refuse(
        `${field} has unknown type ${type}; the types are bool, int2, int4, int8, bytes and uuid`,
      );
  }
}

/**
 * Sorted by their UTF-8 bytes, which is how a Rust `BTreeMap` orders them.
 * `Array.prototype.sort` compares UTF-16 code units and disagrees above
 * U+FFFF, and an object's own key order puts integer-like keys first.
 */
function attributes(value: unknown): string {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    refuse("attributes must be an object, empty for none");
  const entries = Object.entries(value);
  if (entries.length > MAX_ATTRIBUTES)
    refuse(
      `a grant carries at most ${String(MAX_ATTRIBUTES)} attributes, not ${String(entries.length)}`,
    );
  return entries
    .map(([attribute, raw]) => ({
      key: Buffer.from(name("attribute name", attribute), "utf8"),
      json: `${JSON.stringify(attribute)}:${attributeValue(attribute, raw)}`,
    }))
    .sort((left, right) => Buffer.compare(left.key, right.key))
    .map(({ json }) => json)
    .join(",");
}

/**
 * The write capability's JSON, or undefined when there is none, since the Rust
 * issuer omits an empty `commands`.
 */
function commands(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value)) refuse("commands must be a list");
  if (value.length === 0) return undefined;
  if (value.length > MAX_COMMAND_CAPABILITIES)
    refuse(
      `a grant carries at most ${String(MAX_COMMAND_CAPABILITIES)} command capabilities`,
    );
  const named = new Set<string>();
  const entries = value.map((capability: unknown) => {
    if (typeof capability !== "object" || capability === null)
      refuse("a command capability must be an object");
    const fields = Object.keys(capability);
    if (
      fields.length !== 2 ||
      !fields.includes("collection") ||
      !fields.includes("operations")
    )
      refuse(
        `a command capability has exactly a collection and its operations, not ${fields.join(", ")}`,
      );
    const { collection, operations } = capability as Record<string, unknown>;
    if (
      typeof collection !== "string" ||
      collection.length > MAX_COLLECTION_BYTES ||
      !ASCII_GRAPHIC.test(collection)
    )
      refuse(
        `a collection is 1 to ${String(MAX_COLLECTION_BYTES)} printable ASCII characters`,
      );
    if (named.has(collection))
      refuse(`collection ${collection} is named twice`);
    named.add(collection);
    if (!Array.isArray(operations) || operations.length === 0)
      refuse(
        `collection ${collection} names no operation; an empty set is refused, never read as every operation`,
      );
    const seen = new Set<unknown>();
    for (const operation of operations) {
      if (!(OPERATIONS as readonly unknown[]).includes(operation))
        refuse(
          `collection ${collection} names ${JSON.stringify(operation)}; the operations are insert, update and delete`,
        );
      if (seen.has(operation))
        refuse(`collection ${collection} repeats ${String(operation)}`);
      seen.add(operation);
    }
    return `{"collection":${JSON.stringify(collection)},"operations":${JSON.stringify(operations)}}`;
  });
  return `[${entries.join(",")}]`;
}

/**
 * The signed payload, refused if Calque would refuse it.
 *
 * The verifier parses any JSON object with these fields, in any order and with
 * any whitespace, because the signature covers whatever bytes were sent. These
 * are the bytes the Rust issuer writes, so the frozen token vectors can check
 * this package: fields in the order below, no whitespace, attributes sorted by
 * their UTF-8 bytes, operations in the order given, and `commands` omitted
 * when empty.
 */
export function serializeClaims(claims: DatasetGrantClaims): Buffer {
  for (const field of Object.keys(claims))
    if (!CLAIMS.has(field))
      refuse(
        `${field} is not a grant claim; Calque refuses a payload with an unknown field`,
      );
  const issued = unsigned("issuedAtUnixSeconds", claims.issuedAtUnixSeconds);
  const expires = unsigned("expiresAtUnixSeconds", claims.expiresAtUnixSeconds);
  if (expires <= issued)
    refuse("expiresAtUnixSeconds must be later than issuedAtUnixSeconds");
  if (expires - issued > MAX_GRANT_LIFETIME_SECONDS)
    refuse(
      `the validity window is ${String(expires - issued)} s; Calque refuses one longer than ${String(MAX_GRANT_LIFETIME_SECONDS)} s`,
    );
  const fields = [
    `"version":${String(GRANT_VERSION)}`,
    `"keyId":${JSON.stringify(keyId(claims.keyId))}`,
    `"audience":${JSON.stringify(audience(claims.audience))}`,
    `"subject":${JSON.stringify(name("subject", claims.subject))}`,
    `"attributes":{${attributes(claims.attributes)}}`,
    `"authorizationGeneration":${String(unsigned("authorizationGeneration", claims.authorizationGeneration))}`,
    `"issuedAtUnixSeconds":${String(issued)}`,
    `"expiresAtUnixSeconds":${String(expires)}`,
  ];
  const capabilities = commands(claims.commands);
  if (capabilities !== undefined) fields.push(`"commands":${capabilities}`);
  const payload = Buffer.from(`{${fields.join(",")}}`, "utf8");
  // Within this bound a token is at most 5,549 bytes, inside the verifier's
  // 8 KiB token bound, so that one needs no check of its own.
  if (payload.length > MAX_PAYLOAD_BYTES)
    refuse(
      `the payload is ${String(payload.length)} bytes; Calque refuses one over ${String(MAX_PAYLOAD_BYTES)}`,
    );
  return payload;
}

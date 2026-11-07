import { fromBinary, toBinary } from "@bufbuild/protobuf";

import type { PrimaryKey } from "./gen/change_stream_pb.js";
import { PrimaryKeySchema } from "./gen/change_stream_pb.js";
import { equalBytes } from "./compare.js";
import { ProtocolValidationError } from "./validation.js";

export const PRIMARY_KEY_CODEC_VERSION = 1;
export const PRIMARY_KEY_MAX_BYTES = 8 * 1024;
const PRIMARY_KEY_MAX_PARTS = 32;

export function encodePrimaryKey(key: PrimaryKey): Uint8Array<ArrayBuffer> {
  validatePrimaryKey(key, "primaryKey");
  const encoded = toBinary(PrimaryKeySchema, key, {
    writeUnknownFields: false,
  });
  validateEncodedSize(encoded, "primaryKey");
  return encoded;
}

export function decodePrimaryKey(
  encoded: Uint8Array,
  path = "primaryKey",
): PrimaryKey {
  validateEncodedSize(encoded, path);
  let key: PrimaryKey;
  try {
    key = fromBinary(PrimaryKeySchema, encoded, {
      readUnknownFields: false,
    });
  } catch {
    throw new ProtocolValidationError(path, "protobuf is malformed");
  }
  validatePrimaryKey(key, path);
  const canonical = toBinary(PrimaryKeySchema, key, {
    writeUnknownFields: false,
  });
  if (!equalBytes(canonical, encoded)) {
    throw new ProtocolValidationError(
      path,
      "contains unknown fields or is not canonical protobuf",
    );
  }
  return key;
}

function validateEncodedSize(encoded: Uint8Array, path: string): void {
  if (encoded.byteLength === 0) {
    throw new ProtocolValidationError(path, "must not be empty");
  }
  if (encoded.byteLength > PRIMARY_KEY_MAX_BYTES) {
    throw new ProtocolValidationError(
      path,
      `has ${encoded.byteLength} bytes, exceeding limit ${PRIMARY_KEY_MAX_BYTES}`,
    );
  }
}

function validatePrimaryKey(key: PrimaryKey, path: string): void {
  if (key.codecVersion !== PRIMARY_KEY_CODEC_VERSION) {
    throw new ProtocolValidationError(
      `${path}.codecVersion`,
      `unsupported version ${key.codecVersion}`,
    );
  }
  if (key.parts.length === 0) {
    throw new ProtocolValidationError(`${path}.parts`, "must not be empty");
  }
  if (key.parts.length > PRIMARY_KEY_MAX_PARTS) {
    throw new ProtocolValidationError(
      `${path}.parts`,
      `has ${key.parts.length} parts, exceeding limit ${PRIMARY_KEY_MAX_PARTS}`,
    );
  }

  const columnIds = new Set<number>();
  for (const [index, part] of key.parts.entries()) {
    const partPath = `${path}.parts[${index}]`;
    if (part.columnId === 0) {
      throw new ProtocolValidationError(
        `${partPath}.columnId`,
        "must not be zero",
      );
    }
    if (columnIds.has(part.columnId)) {
      throw new ProtocolValidationError(
        `${partPath}.columnId`,
        "must be unique",
      );
    }
    columnIds.add(part.columnId);
    if (part.value === undefined) {
      throw new ProtocolValidationError(`${partPath}.value`, "is required");
    }
    const kind = part.value.kind;
    if (kind.case === undefined) {
      throw new ProtocolValidationError(
        `${partPath}.value.kind`,
        "is required",
      );
    }
    switch (kind.case) {
      case "int64Value":
      case "boolValue":
      case "blobValue":
        break;
      case "textValue":
        // UTF-8 encoding would replace a lone surrogate with U+FFFD, so two
        // distinct JavaScript strings could name one key.
        if (!kind.value.isWellFormed()) {
          throw new ProtocolValidationError(
            `${partPath}.value.kind.textValue`,
            "must be well-formed Unicode",
          );
        }
        break;
      case "uuid16":
        if (kind.value.byteLength !== 16) {
          throw new ProtocolValidationError(
            `${partPath}.value.kind.uuid16`,
            "must contain exactly 16 bytes",
          );
        }
        break;
      case "nullValue":
        throw new ProtocolValidationError(partPath, "must not be null");
      default:
        throw new ProtocolValidationError(
          partPath,
          `${kind.case} is not a v1 key type`,
        );
    }
  }
}

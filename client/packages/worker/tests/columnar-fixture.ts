/**
 * Mints the direct-columnar fixture artifact for the browser suite.
 *
 * Core's artifact fixture needs Node for its digests, so a browser test
 * cannot encode an artifact itself. It asks for one through this Vitest
 * browser command, which runs on the Node side of the browser session; the
 * bytes cross back as base64 because a command's result is serialized.
 */
import type {
  DirectColumnarArtifactIdentity,
  DirectColumnarSchema,
  TrustedArtifactChunks,
} from "@calque/core";
import {
  FIXTURE_ARTIFACT_IDENTITY,
  WORKSPACE_CONTRACT,
  artifactChunks,
  encodeWorkspace,
} from "../../core/src/artifact.fixture.js";
import type { BrowserCommand } from "vitest/node";

declare module "vitest/browser" {
  interface BrowserCommands {
    encodeColumnarFixture: (
      cursor: string,
      chunkBytes?: number,
    ) => Promise<ColumnarFixture>;
  }
}

export interface FixtureUser {
  readonly id: string;
  readonly display_name: string;
  readonly email: string;
  readonly active: boolean;
}

export interface ColumnarFixture {
  /** The artifact bytes, base64. */
  readonly bytes: string;
  readonly byteLength: number;
  readonly digest: `sha256:${string}`;
  readonly chunks: TrustedArtifactChunks;
  readonly identity: DirectColumnarArtifactIdentity;
  readonly schema: DirectColumnarSchema;
  /** The `users` rows the artifact holds; every other table is empty. */
  readonly users: readonly FixtureUser[];
}

const USERS: readonly FixtureUser[] = Array.from(
  { length: 4 },
  (_unused, index) => ({
    id: `user-${String(index)}`,
    display_name: `User ${String(index)}`,
    email: `user${String(index)}@example.test`,
    active: index % 2 === 0,
  }),
);

/**
 * The fixture workspace at `cursor`, with `USERS` and nothing else, its
 * digests taken per chunk of `chunkBytes` (the plan's 4 MiB unless a test
 * needs the artifact in several chunks).
 */
export const encodeColumnarFixture: BrowserCommand<
  [cursor: string, chunkBytes?: number],
  ColumnarFixture
> = (_context, cursor, chunkBytes) => {
  const tables = new Map<string, Record<string, unknown>[]>(
    WORKSPACE_CONTRACT.tables.map((table) => [table.name, []]),
  );
  tables.set(
    "users",
    USERS.map((user) => ({ ...user })),
  );
  const encoded = encodeWorkspace(tables, {
    identity: { ...FIXTURE_ARTIFACT_IDENTITY, cursor },
  });
  return {
    bytes: Buffer.from(encoded.bytes).toString("base64"),
    byteLength: encoded.byteLength,
    digest: encoded.digest,
    chunks: artifactChunks(encoded.bytes, chunkBytes),
    identity: encoded.identity,
    schema: WORKSPACE_CONTRACT,
    users: USERS,
  };
};

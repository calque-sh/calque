import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  nativeServerSettings,
  parsePostgresMajor,
  SERVER_FLAGS,
} from "./bench-postgres.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

test("both servers take docker-compose.yml's server flags", () => {
  const compose = readFileSync(join(root, "docker-compose.yml"), "utf8");
  const command = compose.slice(
    compose.indexOf("    command:"),
    compose.indexOf("    healthcheck:"),
  );
  const flags = [...command.matchAll(/- "([a-z_]+)=([^"]+)"/gu)].map(
    ([, name, value]) => [name, value],
  );
  assert.deepEqual(flags, SERVER_FLAGS);
});

test("the native cluster's settings are the container's flags, in UTC, on 127.0.0.1 alone", () => {
  const settings = Object.fromEntries(
    nativeServerSettings(55_521)
      .split("\n")
      .filter((line) => line !== "" && !line.startsWith("#"))
      .map((line) => {
        const match = /^([a-z_]+) = (.*)$/u.exec(line);
        assert.ok(match, line);
        return [match[1], match[2]];
      }),
  );
  assert.deepEqual(settings, {
    wal_level: "'logical'",
    max_replication_slots: "'10'",
    max_wal_senders: "'10'",
    max_slot_wal_keep_size: "'10GB'",
    timezone: "'UTC'",
    log_timezone: "'UTC'",
    listen_addresses: "'127.0.0.1'",
    port: "55521",
    unix_socket_directories: "''",
  });
});

test("postgres --version names its major", () => {
  assert.equal(parsePostgresMajor("postgres (PostgreSQL) 18.6\n"), 18);
  assert.equal(parsePostgresMajor("postgres (PostgreSQL) 18beta1"), 18);
  assert.equal(parsePostgresMajor("postgres (PostgreSQL) 17.2"), 17);
  assert.equal(
    parsePostgresMajor("postgres (PostgreSQL) 18.0 (Debian 18.0-1)"),
    18,
  );
  assert.equal(parsePostgresMajor("psql (PostgreSQL) 18.6"), null);
  assert.equal(parsePostgresMajor(""), null);
});

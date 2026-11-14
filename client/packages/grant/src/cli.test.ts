import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { run } from "./cli.js";
import { loadSigningKey, verificationKeyEnvironment } from "./key.js";
import { mintGrant } from "./mint.js";
import {
  claimsOf,
  KEY_FILE,
  KEY_ID,
  text,
  vector,
  vectorPath,
} from "./test-vectors.js";

function calqueGrant(args: readonly string[], now = 1_800_000_000) {
  let stdout = "";
  let stderr = "";
  const code = run(args, {
    stdout: { write: (chunk: string) => (stdout += chunk) },
    stderr: { write: (chunk: string) => (stderr += chunk) },
    nowUnixSeconds: () => now,
  });
  return { code, stdout, stderr };
}

let directory: string;
beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), "calque-grant-"));
});
afterEach(() => {
  rmSync(directory, { recursive: true, force: true });
});

const MINT = [
  "mint",
  "--key-file",
  vectorPath(KEY_FILE),
  "--key-id",
  KEY_ID,
  "--audience",
  "calque.vectors.test",
  "--subject",
  "member-1",
  "--generation",
  "9",
  "--lifetime-seconds",
  "300",
  "--attribute",
  "organization_id=uuid:0a1b2c3d4e5f6071829304a5b6c7d8e9",
  "--command",
  "projects=update",
  "--command",
  "tasks=insert+update+delete",
];

describe("calque-grant keygen", () => {
  it("writes an owner-only key, prints the verification-key value, and never overwrites", () => {
    const path = join(directory, "nested", "grant-key.pem");
    const created = calqueGrant([
      "keygen",
      "--key-file",
      path,
      "--key-id",
      "app-1",
    ]);
    expect(created.code).toBe(0);
    expect(created.stderr).toBe("");
    expect(statSync(path).mode & 0o777).toBe(0o600);
    const key = loadSigningKey(readFileSync(path));
    expect(created.stdout).toBe(
      `${verificationKeyEnvironment("app-1", key)}\n`,
    );
    expect(JSON.parse(created.stdout)).toEqual({
      keyId: "app-1",
      publicKey: expect.stringMatching(/^[A-Za-z0-9_-]{43}$/u) as string,
    });

    const before = readFileSync(path, "utf8");
    const again = calqueGrant([
      "keygen",
      "--key-file",
      path,
      "--key-id",
      "app-1",
    ]);
    expect(again.code).toBe(1);
    expect(again.stdout).toBe("");
    expect(again.stderr).toMatch(/exists, and keygen never overwrites a key/u);
    expect(readFileSync(path, "utf8")).toBe(before);
  });

  it("writes nothing for a key id the verifier would refuse", () => {
    const path = join(directory, "grant-key.pem");
    const refused = calqueGrant([
      "keygen",
      "--key-file",
      path,
      "--key-id",
      "a.b",
    ]);
    expect(refused.code).toBe(1);
    expect(refused.stderr).toMatch(/keyId must be 1 to 128/u);
    expect(existsSync(path)).toBe(false);
  });

  it("is a usage error without both flags, or with a flag only mint takes", () => {
    for (const args of [
      ["keygen"],
      ["keygen", "--key-file", join(directory, "k")],
      ["keygen", "--key-id", "x"],
      [
        "keygen",
        "--key-file",
        join(directory, "k"),
        "--key-id",
        "x",
        "--subject",
        "s",
      ],
    ]) {
      const result = calqueGrant(args);
      expect(result.code, args.join(" ")).toBe(2);
      expect(result.stderr).toMatch(/^calque-grant: /u);
      expect(result.stderr).toMatch(/usage: calque-grant keygen/u);
    }
    expect(existsSync(join(directory, "k"))).toBe(false);
  });
});

describe("calque-grant mint", () => {
  it("prints the document calque-dev-grant prints, with the frozen token", () => {
    const result = calqueGrant(MINT);
    expect(result.code, result.stderr).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual({
      authorizationGeneration: 9,
      commands: ["projects:update", "tasks:insert+update+delete"],
      expiresAtUnixSeconds: 1_800_000_300,
      issuedAtUnixSeconds: 1_800_000_000,
      subject: "member-1",
      token: text("command-capability.token"),
      verificationKeyEnvironment: JSON.parse(
        text("test-only-verification-key.json"),
      ) as unknown,
    });
    expect(Object.keys(JSON.parse(result.stdout) as object)).toEqual([
      "authorizationGeneration",
      "commands",
      "expiresAtUnixSeconds",
      "issuedAtUnixSeconds",
      "subject",
      "token",
      "verificationKeyEnvironment",
    ]);
  });

  it("mints a read-only grant without --command, and takes --flag=value", () => {
    const readOnly = claimsOf(vector("read-only.payload.json"));
    const result = calqueGrant(
      [
        "mint",
        `--key-file=${vectorPath(KEY_FILE)}`,
        `--key-id=${readOnly.keyId}`,
        `--audience=${readOnly.audience}`,
        `--subject=${readOnly.subject}`,
        `--generation=${String(readOnly.authorizationGeneration)}`,
        `--lifetime-seconds=${String(readOnly.expiresAtUnixSeconds - readOnly.issuedAtUnixSeconds)}`,
        ...Object.entries(readOnly.attributes).flatMap(([name, value]) => [
          "--attribute",
          `${name}=uuid:${(value as { uuid: string }).uuid}`,
        ]),
      ],
      readOnly.issuedAtUnixSeconds,
    );
    expect(result.code, result.stderr).toBe(0);
    const printed = JSON.parse(result.stdout) as {
      token: string;
      commands: string[];
    };
    expect(printed.token).toBe(text("read-only.token"));
    expect(printed.commands).toEqual([]);
  });

  it("signs with a key keygen wrote, in PEM", () => {
    const path = join(directory, "grant-key.pem");
    calqueGrant(["keygen", "--key-file", path, "--key-id", "app-1"]);
    const args = MINT.map((argument) =>
      argument === vectorPath(KEY_FILE) ? path : argument,
    ).map((argument) => (argument === KEY_ID ? "app-1" : argument));
    const result = calqueGrant(args);
    expect(result.code, result.stderr).toBe(0);
    const key = loadSigningKey(readFileSync(path));
    const { token } = JSON.parse(result.stdout) as { token: string };
    expect(token).toBe(
      mintGrant(
        {
          ...claimsOf(vector("command-capability.payload.json")),
          keyId: "app-1",
        },
        key,
      ),
    );
  });

  it("refuses what the verifier would, exiting 1 and printing nothing", () => {
    const tooLong = MINT.map((argument) =>
      argument === "300" ? "901" : argument,
    );
    const result = calqueGrant(tooLong);
    expect(result.code).toBe(1);
    expect(result.stdout).toBe("");
    expect(result.stderr).toMatch(
      /^calque-grant: the validity window is 901 s/u,
    );
  });

  it("reports an unreadable key file and a key of the wrong kind, exiting 1", () => {
    const missing = calqueGrant(
      MINT.map((a) =>
        a === vectorPath(KEY_FILE) ? join(directory, "absent") : a,
      ),
    );
    expect(missing.code).toBe(1);
    expect(missing.stderr).toMatch(/ENOENT/u);

    const notAKey = join(directory, "not-a-key");
    writeFileSync(notAKey, "hello");
    const wrong = calqueGrant(
      MINT.map((a) => (a === vectorPath(KEY_FILE) ? notAKey : a)),
    );
    expect(wrong.code).toBe(1);
    expect(wrong.stderr).toMatch(/not a PKCS#8 Ed25519 private key/u);
  });

  it("is a usage error for a missing, repeated, malformed or unknown argument", () => {
    const without = (flag: string) => {
      const at = MINT.indexOf(flag);
      return [...MINT.slice(0, at), ...MINT.slice(at + 2)];
    };
    const cases: readonly (readonly [string[], RegExp])[] = [
      [without("--key-file"), /--key-file is required/u],
      [without("--key-id"), /--key-id is required/u],
      [without("--audience"), /--audience is required/u],
      [without("--subject"), /--subject is required/u],
      [without("--generation"), /--generation is required/u],
      [without("--lifetime-seconds"), /--lifetime-seconds is required/u],
      [[...MINT, "--subject", "again"], /--subject was given twice/u],
      [[...MINT, "--unknown", "x"], /unknown option/iu],
      [[...MINT, "--generation", "-1"], /ambiguous/u],
      [
        MINT.map((a) => (a === "9" ? "nine" : a)),
        /--generation must be a whole number/u,
      ],
      [
        [...MINT, "--attribute", "organization_id=uuid:0b"],
        /--attribute organization_id was given twice/u,
      ],
      [[...MINT, "--attribute", "x"], /not <name>=<type>:<value>/u],
      [[...MINT, "--attribute", "x=guid:1"], /unknown type guid/u],
      [[...MINT, "--attribute", "x=int4:one"], /is not an int4/u],
      [[...MINT, "--attribute", "x=bool:yes"], /is not a bool/u],
      [[...MINT, "--command", "tasks"], /not <collection>=<operation>/u],
      [["mint", "--help-me"], /unknown option/iu],
      [["frobnicate"], /unknown command frobnicate/u],
    ];
    for (const [args, reason] of cases) {
      const result = calqueGrant(args);
      expect(result.code, args.join(" ")).toBe(2);
      expect(result.stdout).toBe("");
      expect(result.stderr, args.join(" ")).toMatch(reason);
    }
  });

  it("treats a wildcard or unknown operation as a refusal, not a grant", () => {
    for (const operations of ["*", "truncate", "update+update", ""]) {
      const result = calqueGrant([...MINT, "--command", `notes=${operations}`]);
      expect(result.code, operations).toBe(1);
      expect(result.stdout).toBe("");
    }
  });
});

describe("calque-grant help", () => {
  it("prints the usage on stdout for --help and on stderr, status 2, for nothing", () => {
    for (const args of [["--help"], ["mint", "--help"], ["keygen", "-h"]]) {
      const result = calqueGrant(args);
      expect(result.code).toBe(0);
      expect(result.stdout).toMatch(/usage: calque-grant keygen/u);
      expect(result.stdout).toMatch(/--lifetime-seconds is at most 900/u);
    }
    const none = calqueGrant([]);
    expect(none.code).toBe(2);
    expect(none.stderr).toMatch(/usage: calque-grant keygen/u);
  });
});

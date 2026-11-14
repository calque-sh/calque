import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { parseArgs } from "node:util";

import {
  type GrantAttributeValue,
  type GrantCommandCapability,
  type GrantCommandOperation,
} from "./claims.js";
import {
  generateSigningKey,
  loadSigningKey,
  verificationKeyEnvironment,
} from "./key.js";
import { mintGrant } from "./mint.js";

interface Output {
  write(text: string): unknown;
}

/** What `run` needs from the process, so a test can stand in for it. */
export interface Environment {
  readonly stdout: Output;
  readonly stderr: Output;
  readonly nowUnixSeconds: () => number;
}

const USAGE = `usage: calque-grant keygen --key-file <path> --key-id <id>
       calque-grant mint --key-file <path> --key-id <id> --audience <audience>
                         --subject <subject> --generation <n> --lifetime-seconds <n>
                         [--attribute <name>=<type>:<value>]...
                         [--command <collection>=<operation>[+<operation>]]...
`;

const HELP = `${USAGE}
keygen  Creates an Ed25519 signing key at --key-file, as PKCS#8 PEM readable
        by owner only, and prints the {"keyId":...,"publicKey":...} value that
        \`calque serve --grant-verification-key-env\` reads. It never overwrites
        a file.

mint    Signs one dataset grant with the key at --key-file and prints it as one
        JSON document, with when it expires and the verification-key value.
        --lifetime-seconds is at most 900. An attribute type is uuid (32 hex
        digits), bytes (hex), int8, int4, int2 or bool. A command names a
        collection and insert, update or delete; without one the grant is
        read-only.

      --key-file <path>          the signing key, PEM or DER, PKCS#8
      --key-id <id>              the id the grant and the verification key carry
      --audience <audience>      the audience \`calque serve\` was started with
      --subject <subject>        who the grant is issued to
      --generation <n>           the authorization generation it is valid for
      --lifetime-seconds <n>     how long it is valid
      --attribute <n>=<t>:<v>    repeat for more than one
      --command <c>=<op>[+<op>]  repeat per collection
  -h, --help                     print this text

Exit status: 0 on success, 1 when a grant is refused or a file cannot be read
or written, 2 on a usage error.
`;

class UsageError extends Error {}

const WHOLE_NUMBER = /^[0-9]+$/u;
const SIGNED_NUMBER = /^[+-]?[0-9]+$/u;

/** `<name>=<type>:<value>`, in the vocabulary `calque-dev-grant` reads. */
function parseAttribute(argument: string): [string, GrantAttributeValue] {
  const equals = argument.indexOf("=");
  const colon = argument.indexOf(":", equals + 1);
  if (equals === -1 || colon === -1)
    throw new UsageError(
      `--attribute ${argument} is not <name>=<type>:<value>`,
    );
  const name = argument.slice(0, equals);
  const type = argument.slice(equals + 1, colon);
  const value = argument.slice(colon + 1);
  switch (type) {
    case "uuid":
      return [name, { uuid: value }];
    case "bytes":
      return [name, { bytes: value }];
    case "int8":
      return [name, { int8: value }];
    case "int4":
    case "int2":
      if (!SIGNED_NUMBER.test(value))
        throw new UsageError(`--attribute ${name} is not an ${type}`);
      return [
        name,
        type === "int4" ? { int4: Number(value) } : { int2: Number(value) },
      ];
    case "bool":
      if (value !== "true" && value !== "false")
        throw new UsageError(`--attribute ${name} is not a bool`);
      return [name, { bool: value === "true" }];
    default:
      throw new UsageError(`--attribute ${name} has unknown type ${type}`);
  }
}

/** `<collection>=<operation>[+<operation>…]`; there is no wildcard. */
function parseCommand(argument: string): GrantCommandCapability {
  const equals = argument.indexOf("=");
  if (equals === -1)
    throw new UsageError(
      `--command ${argument} is not <collection>=<operation>[+…]`,
    );
  return {
    collection: argument.slice(0, equals),
    // `mintGrant` refuses an operation outside the three.
    operations: argument
      .slice(equals + 1)
      .split("+") as GrantCommandOperation[],
  };
}

const OPTIONS = {
  "key-file": { type: "string", multiple: true },
  "key-id": { type: "string", multiple: true },
  audience: { type: "string", multiple: true },
  subject: { type: "string", multiple: true },
  generation: { type: "string", multiple: true },
  "lifetime-seconds": { type: "string", multiple: true },
  attribute: { type: "string", multiple: true },
  command: { type: "string", multiple: true },
  help: { type: "boolean", short: "h" },
} as const;

type Flags = ReturnType<typeof parseFlags>;

function parseFlags(argv: readonly string[]) {
  try {
    return parseArgs({
      args: [...argv],
      options: OPTIONS,
      strict: true,
      allowPositionals: false,
    }).values;
  } catch (error) {
    throw new UsageError(
      error instanceof Error ? error.message : String(error),
    );
  }
}

function single(
  flags: Flags,
  name: Exclude<keyof Flags, "help" | "attribute" | "command">,
): string {
  const given = flags[name];
  if (given === undefined) throw new UsageError(`--${name} is required`);
  if (given.length > 1) throw new UsageError(`--${name} was given twice`);
  const [value] = given;
  if (value === undefined) throw new UsageError(`--${name} is required`);
  return value;
}

function wholeNumber(flags: Flags, name: "generation" | "lifetime-seconds") {
  const value = single(flags, name);
  if (!WHOLE_NUMBER.test(value))
    throw new UsageError(`--${name} must be a whole number`);
  return Number(value);
}

function rejectOtherFlags(flags: Flags, allowed: readonly string[]): void {
  for (const name of Object.keys(flags))
    if (name !== "help" && !allowed.includes(name))
      throw new UsageError(`unknown argument --${name}`);
}

function keygen(flags: Flags, io: Environment): void {
  rejectOtherFlags(flags, ["key-file", "key-id"]);
  const path = single(flags, "key-file");
  const id = single(flags, "key-id");
  const { privateKey, pem } = generateSigningKey();
  // Checked before anything is written, so a bad id leaves no key behind.
  const environment = verificationKeyEnvironment(id, privateKey);
  mkdirSync(dirname(path), { recursive: true });
  try {
    writeFileSync(path, pem, { flag: "wx", mode: 0o600 });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST")
      throw new Error(
        `${path} exists, and keygen never overwrites a key: a server configured with the old one would refuse every grant signed with a new one`,
      );
    throw error;
  }
  io.stdout.write(`${environment}\n`);
}

const MINT_FLAGS = [
  "key-file",
  "key-id",
  "audience",
  "subject",
  "generation",
  "lifetime-seconds",
  "attribute",
  "command",
];

function mint(flags: Flags, io: Environment): void {
  rejectOtherFlags(flags, MINT_FLAGS);
  const attributes: Record<string, GrantAttributeValue> = {};
  for (const argument of flags.attribute ?? []) {
    const [name, attribute] = parseAttribute(argument);
    if (Object.hasOwn(attributes, name))
      throw new UsageError(`--attribute ${name} was given twice`);
    attributes[name] = attribute;
  }
  const commands = (flags.command ?? []).map(parseCommand);
  const keyId = single(flags, "key-id");
  const subject = single(flags, "subject");
  const generation = wholeNumber(flags, "generation");
  const lifetime = wholeNumber(flags, "lifetime-seconds");
  const audience = single(flags, "audience");
  const keyFile = single(flags, "key-file");

  const key = loadSigningKey(readFileSync(keyFile));
  const issued = io.nowUnixSeconds();
  const token = mintGrant(
    {
      keyId,
      audience,
      subject,
      attributes,
      authorizationGeneration: generation,
      issuedAtUnixSeconds: issued,
      expiresAtUnixSeconds: issued + lifetime,
      commands,
    },
    key,
  );
  // The keys in the order `calque-dev-grant` prints them.
  io.stdout.write(
    `${JSON.stringify(
      {
        authorizationGeneration: generation,
        commands: commands.map(
          ({ collection, operations }) =>
            `${collection}:${operations.join("+")}`,
        ),
        expiresAtUnixSeconds: issued + lifetime,
        issuedAtUnixSeconds: issued,
        subject,
        token,
        verificationKeyEnvironment: JSON.parse(
          verificationKeyEnvironment(keyId, key),
        ) as unknown,
      },
      null,
      2,
    )}\n`,
  );
}

/** Runs `calque-grant` over `argv` and returns its exit status. */
export function run(argv: readonly string[], io: Environment): number {
  try {
    const [command, ...rest] = argv;
    if (command === undefined || command === "-h" || command === "--help") {
      (command === undefined ? io.stderr : io.stdout).write(
        command === undefined ? USAGE : HELP,
      );
      return command === undefined ? 2 : 0;
    }
    if (command !== "keygen" && command !== "mint")
      throw new UsageError(`unknown command ${command}`);
    const flags = parseFlags(rest);
    if (flags.help === true) {
      io.stdout.write(HELP);
      return 0;
    }
    (command === "keygen" ? keygen : mint)(flags, io);
    return 0;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (error instanceof UsageError) {
      io.stderr.write(`calque-grant: ${message}\n${USAGE}`);
      return 2;
    }
    io.stderr.write(`calque-grant: ${message}\n`);
    return 1;
  }
}

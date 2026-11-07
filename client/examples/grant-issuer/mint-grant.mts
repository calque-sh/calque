#!/usr/bin/env node
/**
 * `calque-dev-grant` in Node: mints one dataset grant and prints it beside the
 * verification key `calque serve` needs to accept it.
 *
 * ```sh
 * node client/examples/grant-issuer/mint-grant.mts --key-file grant-key.pk8 \
 *   --key-id quickstart --audience quickstart.example --generation 1 \
 *   --subject me --lifetime-seconds 900 --command tasks=update
 * ```
 *
 * It takes the flags `calque-dev-grant` takes and prints the same JSON
 * document, so either can stand in for the other. It reads the key file and
 * never creates one: a backend's signing key comes from its own secret store,
 * and on a development machine `calque-dev-grant` creates `grant-key.pk8` on first use.
 */
import { readFileSync, realpathSync } from "node:fs";
import process from "node:process";
import { fileURLToPath } from "node:url";

import {
  loadSigningKey,
  mintGrant,
  verificationKeyEnvironment,
  type GrantAttributeValue,
  type GrantCommandCapability,
  type GrantCommandOperation,
} from "./dataset-grant.mts";

const REQUIRED = [
  "--key-file",
  "--key-id",
  "--audience",
  "--subject",
  "--generation",
  "--lifetime-seconds",
] as const;

const WHOLE_NUMBER = /^[0-9]+$/;
const SIGNED_NUMBER = /^[+-]?[0-9]+$/;

/** `<name>=<type>:<value>`, in the vocabulary `calque-dev-grant` reads. */
function parseAttribute(argument: string): [string, GrantAttributeValue] {
  const equals = argument.indexOf("=");
  const colon = argument.indexOf(":", equals + 1);
  if (equals === -1 || colon === -1)
    throw new Error(`--attribute ${argument} is not <name>=<type>:<value>`);
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
        throw new Error(`--attribute ${name} is not an ${type}`);
      return [
        name,
        type === "int4" ? { int4: Number(value) } : { int2: Number(value) },
      ];
    case "bool":
      if (value !== "true" && value !== "false")
        throw new Error(`--attribute ${name} is not a bool`);
      return [name, { bool: value === "true" }];
    default:
      throw new Error(`--attribute ${name} has unknown type ${type}`);
  }
}

/** `<collection>=<operation>[+<operation>…]`; there is no wildcard. */
function parseCommand(argument: string): GrantCommandCapability {
  const equals = argument.indexOf("=");
  if (equals === -1)
    throw new Error(
      `--command ${argument} is not <collection>=<operation>[+…]`,
    );
  return {
    collection: argument.slice(0, equals),
    // Checked by `mintGrant`, which refuses an operation outside the three.
    operations: argument
      .slice(equals + 1)
      .split("+") as GrantCommandOperation[],
  };
}

/**
 * Everything the command does except read the process arguments and the
 * clock: the JSON document for `argv`, issued at `nowUnixSeconds`.
 */
export function mintGrantCommand(
  argv: readonly string[],
  nowUnixSeconds: number,
): string {
  const single = new Map<string, string>();
  const attributes: Record<string, GrantAttributeValue> = {};
  const commands: GrantCommandCapability[] = [];
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index] ?? "";
    const value = argv[index + 1];
    if (value === undefined) throw new Error(`${flag} needs a value`);
    if (flag === "--attribute") {
      const [name, attribute] = parseAttribute(value);
      if (Object.hasOwn(attributes, name))
        throw new Error(`--attribute ${name} was given twice`);
      attributes[name] = attribute;
    } else if (flag === "--command") {
      commands.push(parseCommand(value));
    } else if ((REQUIRED as readonly string[]).includes(flag)) {
      single.set(flag, value);
    } else {
      throw new Error(`unknown argument ${flag}`);
    }
  }
  const required = (flag: (typeof REQUIRED)[number]): string => {
    const value = single.get(flag);
    if (value === undefined) throw new Error(`${flag} is required`);
    return value;
  };
  const wholeNumber = (flag: (typeof REQUIRED)[number]): number => {
    const value = required(flag);
    if (!WHOLE_NUMBER.test(value))
      throw new Error(`${flag} must be a whole number`);
    return Number(value);
  };

  const keyId = required("--key-id");
  const subject = required("--subject");
  const generation = wholeNumber("--generation");
  const lifetime = wholeNumber("--lifetime-seconds");
  const key = loadSigningKey(readFileSync(required("--key-file")));
  const token = mintGrant(
    {
      keyId,
      audience: required("--audience"),
      subject,
      attributes,
      authorizationGeneration: generation,
      issuedAtUnixSeconds: nowUnixSeconds,
      expiresAtUnixSeconds: nowUnixSeconds + lifetime,
      commands,
    },
    key,
  );
  // The keys in the order `calque-dev-grant` prints them.
  return JSON.stringify(
    {
      authorizationGeneration: generation,
      commands: commands.map(
        ({ collection, operations }) => `${collection}:${operations.join("+")}`,
      ),
      expiresAtUnixSeconds: nowUnixSeconds + lifetime,
      issuedAtUnixSeconds: nowUnixSeconds,
      subject,
      token,
      verificationKeyEnvironment: JSON.parse(
        verificationKeyEnvironment(keyId, key),
      ) as unknown,
    },
    null,
    2,
  );
}

const invoked = process.argv[1];
if (
  invoked !== undefined &&
  realpathSync(invoked) === fileURLToPath(import.meta.url)
) {
  try {
    process.stdout.write(
      `${mintGrantCommand(process.argv.slice(2), Math.floor(Date.now() / 1000))}\n`,
    );
  } catch (error) {
    process.stderr.write(
      `mint-grant: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  }
}

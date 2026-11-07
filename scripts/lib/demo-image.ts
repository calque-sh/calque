import { readFileSync } from "node:fs";
import { join } from "node:path";

/** Where the release workflow publishes the `calque` image. */
export const DEMO_IMAGE_REPOSITORY = "ghcr.io/calque-sh/calque";

/**
 * Layered over `docker-compose.yml` when `calque` runs from its image: it
 * publishes the server's port on the PostgreSQL container, whose network
 * namespace the `calque` container joins.
 */
export const DEMO_IMAGE_COMPOSE_FILE = "docker-compose.demo-image.yml";

/** Where the state directory is mounted in the container. */
export const CONTAINER_STATE_DIRECTORY = "/state";

/**
 * The release version the npm packages carry, which tags the image. The
 * launcher runs the image of the version it was checked out at, so a flag it
 * passes is one that image has.
 */
export function releaseVersion(root: string): string {
  const manifest = JSON.parse(
    readFileSync(join(root, "client/packages/client/package.json"), "utf8"),
  ) as { readonly version?: unknown };
  if (
    typeof manifest.version !== "string" ||
    !/^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$/u.test(manifest.version)
  )
    throw new Error("@calque/client's package.json carries no release version");
  return manifest.version;
}

/**
 * The image `pnpm demo` runs: `CALQUE_DEMO_IMAGE` when set, otherwise the
 * published image at this checkout's release version.
 */
export function demoImageReference(
  version: string,
  environment: NodeJS.ProcessEnv,
): string {
  const configured = environment.CALQUE_DEMO_IMAGE;
  if (configured === undefined) return `${DEMO_IMAGE_REPOSITORY}:${version}`;
  if (!/^[a-z0-9][A-Za-z0-9_./:@-]{0,254}$/u.test(configured))
    throw new Error("CALQUE_DEMO_IMAGE must be an image reference");
  return configured;
}

export interface CalqueContainerOptions {
  readonly image: string;
  readonly name: string;
  /** The PostgreSQL container whose network namespace `calque` joins. */
  readonly postgresContainer: string;
  /** The host's state directory, absolute. */
  readonly stateDirectory: string;
  readonly user: string;
  /** The arguments after `calque`. */
  readonly calqueArguments: readonly string[];
}

/**
 * `docker run` for `calque dev` in its image.
 *
 * The container shares PostgreSQL's network namespace, so the source is at
 * 127.0.0.1: `calque` reaches any other host only over verified TLS. The
 * server's port is published by the PostgreSQL container, on the host's
 * loopback address only. The connection string is passed by name, so it
 * stays out of this process's arguments.
 */
export function buildCalqueContainerArguments(
  options: CalqueContainerOptions,
): readonly string[] {
  return [
    "run",
    "--rm",
    "--name",
    options.name,
    "--network",
    `container:${options.postgresContainer}`,
    "--user",
    options.user,
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    "--volume",
    `${options.stateDirectory}:${CONTAINER_STATE_DIRECTORY}`,
    "--env",
    "CALQUE_DATABASE_URL",
    options.image,
    ...options.calqueArguments,
  ];
}

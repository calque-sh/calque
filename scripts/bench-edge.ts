/**
 * The production edge for a bench run: Caddy terminating TLS, with HTTP/2,
 * in front of the page server and of `calque dev`, as the self-host
 * composition puts it in front of `calque serve`
 * (`docker/selfhost-caddy/Caddyfile`). The composition publishes 443 over TCP
 * only, so a browser there speaks HTTP/1.1 or HTTP/2 and never HTTP/3; the
 * edge offers the same two.
 *
 * Each origin gets its own port on the edge, and the kernel shaper shapes
 * those ports, so every byte the browser moves crosses TLS, HTTP/2 and the
 * shaped link, while the hops from the edge to the page server and to
 * calque stay on unshaped loopback, as they stay on the composition's own
 * network. The Caddy binary is the pinned release `bench-caddy.ts`
 * fetches.
 */
import { execFileSync } from "node:child_process";
import { createHash, X509Certificate } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { connect } from "node:net";
import { join } from "node:path";

import { CADDY } from "./bench-caddy.ts";
import { delay } from "./demo-process.ts";

/** HTTP/1.1 and HTTP/2, as Caddy names them. */
const EDGE_PROTOCOLS = ["h1", "h2"] as const;

/** The edge a run's records name, as `conditions.edge` and the plan's `edge`. */
interface EdgeRecord {
  readonly caddy: string;
  readonly binarySha256: string;
  readonly protocols: readonly string[];
}

/** The pinned edge's record, or null for a run without the edge. */
export function edgeRecord(edge: boolean): EdgeRecord | null {
  return edge
    ? {
        caddy: CADDY.version,
        binarySha256: CADDY.binarySha256,
        protocols: [...EDGE_PROTOCOLS],
      }
    : null;
}

interface EdgePorts {
  /** The page server's own loopback port. */
  readonly page: number;
  /** `calque dev`'s own loopback port. */
  readonly calque: number;
  /** The edge's port for the page origin, which the shaper shapes. */
  readonly pageEdge: number;
  /** The edge's port for calque, which the shaper shapes. */
  readonly calqueEdge: number;
}

export interface EdgePlan {
  readonly caddyfile: string;
  /** The browser-visible page origin. */
  readonly pageOrigin: string;
  /** The browser-visible calque base URL, `calque dev --public-base-url`. */
  readonly calqueBaseUrl: string;
  /** What the browser needs to trust the edge's certificate and no other. */
  readonly chromeArgs: readonly string[];
  readonly environment: NodeJS.ProcessEnv;
}

/**
 * The edge's Caddyfile. calque dev compares Host byte for byte against its
 * own loopback address, so the calque site forwards that, as the self-host
 * site forwards the public host. Neither site has an `encode` directive, so
 * the edge sends each response in the coding its upstream chose.
 */
export function edgeCaddyfile(
  ports: EdgePorts,
  certificate: string,
  key: string,
): string {
  return `{
\tadmin off
\tauto_https disable_redirects
\tskip_install_trust
\tservers {
\t\tprotocols ${EDGE_PROTOCOLS.join(" ")}
\t}
}

https://127.0.0.1:${String(ports.pageEdge)} {
\ttls ${certificate} ${key}
\treverse_proxy 127.0.0.1:${String(ports.page)}
}

https://127.0.0.1:${String(ports.calqueEdge)} {
\ttls ${certificate} ${key}
\treverse_proxy 127.0.0.1:${String(ports.calque)} {
\t\theader_up Host 127.0.0.1:${String(ports.calque)}
\t}
}
`;
}

/**
 * Writes a one-day certificate for 127.0.0.1 and the Caddyfile under
 * `directory`. Chrome trusts the certificate by its key's SPKI hash, which
 * treats it as valid rather than ignoring an error, so nothing about the
 * connection differs from one to a publicly trusted host.
 */
export function planEdge(directory: string, ports: EdgePorts): EdgePlan {
  mkdirSync(directory, { recursive: true });
  const certificate = join(directory, "edge.crt");
  const key = join(directory, "edge.key");
  execFileSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "ec",
      "-pkeyopt",
      "ec_paramgen_curve:prime256v1",
      "-nodes",
      "-keyout",
      key,
      "-out",
      certificate,
      "-days",
      "1",
      "-subj",
      "/CN=127.0.0.1",
      "-addext",
      "subjectAltName=IP:127.0.0.1",
    ],
    { stdio: ["ignore", "ignore", "pipe"] },
  );
  const spki = createHash("sha256")
    .update(
      new X509Certificate(readFileSync(certificate)).publicKey.export({
        type: "spki",
        format: "der",
      }),
    )
    .digest("base64");
  const caddyfile = join(directory, "Caddyfile");
  writeFileSync(caddyfile, edgeCaddyfile(ports, certificate, key));
  return {
    caddyfile,
    pageOrigin: `https://127.0.0.1:${String(ports.pageEdge)}`,
    calqueBaseUrl: `https://127.0.0.1:${String(ports.calqueEdge)}`,
    chromeArgs: [`--ignore-certificate-errors-spki-list=${spki}`],
    // Caddy keeps its storage under the run, not the user's home.
    environment: {
      ...process.env,
      XDG_DATA_HOME: join(directory, "data"),
      XDG_CONFIG_HOME: join(directory, "config"),
    },
  };
}

/** The Caddy binary's version line, for the run's log on stdout. */
export function caddyVersion(caddy: string): string {
  return execFileSync(caddy, ["version"], { encoding: "utf8" }).trim();
}

/** Resolves once every port accepts a TCP connection, or throws after 30 s. */
export async function waitForListening(
  ports: readonly number[],
): Promise<void> {
  const deadline = Date.now() + 30_000;
  for (const port of ports) {
    for (;;) {
      const accepted = await new Promise<boolean>((resolveAccepted) => {
        const socket = connect({ host: "127.0.0.1", port });
        socket.once("connect", () => {
          socket.destroy();
          resolveAccepted(true);
        });
        socket.once("error", () => resolveAccepted(false));
      });
      if (accepted) break;
      if (Date.now() > deadline)
        throw new Error(`the edge did not listen on ${String(port)} in 30 s`);
      await delay(100);
    }
  }
}

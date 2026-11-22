/**
 * Loopback ports an end-to-end script hands to a server it starts later.
 */
import { randomInt } from "node:crypto";
import { createServer } from "node:net";

/**
 * Where `reservedPort` draws from: under Linux's ephemeral range, which starts
 * at 32768 (macOS's starts at 49152). A port the kernel picks for port 0 comes
 * from that range, and on a hosted runner an outgoing connection took one
 * between its release and the listener's bind. The fixed ports CI publishes
 * sit below 30000.
 */
export const RESERVED_PORTS = { first: 30_000, last: 32_767 } as const;

const handedOut = new Set<number>();

/**
 * A free loopback TCP port no other call in this process returned, released
 * immediately.
 */
export async function reservedPort(): Promise<number> {
  const span = RESERVED_PORTS.last - RESERVED_PORTS.first + 1;
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const port = RESERVED_PORTS.first + randomInt(span);
    if (handedOut.has(port)) continue;
    handedOut.add(port);
    if (await bindable(port)) return port;
    handedOut.delete(port);
  }
  throw new Error(
    `no free loopback port in ${String(RESERVED_PORTS.first)}-${String(RESERVED_PORTS.last)} after 200 tries`,
  );
}

async function bindable(port: number): Promise<boolean> {
  return await new Promise((settle, fail) => {
    const server = createServer();
    server.on("error", (error: NodeJS.ErrnoException) => {
      if (error.code === "EADDRINUSE") settle(false);
      else fail(error);
    });
    server.listen(port, "127.0.0.1", () => {
      server.close(() => settle(true));
    });
  });
}

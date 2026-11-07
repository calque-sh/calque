import { createSyncClient } from "@calque/client";
import {
  createGeneratedClientSchema,
  type GeneratedClientSchemaDescriptor,
} from "@calque/client/internal";

import { schema as collectionlessSchema } from "./calque.generated.js";

async function waitUntilReady(client: ReturnType<typeof createSyncClient>) {
  if (client.status.state === "ready") return;
  await new Promise<void>((resolve, reject) => {
    let unsubscribe = () => undefined;
    const timeout = globalThis.setTimeout(() => {
      unsubscribe();
      reject(new Error("consumer client did not become ready"));
    }, 30_000);
    const observe = () => {
      if (client.status.state === "ready") {
        globalThis.clearTimeout(timeout);
        unsubscribe();
        resolve();
      } else if (client.status.state === "error") {
        globalThis.clearTimeout(timeout);
        const error = client.status.error;
        unsubscribe();
        reject(error);
      }
    };
    unsubscribe = client.subscribe(observe);
    observe();
  });
}

/**
 * Generated bindings that declare no collection have nothing to query, so the
 * public factory refuses them rather than returning an empty client.
 */
function collectionlessSchemaRefusal(): string {
  try {
    createSyncClient({
      endpoint: `${globalThis.location.origin}/`,
      auth: () => ({ bearerToken: "example" }),
      schema: collectionlessSchema,
      memoryBudgetBytes: 256 * 1024 * 1024,
      deviceId: "browser-consumer-collectionless",
    });
  } catch (error) {
    return error instanceof TypeError ? error.message : "not a TypeError";
  }
  return "accepted";
}

async function exerciseApplicationLifecycle() {
  const response = await globalThis.fetch("/__calque/descriptor.json");
  const descriptor = (await response.json()) as GeneratedClientSchemaDescriptor;
  const refusal = collectionlessSchemaRefusal();
  const client = createSyncClient({
    endpoint: `${globalThis.location.origin}/`,
    auth: () => ({ bearerToken: "example" }),
    schema: createGeneratedClientSchema(descriptor),
    memoryBudgetBytes: 256 * 1024 * 1024,
    deviceId: "browser-consumer",
  });
  const states: string[] = [];
  await waitUntilReady(client);
  states.push(client.status.state);
  await client.reset();
  await waitUntilReady(client);
  states.push(client.status.state);
  await client.close();
  states.push(client.status.state);
  return { ok: true as const, states, refusal };
}

void exerciseApplicationLifecycle().then(
  (result) => {
    Object.defineProperty(globalThis, "__calqueConsumerResult", {
      configurable: true,
      value: result,
    });
  },
  (error: unknown) => {
    Object.defineProperty(globalThis, "__calqueConsumerResult", {
      configurable: true,
      value: {
        ok: false,
        error:
          error instanceof Error
            ? `${error.name}: ${error.message}`
            : "unknown consumer failure",
      },
    });
  },
);

import type { GeneratedClientSchemaDescriptor } from "./generated-schema.js";
import type { ClientSchema } from "./schema.js";

export interface RegisteredClientSchema {
  readonly descriptor: GeneratedClientSchemaDescriptor;
  readonly retainedBytes: number;
}

const registeredSchemas = new WeakMap<ClientSchema, RegisteredClientSchema>();

export function registerClientSchema(
  descriptor: GeneratedClientSchemaDescriptor,
  retainedBytes: number,
): ClientSchema {
  const facade = Object.freeze(Object.create(null)) as ClientSchema;
  registeredSchemas.set(facade, Object.freeze({ descriptor, retainedBytes }));
  return facade;
}

const INVALID_SCHEMA =
  "SyncClientOptions schema is invalid: pass the `schema` that calque.generated.ts exports, from a bundle that holds one copy of @calque/client";

export function resolveClientSchema(value: unknown): RegisteredClientSchema {
  if (typeof value !== "object" || value === null)
    throw new TypeError(INVALID_SCHEMA);
  const registered = registeredSchemas.get(value as ClientSchema);
  if (!registered) throw new TypeError(INVALID_SCHEMA);
  return registered;
}

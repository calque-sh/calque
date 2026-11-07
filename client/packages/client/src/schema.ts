import type { CollectionShapes } from "./facade/types.js";

declare const clientSchemaCapability: unique symbol;

/**
 * The opaque schema the generated bindings export, which an application passes
 * to `createSyncClient()` and cannot inspect or construct. `Collections` is a
 * type-only record of its collection facades.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars -- phantom type argument, inferred by createSyncClient
export interface ClientSchema<Collections extends CollectionShapes = never> {
  readonly [clientSchemaCapability]: never;
}

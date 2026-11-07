import { createSyncClient } from "@calque/client";
import { renderedConnection } from "@calque/client/bootstrap-start";
import { schema } from "./calque.generated.js";

const connection = renderedConnection();

export const client = createSyncClient({
  endpoint: connection.endpoint,
  auth: () => connection,
  schema,
});

export const { db } = client;

import type { SyncPublication } from "@calque/client";
import {
  createGeneratedClientSchema,
  createInternalColumnarClient,
  type InternalSyncClientOptions,
  type GeneratedClientSchemaDescriptor,
} from "@calque/client/internal";

const generatedDescriptor: GeneratedClientSchemaDescriptor = {
  collections: [
    {
      id: "tasks",
      tableId: "public.tasks",
      columns: [
        { name: "id", kind: "int64" },
        { name: "title", kind: "text" },
      ],
      primaryKey: ["id"],
    },
  ],
};
export const generatedSchema = createGeneratedClientSchema(generatedDescriptor);

export function internalConsumer(
  options: InternalSyncClientOptions,
  engineWorker: () => Worker,
): void {
  const client = createInternalColumnarClient(options, engineWorker);
  const unsubscribe = client.subscribePublications(
    (publication: SyncPublication) => {
      void publication.cursor;
      void publication.changedCollections;
      void publication.latestCommitTimeUnixMicros;
      void publication.transactionCount;
    },
  );
  unsubscribe();
}

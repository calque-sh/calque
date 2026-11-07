import { createSyncClient } from "@calque/client";
import {
  renderedConnection,
  startedAuth,
} from "@calque/client/bootstrap-start";

import { schema } from "../calque.generated.js";

createSyncClient({
  endpoint: renderedConnection().endpoint,
  auth: startedAuth(() => ({ bearerToken: "refreshed" })),
  schema,
});

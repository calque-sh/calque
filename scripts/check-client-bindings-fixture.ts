import process from "node:process";

import { checkPinnedBindings } from "./lib/client-bindings.ts";

const checked = checkPinnedBindings();
process.stdout.write(
  `Client generated bindings drift: PASS (${checked} pinned renders byte-identical, each rendered twice)\n`,
);

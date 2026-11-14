import { createSyncClient } from "@calque/client";
import { schema } from "./calque.generated.js";

const client = createSyncClient({
  endpoint: "https://localhost:8443/",
  // The dev server serves the grant `calque-grant mint` wrote. In production,
  // your backend issues it.
  auth: async () => {
    const response = await fetch("/grant.json", { cache: "no-store" });
    if (!response.ok) throw new Error(await response.text());
    const { token } = (await response.json()) as { readonly token: string };
    return { bearerToken: token };
  },
  schema,
});

const status = document.getElementById("status");
const list = document.getElementById("tasks");

function item(text: string): HTMLLIElement {
  const element = document.createElement("li");
  element.textContent = text;
  return element;
}

client.subscribe(() => {
  const current = client.status;
  if (status)
    status.textContent =
      "error" in current
        ? `${current.state}: ${current.error.message}`
        : current.state;
});

// Runs again whenever the rows change, so a task written in PostgreSQL
// appears without a reload.
const tasks = client.db.tasks.query().orderBy("id").subscribe();
tasks.subscribe(() => {
  const snapshot = tasks.getSnapshot();
  if (snapshot.status === "error")
    list?.replaceChildren(item(snapshot.error.message));
  if (snapshot.status === "ready")
    list?.replaceChildren(
      ...snapshot.result.map((task) =>
        item(`${task.done ? "done" : "open"}: ${task.title}`),
      ),
    );
});

/**
 * Compiles the generated bindings of `client/examples/incidents` against
 * the package root as an application would: the facades under `db`
 * are typed by `calque.generated.ts`, and the negative boundaries below are
 * what the type-level contract must keep refusing.
 */
import {
  and,
  createSyncClient,
  eq,
  gte,
  isNull,
  type LiveQueryResource,
  type SyncClient,
} from "@calque/client";
import {
  schema,
  type CalqueCollections,
  type EventsRow,
  type IssuesInsert,
  type IssuesPatch,
} from "./calque.generated.js";

export function createTypedClient(): SyncClient<CalqueCollections> {
  return createSyncClient({
    endpoint: "https://sync.example.test",
    auth: () => ({ bearerToken: "example" }),
    schema,
    memoryBudgetBytes: 512 * 1024 * 1024,
    deviceId: "typed-consumer",
  });
}

export async function typedReads(
  client: SyncClient<CalqueCollections>,
): Promise<void> {
  const errors: readonly EventsRow[] = await client.db.events
    .query()
    .where(and(eq("level", "error"), isNull("issue_id")))
    .orderBy("occurred_at", "desc")
    .limit(100)
    .run({ maxRows: 100 });
  const firstId: bigint | undefined = errors[0]?.id;
  void firstId;

  const projected = await client.db.events.query().select("id", "level").run();
  const level: string | undefined = projected[0]?.level;
  void level;
  // @ts-expect-error the projection dropped message; only selected columns exist.
  void projected[0]?.message;

  const aliased = await client.db.events
    .query()
    .selectAs("level", "severity")
    .run();
  const severity: string | undefined = aliased[0]?.severity;
  const message: string | undefined = aliased[0]?.message;
  void severity;
  void message;
  const narrowed = await client.db.events
    .query()
    .select("id")
    .selectAs("level", "severity")
    .run();
  const narrowedSeverity: string | undefined = narrowed[0]?.severity;
  void narrowedSeverity;
  // @ts-expect-error after select, selectAs adds its alias and nothing else.
  void narrowed[0]?.level;

  const grouped = await client.db.events
    .query()
    .groupBy("level")
    .count()
    .orderBy("count", "desc")
    .run();
  const count: number | undefined = grouped[0]?.count;
  void count;
  // @ts-expect-error a count is a JavaScript number, never a bigint.
  const bigCount: bigint | undefined = grouped[0]?.count;
  void bigCount;

  const totals = await client.db.issues
    .query()
    .where(eq("status", "open"))
    .sum("priority", "total")
    .sum("bench_index", "benchTotal")
    .max("priority")
    .run();
  const total: bigint | null | undefined = totals[0]?.total;
  const benchTotal: bigint | null | undefined = totals[0]?.benchTotal;
  const highest: number | null | undefined = totals[0]?.priority;
  void total;
  void benchTotal;
  void highest;
  // @ts-expect-error a sum over no non-null value is null, so it is not a bigint.
  const definiteTotal: bigint | undefined = totals[0]?.total;
  void definiteTotal;
  // @ts-expect-error a sum over an int4 column is a bigint, as in PostgreSQL.
  const numberTotal: number | null | undefined = totals[0]?.total;
  void numberTotal;

  const perProject = await client.db.projects
    .query()
    .join(client.db.issues, "issue", { left: "id", right: "project_id" })
    .groupBy("slug")
    .sum("issue.event_count", "events")
    .run();
  const projectEvents: bigint | null | undefined = perProject[0]?.events;
  void projectEvents;

  const joined = await client.db.issues
    .query()
    .join(client.db.projects, "project", { left: "project_id", right: "id" })
    .where(gte("priority", 3))
    .select("id", "title", "project.slug")
    .run();
  const slug: string | undefined = joined[0]?.slug;
  void slug;

  const window: readonly EventsRow[] = await client.db.events.slice(0, 64);
  void window;

  const live: LiveQueryResource<readonly Pick<EventsRow, "id">[]> =
    client.db.events.query().select("id").subscribe({ maxRows: 10 });
  void live;

  const issue = "0a1b2c3d-4e5f-4061-8172-839495a6b7c8";
  const row: IssuesInsert = {
    id: issue,
    organization_id: "9d8c7b6a-5f4e-4d3c-8b2a-190807060504",
    project_id: "1a2b3c4d-5e6f-4a0b-9c1d-2e3f4a5b6c7d",
    assignee_id: null,
    title: "Login fails",
    status: "unresolved",
    priority: 3,
    event_count: 0,
    updated_at: "2026-10-17T12:00:00Z",
    is_sentinel: false,
    bench_index: 1n,
  };
  await client.db.issues.insert(row);
  const { assignee_id, ...unassigned } = row;
  void assignee_id;
  // @ts-expect-error an insert carries every column; a nullable one takes null.
  await client.db.issues.insert(unassigned);
  const patch: IssuesPatch = { status: "resolved", priority: 1 };
  await client.db.issues.update({ id: issue }, patch);
  // @ts-expect-error a patch sets writable columns, and the key is not one.
  await client.db.issues.update({ id: issue }, { id: issue });
  await client.db.issues.delete({ id: issue });
}

/** The bindings register their collections, so no type argument is needed. */
export function registered(client: SyncClient): SyncClient<CalqueCollections> {
  void client.db.issues.query().select("title");
  // @ts-expect-error the registered collections have no audit_log.
  void client.db.audit_log;
  return client;
}

export function refusedAtTheType(client: SyncClient<CalqueCollections>): void {
  // @ts-expect-error unknown columns are refused where the predicate is built.
  client.db.events.query().where(eq("severity", "error"));
  // @ts-expect-error a comparison is never true of NULL; isNull tests for it.
  client.db.events.query().where(eq("issue_id", null));
  // @ts-expect-error a boolean column compares with a boolean.
  client.db.events.query().where(eq("is_sentinel", 1));
  // @ts-expect-error an int4 column holds numbers, not bigints.
  client.db.issues.query().where(gte("priority", 3n));
  // @ts-expect-error an int8 column holds bigints, not numbers.
  client.db.issues.query().where(gte("bench_index", 3));
  // @ts-expect-error sum adds numeric columns only.
  client.db.issues.query().sum("title");
  client.db.issues.query().join(client.db.projects, "project", {
    left: "priority",
    // @ts-expect-error a join compares columns whose values can be equal.
    right: "id",
  });
  // @ts-expect-error orderBy addresses projected output names only.
  client.db.events.query().select("id").orderBy("level");
  // @ts-expect-error no configured command inserts events, so insert is absent.
  void client.db.events.insert;
  // @ts-expect-error no configured command writes users, so delete is absent.
  void client.db.users.delete;
  // @ts-expect-error a uuid key is its text, never its 16 bytes.
  void client.db.issues.delete({ id: new Uint8Array(16) });
  // @ts-expect-error a facade query carries no inspectable plan.
  void client.db.issues.query().toSpec;
  // @ts-expect-error the collection record is readonly.
  client.db.events = undefined as never;
  // @ts-expect-error a join takes a generated facade, never a collection name.
  client.db.issues.query().join("projects", "project", {
    left: "project_id",
    right: "id",
  });
  // @ts-expect-error an unlisted collection does not exist on db.
  void client.db.audit_log;
}

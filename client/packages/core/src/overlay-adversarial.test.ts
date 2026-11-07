/**
 * The cases that break naive implementations, each driven against both
 * implementations and asserted directly rather than sampled.
 *
 * All but four agree and are asserted as equivalent. In the other four, each
 * side's divergence is asserted: SQLite rolls back the whole canonical batch
 * because an optimistic guess no longer applies, while the overlay applies the
 * batch and drops the pending action from the fold, leaving its mutation to
 * wait for the server's verdict.
 */
import { describe, expect, test } from "vitest";

import {
  OverlayEngine,
  OverlayError,
  type CanonicalBatch,
  type KeyPart,
  type OptimisticAction,
  type OptimisticMutation,
  type SqlValue,
} from "./overlay.js";
import { SqliteReference } from "./sqlite-reference.fixture.js";
import {
  applyStepToOverlay,
  applyStepToReference,
  buildPair,
  diff,
  keyBytesOf,
  materializeOverlay,
  materializeReference,
  type Step,
} from "./overlay-oracle.fixture.js";

const SEED = 0xa11ce;

// `issues` column ordinals.
const ID = 0;
const ASSIGNEE = 3;
const STATUS = 4;
const PRIORITY = 5;
const TITLE = 6;
const EVENTS = 7;

const issueKey = (id: string): readonly KeyPart[] => [id];

const issueRow = (id: string): SqlValue[] => [
  id,
  "org-0",
  "project-0",
  null,
  "open",
  1,
  `title-${id}`,
  2,
  "2026-09-01T00:00:00.000000Z",
];

const canonicalUpdate = (
  id: string,
  changed: readonly { column: number; value: SqlValue }[],
): Step => ({
  kind: "canonical",
  batch: {
    transactions: [
      {
        operations: [
          {
            kind: "update",
            collection: "issues",
            primaryKey: keyBytesOf(issueKey(id)),
            key: issueKey(id),
            changed,
          },
        ],
        mutationIds: [],
      },
    ],
  },
});

const canonicalDelete = (id: string): Step => ({
  kind: "canonical",
  batch: {
    transactions: [
      {
        operations: [
          {
            kind: "delete",
            collection: "issues",
            primaryKey: keyBytesOf(issueKey(id)),
            key: issueKey(id),
          },
        ],
        mutationIds: [],
      },
    ],
  },
});

const canonicalInsert = (
  id: string,
  mutationIds: readonly string[] = [],
): Step => ({
  kind: "canonical",
  batch: {
    transactions: [
      {
        operations: [
          {
            kind: "insert",
            collection: "issues",
            primaryKey: keyBytesOf(issueKey(id)),
            key: issueKey(id),
            values: issueRow(id),
          },
        ],
        mutationIds,
      },
    ],
  },
});

const truncateIssues: Step = {
  kind: "canonical",
  batch: {
    transactions: [
      {
        operations: [{ kind: "truncate", collection: "issues" }],
        mutationIds: [],
      },
    ],
  },
};

const mutation = (
  mutationId: string,
  ...actions: OptimisticAction[]
): Step => ({ kind: "optimistic", mutation: { mutationId, actions } });

const incrementAction = (
  id: string,
  column: number,
  value: number,
): OptimisticAction => ({
  kind: "patch",
  collection: "issues",
  primaryKey: keyBytesOf(issueKey(id)),
  key: issueKey(id),
  changes: [{ column, value, mode: "increment" }],
});

const setAction = (
  id: string,
  column: number,
  value: SqlValue,
): OptimisticAction => ({
  kind: "patch",
  collection: "issues",
  primaryKey: keyBytesOf(issueKey(id)),
  key: issueKey(id),
  changes: [{ column, value, mode: "set" }],
});

const insertAction = (id: string): OptimisticAction => ({
  kind: "insert",
  collection: "issues",
  primaryKey: keyBytesOf(issueKey(id)),
  key: issueKey(id),
  values: issueRow(id),
});

const deleteAction = (id: string): OptimisticAction => ({
  kind: "delete",
  collection: "issues",
  primaryKey: keyBytesOf(issueKey(id)),
  key: issueKey(id),
});

interface Harness {
  readonly overlay: OverlayEngine;
  readonly reference: SqliteReference;
  /** Applies one step to both sides and returns what each threw, if any. */
  drive(step: Step): { overlay: string | null; sqlite: string | null };
  agree(): void;
  issue(id: string): SqlValue[] | undefined;
  close(): void;
}

async function harness(): Promise<Harness> {
  const { overlay, reference } = await buildPair(SEED);
  const catchError = (action: () => void): string | null => {
    try {
      action();
      return null;
    } catch (error) {
      return error instanceof Error ? error.message : String(error);
    }
  };
  return {
    overlay,
    reference,
    drive: (step) => ({
      overlay: catchError(() => {
        applyStepToOverlay(overlay, step);
      }),
      sqlite: catchError(() => {
        applyStepToReference(reference, step);
      }),
    }),
    agree: () => {
      expect(
        diff(materializeOverlay(overlay), materializeReference(reference)),
      ).toBeNull();
    },
    issue: (id) => overlay.rows("issues").find((row) => row[ID] === id),
    close: () => {
      reference.close();
    },
  };
}

describe("adversarial cases, both implementations", () => {
  test("an optimistic increment over a row the canonical stream also changed", async () => {
    const test_ = await harness();
    try {
      expect(
        test_.drive(mutation("m1", incrementAction("issue-3", PRIORITY, 3))),
      ).toEqual({ overlay: null, sqlite: null });
      test_.agree();
      // The canonical stream moves the value the increment is a function of.
      expect(
        test_.drive(
          canonicalUpdate("issue-3", [{ column: PRIORITY, value: 100 }]),
        ),
      ).toEqual({ overlay: null, sqlite: null });
      test_.agree();
      // The refold recomputes it; leaving it in place would show 100.
      expect(test_.issue("issue-3")?.[PRIORITY]).toBe(103);
    } finally {
      test_.close();
    }
  });

  test("a rejection in the middle of a pending chain", async () => {
    const test_ = await harness();
    try {
      const before = test_.issue("issue-5")?.[EVENTS];
      expect(typeof before).toBe("number");
      test_.drive(mutation("m1", incrementAction("issue-5", EVENTS, 1)));
      test_.drive(mutation("m2", incrementAction("issue-5", EVENTS, 2)));
      test_.drive(mutation("m3", incrementAction("issue-5", EVENTS, 4)));
      test_.agree();
      expect(test_.issue("issue-5")?.[EVENTS]).toBe(Number(before) + 7);

      expect(test_.drive({ kind: "reject", mutationId: "m2" })).toEqual({
        overlay: null,
        sqlite: null,
      });
      test_.agree();
      expect(test_.issue("issue-5")?.[EVENTS]).toBe(Number(before) + 5);
      expect(test_.overlay.pendingMutationIds()).toEqual(["m1", "m3"]);
      expect(test_.reference.pendingMutationIds()).toEqual(["m1", "m3"]);
    } finally {
      test_.close();
    }
  });

  test("an acknowledgement drops the optimistic delta rather than graduating it", async () => {
    const test_ = await harness();
    try {
      test_.drive(mutation("m1", setAction("issue-2", STATUS, "client-guess")));
      test_.agree();
      expect(test_.issue("issue-2")?.[STATUS]).toBe("client-guess");
      // The server disagreed. The echo carries its value and drops the guess.
      const step: Step = {
        kind: "canonical",
        batch: {
          transactions: [
            {
              operations: [
                {
                  kind: "update",
                  collection: "issues",
                  primaryKey: keyBytesOf(issueKey("issue-2")),
                  key: issueKey("issue-2"),
                  changed: [{ column: STATUS, value: "server-truth" }],
                },
              ],
              mutationIds: ["m1"],
            },
          ],
        },
      };
      expect(test_.drive(step)).toEqual({ overlay: null, sqlite: null });
      test_.agree();
      expect(test_.issue("issue-2")?.[STATUS]).toBe("server-truth");
      expect(test_.overlay.pendingMutationIds()).toEqual([]);
    } finally {
      test_.close();
    }
  });

  test("a pending insert may clear a canonical tombstone", async () => {
    const test_ = await harness();
    try {
      expect(test_.drive(canonicalDelete("issue-7"))).toEqual({
        overlay: null,
        sqlite: null,
      });
      test_.agree();
      expect(test_.issue("issue-7")).toBeUndefined();
      expect(test_.drive(mutation("m1", insertAction("issue-7")))).toEqual({
        overlay: null,
        sqlite: null,
      });
      test_.agree();
      expect(test_.issue("issue-7")?.[TITLE]).toBe("title-issue-7");
    } finally {
      test_.close();
    }
  });

  test("a canonical insert may clear a canonical tombstone", async () => {
    const test_ = await harness();
    try {
      test_.drive(canonicalDelete("issue-9"));
      test_.agree();
      expect(test_.drive(canonicalInsert("issue-9"))).toEqual({
        overlay: null,
        sqlite: null,
      });
      test_.agree();
      expect(test_.issue("issue-9")?.[ASSIGNEE]).toBeNull();
    } finally {
      test_.close();
    }
  });

  test("a canonical insert of a live key is refused by both", async () => {
    const test_ = await harness();
    try {
      const outcome = test_.drive(canonicalInsert("issue-1"));
      expect(outcome.overlay).toMatch(/RESNAPSHOT_REQUIRED/u);
      expect(outcome.sqlite).not.toBeNull();
      test_.agree();
    } finally {
      test_.close();
    }
  });

  test("a canonical update of an absent key is refused by both", async () => {
    const test_ = await harness();
    try {
      test_.drive(canonicalDelete("issue-11"));
      const outcome = test_.drive(
        canonicalUpdate("issue-11", [{ column: STATUS, value: "x" }]),
      );
      expect(outcome.overlay).toMatch(/RESNAPSHOT_REQUIRED/u);
      expect(outcome.sqlite).toMatch(/RESNAPSHOT_REQUIRED/u);
      test_.agree();
    } finally {
      test_.close();
    }
  });

  // ------------------------------------------------- documented divergences

  test("DIVERGENCE: a canonical delete under a pending patch", async () => {
    const test_ = await harness();
    try {
      test_.drive(mutation("m1", setAction("issue-4", STATUS, "guess")));
      test_.agree();
      const outcome = test_.drive(canonicalDelete("issue-4"));

      // SQLite replays the pending patch after the delete. Its read-before-
      // write (`#selectExactRow`) finds no row, so the whole batch is rolled
      // back -- a canonical batch failed because of a client's guess.
      expect(outcome.sqlite).toMatch(
        /optimistic mutation target must match exactly one row/u,
      );

      // The overlay applies the canonical delete and drops the pending patch
      // from the fold: a pending patch must not resurrect a tombstone.
      expect(outcome.overlay).toBeNull();
      expect(test_.issue("issue-4")).toBeUndefined();
      expect(test_.overlay.droppedActions()).toEqual([
        {
          mutationId: "m1",
          collection: "issues",
          actionIndex: 0,
          reason: "absent-row",
        },
      ]);
      // The mutation is still pending: it waits for the server's verdict.
      expect(test_.overlay.pendingMutationIds()).toEqual(["m1"]);
    } finally {
      test_.close();
    }
  });

  test("DIVERGENCE: a pending insert of a key the canonical stream then inserts", async () => {
    const test_ = await harness();
    try {
      test_.drive(mutation("m1", insertAction("issue-new")));
      test_.agree();
      const outcome = test_.drive(canonicalInsert("issue-new"));

      // SQLite's replay re-runs the optimistic INSERT and hits the primary-key
      // constraint, rolling the canonical batch back.
      expect(outcome.sqlite).not.toBeNull();

      // The overlay keeps the canonical row and drops the pending insert.
      expect(outcome.overlay).toBeNull();
      expect(test_.overlay.droppedActions()).toEqual([
        {
          mutationId: "m1",
          collection: "issues",
          actionIndex: 0,
          reason: "duplicate-key",
        },
      ]);
      expect(
        test_.overlay.rows("issues").filter((row) => row[ID] === "issue-new"),
      ).toHaveLength(1);
    } finally {
      test_.close();
    }
  });

  test("DIVERGENCE: a truncate under a pending patch", async () => {
    const test_ = await harness();
    try {
      test_.drive(mutation("m1", setAction("issue-6", STATUS, "guess")));
      const outcome = test_.drive(truncateIssues);
      expect(outcome.sqlite).toMatch(
        /optimistic mutation target must match exactly one row/u,
      );
      expect(outcome.overlay).toBeNull();
      expect(test_.overlay.rows("issues")).toEqual([]);
      expect(test_.overlay.droppedActions()).toHaveLength(1);
    } finally {
      test_.close();
    }
  });

  test("DIVERGENCE: a pending delete of a row the canonical stream deleted", async () => {
    const test_ = await harness();
    try {
      test_.drive(mutation("m1", deleteAction("issue-8")));
      test_.agree();
      const outcome = test_.drive(canonicalDelete("issue-8"));
      expect(outcome.sqlite).toMatch(
        /optimistic mutation target must match exactly one row/u,
      );
      expect(outcome.overlay).toBeNull();
      expect(test_.issue("issue-8")).toBeUndefined();
      expect(test_.overlay.droppedActions()).toHaveLength(1);
    } finally {
      test_.close();
    }
  });

  test("a truncate survives, and the base is suppressed rather than destroyed", async () => {
    const test_ = await harness();
    try {
      expect(test_.drive(truncateIssues)).toEqual({
        overlay: null,
        sqlite: null,
      });
      test_.agree();
      expect(test_.overlay.rows("issues")).toEqual([]);
      // A key still in the base re-enters through a full row image.
      expect(test_.drive(canonicalInsert("issue-0"))).toEqual({
        overlay: null,
        sqlite: null,
      });
      test_.agree();
      expect(test_.overlay.rows("issues")).toHaveLength(1);
      expect(test_.issue("issue-0")?.[TITLE]).toBe("title-issue-0");
    } finally {
      test_.close();
    }
  });
});

describe("increment over NULL, on a nullable numeric column", () => {
  const columns = ["id", "n"];
  const schema = [
    {
      name: "counters",
      columns: [
        { name: "id", affinity: "TEXT" as const, nullable: false },
        { name: "n", affinity: "REAL" as const, nullable: true },
      ],
      primaryKey: [0],
    },
  ];
  /** A base with no rows: every row here arrives as a canonical insert. */
  const emptyBase = { rows: 0, lookupKey: () => -1, valueAt: () => null };

  test("SQLite yields NULL and so does the overlay", () => {
    const overlay = new OverlayEngine([
      { name: "counters", columns, base: emptyBase },
    ]);
    const reference = new SqliteReference(schema, new Map());
    try {
      const key: readonly KeyPart[] = ["a"];
      const batch: CanonicalBatch = {
        transactions: [
          {
            operations: [
              {
                kind: "insert",
                collection: "counters",
                primaryKey: keyBytesOf(key),
                key,
                values: ["a", null],
              },
            ],
            mutationIds: [],
          },
        ],
      };
      overlay.applyCanonical(batch);
      reference.applyBatch(batch);

      const increment: OptimisticMutation = {
        mutationId: "m1",
        actions: [
          {
            kind: "patch",
            collection: "counters",
            primaryKey: keyBytesOf(key),
            key,
            changes: [{ column: 1, value: 5, mode: "increment" }],
          },
        ],
      };
      overlay.applyOptimistic(increment);
      reference.submitMutation(increment);

      expect(reference.materialize("counters")).toEqual([["a", null]]);
      expect(overlay.rows("counters")).toEqual([["a", null]]);
    } finally {
      reference.close();
    }
  });

  test("an increment chains over a canonical value, on both", () => {
    const overlay = new OverlayEngine([
      { name: "counters", columns, base: emptyBase },
    ]);
    const reference = new SqliteReference(schema, new Map());
    try {
      const key: readonly KeyPart[] = ["a"];
      const insert: CanonicalBatch = {
        transactions: [
          {
            operations: [
              {
                kind: "insert",
                collection: "counters",
                primaryKey: keyBytesOf(key),
                key,
                values: ["a", 10],
              },
            ],
            mutationIds: [],
          },
        ],
      };
      overlay.applyCanonical(insert);
      reference.applyBatch(insert);
      for (const [index, delta] of [1, 2, 4].entries()) {
        const step: OptimisticMutation = {
          mutationId: `m${index}`,
          actions: [
            {
              kind: "patch",
              collection: "counters",
              primaryKey: keyBytesOf(key),
              key,
              changes: [{ column: 1, value: delta, mode: "increment" }],
            },
          ],
        };
        overlay.applyOptimistic(step);
        reference.submitMutation(step);
      }
      expect(overlay.rows("counters")).toEqual([["a", 17]]);
      expect(reference.materialize("counters")).toEqual([["a", 17]]);

      overlay.dropMutation("m1");
      reference.rejectMutation("m1");
      expect(overlay.rows("counters")).toEqual([["a", 15]]);
      expect(reference.materialize("counters")).toEqual([["a", 15]]);
    } finally {
      reference.close();
    }
  });

  test("an increment over a non-numeric column is refused", () => {
    const overlay = new OverlayEngine([
      { name: "counters", columns, base: emptyBase },
    ]);
    const key: readonly KeyPart[] = ["a"];
    overlay.applyCanonical({
      transactions: [
        {
          operations: [
            {
              kind: "insert",
              collection: "counters",
              primaryKey: keyBytesOf(key),
              key,
              values: ["a", 1],
            },
          ],
          mutationIds: [],
        },
      ],
    });
    expect(() =>
      overlay.applyOptimistic({
        mutationId: "m1",
        actions: [
          {
            kind: "patch",
            collection: "counters",
            primaryKey: keyBytesOf(key),
            key,
            changes: [{ column: 0, value: 1, mode: "increment" }],
          },
        ],
      }),
    ).toThrow(OverlayError);
  });
});

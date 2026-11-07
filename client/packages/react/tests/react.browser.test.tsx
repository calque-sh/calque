import type {
  LiveQuerySnapshot,
  LiveQueryResource,
  QueryWindow,
  MutationResult,
  MutationStatus,
  SyncClient,
  SyncStatus,
} from "@calque/client";
import { MutationSubmissionError, SyncError } from "@calque/client";
import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Component, StrictMode, Suspense, type ReactNode } from "react";

import {
  SyncProvider,
  useMutationStatus,
  useLiveQuery,
  useLiveWindow,
  usePendingWrite,
  useSuspenseLiveQuery,
  useSyncClient,
  useSyncStatus,
} from "../src/index.js";

/** The row the mutation statuses below write. */
const TASK_ONE = { collection: "tasks", key: { id: 1n } } as const;

/** The rows the stub resource publishes; a generated facade's row shape. */
type CountRows = readonly { readonly count: number }[];

afterEach(cleanup);

class ResourceStub {
  readonly #listeners = new Set<() => void>();
  #snapshot: LiveQuerySnapshot<CountRows> = { status: "loading" };
  #resolve!: () => void;
  readonly promise = new Promise<void>((resolve) => {
    this.#resolve = resolve;
  });

  getSnapshot = () => this.#snapshot;
  getServerSnapshot = () => ({ status: "loading" }) as const;
  subscribe = (listener: () => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  publish(snapshot: LiveQuerySnapshot<CountRows>): void {
    this.#snapshot = snapshot;
    for (const listener of this.#listeners) listener();
    this.#resolve();
  }
}

/**
 * A generated facade query, as the hooks see one: an object that subscribes to
 * a resource.
 */
function countQuery(resource: ResourceStub) {
  let subscriptions = 0;
  const resources = new Set<LiveQueryResource<CountRows>>();
  const source = {
    subscribe: () => {
      subscriptions += 1;
      const created = resource as unknown as LiveQueryResource<CountRows>;
      resources.add(created);
      return created;
    },
  };
  return {
    source,
    get subscriptions() {
      return subscriptions;
    },
    get distinctResources() {
      return resources.size;
    },
  };
}

function client() {
  let status: SyncStatus = { state: "ready" };
  const statusListeners = new Set<() => void>();
  const mutationStatuses = new Map<string, MutationStatus>();
  const mutationListeners = new Map<string, Set<() => void>>();
  const everyMutationListeners = new Set<(status: MutationStatus) => void>();
  const setMutationStatus = (next: MutationStatus) => {
    const retained = Object.freeze(next);
    mutationStatuses.set(retained.mutationId, retained);
    for (const listener of mutationListeners.get(next.mutationId) ?? [])
      listener();
    for (const listener of everyMutationListeners) listener(retained);
  };
  return {
    refresh: vi.fn(async () => undefined),
    mutationStatus: (mutationId: string) => mutationStatuses.get(mutationId),
    subscribeMutation: (mutationId: string, listener: () => void) => {
      let listeners = mutationListeners.get(mutationId);
      if (!listeners) {
        listeners = new Set();
        mutationListeners.set(mutationId, listeners);
      }
      listeners.add(listener);
      return () => listeners?.delete(listener);
    },
    subscribeMutations: (listener: (status: MutationStatus) => void) => {
      everyMutationListeners.add(listener);
      return () => everyMutationListeners.delete(listener);
    },
    subscribe: (listener: () => void) => {
      statusListeners.add(listener);
      return () => statusListeners.delete(listener);
    },
    get status() {
      return status;
    },
    setStatus(next: SyncStatus) {
      status = next;
      for (const listener of statusListeners) listener();
    },
    setMutationStatus(next: MutationStatus) {
      setMutationStatus(next);
    },
  } as unknown as SyncClient & {
    setStatus(next: SyncStatus): void;
    setMutationStatus(next: MutationStatus): void;
  };
}

/** Renders `fallback` once a child throws, and hands `onError` the error. */
class Boundary extends Component<
  {
    readonly fallback: string;
    readonly onError: (error: unknown) => void;
    readonly children: ReactNode;
  },
  { readonly failed: boolean }
> {
  state = { failed: false };

  componentDidCatch(error: unknown): void {
    this.props.onError(error);
    this.setState({ failed: true });
  }

  render(): ReactNode {
    return this.state.failed ? (
      <div>{this.props.fallback}</div>
    ) : (
      this.props.children
    );
  }
}

describe("React query resources", () => {
  it("rejects missing provider misuse with one value-free TypeError", async () => {
    let caught: unknown;
    function Probe() {
      useSyncClient();
      return <div>unexpected client</div>;
    }

    render(
      <Boundary
        fallback="missing provider"
        onError={(error) => (caught = error)}
      >
        <Probe />
      </Boundary>,
    );

    expect(await screen.findByText("missing provider")).toBeTruthy();
    expect(caught).toBeInstanceOf(TypeError);
    expect((caught as TypeError).message).toBe(
      "Calque hooks require a SyncProvider",
    );
    expect(screen.queryByText("unexpected client")).toBeNull();
  });

  it.each([
    { query: "undefined", value: undefined },
    { query: "null", value: null },
    { query: "an object without subscribe", value: {} },
    { query: "a subscribe that is not a function", value: { subscribe: 1 } },
  ])("refuses $query as a query with a TypeError", async ({ value }) => {
    let caught: unknown;
    function Probe() {
      useLiveQuery(value as unknown as ReturnType<typeof countQuery>["source"]);
      return <div>unexpected rows</div>;
    }
    render(
      <SyncProvider client={client()}>
        <Boundary fallback="refused" onError={(error) => (caught = error)}>
          <Probe />
        </Boundary>
      </SyncProvider>,
    );
    expect(await screen.findByText("refused")).toBeTruthy();
    expect(caught).toBeInstanceOf(TypeError);
    expect((caught as TypeError).message).toBe(
      "useLiveQuery needs a generated collection query",
    );
  });

  it("keeps loading row-free and reuses one resource through Strict Mode", async () => {
    const resource = new ResourceStub();
    const sync = client();
    const query = countQuery(resource);
    function Probe() {
      const result = useLiveQuery(query.source);
      return <div>{result.status}</div>;
    }
    render(
      <StrictMode>
        <SyncProvider client={sync}>
          <Probe />
        </SyncProvider>
      </StrictMode>,
    );
    expect(screen.getByText("loading")).toBeTruthy();
    expect(query.subscriptions).toBeGreaterThan(1);
    expect(query.distinctResources).toBe(1);
    resource.publish({ status: "ready", result: [{ count: 1 }] });
    expect(await screen.findByText("ready")).toBeTruthy();
  });

  it("subscribes a generated collection query and publishes its typed rows", async () => {
    const resource = new ResourceStub();
    const sync = client();
    const subscribe = vi.fn(
      () =>
        resource as unknown as LiveQueryResource<
          readonly { readonly level: string }[]
        >,
    );
    const query = { subscribe };
    function Probe() {
      const result = useLiveQuery(query, { maxRows: 5 });
      const level: string =
        result.status === "ready" ? (result.result[0]?.level ?? "") : "";
      return <div>{`${result.status}:${level}`}</div>;
    }
    render(
      <SyncProvider client={sync}>
        <Probe />
      </SyncProvider>,
    );
    expect(screen.getByText("loading:")).toBeTruthy();
    expect(subscribe).toHaveBeenCalledWith({ maxRows: 5 });
    resource.publish({
      status: "ready",
      result: [{ level: "error" }] as unknown as CountRows,
    });
    expect(await screen.findByText("ready:error")).toBeTruthy();
  });

  it("keeps the last window it read while the next range loads", async () => {
    type Window = QueryWindow<{ readonly id: number }>;
    const resources = new Map<string, ResourceStub>();
    const resourceFor = (offset: number, count: number) => {
      const key = `${offset}:${count}`;
      let resource = resources.get(key);
      if (!resource) {
        resource = new ResourceStub();
        resources.set(key, resource);
      }
      return resource;
    };
    const query = {
      subscribeWindow: (offset: number, count: number) =>
        resourceFor(offset, count) as unknown as LiveQueryResource<Window>,
    };
    const publish = (offset: number, count: number, window: Window) =>
      act(() => {
        resourceFor(offset, count).publish({
          status: "ready",
          result: window as unknown as CountRows,
        });
      });
    function Probe({ offset }: { readonly offset: number }) {
      const window = useLiveWindow(query, offset, 2);
      return (
        <div>
          {window.status === "ready"
            ? `${window.stale ? "stale " : ""}${window.result.total}:${window.result.rows.map(({ id }) => id).join(",")}`
            : window.status}
        </div>
      );
    }
    const { rerender } = render(<Probe offset={0} />);
    expect(screen.getByText("loading")).toBeTruthy();
    await publish(0, 2, { offset: 0, total: 9, rows: [{ id: 0 }, { id: 1 }] });
    expect(screen.getByText("9:0,1")).toBeTruthy();
    rerender(<Probe offset={4} />);
    expect(screen.getByText("stale 9:0,1")).toBeTruthy();
    await publish(4, 2, { offset: 4, total: 9, rows: [{ id: 4 }, { id: 5 }] });
    expect(screen.getByText("9:4,5")).toBeTruthy();
    await publish(4, 2, { offset: 4, total: 10, rows: [{ id: 4 }, { id: 5 }] });
    expect(screen.getByText("10:4,5")).toBeTruthy();
  });

  it("uses a cached promise for initial Suspense and exposes stable status", async () => {
    const resource = new ResourceStub();
    const sync = client();
    const source = countQuery(resource).source;
    function Probe() {
      const query = useSuspenseLiveQuery(source);
      const state = useSyncStatus((status) => status.state);
      return <div>{`${state}:${query.result[0]?.count}`}</div>;
    }
    render(
      <SyncProvider client={sync}>
        <Suspense fallback={<div>pending</div>}>
          <Probe />
        </Suspense>
      </SyncProvider>,
    );
    expect(screen.getByText("pending")).toBeTruthy();
    resource.publish({ status: "ready", result: [{ count: 2 }] });
    expect(await screen.findByText("ready:2")).toBeTruthy();
  });

  it("returns the whole status without a selector, and re-renders on change", async () => {
    const sync = client();
    const seen: SyncStatus[] = [];
    function Probe() {
      const status = useSyncStatus();
      seen.push(status);
      return <div>{status.state}</div>;
    }
    render(
      <SyncProvider client={sync}>
        <Probe />
      </SyncProvider>,
    );
    expect(screen.getByText("ready")).toBeTruthy();
    const closed: SyncStatus = { state: "closed" };
    act(() => sync.setStatus(closed));
    expect(await screen.findByText("closed")).toBeTruthy();
    expect(seen.at(-1)).toBe(closed);
  });

  it("throws the exact published SyncError through Suspense", async () => {
    const resource = new ResourceStub();
    const sync = client();
    const expected = new SyncError(
      "NETWORK_ERROR",
      "query",
      "retry",
      "offline",
    );
    let caught: unknown;
    const source = countQuery(resource).source;
    function Probe() {
      useSuspenseLiveQuery(source);
      return <div>ready</div>;
    }

    render(
      <SyncProvider client={sync}>
        <Boundary fallback="failed" onError={(error) => (caught = error)}>
          <Suspense fallback={<div>pending</div>}>
            <Probe />
          </Suspense>
        </Boundary>
      </SyncProvider>,
    );
    expect(screen.getByText("pending")).toBeTruthy();
    resource.publish({ status: "error", error: expected });
    expect(await screen.findByText("failed")).toBeTruthy();
    expect(caught).toBe(expected);
  });

  it("publishes one ambiguous mutation's error through its status", async () => {
    const sync = client();
    const expected = new MutationSubmissionError(
      "mutation-react-error",
      "MUTATION_AMBIGUOUS",
      "ambiguous",
    );
    let statusError: unknown;
    function Probe() {
      const status = useMutationStatus(expected.mutationId);
      if (status?.state === "ambiguous") statusError = status.error;
      return <div>{status?.state ?? "unknown"}</div>;
    }
    render(
      <SyncProvider client={sync}>
        <Probe />
      </SyncProvider>,
    );
    expect(screen.getByText("unknown")).toBeTruthy();
    sync.setMutationStatus({
      mutationId: expected.mutationId,
      ...TASK_ONE,
      state: "ambiguous",
      error: expected,
    });
    expect(await screen.findByText("ambiguous")).toBeTruthy();
    expect(statusError).toBe(expected);
    const status = sync.mutationStatus(expected.mutationId);
    expect(status).toEqual({
      mutationId: expected.mutationId,
      ...TASK_ONE,
      state: "ambiguous",
      error: expected,
    });
    expect(Object.isFrozen(status)).toBe(true);
    expect(Object.isFrozen(expected)).toBe(true);
  });

  it("inherits mutation-ID admission from the client without a hook validator", async () => {
    const sync = client();
    const expected = new TypeError(
      "mutation ID must be a valid bounded identifier",
    );
    const mutationStatus = vi.fn(() => {
      throw expected;
    });
    const boundaryClient = new Proxy(sync, {
      get(target, property, receiver) {
        if (property === "mutationStatus") return mutationStatus;
        return Reflect.get(target, property, receiver);
      },
    });
    let caught: unknown;
    function Probe() {
      useMutationStatus("");
      return <div>ready</div>;
    }

    render(
      <SyncProvider client={boundaryClient}>
        <Boundary
          fallback="invalid mutation ID"
          onError={(error) => (caught = error)}
        >
          <Probe />
        </Boundary>
      </SyncProvider>,
    );

    expect(await screen.findByText("invalid mutation ID")).toBeTruthy();
    expect(caught).toBe(expected);
    expect(mutationStatus).toHaveBeenCalledWith("");
  });

  it("reads no mutation for an undefined id, and follows the id once it is set", async () => {
    const sync = client();
    const subscribeMutation = vi.spyOn(sync, "subscribeMutation");
    const mutationStatus = vi.spyOn(sync, "mutationStatus");
    function Probe({ id }: { readonly id: string | undefined }) {
      const status = useMutationStatus(id);
      return <div>{status?.state ?? "none"}</div>;
    }
    const view = render(
      <SyncProvider client={sync}>
        <Probe id={undefined} />
      </SyncProvider>,
    );
    expect(screen.getByText("none")).toBeTruthy();
    expect(subscribeMutation).not.toHaveBeenCalled();
    expect(mutationStatus).not.toHaveBeenCalled();

    view.rerender(
      <SyncProvider client={sync}>
        <Probe id="mutation-later" />
      </SyncProvider>,
    );
    expect(subscribeMutation).toHaveBeenCalledWith(
      "mutation-later",
      expect.any(Function),
    );
    act(() =>
      sync.setMutationStatus({
        mutationId: "mutation-later",
        ...TASK_ONE,
        state: "submitting",
      }),
    );
    expect(await screen.findByText("submitting")).toBeTruthy();
  });

  it("observes canonical mutation acknowledgement independently of submission", async () => {
    const sync = client();
    function Probe() {
      const status = useMutationStatus("mutation-react");
      return <div>{status?.state ?? "unknown"}</div>;
    }
    render(
      <SyncProvider client={sync}>
        <Probe />
      </SyncProvider>,
    );
    expect(screen.getByText("unknown")).toBeTruthy();
    const result: MutationResult = Object.freeze({
      mutationId: "mutation-react",
      state: "already_canonical",
      message: "canonical",
    });
    sync.setMutationStatus({
      mutationId: "mutation-react",
      ...TASK_ONE,
      state: "canonical",
      result,
    });
    expect(await screen.findByText("canonical")).toBeTruthy();
    expect(sync.mutationStatus("mutation-react")).toMatchObject({ result });
  });
});

describe("usePendingWrite", () => {
  const accepted = (mutationId: string) =>
    Object.freeze({
      mutationId,
      state: "awaiting_canonical_echo" as const,
      message: "accepted",
    });

  function Pending({ id }: { readonly id: bigint }) {
    const write = usePendingWrite("tasks", { id });
    return <div data-testid={`task-${id}`}>{write?.state ?? "none"}</div>;
  }

  function pendingOf(id: bigint): string | null {
    return screen.getByTestId(`task-${id}`).textContent;
  }

  it("follows a row's write from submitting until it is canonical", () => {
    const sync = client();
    render(
      <SyncProvider client={sync}>
        <Pending id={1n} />
        <Pending id={2n} />
      </SyncProvider>,
    );
    expect(pendingOf(1n)).toBe("none");
    act(() =>
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "submitting",
      }),
    );
    expect(pendingOf(1n)).toBe("submitting");
    expect(pendingOf(2n)).toBe("none");
    act(() =>
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "awaiting_canonical_echo",
        result: accepted("m1"),
      }),
    );
    expect(pendingOf(1n)).toBe("awaiting_canonical_echo");
    act(() =>
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "canonical",
        result: accepted("m1"),
      }),
    );
    expect(pendingOf(1n)).toBe("none");
  });

  it("shows the latest write to a row and ignores what an earlier one does next", () => {
    const sync = client();
    render(
      <SyncProvider client={sync}>
        <Pending id={1n} />
      </SyncProvider>,
    );
    act(() => {
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "submitting",
      });
      sync.setMutationStatus({
        mutationId: "m2",
        ...TASK_ONE,
        state: "submitting",
      });
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "canonical",
        result: accepted("m1"),
      });
    });
    expect(pendingOf(1n)).toBe("submitting");
    act(() =>
      sync.setMutationStatus({
        mutationId: "m2",
        ...TASK_ONE,
        state: "awaiting_canonical_echo",
        result: accepted("m2"),
      }),
    );
    expect(pendingOf(1n)).toBe("awaiting_canonical_echo");
  });

  it("keeps a refused write until the next write to that row", () => {
    const sync = client();
    render(
      <SyncProvider client={sync}>
        <Pending id={1n} />
      </SyncProvider>,
    );
    act(() => {
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "submitting",
      });
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "rejected",
        result: { mutationId: "m1", state: "rejected", message: "no" },
      });
    });
    expect(pendingOf(1n)).toBe("rejected");
    act(() => {
      sync.setMutationStatus({
        mutationId: "m2",
        ...TASK_ONE,
        state: "submitting",
      });
      sync.setMutationStatus({
        mutationId: "m2",
        ...TASK_ONE,
        state: "conflict",
        result: {
          mutationId: "m2",
          state: "conflict",
          message: "moved",
          conflict: {
            kind: "row-missing",
            applicationPayload: new Uint8Array(),
          },
        },
      });
    });
    expect(pendingOf(1n)).toBe("conflict");
    act(() =>
      sync.setMutationStatus({
        mutationId: "m3",
        ...TASK_ONE,
        state: "submitting",
      }),
    );
    expect(pendingOf(1n)).toBe("submitting");
  });

  it("keeps a write the server did not settle until its retry or echo", () => {
    const sync = client();
    render(
      <SyncProvider client={sync}>
        <Pending id={1n} />
      </SyncProvider>,
    );
    act(() => {
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "submitting",
      });
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "retryable",
        result: {
          mutationId: "m1",
          state: "retryable",
          message: "later",
          retryAfterMs: 250n,
        },
      });
    });
    expect(pendingOf(1n)).toBe("retryable");
    act(() => {
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "submitting",
      });
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "ambiguous",
        error: new MutationSubmissionError(
          "m1",
          "MUTATION_AMBIGUOUS",
          "lost response",
        ),
      });
    });
    expect(pendingOf(1n)).toBe("ambiguous");
    act(() =>
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "canonical",
        result: {
          mutationId: "m1",
          state: "already_canonical",
          message: "echo",
        },
      }),
    );
    expect(pendingOf(1n)).toBe("none");
  });

  it("matches a key by its columns and their values, not their order", () => {
    const sync = client();
    function Composite() {
      const write = usePendingWrite("memberships", {
        user_id: "u-1",
        project_id: 7n,
      });
      return <div data-testid="membership">{write?.state ?? "none"}</div>;
    }
    render(
      <SyncProvider client={sync}>
        <Composite />
      </SyncProvider>,
    );
    act(() => {
      sync.setMutationStatus({
        mutationId: "text-key",
        collection: "memberships",
        key: { project_id: "7", user_id: "u-1" },
        state: "submitting",
      });
      sync.setMutationStatus({
        mutationId: "other-collection",
        collection: "projects",
        key: { project_id: 7n, user_id: "u-1" },
        state: "submitting",
      });
    });
    expect(screen.getByTestId("membership").textContent).toBe("none");
    act(() =>
      sync.setMutationStatus({
        mutationId: "same-row",
        collection: "memberships",
        key: { project_id: 7n, user_id: "u-1" },
        state: "submitting",
      }),
    );
    expect(screen.getByTestId("membership").textContent).toBe("submitting");
  });

  it("records writes from the provider's first render, before any hook reads them", () => {
    const sync = client();
    const view = render(<SyncProvider client={sync}>{null}</SyncProvider>);
    act(() =>
      sync.setMutationStatus({
        mutationId: "m1",
        ...TASK_ONE,
        state: "submitting",
      }),
    );
    view.rerender(
      <SyncProvider client={sync}>
        <Pending id={1n} />
      </SyncProvider>,
    );
    expect(pendingOf(1n)).toBe("submitting");
  });
});

import { SyncError } from "./error.js";
import { assertMutationId } from "./mutation-id.js";

/** The server's answer to a mutation, as `MutationResult.state`. */
export type MutationState =
  | "awaiting_canonical_echo"
  | "already_canonical"
  | "rejected"
  | "conflict"
  | "retryable";

/** What the server answered for one mutation, by `state`. */
export type MutationResult =
  | {
      readonly mutationId: string;
      readonly state: "awaiting_canonical_echo";
      readonly message: string;
    }
  | {
      readonly mutationId: string;
      readonly state: "already_canonical";
      readonly message: string;
    }
  | {
      readonly mutationId: string;
      readonly state: "rejected";
      readonly message: string;
    }
  | {
      readonly mutationId: string;
      readonly state: "conflict";
      readonly message: string;
      readonly conflict: {
        readonly kind: string;
        readonly applicationPayload: Uint8Array;
      };
    }
  | {
      readonly mutationId: string;
      readonly state: "retryable";
      readonly message: string;
      readonly retryAfterMs: bigint;
    };

type MutationResultFor<State extends MutationState> = Extract<
  MutationResult,
  { readonly state: State }
>;

/**
 * What a write's call resolves with: the server committed the write, or had
 * committed it before under the same id. The page shows it optimistically
 * until PostgreSQL's change reaches the client.
 */
export type MutationAccepted = MutationResultFor<
  "awaiting_canonical_echo" | "already_canonical"
>;

/** The rule a write broke, as the server named it, on `MUTATION_CONFLICT`. */
export type MutationConflict = Extract<
  MutationResult,
  { readonly state: "conflict" }
>["conflict"];

/**
 * Where one mutation stands, as `SyncClient.mutationStatus` reports it.
 * `collection` and `key` name the row it writes: the key columns as the
 * facade took them, with a uuid as lowercase text.
 */
export type MutationStatus =
  | {
      readonly mutationId: string;
      readonly collection: string;
      readonly key: Readonly<Record<string, unknown>>;
      readonly state: "submitting";
    }
  | {
      readonly mutationId: string;
      readonly collection: string;
      readonly key: Readonly<Record<string, unknown>>;
      readonly state: "awaiting_canonical_echo";
      readonly result: MutationResultFor<
        "awaiting_canonical_echo" | "already_canonical"
      >;
    }
  | {
      readonly mutationId: string;
      readonly collection: string;
      readonly key: Readonly<Record<string, unknown>>;
      readonly state: "canonical";
      readonly result: MutationResultFor<
        "awaiting_canonical_echo" | "already_canonical"
      >;
    }
  | {
      readonly mutationId: string;
      readonly collection: string;
      readonly key: Readonly<Record<string, unknown>>;
      readonly state: "rejected";
      readonly result: MutationResultFor<"rejected">;
    }
  | {
      readonly mutationId: string;
      readonly collection: string;
      readonly key: Readonly<Record<string, unknown>>;
      readonly state: "conflict";
      readonly result: MutationResultFor<"conflict">;
    }
  | {
      readonly mutationId: string;
      readonly collection: string;
      readonly key: Readonly<Record<string, unknown>>;
      readonly state: "retryable";
      readonly result: MutationResultFor<"retryable">;
    }
  | {
      readonly mutationId: string;
      readonly collection: string;
      readonly key: Readonly<Record<string, unknown>>;
      readonly state: "ambiguous";
      readonly error: MutationSubmissionError;
    };

/** Freezes a result in place, with a conflict's `conflict`, and returns it. */
export function freezeMutationResult<Result extends MutationResult>(
  result: Result,
): Result {
  if (result.state === "conflict") Object.freeze(result.conflict);
  return Object.freeze(result);
}

/** Freezes a status in place, and its result as `freezeMutationResult` does. */
export function freezeMutationStatus<Status extends MutationStatus>(
  status: Status,
): Status {
  if ("result" in status && status.result) freezeMutationResult(status.result);
  return Object.freeze(status);
}

/** The codes a `MutationSubmissionError` carries. */
export type MutationSubmissionErrorCode = MutationSubmissionError["code"];

/**
 * A `SyncError` from one mutation, carrying its `mutationId`, and on
 * `MUTATION_CONFLICT` the `conflict` the server named.
 */
export class MutationSubmissionError extends SyncError {
  declare readonly code:
    | "AUTH_FAILED"
    | "MUTATION_AMBIGUOUS"
    | "MUTATION_CONFLICT"
    | "MUTATION_REJECTED";
  readonly mutationId: string;
  /**
   * The rule a `MUTATION_CONFLICT` broke, as the server's command named it;
   * `undefined` for every other code.
   */
  declare readonly conflict: MutationConflict | undefined;

  /**
   * The recovery follows from `code`, unless `options.recovery` sets it to
   * `none`: an ambiguous mutation whose mount was torn down has nothing left
   * for `retryMutation` to resend.
   */
  constructor(
    mutationId: string,
    code: MutationSubmissionErrorCode,
    message: string,
    options?: ErrorOptions & {
      readonly recovery?: "none";
      readonly conflict?: MutationConflict;
    },
  ) {
    assertMutationId(mutationId);
    const classification = mutationSubmissionClassification(code);
    super(
      code,
      classification.scope,
      options?.recovery ?? classification.recovery,
      message,
      options,
    );
    this.name = "MutationSubmissionError";
    this.mutationId = mutationId;
    if (options?.conflict)
      this.conflict = Object.freeze({ ...options.conflict });
    Object.freeze(this);
  }
}

function mutationSubmissionClassification(code: SyncError["code"]): {
  readonly scope: "auth" | "mutation";
  readonly recovery: "none" | "retry" | "reset";
} {
  switch (code) {
    case "AUTH_FAILED":
      return { scope: "auth", recovery: "reset" };
    case "MUTATION_AMBIGUOUS":
      return { scope: "mutation", recovery: "retry" };
    case "MUTATION_CONFLICT":
    case "MUTATION_REJECTED":
      return { scope: "mutation", recovery: "none" };
    default:
      throw new TypeError(`invalid mutation submission error code ${code}`);
  }
}

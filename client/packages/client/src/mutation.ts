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

/** A `SyncError` from one mutation, carrying its `mutationId`. */
export class MutationSubmissionError extends SyncError {
  declare readonly code:
    "AUTH_FAILED" | "MUTATION_AMBIGUOUS" | "MUTATION_REJECTED";
  readonly mutationId: string;

  /**
   * The recovery follows from `code`, unless `options.recovery` sets it to
   * `none`: an ambiguous mutation whose mount was torn down has nothing left
   * for `retryMutation` to resend.
   */
  constructor(
    mutationId: string,
    code: "AUTH_FAILED" | "MUTATION_AMBIGUOUS" | "MUTATION_REJECTED",
    message: string,
    options?: ErrorOptions & { readonly recovery?: "none" },
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
    case "MUTATION_REJECTED":
      return { scope: "mutation", recovery: "none" };
    default:
      throw new TypeError(`invalid mutation submission error code ${code}`);
  }
}

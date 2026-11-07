import { describe, expect, it } from "vitest";

import {
  freezeMutationResult,
  freezeMutationStatus,
  MutationSubmissionError,
  type MutationStatus,
} from "./mutation.js";

describe("public mutation snapshots", () => {
  it("classifies every mutation submission code without transport metadata", () => {
    const cases = [
      ["AUTH_FAILED", "auth", "reset"],
      ["MUTATION_AMBIGUOUS", "mutation", "retry"],
      ["MUTATION_REJECTED", "mutation", "none"],
    ] as const;
    for (const [code, scope, recovery] of cases) {
      const cause = new Error(`cause:${code}`);
      const error = new MutationSubmissionError(
        `mutation-${code.toLowerCase()}`,
        code,
        `message:${code}`,
        { cause },
      );
      expect(error).toMatchObject({ code, scope, recovery });
      expect(error.cause).toBe(cause);
      expect(Object.isFrozen(error)).toBe(true);
      expect(Object.keys(error).sort()).toEqual([
        "code",
        "mutationId",
        "name",
        "recovery",
        "scope",
      ]);
    }
  });

  it("takes recovery none for an ambiguous mutation with nothing left to resend", () => {
    const cause = new Error("cause");
    const error = new MutationSubmissionError(
      "mutation-abandoned",
      "MUTATION_AMBIGUOUS",
      "abandoned",
      { cause, recovery: "none" },
    );
    expect({ ...error }).toEqual({
      name: "MutationSubmissionError",
      code: "MUTATION_AMBIGUOUS",
      scope: "mutation",
      recovery: "none",
      mutationId: "mutation-abandoned",
    });
    expect(error.cause).toBe(cause);
  });

  it("rejects every invalid mutation ID before constructing a public error", () => {
    for (const mutationId of [
      "",
      "m".repeat(513),
      "contains space",
      "contains?query",
      "contains\u0000control",
      "-invalid-leading-character",
    ]) {
      expect(
        () =>
          new MutationSubmissionError(
            mutationId,
            "MUTATION_REJECTED",
            "rejected",
          ),
      ).toThrowError(
        new TypeError("mutation ID must be a valid bounded identifier"),
      );
    }

    const exact = "m".repeat(512);
    expect(
      new MutationSubmissionError(exact, "MUTATION_REJECTED", "rejected")
        .mutationId,
    ).toBe(exact);
  });

  it("freezes exact result and status containers without freezing payload bytes", () => {
    const payload = new Uint8Array([1, 2, 3]);
    const result = freezeMutationResult({
      mutationId: "mutation-conflict",
      state: "conflict",
      message: "conflict",
      conflict: { kind: "version", applicationPayload: payload },
    });
    const status = freezeMutationStatus({
      mutationId: result.mutationId,
      collection: "issues",
      key: { id: "issue-1" },
      state: "conflict",
      result,
    });

    expect(Object.keys(result).sort()).toEqual([
      "conflict",
      "message",
      "mutationId",
      "state",
    ]);
    expect(Object.isFrozen(result)).toBe(true);
    expect(Object.isFrozen(result.conflict)).toBe(true);
    expect(Object.isFrozen(payload)).toBe(false);
    payload[0] = 9;
    expect(result.conflict?.applicationPayload[0]).toBe(9);
    expect(Object.isFrozen(status)).toBe(true);
    expect(status.result).toBe(result);
  });

  it("freezes every exact mutation status variant without cloning results or errors", () => {
    const result = freezeMutationResult({
      mutationId: "mutation-status",
      state: "awaiting_canonical_echo",
      message: "accepted",
    });
    const canonicalResult = freezeMutationResult({
      mutationId: result.mutationId,
      state: "already_canonical",
      message: "canonical",
    });
    const error = new MutationSubmissionError(
      result.mutationId,
      "MUTATION_AMBIGUOUS",
      "ambiguous",
    );
    const row = { collection: "issues", key: { id: "issue-1" } };
    const statuses: MutationStatus[] = [
      {
        mutationId: result.mutationId,
        ...row,
        state: "submitting",
      },
      {
        mutationId: result.mutationId,
        ...row,
        state: "awaiting_canonical_echo",
        result,
      },
      {
        mutationId: result.mutationId,
        ...row,
        state: "canonical",
        result: canonicalResult,
      },
      {
        mutationId: result.mutationId,
        ...row,
        state: "retryable",
        result: freezeMutationResult({
          ...result,
          state: "retryable",
          retryAfterMs: 1n,
        }),
      },
      {
        mutationId: result.mutationId,
        ...row,
        state: "ambiguous",
        error,
      },
    ];

    for (const candidate of statuses) {
      const frozen = freezeMutationStatus(candidate);
      expect(frozen).toBe(candidate);
      expect(Object.isFrozen(frozen)).toBe(true);
      if ("result" in frozen && frozen.result) {
        expect(Object.isFrozen(frozen.result)).toBe(true);
      }
      if (frozen.state === "ambiguous") expect(frozen.error).toBe(error);
    }
    expect(Object.keys(statuses[0] ?? {}).sort()).toEqual([
      "collection",
      "key",
      "mutationId",
      "state",
    ]);
    expect(Object.keys(statuses[1] ?? {}).sort()).toEqual([
      "collection",
      "key",
      "mutationId",
      "result",
      "state",
    ]);
    expect(Object.keys(statuses[4] ?? {}).sort()).toEqual([
      "collection",
      "error",
      "key",
      "mutationId",
      "state",
    ]);
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(MutationSubmissionError);
    expect(Object.isFrozen(error)).toBe(true);
    expect(Object.keys(error).sort()).toEqual([
      "code",
      "mutationId",
      "name",
      "recovery",
      "scope",
    ]);
  });
});

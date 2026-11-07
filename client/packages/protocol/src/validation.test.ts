import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import {
  BatchEndSchema,
  BatchStartSchema,
  CellValueSchema,
  ChangeRequestSchema,
  ChangeStreamFrameSchema,
  ColumnValueSchema,
  CursorSchema as ChangeCursorSchema,
  DeleteSchema,
  FullRowSchema,
  HeartbeatSchema,
  NullValue,
  OperationKind,
  OperationSchema,
  PrimaryKeySchema,
  RowPatchSchema,
  SourceGenerationSchema,
  StreamErrorCode,
  StreamErrorSchema,
  TableSubscriptionSchema,
  TransactionFragmentSchema,
  TruncateSchema,
  type BatchEnd,
  type CellValue,
  type ChangeStreamFrame,
  type Operation,
} from "./gen/change_stream_pb.js";
import {
  AffectedRowSchema,
  ConflictSchema,
  CursorSchema as MutationCursorSchema,
  ExpectedVersionSchema,
  MutationBatchRequestSchema,
  MutationBatchResponseSchema,
  MutationEnvelopeSchema,
  MutationFootprintSchema,
  MutationOutcomeCode,
  MutationOutcomeSchema,
  type MutationBatchRequest,
  type MutationOutcome,
} from "./gen/mutation_pb.js";
import { encodePrimaryKey } from "./primary-key.js";
import {
  validateChangeRequest,
  validateChangeStreamFrame,
  validateMutationBatchRequest,
  validateMutationBatchResponse,
} from "./validation.js";

const bytes = (...values: number[]) => Uint8Array.from(values);
const changeCursor = (value = 1) =>
  create(ChangeCursorSchema, { token: bytes(value) });
const mutationCursor = () => create(MutationCursorSchema, { token: bytes(1) });
const primaryKey = () =>
  encodePrimaryKey(
    create(PrimaryKeySchema, {
      codecVersion: 1,
      parts: [
        create(ColumnValueSchema, {
          columnId: 1,
          value: create(CellValueSchema, {
            kind: { case: "int64Value", value: 1n },
          }),
        }),
      ],
    }),
  );

function validGeneration() {
  return create(SourceGenerationSchema, {
    sourceEpoch: "epoch-1",
    publicationGeneration: 1n,
    schemaGeneration: 1n,
    authorizationScopeHash: new Uint8Array(32),
  });
}

function validOperation(kind: OperationKind): Operation {
  const base = { tableId: "public.tasks", kind };
  switch (kind) {
    case OperationKind.INSERT_FULL_ROW:
      return create(OperationSchema, {
        ...base,
        primaryKey: primaryKey(),
        payload: {
          case: "insert",
          value: create(FullRowSchema, {
            columns: [
              create(ColumnValueSchema, {
                columnId: 1,
                value: create(CellValueSchema, {
                  kind: { case: "textValue", value: "task" },
                }),
              }),
            ],
          }),
        },
      });
    case OperationKind.UPDATE_PATCH:
      return create(OperationSchema, {
        ...base,
        primaryKey: primaryKey(),
        payload: {
          case: "update",
          value: create(RowPatchSchema, {
            changedColumns: [
              create(ColumnValueSchema, {
                columnId: 2,
                value: create(CellValueSchema, {
                  kind: { case: "boolValue", value: true },
                }),
              }),
            ],
          }),
        },
      });
    case OperationKind.DELETE_BY_KEY:
      return create(OperationSchema, {
        ...base,
        primaryKey: primaryKey(),
        payload: { case: "delete", value: create(DeleteSchema) },
      });
    case OperationKind.TRUNCATE_AUTHORIZED_TABLE:
      return create(OperationSchema, {
        ...base,
        payload: {
          case: "truncate",
          value: create(TruncateSchema, { reason: "authorized truncate" }),
        },
      });
    default:
      throw new Error(`unsupported test operation kind ${kind}`);
  }
}

function frame(body: ChangeStreamFrame["body"]): ChangeStreamFrame {
  return create(ChangeStreamFrameSchema, {
    protocolVersion: 1,
    bodySha256: new Uint8Array(32),
    body,
  });
}

function validBatchEnd(): BatchEnd {
  return create(BatchEndSchema, {
    throughCursorInclusive: changeCursor(2),
    acknowledgedMutationIds: ["mutation-1"],
    frameCount: 1n,
    framesSha256: new Uint8Array(32),
  });
}

function validFragment(
  operations = [validOperation(OperationKind.DELETE_BY_KEY)],
) {
  return create(TransactionFragmentSchema, {
    transactionId: bytes(1),
    transactionCursor: changeCursor(2),
    firstFragment: true,
    finalFragment: true,
    postgresXid: 1n,
    beginLsn: "0/1",
    commitLsn: "0/2",
    commitTimeUnixMicros: 1n,
    operations,
  });
}

function validChangeRequest() {
  return create(ChangeRequestSchema, {
    protocolVersion: 1,
    planId: "plan-1",
    fromCursorExclusive: changeCursor(),
    tables: [create(TableSubscriptionSchema, { tableId: "public.tasks" })],
    authorizationScopeToken: bytes(2),
    maxBatchBytes: 1_024n,
    maxTransactions: 10,
    clientInstanceId: "client-1",
    deviceId: "device-1",
  });
}

describe("change semantic validation", () => {
  it("accepts an optional live fence and the exact server request bounds", () => {
    const request = validChangeRequest();
    expect(request.throughCursorInclusive).toBeUndefined();
    expect(() => validateChangeRequest(request)).not.toThrow();

    request.throughCursorInclusive = changeCursor(2);
    request.maxBatchBytes = 16n * 1_024n * 1_024n;
    request.maxTransactions = 10_000;
    request.longPollTimeoutMs = 120_000;
    expect(() => validateChangeRequest(request)).not.toThrow();
  });

  it("admits a stream only with a wait", () => {
    const live = validChangeRequest();
    live.longPollTimeoutMs = 1_000;
    live.stream = true;
    expect(() => validateChangeRequest(live)).not.toThrow();

    const fenced = validChangeRequest();
    fenced.longPollTimeoutMs = 1_000;
    fenced.throughCursorInclusive = changeCursor(2);
    fenced.stream = true;
    expect(() => validateChangeRequest(fenced)).not.toThrow();

    const immediate = validChangeRequest();
    immediate.stream = true;
    expect(immediate.longPollTimeoutMs).toBe(0);
    expect(() => validateChangeRequest(immediate)).toThrow(
      /changeRequest\.stream/u,
    );
  });

  it("accepts every change frame body with its required fields", () => {
    const bodies: ChangeStreamFrame["body"][] = [
      {
        case: "batchStart",
        value: create(BatchStartSchema, {
          generation: validGeneration(),
          fromCursorExclusive: changeCursor(),
          tableIds: ["public.tasks"],
          planId: "plan-1",
          tableStarts: [
            create(TableSubscriptionSchema, { tableId: "public.tasks" }),
          ],
          streamId: new Uint8Array(16).fill(1),
        }),
      },
      { case: "transactionFragment", value: validFragment() },
      { case: "batchEnd", value: validBatchEnd() },
      {
        case: "heartbeat",
        value: create(HeartbeatSchema, { durableHead: changeCursor(2) }),
      },
      {
        case: "error",
        value: create(StreamErrorSchema, {
          code: StreamErrorCode.REQUEST_INVALID,
          message: "invalid request",
        }),
      },
    ];
    for (const body of bodies) {
      expect(() => validateChangeStreamFrame(frame(body))).not.toThrow();
    }
  });

  it("accepts all four operation categories", () => {
    for (const kind of [
      OperationKind.INSERT_FULL_ROW,
      OperationKind.UPDATE_PATCH,
      OperationKind.DELETE_BY_KEY,
      OperationKind.TRUNCATE_AUTHORIZED_TABLE,
    ]) {
      expect(() =>
        validateChangeStreamFrame(
          frame({
            case: "transactionFragment",
            value: validFragment([validOperation(kind)]),
          }),
        ),
      ).not.toThrow();
    }
  });

  it("accepts all 14 CellValue kinds in ordinary rows", () => {
    const kinds: CellValue["kind"][] = [
      { case: "nullValue", value: NullValue.NULL_VALUE },
      { case: "int64Value", value: -1n },
      { case: "uint64Value", value: 1n },
      { case: "float64Value", value: 1.5 },
      { case: "textValue", value: "" },
      { case: "blobValue", value: new Uint8Array() },
      { case: "boolValue", value: true },
      { case: "decimalText", value: "1.25" },
      { case: "timestampUnixMicros", value: 1n },
      { case: "uuid16", value: new Uint8Array(16) },
      { case: "jsonText", value: "{}" },
      { case: "dateIso8601", value: "2026-07-14" },
      { case: "timeIso8601", value: "12:34:56" },
      { case: "intervalIso8601", value: "PT1S" },
    ];
    const columns = kinds.map((kind, index) =>
      create(ColumnValueSchema, {
        columnId: index + 1,
        value: create(CellValueSchema, { kind }),
      }),
    );
    const operation = validOperation(OperationKind.INSERT_FULL_ROW);
    operation.payload = {
      case: "insert",
      value: create(FullRowSchema, { columns }),
    };
    expect(() =>
      validateChangeStreamFrame(
        frame({
          case: "transactionFragment",
          value: validFragment([operation]),
        }),
      ),
    ).not.toThrow();
  });

  it("rejects invalid enums, payloads, cells, and required fields", () => {
    const operation = validOperation(OperationKind.INSERT_FULL_ROW);
    operation.payload = { case: "delete", value: create(DeleteSchema) };
    expect(() =>
      validateChangeStreamFrame(
        frame({
          case: "transactionFragment",
          value: validFragment([operation]),
        }),
      ),
    ).toThrow(/must be insert/u);

    operation.kind = 99 as OperationKind;
    expect(() =>
      validateChangeStreamFrame(
        frame({
          case: "transactionFragment",
          value: validFragment([operation]),
        }),
      ),
    ).toThrow(/unknown or unspecified/u);

    const streamError = create(StreamErrorSchema, {
      code: 99 as StreamErrorCode,
      message: "bad enum",
    });
    expect(() =>
      validateChangeStreamFrame(frame({ case: "error", value: streamError })),
    ).toThrow(/unknown or unspecified/u);

    const invalidCell = create(CellValueSchema, {
      kind: { case: "nullValue", value: 99 as NullValue },
    });
    const insert = validOperation(OperationKind.INSERT_FULL_ROW);
    insert.payload = {
      case: "insert",
      value: create(FullRowSchema, {
        columns: [
          create(ColumnValueSchema, { columnId: 1, value: invalidCell }),
        ],
      }),
    };
    expect(() =>
      validateChangeStreamFrame(
        frame({
          case: "transactionFragment",
          value: validFragment([insert]),
        }),
      ),
    ).toThrow(/unknown enum/u);
  });

  it("rejects duplicate table, column, and acknowledgement identities", () => {
    const request = validChangeRequest();
    const table = request.tables[0];
    if (table === undefined) throw new Error("fixture table missing");
    request.tables.push(table);
    expect(() => validateChangeRequest(request)).toThrow(/must be unique/u);

    const column = create(ColumnValueSchema, {
      columnId: 1,
      value: create(CellValueSchema, {
        kind: { case: "textValue", value: "value" },
      }),
    });
    const insert = validOperation(OperationKind.INSERT_FULL_ROW);
    insert.payload = {
      case: "insert",
      value: create(FullRowSchema, { columns: [column, column] }),
    };
    expect(() =>
      validateChangeStreamFrame(
        frame({
          case: "transactionFragment",
          value: validFragment([insert]),
        }),
      ),
    ).toThrow(/must be unique in a row/u);

    expect(() =>
      validateChangeStreamFrame(
        frame({ case: "transactionFragment", value: validFragment([]) }),
      ),
    ).toThrow(/transactionFragment\.operations: must not be empty/u);

    for (const [mutate, message] of [
      [
        (end: BatchEnd) => {
          end.acknowledgedMutationIds = ["mutation-1", "mutation-1"];
        },
        /acknowledgedMutationIds\[1\]: must be unique/u,
      ],
      [
        (end: BatchEnd) => {
          end.frameCount = 0n;
        },
        /batchEnd\.frameCount/u,
      ],
      [
        (end: BatchEnd) => {
          end.framesSha256 = new Uint8Array(31);
        },
        /batchEnd\.framesSha256/u,
      ],
    ] as const) {
      const end = validBatchEnd();
      mutate(end);
      expect(() =>
        validateChangeStreamFrame(frame({ case: "batchEnd", value: end })),
      ).toThrow(message);
    }
  });

  it("rejects request and envelope bounds outside the Rust API limits", () => {
    const cases: [
      string,
      (request: ReturnType<typeof validChangeRequest>) => void,
    ][] = [
      [
        "authorizationScopeToken",
        (request) => {
          request.authorizationScopeToken = new Uint8Array(8 * 1_024 + 1);
        },
      ],
      [
        "maxBatchBytes",
        (request) => {
          request.maxBatchBytes = 1_023n;
        },
      ],
      [
        "maxTransactions",
        (request) => {
          request.maxTransactions = 10_001;
        },
      ],
      [
        "longPollTimeoutMs",
        (request) => {
          request.longPollTimeoutMs = 999;
        },
      ],
      [
        "clientInstanceId",
        (request) => {
          request.clientInstanceId = "client\ninvalid";
        },
      ],
    ];
    for (const [path, mutate] of cases) {
      const request = validChangeRequest();
      mutate(request);
      expect(() => validateChangeRequest(request), path).toThrow(
        new RegExp(path, "u"),
      );
    }

    const oversizedCursor = validChangeRequest();
    oversizedCursor.fromCursorExclusive = create(ChangeCursorSchema, {
      token: new Uint8Array(2_049),
    });
    expect(() => validateChangeRequest(oversizedCursor)).toThrow(
      /at most 2048/u,
    );
  });

  it("requires a bounded batch stream ID and a 32-byte body digest", () => {
    const start = create(BatchStartSchema, {
      generation: validGeneration(),
      fromCursorExclusive: changeCursor(),
      tableIds: ["public.tasks"],
      planId: "plan-1",
      tableStarts: [
        create(TableSubscriptionSchema, { tableId: "public.tasks" }),
      ],
    });
    expect(() =>
      validateChangeStreamFrame(frame({ case: "batchStart", value: start })),
    ).toThrow(/batchStart\.streamId: must not be empty/u);
    start.streamId = new Uint8Array(65);
    expect(() =>
      validateChangeStreamFrame(frame({ case: "batchStart", value: start })),
    ).toThrow(/at most 64 bytes/u);

    const value = frame({
      case: "heartbeat",
      value: create(HeartbeatSchema, { durableHead: changeCursor() }),
    });
    value.bodySha256 = new Uint8Array(31);
    expect(() => validateChangeStreamFrame(value)).toThrow(/exactly 32 bytes/u);
  });
});

function validMutationRequest(): MutationBatchRequest {
  return create(MutationBatchRequestSchema, {
    protocolVersion: 1,
    clientInstanceId: "client-1",
    deviceId: "device-1",
    authorizationScopeToken: bytes(1),
    mutations: [validMutationEnvelope("mutation-1")],
  });
}

function validMutationEnvelope(mutationId: string) {
  return create(MutationEnvelopeSchema, {
    mutationId,
    commandName: "tasks.rename",
    commandVersion: 1,
    canonicalInput: bytes(123),
    baseCursor: mutationCursor(),
    createdAtUnixMicros: 1n,
    declaredFootprint: create(MutationFootprintSchema, {
      tableIds: ["public.tasks"],
      rows: [
        create(AffectedRowSchema, {
          tableId: "public.tasks",
          primaryKey: primaryKey(),
        }),
      ],
    }),
  });
}

function firstMutation(request: MutationBatchRequest) {
  const mutation = request.mutations[0];
  if (mutation === undefined) throw new Error("fixture mutation missing");
  return mutation;
}

function requiredFootprint(mutation: ReturnType<typeof firstMutation>) {
  const footprint = mutation.declaredFootprint;
  if (footprint === undefined) throw new Error("fixture footprint missing");
  return footprint;
}

function firstFootprintRow(footprint: ReturnType<typeof requiredFootprint>) {
  const row = footprint.rows[0];
  if (row === undefined) throw new Error("fixture footprint row missing");
  return row;
}

function outcome(
  mutationId: string,
  code: MutationOutcomeCode,
): MutationOutcome {
  const accepted = [
    MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO,
    MutationOutcomeCode.ALREADY_ACCEPTED_AWAITING_CANONICAL_ECHO,
    MutationOutcomeCode.ALREADY_CANONICAL,
  ].includes(code);
  return create(MutationOutcomeSchema, {
    mutationId,
    code,
    message: `outcome ${code}`,
    serverTransactionId: accepted ? new Uint8Array(16) : new Uint8Array(),
    ...(code === MutationOutcomeCode.ALREADY_CANONICAL
      ? { observedCanonicalCursor: mutationCursor() }
      : {}),
    ...(code === MutationOutcomeCode.CONFLICT
      ? {
          conflict: create(ConflictSchema, {
            kind: "version",
            applicationPayload: bytes(1),
          }),
        }
      : {}),
    ...(code === MutationOutcomeCode.RETRYABLE_FAILURE
      ? { retryAfterMs: 1n }
      : {}),
  });
}

describe("mutation semantic validation", () => {
  it("accepts a request with every server-required envelope field", () => {
    expect(() =>
      validateMutationBatchRequest(validMutationRequest()),
    ).not.toThrow();
  });

  it("accepts all eight mutation outcome categories", () => {
    const codes = [
      MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO,
      MutationOutcomeCode.ALREADY_ACCEPTED_AWAITING_CANONICAL_ECHO,
      MutationOutcomeCode.ALREADY_CANONICAL,
      MutationOutcomeCode.REJECTED_VALIDATION,
      MutationOutcomeCode.REJECTED_AUTHORIZATION,
      MutationOutcomeCode.CONFLICT,
      MutationOutcomeCode.COMMAND_VERSION_UNSUPPORTED,
      MutationOutcomeCode.RETRYABLE_FAILURE,
    ];
    const response = create(MutationBatchResponseSchema, {
      protocolVersion: 1,
      serverTimeUnixMicros: 1n,
      outcomes: codes.map((code, index) =>
        outcome(`mutation-${index + 1}`, code),
      ),
    });
    expect(() => validateMutationBatchResponse(response)).not.toThrow();
  });

  it("rejects duplicate request, expected-version, footprint, response, and marker IDs", () => {
    const request = validMutationRequest();
    request.mutations.push(validMutationEnvelope("mutation-1"));
    expect(() => validateMutationBatchRequest(request)).toThrow(
      /must be unique/u,
    );

    const expectedDuplicate = validMutationRequest();
    const mutation = firstMutation(expectedDuplicate);
    const expected = create(ExpectedVersionSchema, {
      tableId: "public.tasks",
      primaryKey: primaryKey(),
      expectedVersion: bytes(2),
    });
    mutation.expectedVersions = [expected, expected];
    expect(() => validateMutationBatchRequest(expectedDuplicate)).toThrow(
      /duplicates an expected/u,
    );

    const footprintDuplicate = validMutationRequest();
    const footprint = requiredFootprint(firstMutation(footprintDuplicate));
    footprint.rows.push(firstFootprintRow(footprint));
    expect(() => validateMutationBatchRequest(footprintDuplicate)).toThrow(
      /duplicates an affected/u,
    );

    const response = create(MutationBatchResponseSchema, {
      protocolVersion: 1,
      serverTimeUnixMicros: 1n,
      outcomes: [
        outcome("mutation-1", MutationOutcomeCode.REJECTED_VALIDATION),
        outcome("mutation-1", MutationOutcomeCode.REJECTED_AUTHORIZATION),
      ],
    });
    expect(() => validateMutationBatchResponse(response)).toThrow(
      /must be unique/u,
    );
  });

  it("rejects missing required mutation fields and footprint membership errors", () => {
    const cases: [string, (request: MutationBatchRequest) => void][] = [
      [
        "createdAtUnixMicros",
        (request) => {
          firstMutation(request).createdAtUnixMicros = 0n;
        },
      ],
      [
        "declaredFootprint",
        (request) => {
          firstMutation(request).declaredFootprint = undefined;
        },
      ],
      [
        "commandName",
        (request) => {
          firstMutation(request).commandName = "invalid command";
        },
      ],
      [
        "tableId",
        (request) => {
          firstFootprintRow(requiredFootprint(firstMutation(request))).tableId =
            "other.table";
        },
      ],
    ];
    for (const [path, mutate] of cases) {
      const request = validMutationRequest();
      mutate(request);
      expect(() => validateMutationBatchRequest(request), path).toThrow(
        new RegExp(path, "u"),
      );
    }
  });

  it("enforces per-input, aggregate-input, key, scope, and batch-count bounds", () => {
    const oversizedInput = validMutationRequest();
    firstMutation(oversizedInput).canonicalInput = new Uint8Array(
      1_024 * 1_024 + 1,
    );
    expect(() => validateMutationBatchRequest(oversizedInput)).toThrow(
      /canonicalInput.*at most/u,
    );

    const aggregate = validMutationRequest();
    aggregate.mutations = Array.from({ length: 5 }, (_, index) => {
      const mutation = validMutationEnvelope(`mutation-${index + 1}`);
      mutation.canonicalInput = new Uint8Array(1_024 * 1_024);
      return mutation;
    });
    expect(() => validateMutationBatchRequest(aggregate)).toThrow(
      /total at most/u,
    );

    const oversizedKey = validMutationRequest();
    firstFootprintRow(
      requiredFootprint(firstMutation(oversizedKey)),
    ).primaryKey = new Uint8Array(8_193);
    expect(() => validateMutationBatchRequest(oversizedKey)).toThrow(
      /primaryKey.*exceeding limit 8192/u,
    );

    const oversizedScope = validMutationRequest();
    oversizedScope.authorizationScopeToken = new Uint8Array(1_024 * 1_024 + 1);
    expect(() => validateMutationBatchRequest(oversizedScope)).toThrow(
      /authorizationScopeToken.*at most/u,
    );

    const oversizedBatch = validMutationRequest();
    oversizedBatch.mutations = Array.from({ length: 101 }, (_, index) =>
      validMutationEnvelope(`mutation-${index + 1}`),
    );
    expect(() => validateMutationBatchRequest(oversizedBatch)).toThrow(
      /1\.\.100/u,
    );
  });

  it("rejects unknown outcomes and category-specific field mismatches", () => {
    const unknown = outcome(
      "mutation-1",
      MutationOutcomeCode.REJECTED_VALIDATION,
    );
    unknown.code = 99 as MutationOutcomeCode;
    const response = create(MutationBatchResponseSchema, {
      protocolVersion: 1,
      serverTimeUnixMicros: 1n,
      outcomes: [unknown],
    });
    expect(() => validateMutationBatchResponse(response)).toThrow(
      /unknown or unspecified/u,
    );

    const accepted = outcome(
      "mutation-1",
      MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO,
    );
    accepted.serverTransactionId = new Uint8Array();
    response.outcomes = [accepted];
    expect(() => validateMutationBatchResponse(response)).toThrow(
      /exactly 16 bytes/u,
    );

    const canonical = outcome(
      "mutation-1",
      MutationOutcomeCode.ALREADY_CANONICAL,
    );
    canonical.observedCanonicalCursor = undefined;
    response.outcomes = [canonical];
    expect(() => validateMutationBatchResponse(response)).toThrow(
      /observedCanonicalCursor.*required/u,
    );

    const retryable = outcome(
      "mutation-1",
      MutationOutcomeCode.RETRYABLE_FAILURE,
    );
    retryable.retryAfterMs = 0n;
    response.outcomes = [retryable];
    expect(() => validateMutationBatchResponse(response)).toThrow(
      /retryAfterMs.*greater than zero/u,
    );
  });
});

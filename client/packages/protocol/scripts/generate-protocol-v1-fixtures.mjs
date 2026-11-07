import { create } from "@bufbuild/protobuf";
import { Buffer } from "node:buffer";
import { createPrivateKey, sign as signEd25519 } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import process from "node:process";
import { TextEncoder } from "node:util";

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
  RevocationBarrierSchema,
  RowPatchSchema,
  SourceGenerationSchema,
  StreamErrorCode,
  StreamErrorSchema,
  TableSubscriptionSchema,
  TransactionFragmentSchema,
  TruncateSchema,
} from "../dist/gen/change_stream_pb.js";
import {
  AffectedRowSchema,
  CanonicalRowSchema,
  ConflictSchema,
  CursorSchema as MutationCursorSchema,
  ExpectedVersionSchema,
  MutationBatchRequestSchema,
  MutationBatchResponseSchema,
  MutationEnvelopeSchema,
  MutationFootprintSchema,
  MutationLogicalMarkerSchema,
  MutationOutcomeCode,
  MutationOutcomeSchema,
} from "../dist/gen/mutation_pb.js";
import { encodeFrame } from "../dist/framing.js";
import { BatchFramesDigest, changeFrameBodySha256 } from "../dist/integrity.js";
import { encodePrimaryKey } from "../dist/primary-key.js";
import { revocationBarrierSigningPreimage } from "../dist/revocation.js";

const output = process.argv[2];
if (!output)
  throw new Error("usage: generate-protocol-v1-fixtures.mjs OUTPUT_DIR");
mkdirSync(output, { recursive: true });
mkdirSync(join(output, "invalid"), { recursive: true });

const bytes = (...values) => Uint8Array.from(values);
const utf8 = (value) => new TextEncoder().encode(value);
const range = (start, end) =>
  Uint8Array.from({ length: end - start }, (_, index) => start + index);
const concat = (...chunks) => {
  const result = new Uint8Array(
    chunks.reduce((total, chunk) => total + chunk.byteLength, 0),
  );
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
};
const changeCursor = (token) =>
  create(ChangeCursorSchema, { token: utf8(token) });
const mutationCursor = (token) =>
  create(MutationCursorSchema, { token: utf8(token) });
const column = (columnId, kind) =>
  create(ColumnValueSchema, {
    columnId,
    value: create(CellValueSchema, { kind }),
  });

const primaryKeyMessage = () =>
  create(PrimaryKeySchema, {
    codecVersion: 1,
    parts: [
      column(1, { case: "int64Value", value: -42n }),
      column(2, { case: "boolValue", value: true }),
      column(3, { case: "blobValue", value: bytes(0, 1, 2, 255) }),
      column(4, { case: "uuid16", value: range(0, 16) }),
    ],
  });
const primaryKey = () => encodePrimaryKey(primaryKeyMessage());
const textPrimaryKey = () =>
  encodePrimaryKey(
    create(PrimaryKeySchema, {
      codecVersion: 1,
      parts: [
        column(1, { case: "int64Value", value: -42n }),
        column(2, { case: "textValue", value: "é-\u{1d11e}" }),
      ],
    }),
  );
const framed = (schema, messages) =>
  concat(...messages.map((message) => encodeFrame(schema, message)));

const changeRequests = () =>
  framed(ChangeRequestSchema, [
    create(ChangeRequestSchema, {
      protocolVersion: 1,
      fromCursorExclusive: changeCursor("cursor-0"),
      tables: [create(TableSubscriptionSchema, { tableId: "public.tasks" })],
      authorizationScopeToken: utf8("scope-token"),
      maxBatchBytes: 1_024n,
      maxTransactions: 1,
      longPollTimeoutMs: 0,
      clientInstanceId: "client-1",
      acceptTransactionFragmentation: false,
      acceptRevocationBarrier: false,
      planId: "plan-1",
      deviceId: "device-1",
      stream: false,
    }),
    create(ChangeRequestSchema, {
      protocolVersion: 1,
      fromCursorExclusive: changeCursor("cursor-0"),
      throughCursorInclusive: changeCursor("cursor-h"),
      tables: [
        create(TableSubscriptionSchema, {
          tableId: "public.tasks",
          shardSnapshotCursor: changeCursor("cursor-p"),
        }),
        create(TableSubscriptionSchema, { tableId: "public.users" }),
      ],
      authorizationScopeToken: utf8("scope-token"),
      maxBatchBytes: 16n * 1024n * 1024n,
      maxTransactions: 10_000,
      longPollTimeoutMs: 120_000,
      clientInstanceId: "client-1",
      acceptTransactionFragmentation: true,
      acceptRevocationBarrier: true,
      planId: "plan-1",
      deviceId: "device-1",
      pendingMutationIds: ["mutation-1", "mutation-2"],
      stream: false,
    }),
    create(ChangeRequestSchema, {
      protocolVersion: 1,
      fromCursorExclusive: changeCursor("cursor-h"),
      tables: [create(TableSubscriptionSchema, { tableId: "public.tasks" })],
      authorizationScopeToken: utf8("scope-token"),
      maxBatchBytes: 1_024n,
      maxTransactions: 1_000,
      longPollTimeoutMs: 1_000,
      clientInstanceId: "client-1",
      acceptTransactionFragmentation: true,
      acceptRevocationBarrier: false,
      planId: "plan-1",
      deviceId: "device-1",
      stream: true,
    }),
  ]);

const limitChangeRequest = (authorizationTokenBytes) =>
  create(ChangeRequestSchema, {
    protocolVersion: 1,
    fromCursorExclusive: create(ChangeCursorSchema, {
      token: new Uint8Array(2_048).fill("c".charCodeAt(0)),
    }),
    throughCursorInclusive: create(ChangeCursorSchema, {
      token: new Uint8Array(2_048).fill("f".charCodeAt(0)),
    }),
    tables: Array.from({ length: 1_024 }, (_, index) =>
      create(TableSubscriptionSchema, { tableId: `t${String(index)}` }),
    ),
    authorizationScopeToken: new Uint8Array(authorizationTokenBytes).fill(1),
    maxBatchBytes: 16n * 1_024n * 1_024n,
    maxTransactions: 10_000,
    longPollTimeoutMs: 120_000,
    clientInstanceId: "c".repeat(1_024),
    acceptTransactionFragmentation: true,
    acceptRevocationBarrier: true,
    planId: "p".repeat(512),
    deviceId: "d".repeat(1_024),
    pendingMutationIds: Array.from(
      { length: 256 },
      (_, index) => `m${String(index)}`,
    ),
  });

const signed = async (body) => {
  const frame = create(ChangeStreamFrameSchema, { protocolVersion: 1, body });
  frame.bodySha256 = await changeFrameBodySha256(frame);
  return frame;
};

/** The `batchEnd` closing a batch of exactly `previous`, in order. */
const batchEnd = async (previous, fields) => {
  const digest = new BatchFramesDigest();
  for (const frame of previous) digest.push(frame.bodySha256);
  return signed({
    case: "batchEnd",
    value: create(BatchEndSchema, {
      ...fields,
      frameCount: BigInt(digest.frames),
      framesSha256: await digest.finish(),
    }),
  });
};

const allRowValues = () => [
  column(1, { case: "nullValue", value: NullValue.NULL_VALUE }),
  column(2, { case: "int64Value", value: -9n }),
  column(3, { case: "uint64Value", value: 18_446_744_073_709_551_614n }),
  column(4, { case: "float64Value", value: 1.5 }),
  column(5, { case: "textValue", value: "café" }),
  column(6, { case: "blobValue", value: bytes(0, 255) }),
  column(7, { case: "boolValue", value: true }),
  column(8, { case: "decimalText", value: "123.4500" }),
  column(9, { case: "timestampUnixMicros", value: -1_234_567n }),
  column(10, { case: "uuid16", value: range(16, 32) }),
  column(11, { case: "jsonText", value: '{"a":1}' }),
  column(12, { case: "dateIso8601", value: "2026-07-14" }),
  column(13, { case: "timeIso8601", value: "21:30:00Z" }),
  column(14, { case: "intervalIso8601", value: "P1DT2H" }),
];

const changeStreamFrames = async () => {
  const generation = create(SourceGenerationSchema, {
    sourceEpoch: "epoch-1",
    publicationGeneration: 2n,
    schemaGeneration: 3n,
    authorizationGeneration: 4n,
    authorizationScopeHash: new Uint8Array(32).fill(5),
  });
  const key = primaryKey();
  const bodies = [
    {
      case: "batchStart",
      value: create(BatchStartSchema, {
        generation,
        fromCursorExclusive: changeCursor("cursor-0"),
        requestedFence: changeCursor("cursor-h"),
        tableIds: ["public.tasks", "public.users"],
        serverTimeUnixMicros: 1n,
        planId: "plan-1",
        tableStarts: [
          create(TableSubscriptionSchema, {
            tableId: "public.tasks",
            shardSnapshotCursor: changeCursor("cursor-p"),
          }),
          create(TableSubscriptionSchema, { tableId: "public.users" }),
        ],
        streamId: utf8("stream-1"),
      }),
    },
    {
      case: "batchStart",
      value: create(BatchStartSchema, {
        generation,
        fromCursorExclusive: changeCursor("cursor-0"),
        tableIds: ["public.tasks"],
        planId: "plan-1",
        tableStarts: [
          create(TableSubscriptionSchema, { tableId: "public.tasks" }),
        ],
        streamId: utf8("stream-2"),
      }),
    },
    {
      case: "transactionFragment",
      value: create(TransactionFragmentSchema, {
        transactionId: range(32, 48),
        transactionCursor: changeCursor("cursor-1"),
        firstFragment: true,
        finalFragment: true,
        postgresXid: 7n,
        beginLsn: "0/10",
        commitLsn: "0/20",
        commitTimeUnixMicros: 9n,
        operations: [
          create(OperationSchema, {
            tableId: "public.tasks",
            kind: OperationKind.INSERT_FULL_ROW,
            primaryKey: key,
            payload: {
              case: "insert",
              value: create(FullRowSchema, { columns: allRowValues() }),
            },
          }),
          create(OperationSchema, {
            tableId: "public.tasks",
            kind: OperationKind.UPDATE_PATCH,
            primaryKey: key,
            payload: {
              case: "update",
              value: create(RowPatchSchema, {
                changedColumns: [
                  column(5, { case: "textValue", value: "updated" }),
                ],
              }),
            },
          }),
          create(OperationSchema, {
            tableId: "public.tasks",
            kind: OperationKind.DELETE_BY_KEY,
            primaryKey: key,
            payload: { case: "delete", value: create(DeleteSchema) },
          }),
          create(OperationSchema, {
            tableId: "public.users",
            kind: OperationKind.TRUNCATE_AUTHORIZED_TABLE,
            payload: {
              case: "truncate",
              value: create(TruncateSchema, { reason: "authorized reset" }),
            },
          }),
        ],
      }),
    },
  ];
  const frames = [];
  for (const body of bodies) frames.push(await signed(body));
  frames.push(
    await batchEnd(frames, {
      throughCursorInclusive: changeCursor("cursor-h"),
      reachedRequestedFence: true,
      relevantTransactionCount: 1n,
      relevantOperationCount: 4n,
      serverTimeUnixMicros: 10n,
      acknowledgedMutationIds: ["mutation-1", "mutation-2"],
    }),
  );
  for (const body of [
    {
      case: "heartbeat",
      value: create(HeartbeatSchema, {
        durableHead: changeCursor("cursor-h"),
        serverTimeUnixMicros: 11n,
      }),
    },
    barrierBody(4n),
  ])
    frames.push(await signed(body));
  for (const code of [
    StreamErrorCode.CURSOR_EPOCH_MISMATCH,
    StreamErrorCode.CURSOR_BEFORE_RETENTION,
    StreamErrorCode.SCHEMA_GENERATION_MISMATCH,
    StreamErrorCode.AUTHORIZATION_CHANGED,
    StreamErrorCode.TABLE_SET_INVALID,
    StreamErrorCode.PROTOCOL_VERSION_UNSUPPORTED,
    StreamErrorCode.RATE_LIMITED,
    StreamErrorCode.INTERNAL_RETRYABLE,
    StreamErrorCode.REQUEST_INVALID,
  ]) {
    frames.push(
      await signed({
        case: "error",
        value: create(StreamErrorSchema, {
          code,
          message: `error-${String(code)}`,
          resnapshotRequired: [1, 2, 3, 4].includes(code),
          minimumRetainedCursor: changeCursor("cursor-min"),
          durableHead: changeCursor("cursor-h"),
          retryAfterMs: 25n,
        }),
      }),
    );
  }
  return framed(ChangeStreamFrameSchema, frames);
};

/**
 * The fixed Ed25519 key pair every revocation-barrier vector is signed with,
 * in TypeScript and Rust alike. A fixture key, never a deployment one.
 */
const BARRIER_SEED = range(0, 32);
const BARRIER_KEY_ID = "revocation-key-1";
/**
 * The authorization scope every barrier vector retires: the batch start's
 * own.
 */
const BARRIER_SCOPE_HASH = () => new Uint8Array(32).fill(5);
const barrierPrivateKey = createPrivateKey({
  key: Buffer.concat([
    Buffer.from("302e020100300506032b657004220420", "hex"),
    Buffer.from(BARRIER_SEED),
  ]),
  format: "der",
  type: "pkcs8",
});

const revocationBarrier = (revokedGeneration) => {
  const barrier = create(RevocationBarrierSchema, {
    barrierCursor: changeCursor("cursor-h"),
    revokedAuthorizationGeneration: revokedGeneration,
    effectiveAuthorizationGeneration: revokedGeneration + 1n,
    revokedAuthorizationScopeHash: BARRIER_SCOPE_HASH(),
    barrierId: "barrier-1",
    issuedAtUnixMicros: 1_700_000_000_000_000n,
    signingKeyId: BARRIER_KEY_ID,
  });
  barrier.signature = new Uint8Array(
    signEd25519(
      null,
      Buffer.from(revocationBarrierSigningPreimage(barrier)),
      barrierPrivateKey,
    ),
  );
  return barrier;
};

const barrierBatchStart = () => ({
  case: "batchStart",
  value: create(BatchStartSchema, {
    generation: create(SourceGenerationSchema, {
      sourceEpoch: "epoch-1",
      publicationGeneration: 2n,
      schemaGeneration: 3n,
      authorizationGeneration: 4n,
      authorizationScopeHash: BARRIER_SCOPE_HASH(),
    }),
    fromCursorExclusive: changeCursor("cursor-0"),
    requestedFence: changeCursor("cursor-h"),
    tableIds: ["public.tasks"],
    serverTimeUnixMicros: 1n,
    planId: "plan-1",
    tableStarts: [
      create(TableSubscriptionSchema, {
        tableId: "public.tasks",
        shardSnapshotCursor: changeCursor("cursor-p"),
      }),
    ],
    streamId: utf8("stream-1"),
  }),
});

const barrierFragment = () => ({
  case: "transactionFragment",
  value: create(TransactionFragmentSchema, {
    transactionId: range(48, 64),
    transactionCursor: changeCursor("cursor-1"),
    fragmentIndex: 0,
    firstFragment: true,
    finalFragment: true,
    postgresXid: 11n,
    beginLsn: "0/30",
    commitLsn: "0/40",
    commitTimeUnixMicros: 12n,
    operations: [
      create(OperationSchema, {
        tableId: "public.tasks",
        kind: OperationKind.TRUNCATE_AUTHORIZED_TABLE,
        payload: {
          case: "truncate",
          value: create(TruncateSchema, { reason: "authorized reset" }),
        },
      }),
    ],
  }),
});

const barrierBatchEnd = (previous) =>
  batchEnd(previous, {
    throughCursorInclusive: changeCursor("cursor-h"),
    reachedRequestedFence: true,
    currentlyUpToDate: true,
    relevantTransactionCount: 1n,
    relevantOperationCount: 1n,
    serverTimeUnixMicros: 13n,
  });

const barrierBody = (revokedGeneration) => ({
  case: "revocationBarrier",
  value: revocationBarrier(revokedGeneration),
});

/**
 * The positive barrier vector: one batch that delivers a transaction, then the
 * barrier, then ends exactly on the barrier's own cursor.
 */
const revocationBarrierStream = async () => {
  const bodies = [barrierBatchStart(), barrierFragment(), barrierBody(4n)];
  const frames = [];
  for (const body of bodies) frames.push(await signed(body));
  frames.push(await barrierBatchEnd(frames));
  return framed(ChangeStreamFrameSchema, frames);
};

const barrierNegativeFixtures = async () => {
  const malformed = revocationBarrier(4n);
  malformed.signature = malformed.signature.slice(0, 63);

  const reordered = framed(ChangeStreamFrameSchema, [
    await signed(barrierBatchStart()),
    await signed(barrierBody(4n)),
    await signed(barrierFragment()),
  ]);
  const duplicated = framed(ChangeStreamFrameSchema, [
    await signed(barrierBatchStart()),
    await signed(barrierBody(4n)),
    await signed(barrierBody(4n)),
  ]);
  const crossEpoch = framed(ChangeStreamFrameSchema, [
    await signed(barrierBatchStart()),
    await signed(barrierBody(3n)),
  ]);

  // The valid barrier frame, cut short of its declared payload, and the same
  // payload behind a 4 MiB-plus-one length prefix. Both are refused by the
  // frame decoder before any barrier field is looked at.
  const whole = encodeFrame(
    ChangeStreamFrameSchema,
    await signed(barrierBody(4n)),
  );
  const truncated = whole.slice(0, whole.byteLength - 8);
  const oversized = concat(bytes(0x00, 0x40, 0x00, 0x01), whole.slice(4));

  return [
    [
      "revocation-barrier-malformed.frame.hex",
      encodeFrame(
        ChangeStreamFrameSchema,
        await signed({ case: "revocationBarrier", value: malformed }),
      ),
    ],
    ["revocation-barrier-truncated.hex", truncated],
    ["revocation-barrier-oversized.hex", oversized],
    ["revocation-barrier-reordered.frame.hex", reordered],
    ["revocation-barrier-duplicated.frame.hex", duplicated],
    ["revocation-barrier-cross-epoch.frame.hex", crossEpoch],
  ];
};

const mutationRequests = () => {
  const key = primaryKey();
  return framed(MutationBatchRequestSchema, [
    create(MutationBatchRequestSchema, {
      protocolVersion: 1,
      clientInstanceId: "client-1",
      deviceId: "device-1",
      authorizationScopeToken: utf8("scope-token"),
      mutations: [
        create(MutationEnvelopeSchema, {
          mutationId: "mutation-1",
          commandName: "tasks.rename",
          commandVersion: 1,
          canonicalInput: utf8('{"title":"next"}'),
          baseCursor: mutationCursor("cursor-0"),
          createdAtUnixMicros: 1n,
          declaredFootprint: create(MutationFootprintSchema, {
            tableIds: ["public.tasks"],
          }),
        }),
      ],
    }),
    create(MutationBatchRequestSchema, {
      protocolVersion: 1,
      clientInstanceId: "client-1",
      deviceId: "device-1",
      authorizationScopeToken: utf8("scope-token"),
      mutations: [
        create(MutationEnvelopeSchema, {
          mutationId: "mutation-2",
          commandName: "tasks.complete",
          commandVersion: 2,
          canonicalInput: bytes(0, 1, 2, 255),
          baseCursor: mutationCursor("cursor-1"),
          createdAtUnixMicros: 9_007_199_254_740_993n,
          expectedVersions: [
            create(ExpectedVersionSchema, {
              tableId: "public.tasks",
              primaryKey: key,
              expectedVersion: utf8("version-1"),
            }),
          ],
          declaredFootprint: create(MutationFootprintSchema, {
            rows: [
              create(AffectedRowSchema, {
                tableId: "public.tasks",
                primaryKey: key,
              }),
            ],
            tableIds: ["public.tasks", "public.audit"],
          }),
        }),
      ],
    }),
  ]);
};

const rejected = (mutationId, code) =>
  create(MutationOutcomeSchema, {
    mutationId,
    code,
    message: "rejected",
  });

const mutationResponse = () => {
  const transaction = range(64, 80);
  return encodeFrame(
    MutationBatchResponseSchema,
    create(MutationBatchResponseSchema, {
      protocolVersion: 1,
      serverTimeUnixMicros: 99n,
      outcomes: [
        create(MutationOutcomeSchema, {
          mutationId: "mutation-1",
          code: MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO,
          message: "accepted",
          serverTransactionId: transaction,
        }),
        create(MutationOutcomeSchema, {
          mutationId: "mutation-2",
          code: MutationOutcomeCode.ALREADY_ACCEPTED_AWAITING_CANONICAL_ECHO,
          message: "already accepted",
          serverTransactionId: transaction,
        }),
        create(MutationOutcomeSchema, {
          mutationId: "mutation-3",
          code: MutationOutcomeCode.ALREADY_CANONICAL,
          message: "canonical",
          serverTransactionId: transaction,
          observedCanonicalCursor: mutationCursor("cursor-3"),
        }),
        rejected("mutation-4", MutationOutcomeCode.REJECTED_VALIDATION),
        rejected("mutation-5", MutationOutcomeCode.REJECTED_AUTHORIZATION),
        create(MutationOutcomeSchema, {
          mutationId: "mutation-6",
          code: MutationOutcomeCode.CONFLICT,
          message: "conflict",
          conflict: create(ConflictSchema, {
            kind: "version",
            rows: [
              create(CanonicalRowSchema, {
                tableId: "public.tasks",
                primaryKey: primaryKey(),
                encodedFullRow: utf8("row"),
                version: utf8("version-2"),
              }),
            ],
            applicationPayload: utf8("details"),
          }),
        }),
        rejected("mutation-7", MutationOutcomeCode.COMMAND_VERSION_UNSUPPORTED),
        create(MutationOutcomeSchema, {
          ...rejected("mutation-8", MutationOutcomeCode.RETRYABLE_FAILURE),
          retryAfterMs: 25n,
        }),
      ],
    }),
  );
};

const mutationMarker = () =>
  encodeFrame(
    MutationLogicalMarkerSchema,
    create(MutationLogicalMarkerSchema, {
      protocolVersion: 1,
      mutationIds: ["mutation-1", "mutation-2"],
      serverTransactionId: range(80, 96),
    }),
  );

const negativeFixtures = async () => {
  const wrongChecksum = await signed({
    case: "heartbeat",
    value: create(HeartbeatSchema, {
      durableHead: changeCursor("cursor-h"),
      serverTimeUnixMicros: 1n,
    }),
  });
  wrongChecksum.bodySha256 = new Uint8Array(32);

  const unknownEnum = await signed({
    case: "transactionFragment",
    value: create(TransactionFragmentSchema, {
      transactionId: bytes(1),
      transactionCursor: changeCursor("cursor-1"),
      firstFragment: true,
      finalFragment: true,
      postgresXid: 1n,
      beginLsn: "0/1",
      commitLsn: "0/2",
      commitTimeUnixMicros: 1n,
      operations: [
        create(OperationSchema, {
          tableId: "public.tasks",
          kind: 99,
          payload: {
            case: "truncate",
            value: create(TruncateSchema, { reason: "invalid enum" }),
          },
        }),
      ],
    }),
  });

  const missingOneof = create(ChangeStreamFrameSchema, {
    protocolVersion: 1,
    bodySha256: new Uint8Array(32),
  });

  const duplicateRequest = limitChangeRequest(8 * 1_024);
  duplicateRequest.fromCursorExclusive = changeCursor("cursor-0");
  duplicateRequest.throughCursorInclusive = undefined;
  duplicateRequest.authorizationScopeToken = utf8("scope-token");
  duplicateRequest.maxBatchBytes = 1_024n;
  duplicateRequest.maxTransactions = 1;
  duplicateRequest.longPollTimeoutMs = 0;
  duplicateRequest.clientInstanceId = "client-1";
  duplicateRequest.planId = "plan-1";
  duplicateRequest.deviceId = "device-1";
  duplicateRequest.pendingMutationIds = [];
  duplicateRequest.tables = [
    create(TableSubscriptionSchema, { tableId: "public.tasks" }),
    create(TableSubscriptionSchema, { tableId: "public.tasks" }),
  ];

  const beforeStart = await signed({
    case: "heartbeat",
    value: create(HeartbeatSchema, {
      durableHead: changeCursor("cursor-h"),
      serverTimeUnixMicros: 1n,
    }),
  });

  const start = await signed({
    case: "batchStart",
    value: create(BatchStartSchema, {
      generation: create(SourceGenerationSchema, {
        sourceEpoch: "epoch-1",
        publicationGeneration: 2n,
        schemaGeneration: 3n,
        authorizationGeneration: 4n,
        authorizationScopeHash: new Uint8Array(32).fill(5),
      }),
      fromCursorExclusive: changeCursor("cursor-0"),
      requestedFence: changeCursor("cursor-h"),
      tableIds: ["public.tasks"],
      serverTimeUnixMicros: 1n,
      planId: "plan-1",
      tableStarts: [
        create(TableSubscriptionSchema, {
          tableId: "public.tasks",
          shardSnapshotCursor: changeCursor("cursor-p"),
        }),
      ],
      streamId: utf8("stream-1"),
    }),
  });
  const fragmentGap = await signed({
    case: "transactionFragment",
    value: create(TransactionFragmentSchema, {
      transactionId: bytes(1),
      transactionCursor: changeCursor("cursor-1"),
      fragmentIndex: 1,
      firstFragment: false,
      finalFragment: true,
      postgresXid: 1n,
      beginLsn: "0/1",
      commitLsn: "0/2",
      commitTimeUnixMicros: 1n,
      operations: [
        create(OperationSchema, {
          tableId: "public.tasks",
          kind: OperationKind.TRUNCATE_AUTHORIZED_TABLE,
          payload: {
            case: "truncate",
            value: create(TruncateSchema, { reason: "authorized reset" }),
          },
        }),
      ],
    }),
  });

  return new Map([
    ...(await barrierNegativeFixtures()),
    ["framing-truncated-prefix.hex", bytes(0, 0, 0)],
    ["framing-oversized.hex", bytes(0, 64, 0, 1)],
    ["framing-malformed-protobuf.hex", bytes(0, 0, 0, 1, 0x0f)],
    [
      "primary-key-noncanonical.hex",
      concat(primaryKey(), bytes(0x98, 0x06, 0x01)),
    ],
    [
      "primary-key-text-invalid-utf8.hex",
      bytes(0x08, 0x01, 0x12, 0x07, 0x08, 0x02, 0x12, 0x03, 0x2a, 0x01, 0xff),
    ],
    [
      "wrong-checksum.frame.hex",
      encodeFrame(ChangeStreamFrameSchema, wrongChecksum),
    ],
    [
      "unknown-enum.frame.hex",
      encodeFrame(ChangeStreamFrameSchema, unknownEnum),
    ],
    [
      "missing-oneof.frame.hex",
      encodeFrame(ChangeStreamFrameSchema, missingOneof),
    ],
    [
      "duplicate-change-request.frame.hex",
      encodeFrame(ChangeRequestSchema, duplicateRequest),
    ],
    [
      "order-before-start.frame.hex",
      encodeFrame(ChangeStreamFrameSchema, beforeStart),
    ],
    [
      "fragment-gap.frame.hex",
      framed(ChangeStreamFrameSchema, [start, fragmentGap]),
    ],
    [
      "exact-limit-change-request.frame.hex",
      encodeFrame(ChangeRequestSchema, limitChangeRequest(8 * 1_024)),
    ],
    [
      "over-limit-change-request.frame.hex",
      encodeFrame(ChangeRequestSchema, limitChangeRequest(8 * 1_024 + 1)),
    ],
  ]);
};

const fixtures = new Map([
  ["change-request.frame.hex", changeRequests()],
  ["change-stream.frame.hex", await changeStreamFrames()],
  ["revocation-barrier.frame.hex", await revocationBarrierStream()],
  ["mutation-request.frame.hex", mutationRequests()],
  ["mutation-response.frame.hex", mutationResponse()],
  ["mutation-marker.frame.hex", mutationMarker()],
  ["primary-key.hex", primaryKey()],
  ["primary-key-text.hex", textPrimaryKey()],
]);
for (const [name, value] of fixtures) {
  writeFileSync(join(output, name), `${Buffer.from(value).toString("hex")}\n`);
}
for (const [name, value] of await negativeFixtures()) {
  writeFileSync(
    join(output, "invalid", name),
    `${Buffer.from(value).toString("hex")}\n`,
  );
}

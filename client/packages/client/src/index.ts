export { createSyncClient } from "./client.js";
export type {
  Register,
  RegisteredCollections,
  GrantVerificationKey,
  SyncAuth,
  SyncClient,
  SyncClientCollections,
  SyncClientOptions,
  SyncPublication,
  SyncStatus,
} from "./client.js";
export { SyncError } from "./error.js";
export type { SyncErrorCode, SyncErrorScope, SyncRecovery } from "./error.js";
export type { LiveQueryResource, LiveQuerySnapshot } from "./resource.js";
export type { ClientSchema } from "./schema.js";
export {
  and,
  eq,
  gt,
  gte,
  isNotNull,
  isNull,
  lt,
  lte,
  neq,
  not,
  or,
} from "./facade/predicates.js";
export type {
  CollectionFacade,
  CollectionFacades,
  CollectionShape,
  CollectionShapes,
  GroupedQueryBuilder,
  JoinSource,
  JoinedColumns,
  ProjectedName,
  ProjectedRow,
  QueryBuilder,
  QueryJoinKind,
  QueryLiveOptions,
  QueryLiveWindowOptions,
  QueryNullPlacement,
  QueryOrderDirection,
  QueryPredicate,
  QueryRunOptions,
  QueryWindow,
  QueryWindowOptions,
} from "./facade/types.js";
export { MutationSubmissionError } from "./mutation.js";
export type {
  MutationAccepted,
  MutationConflict,
  MutationResult,
  MutationState,
  MutationStatus,
  MutationSubmissionErrorCode,
} from "./mutation.js";

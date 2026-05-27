export type OpenMode = "openOrCreate" | "open" | "create";
export type ResultTransport = "binary" | "json";
export type OwnerRuntime = "dedicated-worker" | "remote-owner";
export type CoordinationModel = "broadcastchannel-weblocks-dedicated-owner";
export type BrowserSqlProfile = "browser-app-v1" | "browser-app-v2";

export const BROWSER_PROTOCOL_VERSION = 2;
export const BROWSER_SQL_PROFILE: BrowserSqlProfile = "browser-app-v2";

export interface BrowserCapabilities {
  protocolVersion: number;
  engineVersion?: string;
  parserProfile: BrowserSqlProfile;
  resultTransports: ResultTransport[];
  transactions: boolean;
  savepoints: boolean;
  preparedStatements: boolean;
  statementReset: boolean;
  statementClearBindings: boolean;
  statementPaging: boolean;
  asyncStatementIteration: boolean;
  importExport: boolean;
  metrics: boolean;
  relayHttp: boolean;
  relayWebSocket: boolean;
  changesetApply: boolean;
  branchSnapshots: boolean;
  browserTdeOpenOptions: boolean;
  cooperativeCancellation: boolean;
}

export type RpcKind =
  | "open"
  | "close"
  | "exec"
  | "query"
  | "prepare"
  | "statement_bind"
  | "statement_step"
  | "statement_reset"
  | "statement_clear_bindings"
  | "statement_page"
  | "statement_close"
  | "checkpoint"
  | "export"
  | "import"
  | "persist"
  | "metrics"
  | "sync_configure_peer"
  | "sync_apply_changeset"
  | "sync_run";

export type QueryValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | ArrayBuffer
  | { kind: string; [key: string]: unknown };

export type QueryRow = Record<string, QueryValue>;

export interface QueryErrorPayload {
  code: string;
  message: string;
  details?: string;
}

export interface OpenRequest {
  kind: "open";
  requestId: number;
  payload: {
    path: string;
    mode: OpenMode;
    options?: {
      sharedMemory: boolean;
      readOnly: boolean;
      wasmUrl?: string;
      resultTransport?: ResultTransport;
    };
  };
}

export interface CloseRequest {
  kind: "close";
  requestId: number;
  payload: {
    dbId: number;
  };
}

export interface ExecRequest {
  kind: "exec";
  requestId: number;
  payload: {
    dbId: number;
    sql: string;
    params?: QueryValue[];
  };
}

export interface QueryRequest {
  kind: "query";
  requestId: number;
  payload: {
    dbId: number;
    sql: string;
    params?: QueryValue[];
  };
}

export interface PrepareRequest {
  kind: "prepare";
  requestId: number;
  payload: {
    dbId: number;
    sql: string;
  };
}

export interface StatementBindRequest {
  kind: "statement_bind";
  requestId: number;
  payload: {
    statementId: number;
    params?: QueryValue[];
  };
}

export interface StatementStepRequest {
  kind: "statement_step";
  requestId: number;
  payload: {
    statementId: number;
  };
}

export interface StatementResetRequest {
  kind: "statement_reset";
  requestId: number;
  payload: {
    statementId: number;
  };
}

export interface StatementClearBindingsRequest {
  kind: "statement_clear_bindings";
  requestId: number;
  payload: {
    statementId: number;
  };
}

export interface StatementPageRequest {
  kind: "statement_page";
  requestId: number;
  payload: {
    statementId: number;
    pageSize: number;
  };
}

export interface StatementCloseRequest {
  kind: "statement_close";
  requestId: number;
  payload: {
    statementId: number;
  };
}

export interface CheckpointRequest {
  kind: "checkpoint";
  requestId: number;
  payload: {
    dbId: number;
  };
}

export interface ExportRequest {
  kind: "export";
  requestId: number;
  payload: {
    dbId: number;
  };
}

export interface ImportRequest {
  kind: "import";
  requestId: number;
  payload: {
    dbId: number;
    bytes: ArrayBuffer;
  };
}

export interface PersistRequest {
  kind: "persist";
  requestId: number;
  payload: {
    dbId: number;
  };
}

export interface MetricsRequest {
  kind: "metrics";
  requestId: number;
  payload: {
    dbId: number;
  };
}

export interface SyncConfigurePeerRequest {
  kind: "sync_configure_peer";
  requestId: number;
  payload: {
    dbId: number;
    name: string;
    endpoint: string;
  };
}

export interface SyncApplyChangesetRequest {
  kind: "sync_apply_changeset";
  requestId: number;
  payload: {
    dbId: number;
    changeset: unknown;
    options?: Record<string, unknown>;
  };
}

export interface SyncRunRequest {
  kind: "sync_run";
  requestId: number;
  payload: {
    dbId: number;
    peer: string;
    direction: "push" | "pull" | "both";
    timeoutMs?: number;
  };
}

export type RpcRequest =
  | OpenRequest
  | CloseRequest
  | ExecRequest
  | QueryRequest
  | PrepareRequest
  | StatementBindRequest
  | StatementStepRequest
  | StatementResetRequest
  | StatementClearBindingsRequest
  | StatementPageRequest
  | StatementCloseRequest
  | CheckpointRequest
  | ExportRequest
  | ImportRequest
  | PersistRequest
  | MetricsRequest
  | SyncConfigurePeerRequest
  | SyncApplyChangesetRequest
  | SyncRunRequest;

export interface OpenResult {
  dbId: number;
  path: string;
  mode: OpenMode;
  runtime: OwnerRuntime;
  ownerId: string;
  coordinationModel: CoordinationModel;
  attachedClientCount: number;
  protocolVersion: number;
  engineVersion?: string;
  parserProfile: BrowserSqlProfile;
  capabilities: BrowserCapabilities;
  engineReady: boolean;
  notes?: string[];
}

export interface ExecResult {
  rowCount: number;
  changedRows?: number;
}

export interface QueryResult {
  columns: string[];
  rows: QueryRow[];
}

export interface PrepareResult {
  statementId: number;
  sql: string;
}

export interface StatementStepResult {
  hasRow: boolean;
  row?: QueryRow;
}

export interface StatementPageResult {
  columns: string[];
  rows: QueryRow[];
  done: boolean;
}

export interface CheckpointResult {
  truncatedWalBytes?: number;
  checkpointedAtMs?: number;
}

export interface ExportResult {
  bytes: ArrayBuffer;
  size: number;
  exportedAtMs?: number;
}

export interface PersistResult {
  persisted: boolean;
}

export interface MetricsResult {
  wasmMemoryBytes?: number;
  wasmMemoryPages?: number;
  jsHeapBytes?: number;
  opfsSupported?: boolean;
  opfsSyncAccessHandleSupported?: boolean;
  persistentStorageGranted?: boolean;
  quotaBytes?: number;
  storageUsageBytes?: number;
  ownerId?: string;
  ownerRuntime?: OwnerRuntime;
  attachedClientCount?: number;
  staleOwnerRecoveries?: number;
  coordinationModel?: string;
  parserProfile?: string;
  protocolVersion?: number;
  engineVersion?: string;
  capabilities?: BrowserCapabilities;
  syncConfiguredPeers?: number;
  syncDeferred?: boolean;
  syncRelayHttpPull?: boolean;
  syncRelayWebSocketShapes?: boolean;
  lastCheckpointMs?: number;
  lastExportMs?: number;
  lastImportMs?: number;
  storagePressure?: "unknown" | "ok" | "warning" | "critical";
}

export interface SyncApplyChangesetResult {
  outcome: string;
  changeset_id?: string;
  changesetId?: string;
  rows_seen?: number;
  rows_applied?: number;
  rows_skipped?: number;
  rows_conflicted?: number;
  checkpoint_after?: number;
  [key: string]: unknown;
}

export interface SyncRunResult {
  status: "deferred";
  message: string;
}

export interface RpcResponse {
  requestId: number;
  kind: RpcKind;
  ok: boolean;
  result?:
    | OpenResult
    | ExecResult
    | QueryResult
    | PrepareResult
    | StatementStepResult
    | StatementPageResult
    | CheckpointResult
    | ExportResult
    | PersistResult
    | MetricsResult
    | SyncApplyChangesetResult
    | SyncRunResult;
  error?: QueryErrorPayload;
}

export const ERR_ENGINE_NOT_AVAILABLE = "ERR_BROWSER_WASM_EXPORT_NOT_AVAILABLE";
export const ERR_OPERATION_FAILED = "ERR_BROWSER_OPERATION_FAILED";
export const ERR_NOT_FOUND = "ERR_BROWSER_HANDLE_NOT_FOUND";
export const ERR_BROWSER_UNSUPPORTED = "ERR_BROWSER_UNSUPPORTED";
export const ERR_BROWSER_OPFS_UNAVAILABLE = "ERR_BROWSER_OPFS_UNAVAILABLE";
export const ERR_BROWSER_SYNC_ACCESS_HANDLE_UNAVAILABLE =
  "ERR_BROWSER_SYNC_ACCESS_HANDLE_UNAVAILABLE";
export const ERR_BROWSER_COORDINATION_UNAVAILABLE =
  "ERR_BROWSER_COORDINATION_UNAVAILABLE";
export const ERR_BROWSER_OWNER_TIMEOUT = "ERR_BROWSER_OWNER_TIMEOUT";
export const ERR_BROWSER_OWNER_STALE = "ERR_BROWSER_OWNER_STALE";
export const ERR_BROWSER_OWNER_RECOVERY_FAILED = "ERR_BROWSER_OWNER_RECOVERY_FAILED";
export const ERR_BROWSER_STORAGE_PERSISTENCE_DENIED =
  "ERR_BROWSER_STORAGE_PERSISTENCE_DENIED";
export const ERR_BROWSER_QUOTA_EXCEEDED = "ERR_BROWSER_QUOTA_EXCEEDED";
export const ERR_BROWSER_PRIVATE_MODE_UNSUPPORTED =
  "ERR_BROWSER_PRIVATE_MODE_UNSUPPORTED";
export const ERR_BROWSER_SQL_PROFILE_UNSUPPORTED =
  "ERR_BROWSER_SQL_PROFILE_UNSUPPORTED";
export const ERR_BROWSER_SQL_UNSUPPORTED = "ERR_BROWSER_SQL_UNSUPPORTED";
export const ERR_BROWSER_SQL_PARSE = "ERR_BROWSER_SQL_PARSE";
export const ERR_BROWSER_SQL_PROFILE_MISMATCH =
  "ERR_BROWSER_SQL_PROFILE_MISMATCH";
export const ERR_BROWSER_DB_CLOSED = "ERR_BROWSER_DB_CLOSED";
export const ERR_BROWSER_STATEMENT_CLOSED = "ERR_BROWSER_STATEMENT_CLOSED";
export const ERR_BROWSER_ACTIVE_STATEMENTS = "ERR_BROWSER_ACTIVE_STATEMENTS";
export const ERR_BROWSER_TRANSACTION_ACTIVE = "ERR_BROWSER_TRANSACTION_ACTIVE";
export const ERR_BROWSER_BRANCH_UNSUPPORTED = "ERR_BROWSER_BRANCH_UNSUPPORTED";
export const ERR_BROWSER_TDE_UNSUPPORTED = "ERR_BROWSER_TDE_UNSUPPORTED";
export const ERR_BROWSER_PROTOCOL_MISMATCH = "ERR_BROWSER_PROTOCOL_MISMATCH";
export const ERR_BROWSER_SERVICE_WORKER_UNSUPPORTED =
  "ERR_BROWSER_SERVICE_WORKER_UNSUPPORTED";
export const ERR_BROWSER_SYNC_DEFERRED = "ERR_BROWSER_SYNC_DEFERRED";
export const ERR_BROWSER_PROBE_FAILED = "ERR_BROWSER_PROBE_FAILED";

export function createErrorPayload(code: string, message: string, details?: string): QueryErrorPayload {
  return {
    code,
    message,
    details,
  };
}

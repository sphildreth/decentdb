export type OpenMode = "openOrCreate" | "open" | "create";
export type ResultTransport = "binary" | "json";
export type OwnerRuntime = "dedicated-worker" | "remote-owner";
export type CoordinationModel = "broadcastchannel-weblocks-dedicated-owner";

export type RpcKind =
  | "open"
  | "close"
  | "exec"
  | "query"
  | "prepare"
  | "statement_bind"
  | "statement_step"
  | "statement_close"
  | "checkpoint"
  | "export"
  | "import"
  | "persist"
  | "metrics"
  | "sync_configure_peer"
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
  | StatementCloseRequest
  | CheckpointRequest
  | ExportRequest
  | ImportRequest
  | PersistRequest
  | MetricsRequest
  | SyncConfigurePeerRequest
  | SyncRunRequest;

export interface OpenResult {
  dbId: number;
  path: string;
  mode: OpenMode;
  runtime: OwnerRuntime;
  ownerId: string;
  coordinationModel: CoordinationModel;
  attachedClientCount: number;
  parserProfile: string;
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

export interface CheckpointResult {
  truncatedWalBytes?: number;
}

export interface ExportResult {
  bytes: ArrayBuffer;
  size: number;
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
  syncConfiguredPeers?: number;
  syncDeferred?: boolean;
  syncRelayHttpPull?: boolean;
  syncRelayWebSocketShapes?: boolean;
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
    | CheckpointResult
    | ExportResult
    | PersistResult
    | MetricsResult
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

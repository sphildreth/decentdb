export type OpenMode = "openOrCreate" | "open" | "create";
export type ResultTransport = "binary" | "json";

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
  | "metrics";

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
  | MetricsRequest;

export interface OpenResult {
  dbId: number;
  path: string;
  mode: OpenMode;
  runtime: "worker";
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
}

export interface RpcResponse {
  requestId: number;
  kind: RpcKind;
  ok: boolean;
  result?: OpenResult | ExecResult | QueryResult | PrepareResult | StatementStepResult | CheckpointResult | ExportResult | PersistResult | MetricsResult;
  error?: QueryErrorPayload;
}

export const ERR_ENGINE_NOT_AVAILABLE = "ERR_BROWSER_WASM_EXPORT_NOT_AVAILABLE";
export const ERR_OPERATION_FAILED = "ERR_BROWSER_OPERATION_FAILED";
export const ERR_NOT_FOUND = "ERR_BROWSER_HANDLE_NOT_FOUND";

export function createErrorPayload(code: string, message: string, details?: string): QueryErrorPayload {
  return {
    code,
    message,
    details,
  };
}

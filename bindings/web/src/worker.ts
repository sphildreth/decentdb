import { installOpfsHost, prepareDatabase, replaceDatabaseBytes } from "./opfs-host.js";
import { decodeBinaryResult } from "./binary.js";
import {
  ERR_ENGINE_NOT_AVAILABLE,
  ERR_NOT_FOUND,
  ERR_OPERATION_FAILED,
  type CheckpointResult,
  type ExecResult,
  type ExportResult,
  type MetricsResult,
  type OpenMode,
  type OpenResult,
  type PersistResult,
  type QueryErrorPayload,
  type QueryResult,
  QueryRow,
  type QueryValue,
  type ResultTransport,
  type RpcKind,
  type RpcRequest,
  type RpcResponse,
  type StatementStepResult,
  createErrorPayload,
} from "./protocol.js";

type WasmDb = {
  execJson(sql: string, paramsJson: string): string;
  execBinary?: (sql: string, paramsJson: string) => Uint8Array;
  checkpoint(): void;
  exportBytes(): Uint8Array;
  importBytes(bytes: Uint8Array): void;
  close(): void;
};

type WasmModule = {
  default?: (input?: unknown) => Promise<WasmInitOutput>;
  decentdbOpen(path: string, mode: OpenMode): WasmDb;
  decentdbVersion?: () => string;
};

type WasmInitOutput = {
  memory?: WebAssembly.Memory;
};

type LoadedWasmModule = {
  module: WasmModule;
  memory?: WebAssembly.Memory;
};

type EngineResult = {
  columns: string[];
  rows: QueryRow[];
  affectedRows: number;
};

type DatabaseRecord = {
  path: string;
  mode: OpenMode;
  wasmUrl: string;
  wasmMemory?: WebAssembly.Memory;
  resultTransport: ResultTransport;
  handle: WasmDb;
};

type StatementRecord = {
  dbId: number;
  sql: string;
  params: QueryValue[];
  stepped: boolean;
};

interface DbState {
  nextDbId: number;
  nextStmtId: number;
  dbById: Map<number, DatabaseRecord>;
  stmtToDb: Map<number, StatementRecord>;
  wasmByUrl: Map<string, Promise<LoadedWasmModule>>;
}

const state: DbState = {
  nextDbId: 1,
  nextStmtId: 1,
  dbById: new Map<number, DatabaseRecord>(),
  stmtToDb: new Map<number, StatementRecord>(),
  wasmByUrl: new Map<string, Promise<LoadedWasmModule>>(),
};

installOpfsHost();

function wasmUrlFromRequest(request: Extract<RpcRequest, { kind: "open" }>): string {
  return request.payload.options?.wasmUrl ?? new URL("./decentdb_wasm.js", import.meta.url).toString();
}

async function loadWasm(wasmUrl: string): Promise<LoadedWasmModule> {
  let existing = state.wasmByUrl.get(wasmUrl);
  if (!existing) {
    existing = import(/* @vite-ignore */ wasmUrl)
      .then(async (module: WasmModule) => {
        let initOutput: WasmInitOutput | undefined;
        if (typeof module.default === "function") {
          initOutput = await module.default();
        }
        if (typeof module.decentdbOpen !== "function") {
          throw new Error("WASM module does not export decentdbOpen(path, mode).");
        }
        return {
          module,
          memory: initOutput?.memory,
        };
      })
      .catch((error: unknown) => {
        throw createErrorPayload(
          ERR_ENGINE_NOT_AVAILABLE,
          "Browser WASM exports could not be loaded.",
          error instanceof Error ? error.message : String(error)
        );
      });
    state.wasmByUrl.set(wasmUrl, existing);
  }
  return existing;
}

function withDatabase(dbId: number): DatabaseRecord {
  const db = state.dbById.get(dbId);
  if (!db) {
    throw createErrorPayload(
      ERR_NOT_FOUND,
      `Unknown database handle ${dbId}.`,
      "Open the database first via open(), then retry."
    );
  }
  return db;
}

function withStatement(statementId: number): StatementRecord {
  const statement = state.stmtToDb.get(statementId);
  if (!statement) {
    throw createErrorPayload(
      ERR_NOT_FOUND,
      `Unknown statement handle ${statementId}.`,
      "Prepare a statement again or use a valid handle."
    );
  }
  return statement;
}

function errorResponse(requestId: number, kind: RpcKind, error: QueryErrorPayload): RpcResponse {
  return {
    requestId,
    kind,
    ok: false,
    error,
  };
}

function successResponse(requestId: number, kind: RpcKind, result: RpcResponse["result"]): RpcResponse {
  return {
    requestId,
    kind,
    ok: true,
    result,
  };
}

function paramsJson(params?: QueryValue[]): string {
  const encoded = (params ?? []).map((value) => {
    if (
      value === null ||
      typeof value === "boolean" ||
      typeof value === "number" ||
      typeof value === "string"
    ) {
      return value;
    }
    throw createErrorPayload(
      ERR_OPERATION_FAILED,
      "Unsupported browser parameter value.",
      "The current browser binding accepts null, boolean, number, and string parameters."
    );
  });
  return JSON.stringify(encoded);
}

function runSql(db: DatabaseRecord, sql: string, params?: QueryValue[]): EngineResult {
  if (db.resultTransport === "binary" && typeof db.handle.execBinary === "function") {
    return decodeBinaryResult(db.handle.execBinary(sql, paramsJson(params)));
  }
  const raw = db.handle.execJson(sql, paramsJson(params));
  const parsed = JSON.parse(raw) as EngineResult;
  return {
    columns: parsed.columns ?? [],
    rows: parsed.rows ?? [],
    affectedRows: parsed.affectedRows ?? parsed.rows?.length ?? 0,
  };
}

function toExportResult(bytes: Uint8Array): ExportResult {
  const view = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(view).set(bytes);
  return {
    bytes: view,
    size: bytes.byteLength,
  };
}

async function handleOpen(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "open") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected open request"));
  }
  if (!request.payload.path) {
    return errorResponse(request.requestId, "open", createErrorPayload(ERR_OPERATION_FAILED, "path is required", "Provide a non-empty path"));
  }

  await prepareDatabase(request.payload.path, request.payload.mode);
  const wasmUrl = wasmUrlFromRequest(request);
  const wasm = await loadWasm(wasmUrl);
  const handle = wasm.module.decentdbOpen(request.payload.path, request.payload.mode);
  const dbId = state.nextDbId++;
  state.dbById.set(dbId, {
    path: request.payload.path,
    mode: request.payload.mode,
    wasmUrl,
    wasmMemory: wasm.memory,
    resultTransport: request.payload.options?.resultTransport ?? "binary",
    handle,
  });

  const result: OpenResult = {
    dbId,
    path: request.payload.path,
    mode: request.payload.mode,
    runtime: "worker",
    engineReady: true,
    notes: [
      "The database is owned by this Dedicated Worker.",
      "Writes are serialized through this worker; cross-tab write coordination is not part of v1.",
    ],
  };
  return successResponse(request.requestId, "open", result);
}

async function handleClose(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "close") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected close request"));
  }

  const db = withDatabase(request.payload.dbId);
  db.handle.close();
  for (const [statementId, statement] of state.stmtToDb) {
    if (statement.dbId === request.payload.dbId) {
      state.stmtToDb.delete(statementId);
    }
  }
  state.dbById.delete(request.payload.dbId);
  return successResponse(request.requestId, "close", undefined);
}

async function handleExec(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "exec") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected exec request"));
  }
  const db = withDatabase(request.payload.dbId);
  const engineResult = runSql(db, request.payload.sql, request.payload.params);
  const result: ExecResult = {
    rowCount: engineResult.affectedRows,
    changedRows: engineResult.affectedRows,
  };
  return successResponse(request.requestId, "exec", result);
}

async function handleQuery(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "query") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected query request"));
  }
  const db = withDatabase(request.payload.dbId);
  const engineResult = runSql(db, request.payload.sql, request.payload.params);
  const result: QueryResult = {
    columns: engineResult.columns,
    rows: engineResult.rows,
  };
  return successResponse(request.requestId, "query", result);
}

async function handlePrepare(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "prepare") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected prepare request"));
  }
  const { dbId, sql } = request.payload;
  withDatabase(dbId);

  const statementId = state.nextStmtId++;
  state.stmtToDb.set(statementId, { dbId, sql, params: [], stepped: false });
  return successResponse(request.requestId, "prepare", {
    statementId,
    sql,
  });
}

async function handleStatementBind(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "statement_bind") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected statement_bind request"));
  }
  const statement = withStatement(request.payload.statementId);
  statement.params = request.payload.params ?? [];
  statement.stepped = false;
  return successResponse(request.requestId, "statement_bind", undefined);
}

async function handleStatementStep(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "statement_step") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected statement_step request"));
  }
  const statement = withStatement(request.payload.statementId);
  if (statement.stepped) {
    return successResponse(request.requestId, "statement_step", { hasRow: false });
  }
  statement.stepped = true;
  const db = withDatabase(statement.dbId);
  const result = runSql(db, statement.sql, statement.params);
  const step: StatementStepResult = {
    hasRow: result.rows.length > 0,
    row: result.rows[0],
  };
  return successResponse(request.requestId, "statement_step", step);
}

async function handleStatementClose(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "statement_close") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected statement_close request"));
  }
  const existed = state.stmtToDb.delete(request.payload.statementId);
  if (!existed) {
    return errorResponse(request.requestId, "statement_close", createErrorPayload(ERR_NOT_FOUND, "Unknown statement handle", `No statement ${request.payload.statementId}`));
  }
  return successResponse(request.requestId, "statement_close", undefined);
}

async function handleCheckpoint(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "checkpoint") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected checkpoint request"));
  }
  const db = withDatabase(request.payload.dbId);
  db.handle.checkpoint();
  const result: CheckpointResult = {
    truncatedWalBytes: 0,
  };
  return successResponse(request.requestId, "checkpoint", result);
}

async function handleExport(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "export") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected export request"));
  }
  const db = withDatabase(request.payload.dbId);
  return successResponse(request.requestId, "export", toExportResult(db.handle.exportBytes()));
}

async function handleImport(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "import") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected import request"));
  }
  const db = withDatabase(request.payload.dbId);
  db.handle.close();
  await replaceDatabaseBytes(db.path, new Uint8Array(request.payload.bytes));
  const wasm = await loadWasm(db.wasmUrl);
  db.handle = wasm.module.decentdbOpen(db.path, "openOrCreate");
  db.wasmMemory = wasm.memory;
  return successResponse(request.requestId, "import", undefined);
}

async function handlePersist(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "persist") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected persist request"));
  }
  withDatabase(request.payload.dbId);
  const result: PersistResult = {
    persisted: typeof navigator.storage.persist === "function" ? await navigator.storage.persist() : false,
  };
  return successResponse(request.requestId, "persist", result);
}

async function handleMetrics(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "metrics") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected metrics request"));
  }
  const db = withDatabase(request.payload.dbId);
  const result: MetricsResult = {};
  if (db.wasmMemory) {
    result.wasmMemoryBytes = db.wasmMemory.buffer.byteLength;
    result.wasmMemoryPages = Math.floor(db.wasmMemory.buffer.byteLength / 65_536);
  }
  const performanceWithMemory = performance as Performance & { memory?: { usedJSHeapSize?: number } };
  if (typeof performanceWithMemory.memory?.usedJSHeapSize === "number") {
    result.jsHeapBytes = performanceWithMemory.memory.usedJSHeapSize;
  }
  return successResponse(request.requestId, "metrics", result);
}

async function dispatch(request: RpcRequest): Promise<RpcResponse> {
  switch (request.kind) {
    case "open":
      return handleOpen(request);
    case "close":
      return handleClose(request);
    case "exec":
      return handleExec(request);
    case "query":
      return handleQuery(request);
    case "prepare":
      return handlePrepare(request);
    case "statement_bind":
      return handleStatementBind(request);
    case "statement_step":
      return handleStatementStep(request);
    case "statement_close":
      return handleStatementClose(request);
    case "checkpoint":
      return handleCheckpoint(request);
    case "export":
      return handleExport(request);
    case "import":
      return handleImport(request);
    case "persist":
      return handlePersist(request);
    case "metrics":
      return handleMetrics(request);
    default: {
      const unknownRequest = request as RpcRequest;
      return errorResponse(unknownRequest.requestId, unknownRequest.kind, createErrorPayload(ERR_OPERATION_FAILED, "Unsupported request", `Unhandled kind ${unknownRequest.kind}`));
    }
  }
}

self.onmessage = async (event: MessageEvent<RpcRequest>): Promise<void> => {
  const request = event.data;
  const reply = await dispatch(request).catch((error: unknown): RpcResponse => {
    if (error && typeof error === "object" && "code" in error && "message" in error) {
      return errorResponse(request.requestId, request.kind, error as QueryErrorPayload);
    }
    const details = error instanceof Error ? (error.stack ?? error.message) : String(error);
    console.error("DecentDB worker error", details);
    return errorResponse(
      request.requestId,
      request.kind,
      createErrorPayload(
        ERR_OPERATION_FAILED,
        "Unhandled worker error",
        details
      )
    );
  });

  self.postMessage(reply);
};

export {};

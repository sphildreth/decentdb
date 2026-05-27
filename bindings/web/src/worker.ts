import { installOpfsHost, prepareDatabase, replaceDatabaseBytes } from "./opfs-host.js";
import { decodeBinaryResult } from "./binary.js";
import {
  BROWSER_PROTOCOL_VERSION,
  BROWSER_SQL_PROFILE,
  type BrowserCapabilities,
  type BrowserSqlProfile,
  ERR_BROWSER_ACTIVE_STATEMENTS,
  ERR_BROWSER_OPFS_UNAVAILABLE,
  ERR_BROWSER_QUOTA_EXCEEDED,
  ERR_BROWSER_SQL_PARSE,
  ERR_BROWSER_SQL_PROFILE_MISMATCH,
  ERR_BROWSER_SQL_UNSUPPORTED,
  ERR_BROWSER_SYNC_DEFERRED,
  ERR_BROWSER_TRANSACTION_ACTIVE,
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
  type QueryRow,
  type QueryValue,
  type ResultTransport,
  type RpcKind,
  type RpcRequest,
  type RpcResponse,
  type StatementPageResult,
  type StatementStepResult,
  type SyncApplyChangesetResult,
  type SyncRunResult,
  createErrorPayload,
} from "./protocol.js";

type WasmDb = {
  execJson(sql: string, paramsJson: string): string;
  execBinary?: (sql: string, paramsJson: string) => Uint8Array;
  checkpoint(): void;
  exportBytes(): Uint8Array;
  importBytes(bytes: Uint8Array): void;
  syncExecuteJson?: (requestJson: string) => string;
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

type OwnerRecord = {
  ownerDbId: number;
  ownerId: string;
  path: string;
  mode: OpenMode;
  wasmUrl: string;
  wasmMemory?: WebAssembly.Memory;
  resultTransport: ResultTransport;
  parserProfile: BrowserSqlProfile;
  engineVersion?: string;
  capabilities: BrowserCapabilities;
  handle: WasmDb;
  attachedClientCount: number;
  staleOwnerRecoveries: number;
  lastHeartbeatMs: number;
  activeTransaction: boolean;
  lastCheckpointMs?: number;
  lastExportMs?: number;
  lastImportMs?: number;
  syncPeers: Map<string, { endpoint: string }>;
};

type ClientDbHandle = {
  clientDbId: number;
  ownerDbId: number;
};

type StatementRecord = {
  clientDbId: number;
  sql: string;
  params: QueryValue[];
  cached?: EngineResult;
  cursor: number;
};

interface DbState {
  nextOwnerDbId: number;
  nextClientDbId: number;
  nextStmtId: number;
  ownerByDbId: Map<number, OwnerRecord>;
  ownerDbIdByPath: Map<string, number>;
  clientDbById: Map<number, ClientDbHandle>;
  stmtToDb: Map<number, StatementRecord>;
  wasmByUrl: Map<string, Promise<LoadedWasmModule>>;
}

const state: DbState = {
  nextOwnerDbId: 1,
  nextClientDbId: 1,
  nextStmtId: 1,
  ownerByDbId: new Map<number, OwnerRecord>(),
  ownerDbIdByPath: new Map<string, number>(),
  clientDbById: new Map<number, ClientDbHandle>(),
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

function withClientDb(dbId: number): ClientDbHandle {
  const db = state.clientDbById.get(dbId);
  if (!db) {
    throw createErrorPayload(
      ERR_NOT_FOUND,
      `Unknown database handle ${dbId}.`,
      "Open the database first via open(), then retry."
    );
  }
  return db;
}

function withOwner(ownerDbId: number): OwnerRecord {
  const owner = state.ownerByDbId.get(ownerDbId);
  if (!owner) {
    throw createErrorPayload(
      ERR_NOT_FOUND,
      `Unknown owner handle ${ownerDbId}.`,
      "Open the database first via open(), then retry."
    );
  }
  owner.lastHeartbeatMs = Date.now();
  return owner;
}

function withOwnerFromClientDb(clientDbId: number): OwnerRecord {
  const client = withClientDb(clientDbId);
  return withOwner(client.ownerDbId);
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

function browserCapabilities(engineVersion?: string): BrowserCapabilities {
  return {
    protocolVersion: BROWSER_PROTOCOL_VERSION,
    engineVersion,
    parserProfile: BROWSER_SQL_PROFILE,
    resultTransports: ["binary", "json"],
    transactions: true,
    savepoints: true,
    preparedStatements: true,
    statementReset: true,
    statementClearBindings: true,
    statementPaging: true,
    asyncStatementIteration: true,
    importExport: true,
    metrics: true,
    relayHttp: true,
    relayWebSocket: true,
    changesetApply: true,
    branchSnapshots: false,
    browserTdeOpenOptions: false,
    cooperativeCancellation: false,
  };
}

const BROWSER_SQL_ERROR_SEPARATOR = "|";

function mapBrowserSqlError(message: string): QueryErrorPayload | undefined {
  const codeStart = [
    ERR_BROWSER_SQL_UNSUPPORTED,
    ERR_BROWSER_SQL_PARSE,
    ERR_BROWSER_SQL_PROFILE_MISMATCH,
  ]
    .map((code) => message.indexOf(code))
    .filter((index) => index >= 0)
    .sort((left, right) => left - right)[0];
  if (codeStart === undefined) {
    return undefined;
  }
  const normalized = message.slice(codeStart).trim();
  const parts = normalized.split(BROWSER_SQL_ERROR_SEPARATOR);
  if (parts.length < 3) {
    return undefined;
  }
  const code = parts[0];
  const profile = parts[1];
  const feature = parts[2];
  const details = parts.slice(3).join(BROWSER_SQL_ERROR_SEPARATOR);
  if (code === ERR_BROWSER_SQL_UNSUPPORTED) {
    return createErrorPayload(
      ERR_BROWSER_SQL_UNSUPPORTED,
      `Unsupported browser SQL feature for profile '${profile}'.`,
      `Feature: ${feature}${details ? `. ${details}` : ""}`
    );
  }
  if (code === ERR_BROWSER_SQL_PARSE) {
    return createErrorPayload(
      ERR_BROWSER_SQL_PARSE,
      `Browser SQL parse error for profile '${profile}'.`,
      details || feature
    );
  }
  if (code === ERR_BROWSER_SQL_PROFILE_MISMATCH) {
    return createErrorPayload(
      ERR_BROWSER_SQL_PROFILE_MISMATCH,
      `Browser SQL profile mismatch for profile '${profile}'.`,
      `Feature: ${feature}${details ? `. ${details}` : ""}`
    );
  }
  return undefined;
}

function base64Encode(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

function encodeParam(value: QueryValue): unknown {
  if (value === null || typeof value === "boolean" || typeof value === "number" || typeof value === "string") {
    return value;
  }
  if (typeof value === "bigint") {
    return { kind: "int64", value: value.toString() };
  }
  if (value instanceof Uint8Array) {
    return { kind: "bytes", base64: base64Encode(value) };
  }
  if (value instanceof ArrayBuffer) {
    return { kind: "bytes", base64: base64Encode(new Uint8Array(value)) };
  }
  if (typeof value === "object" && value !== null && "kind" in value) {
    return value;
  }
  throw createErrorPayload(
    ERR_BROWSER_SQL_UNSUPPORTED,
    "Unsupported browser parameter value.",
    "Use null, boolean, number, string, bigint, Uint8Array, ArrayBuffer, or a tagged { kind, ... } value."
  );
}

function paramsJson(params?: QueryValue[]): string {
  return JSON.stringify((params ?? []).map((value) => encodeParam(value)));
}

function estimateDiagnostics(): Promise<{ quotaBytes?: number; usageBytes?: number }> {
  const maybeStorage = navigator.storage as StorageManager;
  if (typeof maybeStorage.estimate !== "function") {
    return Promise.resolve({});
  }
  return maybeStorage
    .estimate()
    .then((estimate) => ({
      quotaBytes: estimate.quota,
      usageBytes: estimate.usage,
    }))
    .catch(() => ({}));
}

function storagePressure(diagnostics: { quotaBytes?: number; usageBytes?: number }): "unknown" | "ok" | "warning" | "critical" {
  if (!diagnostics.quotaBytes || diagnostics.quotaBytes <= 0 || diagnostics.usageBytes === undefined) {
    return "unknown";
  }
  const ratio = diagnostics.usageBytes / diagnostics.quotaBytes;
  if (ratio >= 0.95) {
    return "critical";
  }
  if (ratio >= 0.8) {
    return "warning";
  }
  return "ok";
}

function runBrowserSystemView(owner: OwnerRecord, sql: string): EngineResult | undefined {
  const normalized = sql.trim().replace(/\s+/g, " ").toLowerCase();
  if (!normalized.startsWith("select * from sys.browser_")) {
    return undefined;
  }

  if (normalized === "select * from sys.browser_runtime") {
    return {
      columns: [
        "owner_id",
        "runtime",
        "coordination_model",
        "protocol_version",
        "engine_version",
        "parser_profile",
        "attached_client_count",
        "stale_owner_recoveries",
        "transaction_active",
      ],
      rows: [
        {
          owner_id: owner.ownerId,
          runtime: "dedicated-worker",
          coordination_model: "broadcastchannel-weblocks-dedicated-owner",
          protocol_version: BROWSER_PROTOCOL_VERSION,
          engine_version: owner.engineVersion ?? null,
          parser_profile: owner.parserProfile,
          attached_client_count: owner.attachedClientCount,
          stale_owner_recoveries: owner.staleOwnerRecoveries,
          transaction_active: owner.activeTransaction,
        },
      ],
      affectedRows: 1,
    };
  }

  if (normalized === "select * from sys.browser_owner") {
    return {
      columns: ["owner_id", "database_path", "attached_client_count", "last_heartbeat_ms"],
      rows: [
        {
          owner_id: owner.ownerId,
          database_path: owner.path,
          attached_client_count: owner.attachedClientCount,
          last_heartbeat_ms: owner.lastHeartbeatMs,
        },
      ],
      affectedRows: 1,
    };
  }

  if (normalized === "select * from sys.browser_storage") {
    return {
      columns: [
        "opfs_supported",
        "sync_access_handle_supported",
        "persistence_api_supported",
        "last_checkpoint_ms",
        "last_export_ms",
        "last_import_ms",
      ],
      rows: [
        {
          opfs_supported: true,
          sync_access_handle_supported: true,
          persistence_api_supported: typeof navigator.storage.persist === "function",
          last_checkpoint_ms: owner.lastCheckpointMs ?? null,
          last_export_ms: owner.lastExportMs ?? null,
          last_import_ms: owner.lastImportMs ?? null,
        },
      ],
      affectedRows: 1,
    };
  }

  if (normalized === "select * from sys.browser_sync") {
    return {
      columns: ["configured_peers", "relay_http_pull", "relay_websocket_shapes", "legacy_run_deferred"],
      rows: [
        {
          configured_peers: owner.syncPeers.size,
          relay_http_pull: true,
          relay_websocket_shapes: true,
          legacy_run_deferred: true,
        },
      ],
      affectedRows: 1,
    };
  }

  return undefined;
}

function normalizedSql(sql: string): string {
  return sql.trim().replace(/;+\s*$/, "").replace(/\s+/g, " ").toUpperCase();
}

function noteTransactionState(owner: OwnerRecord, sql: string): void {
  const normalized = normalizedSql(sql);
  if (
    normalized === "BEGIN" ||
    normalized === "BEGIN TRANSACTION" ||
    normalized === "BEGIN DEFERRED" ||
    normalized === "BEGIN DEFERRED TRANSACTION" ||
    normalized === "BEGIN IMMEDIATE" ||
    normalized === "BEGIN IMMEDIATE TRANSACTION" ||
    normalized === "BEGIN EXCLUSIVE" ||
    normalized === "BEGIN EXCLUSIVE TRANSACTION"
  ) {
    owner.activeTransaction = true;
    return;
  }
  if (
    normalized === "COMMIT" ||
    normalized === "END" ||
    normalized === "END TRANSACTION" ||
    normalized === "ROLLBACK" ||
    normalized === "ROLLBACK TRANSACTION"
  ) {
    owner.activeTransaction = false;
  }
}

function runSql(owner: OwnerRecord, sql: string, params?: QueryValue[]): EngineResult {
  const systemView = runBrowserSystemView(owner, sql);
  if (systemView) {
    return systemView;
  }

  try {
    if (owner.resultTransport === "binary" && typeof owner.handle.execBinary === "function") {
      const result = decodeBinaryResult(owner.handle.execBinary(sql, paramsJson(params)));
      noteTransactionState(owner, sql);
      return result;
    }
    const raw = owner.handle.execJson(sql, paramsJson(params));
    const parsed = JSON.parse(raw) as EngineResult;
    noteTransactionState(owner, sql);
    return {
      columns: parsed.columns ?? [],
      rows: parsed.rows ?? [],
      affectedRows: parsed.affectedRows ?? parsed.rows?.length ?? 0,
    };
  } catch (error) {
    const normalized = normalizedSql(sql);
    if (normalized === "ROLLBACK" || normalized === "ROLLBACK TRANSACTION") {
      owner.activeTransaction = false;
    }
    throw error;
  }
}

function toExportResult(bytes: Uint8Array): ExportResult {
  const view = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(view).set(bytes);
  return {
    bytes: view,
    size: bytes.byteLength,
  };
}

function createOwnerId(): string {
  return `owner_${Math.random().toString(36).slice(2)}_${Date.now().toString(36)}`;
}

async function handleOpen(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "open") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected open request"));
  }
  if (!request.payload.path) {
    return errorResponse(request.requestId, "open", createErrorPayload(ERR_OPERATION_FAILED, "path is required", "Provide a non-empty path"));
  }

  const path = request.payload.path;
  const resultTransport = request.payload.options?.resultTransport ?? "binary";
  let owner = state.ownerByDbId.get(state.ownerDbIdByPath.get(path) ?? -1);

  if (!owner) {
    await prepareDatabase(path, request.payload.mode).catch((error) => {
      throw createErrorPayload(
        ERR_BROWSER_OPFS_UNAVAILABLE,
        "Could not prepare OPFS database files.",
        error instanceof Error ? error.message : String(error)
      );
    });
    const wasmUrl = wasmUrlFromRequest(request);
    const wasm = await loadWasm(wasmUrl);
    const handle = wasm.module.decentdbOpen(path, request.payload.mode);
    const engineVersion = wasm.module.decentdbVersion?.();
    owner = {
      ownerDbId: state.nextOwnerDbId++,
      ownerId: createOwnerId(),
      path,
      mode: request.payload.mode,
      wasmUrl,
      wasmMemory: wasm.memory,
      resultTransport,
      parserProfile: BROWSER_SQL_PROFILE,
      engineVersion,
      capabilities: browserCapabilities(engineVersion),
      handle,
      attachedClientCount: 0,
      staleOwnerRecoveries: 0,
      lastHeartbeatMs: Date.now(),
      activeTransaction: false,
      syncPeers: new Map<string, { endpoint: string }>(),
    };
    state.ownerByDbId.set(owner.ownerDbId, owner);
    state.ownerDbIdByPath.set(path, owner.ownerDbId);
  }

  owner.attachedClientCount += 1;
  const clientDbId = state.nextClientDbId++;
  state.clientDbById.set(clientDbId, {
    clientDbId,
    ownerDbId: owner.ownerDbId,
  });

  const result: OpenResult = {
    dbId: clientDbId,
    path: owner.path,
    mode: owner.mode,
    runtime: "dedicated-worker",
    ownerId: owner.ownerId,
    coordinationModel: "broadcastchannel-weblocks-dedicated-owner",
    attachedClientCount: owner.attachedClientCount,
    protocolVersion: BROWSER_PROTOCOL_VERSION,
    engineVersion: owner.engineVersion,
    parserProfile: owner.parserProfile,
    capabilities: owner.capabilities,
    engineReady: true,
    notes: [
      "This logical database path is routed through a Dedicated Worker owner.",
      "BroadcastChannel routing and Web Locks prevent competing browser owners.",
      "Service workers cannot own DecentDB browser handles.",
    ],
  };
  return successResponse(request.requestId, "open", result);
}

async function handleClose(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "close") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected close request"));
  }

  const client = withClientDb(request.payload.dbId);
  const owner = withOwner(client.ownerDbId);

  for (const [statementId, statement] of state.stmtToDb) {
    if (statement.clientDbId === request.payload.dbId) {
      return errorResponse(
        request.requestId,
        "close",
        createErrorPayload(
          ERR_BROWSER_ACTIVE_STATEMENTS,
          "Cannot close a browser database while prepared statements are active.",
          `Close statement ${statementId} before closing the database.`
        )
      );
    }
  }

  if (owner.activeTransaction) {
    return errorResponse(
      request.requestId,
      "close",
      createErrorPayload(
        ERR_BROWSER_TRANSACTION_ACTIVE,
        "Cannot close a browser database with an active transaction.",
        "Commit or roll back the transaction before closing the database."
      )
    );
  }

  state.clientDbById.delete(request.payload.dbId);
  owner.attachedClientCount = Math.max(0, owner.attachedClientCount - 1);
  if (owner.attachedClientCount === 0) {
    owner.handle.close();
    state.ownerByDbId.delete(owner.ownerDbId);
    state.ownerDbIdByPath.delete(owner.path);
  }
  return successResponse(request.requestId, "close", undefined);
}

async function handleExec(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "exec") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected exec request"));
  }
  const owner = withOwnerFromClientDb(request.payload.dbId);
  const engineResult = runSql(owner, request.payload.sql, request.payload.params);
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
  const owner = withOwnerFromClientDb(request.payload.dbId);
  const engineResult = runSql(owner, request.payload.sql, request.payload.params);
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
  withOwnerFromClientDb(request.payload.dbId);

  const statementId = state.nextStmtId++;
  state.stmtToDb.set(statementId, {
    clientDbId: request.payload.dbId,
    sql: request.payload.sql,
    params: [],
    cursor: 0,
  });
  return successResponse(request.requestId, "prepare", {
    statementId,
    sql: request.payload.sql,
  });
}

async function handleStatementBind(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "statement_bind") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected statement_bind request"));
  }
  const statement = withStatement(request.payload.statementId);
  statement.params = request.payload.params ?? [];
  statement.cached = undefined;
  statement.cursor = 0;
  return successResponse(request.requestId, "statement_bind", undefined);
}

async function handleStatementStep(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "statement_step") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected statement_step request"));
  }
  const statement = withStatement(request.payload.statementId);
  const owner = withOwnerFromClientDb(statement.clientDbId);
  if (!statement.cached) {
    statement.cached = runSql(owner, statement.sql, statement.params);
    statement.cursor = 0;
  }
  if (statement.cursor >= statement.cached.rows.length) {
    return successResponse(request.requestId, "statement_step", { hasRow: false });
  }
  const row = statement.cached.rows[statement.cursor];
  statement.cursor += 1;
  const step: StatementStepResult = {
    hasRow: true,
    row,
  };
  return successResponse(request.requestId, "statement_step", step);
}

async function handleStatementReset(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "statement_reset") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected statement_reset request"));
  }
  const statement = withStatement(request.payload.statementId);
  statement.cached = undefined;
  statement.cursor = 0;
  return successResponse(request.requestId, "statement_reset", undefined);
}

async function handleStatementClearBindings(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "statement_clear_bindings") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected statement_clear_bindings request"));
  }
  const statement = withStatement(request.payload.statementId);
  statement.params = [];
  statement.cached = undefined;
  statement.cursor = 0;
  return successResponse(request.requestId, "statement_clear_bindings", undefined);
}

async function handleStatementPage(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "statement_page") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected statement_page request"));
  }
  const statement = withStatement(request.payload.statementId);
  const pageSize = Math.max(1, Math.min(10_000, request.payload.pageSize));
  const owner = withOwnerFromClientDb(statement.clientDbId);
  if (!statement.cached) {
    statement.cached = runSql(owner, statement.sql, statement.params);
    statement.cursor = 0;
  }
  const start = statement.cursor;
  const end = Math.min(statement.cached.rows.length, start + pageSize);
  statement.cursor = end;
  const result: StatementPageResult = {
    columns: statement.cached.columns,
    rows: statement.cached.rows.slice(start, end),
    done: statement.cursor >= statement.cached.rows.length,
  };
  return successResponse(request.requestId, "statement_page", result);
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
  const owner = withOwnerFromClientDb(request.payload.dbId);
  owner.handle.checkpoint();
  owner.lastCheckpointMs = Date.now();
  const result: CheckpointResult = {
    truncatedWalBytes: 0,
    checkpointedAtMs: owner.lastCheckpointMs,
  };
  return successResponse(request.requestId, "checkpoint", result);
}

async function handleExport(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "export") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected export request"));
  }
  const owner = withOwnerFromClientDb(request.payload.dbId);
  owner.lastExportMs = Date.now();
  return successResponse(request.requestId, "export", {
    ...toExportResult(owner.handle.exportBytes()),
    exportedAtMs: owner.lastExportMs,
  });
}

async function handleImport(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "import") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected import request"));
  }
  const owner = withOwnerFromClientDb(request.payload.dbId);
  if (owner.attachedClientCount > 1) {
    return errorResponse(
      request.requestId,
      "import",
      createErrorPayload(
        ERR_OPERATION_FAILED,
        "Import requires exclusive ownership.",
        "Close other attached clients before importing a replacement database image."
      )
    );
  }
  for (const [, statement] of state.stmtToDb) {
    if (statement.clientDbId === request.payload.dbId) {
      return errorResponse(
        request.requestId,
        "import",
        createErrorPayload(
          ERR_BROWSER_ACTIVE_STATEMENTS,
          "Import requires no active prepared statements.",
          "Close prepared statements before importing a replacement database image."
        )
      );
    }
  }
  if (owner.activeTransaction) {
    return errorResponse(
      request.requestId,
      "import",
      createErrorPayload(
        ERR_BROWSER_TRANSACTION_ACTIVE,
        "Import requires no active transaction.",
        "Commit or roll back before importing a replacement database image."
      )
    );
  }

  owner.handle.close();
  await replaceDatabaseBytes(owner.path, new Uint8Array(request.payload.bytes));
  const wasm = await loadWasm(owner.wasmUrl);
  owner.handle = wasm.module.decentdbOpen(owner.path, "openOrCreate");
  owner.wasmMemory = wasm.memory;
  owner.lastImportMs = Date.now();
  return successResponse(request.requestId, "import", undefined);
}

async function handlePersist(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "persist") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected persist request"));
  }
  withOwnerFromClientDb(request.payload.dbId);
  const persisted = typeof navigator.storage.persist === "function" ? await navigator.storage.persist() : false;
  const result: PersistResult = {
    persisted,
  };
  return successResponse(request.requestId, "persist", result);
}

async function handleMetrics(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "metrics") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected metrics request"));
  }
  const owner = withOwnerFromClientDb(request.payload.dbId);
  const diagnostics = await estimateDiagnostics();
  const persisted = typeof navigator.storage.persisted === "function" ? await navigator.storage.persisted().catch(() => undefined) : undefined;
  const result: MetricsResult = {
    opfsSupported: true,
    opfsSyncAccessHandleSupported: true,
    persistentStorageGranted: persisted,
    quotaBytes: diagnostics.quotaBytes,
    storageUsageBytes: diagnostics.usageBytes,
    ownerId: owner.ownerId,
    ownerRuntime: "dedicated-worker",
    attachedClientCount: owner.attachedClientCount,
    staleOwnerRecoveries: owner.staleOwnerRecoveries,
    coordinationModel: "broadcastchannel-weblocks-dedicated-owner",
    parserProfile: owner.parserProfile,
    protocolVersion: BROWSER_PROTOCOL_VERSION,
    engineVersion: owner.engineVersion,
    capabilities: owner.capabilities,
    syncConfiguredPeers: owner.syncPeers.size,
    syncDeferred: true,
    syncRelayHttpPull: true,
    syncRelayWebSocketShapes: true,
    lastCheckpointMs: owner.lastCheckpointMs,
    lastExportMs: owner.lastExportMs,
    lastImportMs: owner.lastImportMs,
    storagePressure: storagePressure(diagnostics),
  };
  if (owner.wasmMemory) {
    result.wasmMemoryBytes = owner.wasmMemory.buffer.byteLength;
    result.wasmMemoryPages = Math.floor(owner.wasmMemory.buffer.byteLength / 65_536);
  }
  const performanceWithMemory = performance as Performance & { memory?: { usedJSHeapSize?: number } };
  if (typeof performanceWithMemory.memory?.usedJSHeapSize === "number") {
    result.jsHeapBytes = performanceWithMemory.memory.usedJSHeapSize;
  }
  return successResponse(request.requestId, "metrics", result);
}

async function handleSyncConfigurePeer(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "sync_configure_peer") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected sync_configure_peer request"));
  }
  const owner = withOwnerFromClientDb(request.payload.dbId);
  owner.syncPeers.set(request.payload.name, { endpoint: request.payload.endpoint });
  return successResponse(request.requestId, "sync_configure_peer", undefined);
}

async function handleSyncApplyChangeset(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "sync_apply_changeset") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected sync_apply_changeset request"));
  }
  const owner = withOwnerFromClientDb(request.payload.dbId);
  if (typeof owner.handle.syncExecuteJson !== "function") {
    return errorResponse(
      request.requestId,
      "sync_apply_changeset",
      createErrorPayload(
        ERR_OPERATION_FAILED,
        "Browser WASM module does not expose sync changeset APIs.",
        "Rebuild the wasm artifact from a DecentDB version with syncExecuteJson."
      )
    );
  }
  const raw = owner.handle.syncExecuteJson(
    JSON.stringify({
      op: "changeset_apply",
      changeset: request.payload.changeset,
      options: request.payload.options ?? {},
    })
  );
  const result = JSON.parse(raw) as SyncApplyChangesetResult;
  return successResponse(request.requestId, "sync_apply_changeset", result);
}

async function handleSyncRun(request: RpcRequest): Promise<RpcResponse> {
  if (request.kind !== "sync_run") {
    return errorResponse(request.requestId, request.kind, createErrorPayload(ERR_OPERATION_FAILED, "Wrong handler", "Expected sync_run request"));
  }
  const owner = withOwnerFromClientDb(request.payload.dbId);
  if (!owner.syncPeers.has(request.payload.peer)) {
    return errorResponse(
      request.requestId,
      "sync_run",
      createErrorPayload(
        ERR_BROWSER_SYNC_DEFERRED,
        `Sync peer '${request.payload.peer}' is not configured for this runtime owner.`,
        "Configure a peer first; browser transport remains a deferred shell in this release."
      )
    );
  }
  const result: SyncRunResult = {
    status: "deferred",
    message: "Browser sync transport is deferred. Owner-routed API shell is available for forward compatibility.",
  };
  return successResponse(request.requestId, "sync_run", result);
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
    case "statement_reset":
      return handleStatementReset(request);
    case "statement_clear_bindings":
      return handleStatementClearBindings(request);
    case "statement_page":
      return handleStatementPage(request);
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
    case "sync_configure_peer":
      return handleSyncConfigurePeer(request);
    case "sync_apply_changeset":
      return handleSyncApplyChangeset(request);
    case "sync_run":
      return handleSyncRun(request);
    default: {
      const unknownRequest = request as RpcRequest;
      return errorResponse(unknownRequest.requestId, unknownRequest.kind, createErrorPayload(ERR_OPERATION_FAILED, "Unsupported request", `Unhandled kind ${unknownRequest.kind}`));
    }
  }
}

async function handleRequest(request: RpcRequest, post: (response: RpcResponse) => void): Promise<void> {
  const reply = await dispatch(request).catch((error: unknown): RpcResponse => {
    if (error && typeof error === "object" && "code" in error && "message" in error) {
      return errorResponse(request.requestId, request.kind, error as QueryErrorPayload);
    }
    const details = error instanceof Error ? (error.stack ?? error.message) : String(error);
    const browserSqlError = mapBrowserSqlError(details);
    if (browserSqlError) {
      return errorResponse(request.requestId, request.kind, browserSqlError);
    }
    const quotaError = details.toLowerCase().includes("quota") || details.toLowerCase().includes("exceeded");
    return errorResponse(
      request.requestId,
      request.kind,
      createErrorPayload(
        quotaError ? ERR_BROWSER_QUOTA_EXCEEDED : ERR_OPERATION_FAILED,
        quotaError ? "Browser storage quota was exceeded." : "Unhandled worker error",
        details
      )
    );
  });

  post(reply);
}

let requestQueue = Promise.resolve();

function enqueueRequest(request: RpcRequest, post: (response: RpcResponse) => void): void {
  requestQueue = requestQueue.then(
    () => handleRequest(request, post),
    () => handleRequest(request, post)
  );
}

const scope = self as unknown as {
  onmessage?: (event: MessageEvent<RpcRequest>) => void;
};

scope.onmessage = (event: MessageEvent<RpcRequest>): void => {
  enqueueRequest(event.data, (response) => self.postMessage(response));
};

export {};

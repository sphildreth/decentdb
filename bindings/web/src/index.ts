import {
  type CheckpointResult,
  type ExportResult,
  type OpenMode,
  type ResultTransport,
  type OpenResult,
  type PersistResult,
  type QueryRow,
  type QueryValue,
  type RpcKind,
  type RpcRequest,
  type RpcResponse,
  type QueryErrorPayload,
  type ExecResult,
  type QueryResult,
  type PrepareResult,
  createErrorPayload,
  ERR_OPERATION_FAILED,
} from "./protocol";

export type { OpenMode, ResultTransport, QueryRow, QueryValue, OpenResult, ExecResult, QueryResult, CheckpointResult, PersistResult };

export interface OpenOptions {
  path: string;
  mode?: OpenMode;
  sharedMemory?: boolean;
  readOnly?: boolean;
  workerUrl?: string;
  wasmUrl?: string;
  resultTransport?: ResultTransport;
}

export type Params = QueryValue[];

export interface QueryResultShape<T = QueryRow> {
  rowCount: number;
  rows: T[];
  columns: string[];
}

export interface ExecResultShape {
  rowCount: number;
}

export type RequestPayload<K extends RpcKind> = Extract<RpcRequest, { kind: K }>["payload"];

export class DecentDBWebError extends Error {
  public readonly code: string;
  public readonly details?: string;

  constructor(payload: QueryErrorPayload) {
    super(payload.message);
    this.name = "DecentDBWebError";
    this.code = payload.code;
    this.details = payload.details;
  }
}

type Pending = {
  resolve: (value: RpcResponse) => void;
  reject: (error: DecentDBWebError) => void;
};

class RpcTransport {
  private nextRequestId = 1;
  private readonly pending = new Map<number, Pending>();

  constructor(private readonly worker: Worker) {
    this.worker.onmessage = (event) => {
      const response = event.data as RpcResponse;
      const pending = this.pending.get(response.requestId);
      if (!pending) {
        return;
      }
      this.pending.delete(response.requestId);

      if (!response.ok) {
        pending.reject(
          new DecentDBWebError(response.error ?? createErrorPayload(ERR_OPERATION_FAILED, "unknown", "unknown worker error"))
        );
        return;
      }
      pending.resolve(response);
    };

    this.worker.onerror = (event) => {
      for (const [, pending] of this.pending) {
        pending.reject(
          new DecentDBWebError({
            code: "ERR_WORKER_ERROR",
            message: event.message,
            details: "Worker encountered an uncaught exception.",
          })
        );
      }
      this.pending.clear();
    };
  }

  request(request: Omit<RpcRequest, "requestId">): Promise<RpcResponse> {
    const requestId = this.nextRequestId++;
    const envelope = {
      ...request,
      requestId,
    } as Omit<RpcRequest, "requestId"> & { requestId: number };

    return new Promise<RpcResponse>((resolve, reject) => {
      this.pending.set(requestId, { resolve, reject: (error) => reject(error) });
      this.worker.postMessage(envelope);
    }).then((response) => response);
  }

  terminate(): void {
    this.worker.terminate();
    for (const [, pending] of this.pending) {
      pending.reject(
        new DecentDBWebError({
          code: "ERR_WORKER_TERM",
          message: "Worker terminated",
        })
      );
    }
    this.pending.clear();
  }
}

function getWorkerRuntime(workerUrl?: string): RpcTransport {
  if (typeof Worker === "undefined") {
    throw new Error("Worker is not available in this environment.");
  }
  const url = workerUrl ?? new URL("./worker.js", import.meta.url).toString();
  const worker = new Worker(url, { type: "module" });
  return new RpcTransport(worker);
}

export class Statement {
  private closed = false;

  constructor(
    private readonly db: Database,
    private readonly statementId: number,
    private readonly sql: string
  ) {}

  get statement(): string {
    return this.sql;
  }

  async bind(params: Params): Promise<this> {
    this.assertOpen();
    await this.db.execRequest("statement_bind", { statementId: this.statementId, params });
    return this;
  }

  async step<T = QueryRow>(): Promise<T | undefined> {
    this.assertOpen();
    const response = await this.db.execRequest("statement_step", { statementId: this.statementId });
    const step = response.result as { hasRow: boolean; row?: T };
    if (!step?.hasRow || !step.row) {
      return undefined;
    }
    return step.row;
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    await this.db.execRequest("statement_close", { statementId: this.statementId });
    this.closed = true;
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new Error("statement closed");
    }
  }
}

export class Database {
  private closed = false;

  private constructor(
    private readonly transport: RpcTransport,
    private readonly result: OpenResult
  ) {}

  static async open(options: OpenOptions): Promise<Database> {
    const transport = getWorkerRuntime(options.workerUrl);
    const response = await transport.request({
      kind: "open",
      payload: {
        path: options.path,
        mode: options.mode ?? "openOrCreate",
        options: {
          sharedMemory: options.sharedMemory ?? false,
          readOnly: options.readOnly ?? false,
          wasmUrl: options.wasmUrl,
          resultTransport: options.resultTransport ?? "binary",
        },
      },
    });
    const result = response.result;
    if (!result) {
      throw new DecentDBWebError({
        code: "ERR_OPEN_FAILED",
        message: "Open returned no result.",
      });
    }
    return new Database(transport, result as OpenResult);
  }

  get id(): number {
    return this.result.dbId;
  }

  get path(): string {
    return this.result.path;
  }

  get mode(): OpenMode {
    return this.result.mode;
  }

  get isReady(): boolean {
    return this.result.engineReady;
  }

  async exec(sql: string, params?: Params): Promise<ExecResultShape> {
    this.assertOpen();
    const response = await this.execRequest("exec", {
      dbId: this.id,
      sql,
      params,
    });
    const result = response.result as ExecResult | undefined;
    return {
      rowCount: result?.rowCount ?? 0,
    };
  }

  async query<T extends QueryRow = QueryRow>(sql: string, params?: Params): Promise<QueryResultShape<T>> {
    this.assertOpen();
    const response = await this.execRequest("query", {
      dbId: this.id,
      sql,
      params,
    });
    const result = response.result as QueryResult | undefined;
    const rows = (result?.rows ?? []) as T[];
    return {
      rowCount: rows.length,
      columns: result?.columns ?? [],
      rows,
    };
  }

  async prepare(sql: string): Promise<Statement> {
    this.assertOpen();
    const response = await this.execRequest("prepare", {
      dbId: this.id,
      sql,
    });
    const prepared = response.result as PrepareResult | undefined;
    if (!prepared) {
      throw new DecentDBWebError({
        code: "ERR_PREPARE_FAILED",
        message: "Prepare returned no statement handle.",
      });
    }
    return new Statement(this, prepared.statementId, prepared.sql);
  }

  async checkpoint(): Promise<CheckpointResult> {
    this.assertOpen();
    const response = await this.execRequest("checkpoint", { dbId: this.id });
    return (response.result as CheckpointResult) ?? { truncatedWalBytes: 0 };
  }

  async export(): Promise<ExportResult> {
    this.assertOpen();
    const response = await this.execRequest("export", { dbId: this.id });
    const exportResult = response.result as ExportResult | undefined;
    if (!exportResult) {
      throw new DecentDBWebError({
        code: "ERR_EXPORT_FAILED",
        message: "Export returned no data.",
      });
    }
    return exportResult;
  }

  async import(bytes: ArrayBuffer): Promise<void> {
    this.assertOpen();
    await this.execRequest("import", { dbId: this.id, bytes });
  }

  async persist(): Promise<boolean> {
    this.assertOpen();
    const response = await this.execRequest("persist", { dbId: this.id });
    const result = response.result as PersistResult | undefined;
    return result?.persisted ?? false;
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    await this.execRequest("close", { dbId: this.id });
    this.closed = true;
    this.transport.terminate();
  }

  async execRequest<K extends RpcKind>(kind: K, payload: RequestPayload<K>): Promise<RpcResponse> {
    this.assertOpen();
    const request = { kind, payload } as Omit<RpcRequest, "requestId">;
    return this.transport.request(request);
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new Error("database closed");
    }
  }
}

export async function open(options: OpenOptions): Promise<Database> {
  return Database.open(options);
}

import {
  type CheckpointResult,
  type ExportResult,
  type MetricsResult,
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
  type SyncRunResult,
  type OwnerRuntime,
  createErrorPayload,
  ERR_BROWSER_COORDINATION_UNAVAILABLE,
  ERR_BROWSER_OWNER_STALE,
  ERR_BROWSER_OWNER_TIMEOUT,
  ERR_BROWSER_PROBE_FAILED,
  ERR_BROWSER_SERVICE_WORKER_UNSUPPORTED,
  ERR_BROWSER_UNSUPPORTED,
  ERR_OPERATION_FAILED,
} from "./protocol.js";
import { type BrowserRuntimeProbe, probeRuntime } from "./probe.js";

export type {
  OpenMode,
  ResultTransport,
  QueryRow,
  QueryValue,
  OpenResult,
  ExecResult,
  QueryResult,
  CheckpointResult,
  PersistResult,
  MetricsResult,
  BrowserRuntimeProbe,
  OwnerRuntime,
};

export interface OpenOptions {
  path: string;
  mode?: OpenMode;
  sharedMemory?: boolean;
  readOnly?: boolean;
  workerUrl?: string;
  wasmUrl?: string;
  resultTransport?: ResultTransport;
  openTimeoutMs?: number;
  skipRuntimeProbe?: boolean;
}

export interface SyncConfigurePeerOptions {
  name: string;
  endpoint: string;
  token?: string;
  headers?: Record<string, string>;
}

export interface SyncRunOptions {
  peer: string;
  direction: "push" | "pull" | "both";
  timeoutMs?: number;
}

export interface RelayRequestOptions {
  peer: string;
  headers?: Record<string, string>;
}

export interface ShapePullOptions extends RelayRequestOptions {
  shapeId: string;
  since?: number;
}

export interface ShapeSnapshotOptions extends RelayRequestOptions {
  shapeId: string;
  clientReplicaId: string;
}

export interface ShapeAckOptions extends RelayRequestOptions {
  shapeId: string;
  tenantId: string;
  clientReplicaId: string;
  subjectId: string;
  shapeSequence: number;
  sourceHighWatermark: number;
  changesetId?: string;
  sessionId?: string;
}

export interface RelayPrincipalOptions {
  tenantId: string;
  subjectId: string;
  subjectKind?: "user" | "device" | "service" | "agent";
  issuer?: string;
  roles?: string[];
  allowedScopes?: string[];
  allowedShapes?: string[];
  sessionId?: string;
  requestId?: string;
}

export interface ShapeStreamCheckpoint {
  shape_sequence: number;
  source_high_watermark: number;
}

export interface ShapeSubscribeOptions extends RelayRequestOptions {
  shapeId: string;
  clientReplicaId: string;
  mode?: "snapshot" | "resume";
  principal?: RelayPrincipalOptions;
  lastAckCheckpoint?: ShapeStreamCheckpoint;
  onMessage?: (message: unknown) => void;
  onError?: (error: Error) => void;
}

export interface ShapeSubscription {
  close: () => void;
  ack: (message: unknown) => void;
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
  timeoutId?: ReturnType<typeof setTimeout>;
};

interface TransportEndpoint {
  postMessage: (message: unknown) => void;
  close: () => void;
  setOnMessage: (handler: (event: MessageEvent<RpcResponse>) => void) => void;
  setOnError: (handler: (event: ErrorEvent) => void) => void;
}

class WorkerEndpoint implements TransportEndpoint {
  constructor(private readonly worker: Worker) {}

  postMessage(message: unknown): void {
    this.worker.postMessage(message);
  }

  close(): void {
    this.worker.terminate();
  }

  setOnMessage(handler: (event: MessageEvent<RpcResponse>) => void): void {
    this.worker.onmessage = handler;
  }

  setOnError(handler: (event: ErrorEvent) => void): void {
    this.worker.onerror = handler;
  }
}

const OWNER_PROTOCOL = "decentdb.browser.owner.v1";

type RelayRequestMessage = {
  protocol: typeof OWNER_PROTOCOL;
  type: "request";
  path: string;
  ownerId: string;
  clientId: string;
  request: RpcRequest;
};

type RelayResponseMessage = {
  protocol: typeof OWNER_PROTOCOL;
  type: "response";
  path: string;
  ownerId: string;
  clientId: string;
  response: RpcResponse;
};

type DiscoverMessage = {
  protocol: typeof OWNER_PROTOCOL;
  type: "discover";
  path: string;
  clientId: string;
};

type OwnerHelloMessage = {
  protocol: typeof OWNER_PROTOCOL;
  type: "owner-hello";
  path: string;
  ownerId: string;
  runtime: "dedicated-worker";
};

type OwnerClosingMessage = {
  protocol: typeof OWNER_PROTOCOL;
  type: "owner-closing";
  path: string;
  ownerId: string;
};

type OwnerMessage =
  | RelayRequestMessage
  | RelayResponseMessage
  | DiscoverMessage
  | OwnerHelloMessage
  | OwnerClosingMessage;

class BroadcastRelayEndpoint implements TransportEndpoint {
  private onMessage?: (event: MessageEvent<RpcResponse>) => void;
  private onError?: (event: ErrorEvent) => void;
  private stale = false;

  constructor(
    private readonly channel: BroadcastChannel,
    private readonly path: string,
    private readonly ownerId: string,
    private readonly clientId: string
  ) {
    this.channel.onmessage = (event: MessageEvent<OwnerMessage>) => {
      const message = event.data;
      if (!isOwnerMessage(message) || message.path !== this.path) {
        return;
      }
      if (
        message.type === "response" &&
        message.ownerId === this.ownerId &&
        message.clientId === this.clientId
      ) {
        this.onMessage?.(new MessageEvent("message", { data: message.response }));
      } else if (message.type === "owner-closing" && message.ownerId === this.ownerId) {
        this.stale = true;
        this.onError?.(
          new ErrorEvent("error", {
            message: "Browser database owner closed; retrying can recover ownership.",
          })
        );
      }
    };
  }

  postMessage(message: unknown): void {
    if (this.stale) {
      this.onError?.(
        new ErrorEvent("error", {
          message: "Browser database owner is stale.",
        })
      );
      return;
    }
    this.channel.postMessage({
      protocol: OWNER_PROTOCOL,
      type: "request",
      path: this.path,
      ownerId: this.ownerId,
      clientId: this.clientId,
      request: message as RpcRequest,
    } satisfies RelayRequestMessage);
  }

  close(): void {
    this.channel.close();
  }

  setOnMessage(handler: (event: MessageEvent<RpcResponse>) => void): void {
    this.onMessage = handler;
  }

  setOnError(handler: (event: ErrorEvent) => void): void {
    this.onError = handler;
  }
}

class RpcTransport {
  private nextRequestId = 1;
  private readonly pending = new Map<number, Pending>();

  constructor(
    private readonly endpoint: TransportEndpoint,
    private readonly requestTimeoutMs: number
  ) {
    this.endpoint.setOnMessage((event) => {
      const response = event.data as RpcResponse;
      const pending = this.pending.get(response.requestId);
      if (!pending) {
        return;
      }
      this.pending.delete(response.requestId);
      if (pending.timeoutId) {
        clearTimeout(pending.timeoutId);
      }

      if (!response.ok) {
        pending.reject(
          new DecentDBWebError(
            response.error ??
              createErrorPayload(ERR_OPERATION_FAILED, "unknown", "unknown worker error")
          )
        );
        return;
      }
      pending.resolve(response);
    });

    this.endpoint.setOnError((event) => {
      for (const [, pending] of this.pending) {
        if (pending.timeoutId) {
          clearTimeout(pending.timeoutId);
        }
        pending.reject(
          new DecentDBWebError({
            code: event.message.includes("stale") || event.message.includes("owner closed")
              ? ERR_BROWSER_OWNER_STALE
              : "ERR_WORKER_ERROR",
            message: event.message || "Worker encountered an uncaught exception.",
            details: "Worker encountered an uncaught exception.",
          })
        );
      }
      this.pending.clear();
    });
  }

  request(request: Omit<RpcRequest, "requestId">): Promise<RpcResponse> {
    const requestId = this.nextRequestId++;
    const envelope = {
      ...request,
      requestId,
    } as Omit<RpcRequest, "requestId"> & { requestId: number };

    return new Promise<RpcResponse>((resolve, reject) => {
      const pending: Pending = {
        resolve,
        reject: (error) => reject(error),
      };
      if (this.requestTimeoutMs > 0) {
        pending.timeoutId = setTimeout(() => {
          this.pending.delete(requestId);
          reject(
            new DecentDBWebError(
              createErrorPayload(
                ERR_BROWSER_OWNER_TIMEOUT,
                `Browser owner request timed out after ${this.requestTimeoutMs}ms.`
              )
            )
          );
        }, this.requestTimeoutMs);
      }
      this.pending.set(requestId, pending);
      this.endpoint.postMessage(envelope);
    }).then((response) => response);
  }

  close(): void {
    this.endpoint.close();
    for (const [, pending] of this.pending) {
      if (pending.timeoutId) {
        clearTimeout(pending.timeoutId);
      }
      pending.reject(
        new DecentDBWebError({
          code: "ERR_WORKER_TERM",
          message: "Worker connection closed",
        })
      );
    }
    this.pending.clear();
  }
}

function ensureSupportedProbe(probe: BrowserRuntimeProbe): void {
  if (probe.runtime.serviceWorker) {
    throw new DecentDBWebError(
      createErrorPayload(
        ERR_BROWSER_SERVICE_WORKER_UNSUPPORTED,
        "Service worker contexts cannot open DecentDB browser databases."
      )
    );
  }
  if (!probe.supported) {
    const first = probe.errors[0];
    throw new DecentDBWebError(
      first ?? createErrorPayload(ERR_BROWSER_UNSUPPORTED, "Browser runtime probe failed.")
    );
  }
}

type BrowserLocks = {
  request<T>(
    name: string,
    options: { mode?: "exclusive" | "shared"; ifAvailable?: boolean },
    callback: (lock: unknown | null) => T | Promise<T>
  ): Promise<T>;
};

type RuntimeHandle = {
  transport: RpcTransport;
  ownerId?: string;
  closeTransportOnClose: boolean;
};

type OwnerCoordinator = {
  ownerId: string;
  path: string;
  channel: BroadcastChannel;
  transport: RpcTransport;
  release: () => void;
};

const ownersByPath = new Map<string, OwnerCoordinator>();

function randomId(prefix: string): string {
  const cryptoLike = globalThis.crypto;
  if (cryptoLike && typeof cryptoLike.randomUUID === "function") {
    return `${prefix}_${cryptoLike.randomUUID()}`;
  }
  return `${prefix}_${Math.random().toString(36).slice(2)}_${Date.now().toString(36)}`;
}

function channelName(path: string): string {
  return `decentdb.browser.owner.v1:${encodeURIComponent(path)}`;
}

function lockName(path: string): string {
  return `decentdb.browser.owner.v1:${path}`;
}

function isOwnerMessage(message: unknown): message is OwnerMessage {
  return (
    typeof message === "object" &&
    message !== null &&
    "protocol" in message &&
    (message as { protocol?: unknown }).protocol === OWNER_PROTOCOL &&
    "type" in message
  );
}

async function discoverOwner(path: string, timeoutMs: number): Promise<OwnerHelloMessage | undefined> {
  if (typeof BroadcastChannel === "undefined") {
    return undefined;
  }
  const channel = new BroadcastChannel(channelName(path));
  const clientId = randomId("discover");
  try {
    return await new Promise<OwnerHelloMessage | undefined>((resolve) => {
      const timer = setTimeout(() => resolve(undefined), timeoutMs);
      channel.onmessage = (event: MessageEvent<OwnerMessage>) => {
        const message = event.data;
        if (
          isOwnerMessage(message) &&
          message.type === "owner-hello" &&
          message.path === path
        ) {
          clearTimeout(timer);
          resolve(message);
        }
      };
      channel.postMessage({
        protocol: OWNER_PROTOCOL,
        type: "discover",
        path,
        clientId,
      } satisfies DiscoverMessage);
    });
  } finally {
    channel.close();
  }
}

function bindOwnerCoordinator(owner: OwnerCoordinator): void {
  owner.channel.onmessage = (event: MessageEvent<OwnerMessage>) => {
    const message = event.data;
    if (!isOwnerMessage(message) || message.path !== owner.path) {
      return;
    }
    if (message.type === "discover") {
      owner.channel.postMessage({
        protocol: OWNER_PROTOCOL,
        type: "owner-hello",
        path: owner.path,
        ownerId: owner.ownerId,
        runtime: "dedicated-worker",
      } satisfies OwnerHelloMessage);
      return;
    }
    if (message.type !== "request" || message.ownerId !== owner.ownerId) {
      return;
    }
    const request = message.request;
    const forwarded = {
      kind: request.kind,
      payload: request.payload,
    } as Omit<RpcRequest, "requestId">;
    owner.transport
      .request(forwarded)
      .then((response) => {
        owner.channel.postMessage({
          protocol: OWNER_PROTOCOL,
          type: "response",
          path: owner.path,
          ownerId: owner.ownerId,
          clientId: message.clientId,
          response: {
            ...response,
            requestId: request.requestId,
          },
        } satisfies RelayResponseMessage);
      })
      .catch((error: unknown) => {
        const payload =
          error instanceof DecentDBWebError
            ? createErrorPayload(error.code, error.message, error.details)
            : createErrorPayload(
                ERR_OPERATION_FAILED,
                "Owner request failed.",
                error instanceof Error ? error.message : String(error)
              );
        owner.channel.postMessage({
          protocol: OWNER_PROTOCOL,
          type: "response",
          path: owner.path,
          ownerId: owner.ownerId,
          clientId: message.clientId,
          response: {
            requestId: request.requestId,
            kind: request.kind,
            ok: false,
            error: payload,
          },
        } satisfies RelayResponseMessage);
      });
  };
}

function releaseOwner(owner: OwnerCoordinator): void {
  if (ownersByPath.get(owner.path) !== owner) {
    return;
  }
  ownersByPath.delete(owner.path);
  owner.channel.postMessage({
    protocol: OWNER_PROTOCOL,
    type: "owner-closing",
    path: owner.path,
    ownerId: owner.ownerId,
  } satisfies OwnerClosingMessage);
  owner.channel.close();
  owner.release();
}

async function createDedicatedOwner(
  path: string,
  workerUrl: string | undefined,
  timeoutMs: number
): Promise<RuntimeHandle | undefined> {
  if (typeof Worker === "undefined" || typeof BroadcastChannel === "undefined") {
    throw new DecentDBWebError(
      createErrorPayload(
        ERR_BROWSER_COORDINATION_UNAVAILABLE,
        "Dedicated Worker and BroadcastChannel are required for browser owner coordination."
      )
    );
  }
  const locks = (navigator as Navigator & { locks?: BrowserLocks }).locks;
  if (!locks) {
    throw new DecentDBWebError(
      createErrorPayload(
        ERR_BROWSER_COORDINATION_UNAVAILABLE,
        "Web Locks are required to prevent competing browser database owners."
      )
    );
  }

  const workerModuleUrl = workerUrl ?? new URL("./worker.js", import.meta.url).toString();
  const acquired = new Promise<RuntimeHandle | undefined>((resolve, reject) => {
    void locks
      .request(lockName(path), { mode: "exclusive", ifAvailable: true }, async (lock) => {
        if (!lock) {
          resolve(undefined);
          return;
        }

        const worker = new Worker(workerModuleUrl, { type: "module" });
        const transport = new RpcTransport(new WorkerEndpoint(worker), timeoutMs);
        const ownerId = randomId("owner");
        const channel = new BroadcastChannel(channelName(path));
        let releaseLock!: () => void;
        const lockLifetime = new Promise<void>((release) => {
          releaseLock = release;
        });
        const owner: OwnerCoordinator = {
          ownerId,
          path,
          channel,
          transport,
          release: () => {
            releaseLock();
            transport.close();
          },
        };
        ownersByPath.set(path, owner);
        bindOwnerCoordinator(owner);
        globalThis.addEventListener?.("pagehide", () => releaseOwner(owner), { once: true });
        resolve({
          transport,
          ownerId,
          closeTransportOnClose: false,
        });
        await lockLifetime;
      })
      .catch(reject);
  });

  return withTimeout(acquired, timeoutMs, "Acquire browser database owner lock");
}

async function ownerRuntime(options: OpenOptions): Promise<RuntimeHandle> {
  const timeoutMs = options.openTimeoutMs ?? 3_000;
  const path = options.path;

  const discovered = await discoverOwner(path, Math.min(250, timeoutMs));
  if (discovered) {
    const channel = new BroadcastChannel(channelName(path));
    return {
      transport: new RpcTransport(
        new BroadcastRelayEndpoint(channel, path, discovered.ownerId, randomId("client")),
        timeoutMs
      ),
      ownerId: discovered.ownerId,
      closeTransportOnClose: true,
    };
  }

  const created = await createDedicatedOwner(path, options.workerUrl, timeoutMs);
  if (created) {
    return created;
  }

  const rediscovered = await discoverOwner(path, Math.min(500, timeoutMs));
  if (rediscovered) {
    const channel = new BroadcastChannel(channelName(path));
    return {
      transport: new RpcTransport(
        new BroadcastRelayEndpoint(channel, path, rediscovered.ownerId, randomId("client")),
        timeoutMs
      ),
      ownerId: rediscovered.ownerId,
      closeTransportOnClose: true,
    };
  }

  throw new DecentDBWebError(
    createErrorPayload(
      ERR_BROWSER_OWNER_TIMEOUT,
      "Timed out discovering or acquiring the browser database owner.",
      "Another tab may be opening this database; retry after the owner is reachable."
    )
  );
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => {
          reject(
            new DecentDBWebError(
              createErrorPayload(
                ERR_BROWSER_OWNER_TIMEOUT,
                `${label} timed out after ${timeoutMs}ms.`
              )
            )
          );
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
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

class SyncController {
  private readonly peers = new Map<string, SyncConfigurePeerOptions>();

  constructor(private readonly db: Database) {}

  async configurePeer(options: SyncConfigurePeerOptions): Promise<void> {
    this.db.assertOpen();
    this.peers.set(options.name, options);
    await this.db.execRequest("sync_configure_peer", {
      dbId: this.db.id,
      name: options.name,
      endpoint: options.endpoint,
    });
  }

  async run(options: SyncRunOptions): Promise<SyncRunResult> {
    this.db.assertOpen();
    const response = await this.db.execRequest("sync_run", {
      dbId: this.db.id,
      peer: options.peer,
      direction: options.direction,
      timeoutMs: options.timeoutMs,
    });
    return (response.result as SyncRunResult) ?? {
      status: "deferred",
      message: "Browser sync transport is deferred for this build profile.",
    };
  }

  async relayHello(options: RelayRequestOptions): Promise<unknown> {
    return this.fetchRelay(options.peer, "/decentdb/sync/v2/hello", {
      method: "GET",
      headers: options.headers,
    });
  }

  async pullShape(options: ShapePullOptions): Promise<unknown> {
    const since = options.since ?? 0;
    return this.fetchRelay(
      options.peer,
      `/decentdb/sync/v2/shapes/${encodeURIComponent(options.shapeId)}/changes?since=${since}`,
      {
        method: "GET",
        headers: options.headers,
      }
    );
  }

  async shapeSnapshot(options: ShapeSnapshotOptions): Promise<unknown> {
    return this.fetchRelay(
      options.peer,
      `/decentdb/sync/v2/shapes/${encodeURIComponent(options.shapeId)}/snapshot`,
      {
        method: "POST",
        headers: options.headers,
        body: JSON.stringify({ client_replica_id: options.clientReplicaId }),
      }
    );
  }

  async ackShape(options: ShapeAckOptions): Promise<unknown> {
    return this.fetchRelay(options.peer, "/decentdb/sync/v2/acks", {
      method: "POST",
      headers: options.headers,
      body: JSON.stringify({
        shape_id: options.shapeId,
        tenant_id: options.tenantId,
        client_replica_id: options.clientReplicaId,
        subject_id: options.subjectId,
        session_id: options.sessionId ?? null,
        shape_sequence: options.shapeSequence,
        source_high_watermark: options.sourceHighWatermark,
        changeset_id: options.changesetId ?? null,
      }),
    });
  }

  subscribeShape(options: ShapeSubscribeOptions): ShapeSubscription {
    this.db.assertOpen();
    const peer = this.requirePeer(options.peer);
    const principal = options.principal ?? this.principalFromHeaders(peer, options);
    if (!principal) {
      throw new DecentDBWebError({
        code: "ERR_BROWSER_SYNC_PRINCIPAL_REQUIRED",
        message: "WebSocket shape subscriptions require relay principal context.",
        details: "Pass options.principal or configure x-decentdb-* peer headers.",
      });
    }
    const socket = new WebSocket(
      this.relayWebSocketUrl(peer, "/decentdb/sync/v2/stream", principal, options.shapeId),
      "decentdb.sync.v2"
    );
    const send = (payload: unknown): void => {
      const text = JSON.stringify(payload);
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(text);
      } else {
        socket.addEventListener("open", () => socket.send(text), { once: true });
      }
    };
    socket.addEventListener("open", () => {
      send({
        type: "hello",
        request_id: principal.requestId,
        client_replica_id: options.clientReplicaId,
        supported_changeset_versions: [1],
        supported_shape_stream_versions: [1],
        supported_compression: ["none"],
      });
      send({
        type: "subscribe_shape",
        request_id: principal.requestId,
        shape_id: options.shapeId,
        client_replica_id: options.clientReplicaId,
        mode: options.mode ?? "snapshot",
        last_ack_checkpoint: options.lastAckCheckpoint ?? null,
      });
    });
    socket.addEventListener("message", (event: MessageEvent<string>) => {
      try {
        options.onMessage?.(JSON.parse(event.data));
      } catch (error) {
        options.onError?.(error instanceof Error ? error : new Error(String(error)));
      }
    });
    socket.addEventListener("error", () => {
      options.onError?.(
        new DecentDBWebError({
          code: "ERR_BROWSER_SYNC_WEBSOCKET",
          message: "Relay WebSocket failed.",
          details: peer.endpoint,
        })
      );
    });
    return {
      close: () => socket.close(),
      ack: (message: unknown): void => {
        const value = message as Record<string, unknown>;
        const checkpoint = (value.checkpoint ?? {}) as Record<string, unknown>;
        const changeset = (value.changeset ?? {}) as Record<string, unknown>;
        send({
          type: "ack",
          shape_id: String(value.shape_id ?? options.shapeId),
          client_replica_id: options.clientReplicaId,
          shape_sequence: Number(value.shape_sequence ?? checkpoint.shape_sequence ?? 0),
          source_high_watermark: Number(
            checkpoint.source_high_watermark ?? value.source_high_watermark ?? 0
          ),
          changeset_id:
            typeof changeset.changeset_id === "string"
              ? changeset.changeset_id
              : typeof value.changeset_id === "string"
                ? value.changeset_id
                : undefined,
        });
      },
    };
  }

  private async fetchRelay(
    peerName: string,
    path: string,
    init: RequestInit
  ): Promise<unknown> {
    this.db.assertOpen();
    const peer = this.requirePeer(peerName);
    const headers = new Headers(init.headers);
    headers.set("content-type", "application/json");
    for (const [name, value] of Object.entries(peer.headers ?? {})) {
      headers.set(name, value);
    }
    if (peer.token) {
      headers.set("authorization", `Bearer ${peer.token}`);
    }
    const endpoint = `${peer.endpoint.replace(/\/$/, "")}${path}`;
    const response = await fetch(endpoint, { ...init, headers });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new DecentDBWebError({
        code: String(payload.error_code ?? "ERR_BROWSER_SYNC_RELAY"),
        message: String(payload.error ?? `Relay request failed with ${response.status}.`),
        details: endpoint,
      });
    }
    return payload;
  }

  private requirePeer(peerName: string): SyncConfigurePeerOptions {
    const peer = this.peers.get(peerName);
    if (!peer) {
      throw new DecentDBWebError({
        code: "ERR_BROWSER_SYNC_PEER_NOT_CONFIGURED",
        message: `Sync peer '${peerName}' is not configured for this runtime owner.`,
        details: "Call db.sync.configurePeer() with a production relay endpoint first.",
      });
    }
    return peer;
  }

  private principalFromHeaders(
    peer: SyncConfigurePeerOptions,
    options: RelayRequestOptions
  ): RelayPrincipalOptions | undefined {
    const headers = new Map<string, string>();
    for (const [name, value] of Object.entries(peer.headers ?? {})) {
      headers.set(name.toLowerCase(), value);
    }
    for (const [name, value] of Object.entries(options.headers ?? {})) {
      headers.set(name.toLowerCase(), value);
    }
    const tenantId = headers.get("x-decentdb-tenant");
    const subjectId = headers.get("x-decentdb-subject");
    if (!tenantId || !subjectId) {
      return undefined;
    }
    return {
      tenantId,
      subjectId,
      subjectKind: (headers.get("x-decentdb-subject-kind") as RelayPrincipalOptions["subjectKind"]) ?? "user",
      issuer: headers.get("x-decentdb-issuer"),
      roles: splitHeaderList(headers.get("x-decentdb-roles")),
      allowedScopes: splitHeaderList(headers.get("x-decentdb-scopes")),
      allowedShapes: splitHeaderList(headers.get("x-decentdb-shapes")),
      sessionId: headers.get("x-decentdb-session"),
      requestId: headers.get("x-decentdb-request"),
    };
  }

  private relayWebSocketUrl(
    peer: SyncConfigurePeerOptions,
    path: string,
    principal: RelayPrincipalOptions,
    shapeId: string
  ): string {
    const url = new URL(`${peer.endpoint.replace(/\/$/, "")}${path}`);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    if (peer.token) {
      url.searchParams.set("token", peer.token);
    }
    url.searchParams.set("tenant", principal.tenantId);
    url.searchParams.set("subject", principal.subjectId);
    url.searchParams.set("subject_kind", principal.subjectKind ?? "user");
    if (principal.issuer) {
      url.searchParams.set("issuer", principal.issuer);
    }
    if (principal.roles?.length) {
      url.searchParams.set("roles", principal.roles.join(","));
    }
    if (principal.allowedScopes?.length) {
      url.searchParams.set("scopes", principal.allowedScopes.join(","));
    }
    url.searchParams.set("shapes", (principal.allowedShapes ?? [shapeId]).join(","));
    if (principal.sessionId) {
      url.searchParams.set("session", principal.sessionId);
    }
    if (principal.requestId) {
      url.searchParams.set("request", principal.requestId);
    }
    return url.toString();
  }
}

function splitHeaderList(value: string | undefined): string[] {
  return value
    ? value
        .split(",")
        .map((part) => part.trim())
        .filter(Boolean)
    : [];
}

export class Database {
  public readonly sync: SyncController;
  private closed = false;

  private constructor(
    private transport: RpcTransport,
    private result: OpenResult,
    public readonly runtimeProbe: BrowserRuntimeProbe,
    private readonly openOptions: OpenOptions,
    private closeTransportOnClose: boolean
  ) {
    this.sync = new SyncController(this);
  }

  static async open(options: OpenOptions): Promise<Database> {
    const probe: BrowserRuntimeProbe = options.skipRuntimeProbe
      ? {
          supported: true,
          tier: "compatible",
          runtime: {
            dedicatedWorker: typeof Worker !== "undefined",
            sharedWorker: typeof SharedWorker !== "undefined",
            broadcastChannel: typeof BroadcastChannel !== "undefined",
            webLocks: typeof navigator !== "undefined" && "locks" in navigator,
            serviceWorker: false,
          },
          storage: {
            opfsDirectory: true,
            syncAccessHandle: true,
            exclusiveAccessHandleLock: true,
            persistApi: typeof navigator.storage.persist === "function",
          },
          decentdb: {
            wasmModule: true,
            parserProfile: "browser-app-v1",
            resultTransport: options.resultTransport ?? "binary",
          },
          errors: [],
        }
      : await probeRuntime({
          wasmUrl: options.wasmUrl,
          resultTransport: options.resultTransport,
        }).catch((error: unknown) => {
          throw new DecentDBWebError(
            createErrorPayload(
              ERR_BROWSER_PROBE_FAILED,
              "Browser runtime probe failed.",
              error instanceof Error ? error.message : String(error)
            )
          );
        });

    ensureSupportedProbe(probe);
    const runtime = await ownerRuntime(options);
    const response = await runtime.transport.request({
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
    return new Database(
      runtime.transport,
      result as OpenResult,
      probe,
      { ...options, mode: options.mode ?? "openOrCreate" },
      runtime.closeTransportOnClose
    );
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

  get ownerId(): string {
    return this.result.ownerId;
  }

  get ownerRuntime(): OwnerRuntime {
    return this.result.runtime;
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

  async metrics(): Promise<MetricsResult> {
    this.assertOpen();
    const response = await this.execRequest("metrics", { dbId: this.id });
    return (response.result as MetricsResult | undefined) ?? {};
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    await this.execRequest("close", { dbId: this.id });
    this.closed = true;
    if (this.closeTransportOnClose) {
      this.transport.close();
    }
  }

  async execRequest<K extends RpcKind>(kind: K, payload: RequestPayload<K>): Promise<RpcResponse> {
    this.assertOpen();
    const request = { kind, payload } as Omit<RpcRequest, "requestId">;
    try {
      return await this.transport.request(request);
    } catch (error) {
      if (!this.isRecoverableOwnerError(error) || kind === "close") {
        throw error;
      }
      await this.recoverOwner();
      const retryPayload = this.rewriteDbId(payload);
      return this.transport.request({ kind, payload: retryPayload } as Omit<RpcRequest, "requestId">);
    }
  }

  assertOpen(): void {
    if (this.closed) {
      throw new Error("database closed");
    }
  }

  private isRecoverableOwnerError(error: unknown): boolean {
    return (
      error instanceof DecentDBWebError &&
      (error.code === ERR_BROWSER_OWNER_TIMEOUT ||
        error.code === ERR_BROWSER_OWNER_STALE ||
        error.code === "ERR_WORKER_ERROR")
    );
  }

  private async recoverOwner(): Promise<void> {
    if (this.closeTransportOnClose) {
      this.transport.close();
    }
    const runtime = await ownerRuntime(this.openOptions);
    const response = await runtime.transport.request({
      kind: "open",
      payload: {
        path: this.openOptions.path,
        mode: "openOrCreate",
        options: {
          sharedMemory: this.openOptions.sharedMemory ?? false,
          readOnly: this.openOptions.readOnly ?? false,
          wasmUrl: this.openOptions.wasmUrl,
          resultTransport: this.openOptions.resultTransport ?? "binary",
        },
      },
    });
    if (!response.result) {
      throw new DecentDBWebError({
        code: ERR_BROWSER_OWNER_STALE,
        message: "Recovered browser owner did not return an open handle.",
      });
    }
    this.transport = runtime.transport;
    this.result = response.result as OpenResult;
    this.closeTransportOnClose = runtime.closeTransportOnClose;
  }

  private rewriteDbId<K extends RpcKind>(payload: RequestPayload<K>): RequestPayload<K> {
    if (payload && typeof payload === "object" && "dbId" in payload) {
      return {
        ...payload,
        dbId: this.id,
      } as RequestPayload<K>;
    }
    return payload;
  }
}

export async function open(options: OpenOptions): Promise<Database> {
  return Database.open(options);
}

export { probeRuntime };

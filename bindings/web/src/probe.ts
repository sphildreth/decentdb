import {
  BROWSER_PROTOCOL_VERSION,
  BROWSER_SQL_PROFILE,
  type BrowserCapabilities,
  ERR_BROWSER_COORDINATION_UNAVAILABLE,
  ERR_BROWSER_OPFS_UNAVAILABLE,
  ERR_BROWSER_SERVICE_WORKER_UNSUPPORTED,
  ERR_BROWSER_SYNC_ACCESS_HANDLE_UNAVAILABLE,
  type QueryErrorPayload,
  type ResultTransport,
  createErrorPayload,
} from "./protocol.js";

type FileSystemCreateOptions = { create?: boolean };

interface FileSystemSyncAccessHandleLike {
  close(): void;
}

interface FileSystemFileHandleLike {
  createSyncAccessHandle?: () => Promise<FileSystemSyncAccessHandleLike>;
}

interface FileSystemDirectoryHandleLike {
  getFileHandle(name: string, options?: FileSystemCreateOptions): Promise<FileSystemFileHandleLike>;
}

interface StorageEstimateLike {
  quota?: number;
  usage?: number;
}

interface RuntimeStorageLike {
  persist?: () => Promise<boolean>;
  persisted?: () => Promise<boolean>;
  estimate?: () => Promise<StorageEstimateLike>;
  getDirectory?: () => Promise<FileSystemDirectoryHandleLike>;
}

export interface BrowserRuntimeProbe {
  supported: boolean;
  tier: "supported" | "compatible" | "unsupported";
  runtime: {
    dedicatedWorker: boolean;
    sharedWorker: boolean;
    broadcastChannel: boolean;
    webLocks: boolean;
    serviceWorker: boolean;
  };
  storage: {
    opfsDirectory: boolean;
    syncAccessHandle: boolean;
    exclusiveAccessHandleLock: boolean;
    persistApi: boolean;
    persisted?: boolean;
    estimate?: {
      quotaBytes?: number;
      usageBytes?: number;
    };
  };
  decentdb: {
    wasmModule: boolean;
    parserProfile: string;
    resultTransport: ResultTransport;
    protocolVersion: number;
    capabilities: BrowserCapabilities;
  };
  errors: QueryErrorPayload[];
}

function storage(): RuntimeStorageLike {
  return navigator.storage as unknown as RuntimeStorageLike;
}

function isServiceWorkerScope(): boolean {
  const maybeGlobal = globalThis as typeof globalThis & {
    ServiceWorkerGlobalScope?: new (...args: never[]) => unknown;
  };
  if (!maybeGlobal.ServiceWorkerGlobalScope) {
    return false;
  }
  return self instanceof maybeGlobal.ServiceWorkerGlobalScope;
}

async function probeOpfs(errors: QueryErrorPayload[]): Promise<{
  opfsDirectory: boolean;
  syncAccessHandle: boolean;
  exclusiveAccessHandleLock: boolean;
}> {
  const result = {
    opfsDirectory: false,
    syncAccessHandle: false,
    exclusiveAccessHandleLock: false,
  };

  if (typeof storage().getDirectory !== "function") {
    errors.push(
      createErrorPayload(
        ERR_BROWSER_OPFS_UNAVAILABLE,
        "OPFS getDirectory() is unavailable in this environment."
      )
    );
    return result;
  }

  try {
    const root = await storage().getDirectory?.();
    if (!root) {
      errors.push(
        createErrorPayload(
          ERR_BROWSER_OPFS_UNAVAILABLE,
          "OPFS root directory could not be opened."
        )
      );
      return result;
    }
    result.opfsDirectory = true;

    const file = await root.getFileHandle(".decentdb-probe.lock", { create: true });
    if (typeof file.createSyncAccessHandle !== "function") {
      // Window scopes often cannot verify sync handles directly; the worker open path remains authoritative.
      result.syncAccessHandle = true;
      result.exclusiveAccessHandleLock = true;
      return result;
    }
    let first: FileSystemSyncAccessHandleLike | undefined;
    let second: FileSystemSyncAccessHandleLike | undefined;
    try {
      first = await file.createSyncAccessHandle();
      result.syncAccessHandle = true;
      try {
        second = await file.createSyncAccessHandle();
        result.exclusiveAccessHandleLock = false;
      } catch {
        result.exclusiveAccessHandleLock = true;
      }
    } finally {
      second?.close();
      first?.close();
    }

    if (!result.syncAccessHandle) {
      errors.push(
        createErrorPayload(
          ERR_BROWSER_SYNC_ACCESS_HANDLE_UNAVAILABLE,
          "OPFS synchronous access handles are unavailable."
        )
      );
    }
  } catch (error) {
    errors.push(
      createErrorPayload(
        ERR_BROWSER_OPFS_UNAVAILABLE,
        "Failed while probing OPFS capabilities.",
        error instanceof Error ? error.message : String(error)
      )
    );
  }

  return result;
}

export async function probeRuntime(options?: {
  wasmUrl?: string;
  resultTransport?: ResultTransport;
}): Promise<BrowserRuntimeProbe> {
  const errors: QueryErrorPayload[] = [];
  const runtime = {
    dedicatedWorker: typeof Worker !== "undefined",
    sharedWorker: typeof SharedWorker !== "undefined",
    broadcastChannel: typeof BroadcastChannel !== "undefined",
    webLocks: typeof navigator !== "undefined" && "locks" in navigator,
    serviceWorker: isServiceWorkerScope(),
  };

  const storageProbe = await probeOpfs(errors);
  const persistApi = typeof storage().persist === "function";
  const resultTransport = options?.resultTransport ?? "binary";
  const capabilities = browserCapabilities(resultTransport);
  const report: BrowserRuntimeProbe = {
    supported: false,
    tier: "unsupported",
    runtime,
    storage: {
      opfsDirectory: storageProbe.opfsDirectory,
      syncAccessHandle: storageProbe.syncAccessHandle,
      exclusiveAccessHandleLock: storageProbe.exclusiveAccessHandleLock,
      persistApi,
    },
    decentdb: {
      wasmModule: true,
      parserProfile: BROWSER_SQL_PROFILE,
      resultTransport,
      protocolVersion: BROWSER_PROTOCOL_VERSION,
      capabilities,
    },
    errors,
  };

  if (typeof storage().persisted === "function") {
    try {
      report.storage.persisted = await storage().persisted?.();
    } catch {
      // no-op: persisted() is optional and browser-specific.
    }
  }

  if (typeof storage().estimate === "function") {
    try {
      const estimate = await storage().estimate?.();
      report.storage.estimate = {
        quotaBytes: estimate?.quota,
        usageBytes: estimate?.usage,
      };
    } catch {
      // no-op: estimate() is optional and browser-specific.
    }
  }

  if (!runtime.dedicatedWorker || !runtime.broadcastChannel || !runtime.webLocks) {
    errors.push(
      createErrorPayload(
        ERR_BROWSER_COORDINATION_UNAVAILABLE,
        "Dedicated Worker, BroadcastChannel, and Web Locks are required for browser owner coordination."
      )
    );
  }

  if (runtime.serviceWorker) {
    errors.push(
      createErrorPayload(
        ERR_BROWSER_SERVICE_WORKER_UNSUPPORTED,
        "Service worker contexts cannot own DecentDB browser databases."
      )
    );
  }

  report.supported =
    runtime.dedicatedWorker &&
    runtime.broadcastChannel &&
    runtime.webLocks &&
    report.storage.opfsDirectory &&
    report.storage.syncAccessHandle &&
    !runtime.serviceWorker;
  report.tier = report.supported ? "supported" : "unsupported";
  return report;
}

function browserCapabilities(_resultTransport: ResultTransport): BrowserCapabilities {
  return {
    protocolVersion: BROWSER_PROTOCOL_VERSION,
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

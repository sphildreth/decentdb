type FileSystemCreateOptions = { create?: boolean };
type FileSystemReadWriteOptions = { at?: number };

interface FileSystemSyncAccessHandleLike {
  read(buffer: BufferSource, options?: FileSystemReadWriteOptions): number;
  write(buffer: BufferSource, options?: FileSystemReadWriteOptions): number;
  truncate(size: number): void;
  getSize(): number;
  flush(): void;
  close(): void;
}

interface FileSystemFileHandleLike {
  createSyncAccessHandle(): Promise<FileSystemSyncAccessHandleLike>;
}

interface FileSystemDirectoryHandleLike {
  getFileHandle(name: string, options?: FileSystemCreateOptions): Promise<FileSystemFileHandleLike>;
}

interface OpfsNavigatorStorage {
  persist?: () => Promise<boolean>;
  getDirectory?: () => Promise<FileSystemDirectoryHandleLike>;
}

type OpenMode = "createNew" | "openExisting" | "openOrCreate";

type FileRecord = {
  path: string;
  exists: boolean;
  handle: FileSystemFileHandleLike;
  access: FileSystemSyncAccessHandleLike;
};

const files = new Map<string, FileRecord>();

function storage(): OpfsNavigatorStorage {
  return navigator.storage as unknown as OpfsNavigatorStorage;
}

function assertOpfsAvailable(): void {
  if (!storage().getDirectory) {
    throw new Error("OPFS getDirectory() is unavailable in this browser worker.");
  }
}

function fileNameForPath(path: string): string {
  return encodeURIComponent(path).replaceAll("%", "_");
}

function walPathForDb(path: string): string {
  return `opfs://${path}.wal`;
}

async function fileExists(root: FileSystemDirectoryHandleLike, path: string): Promise<boolean> {
  try {
    await root.getFileHandle(fileNameForPath(path), { create: false });
    return true;
  } catch {
    return false;
  }
}

async function prepareFile(root: FileSystemDirectoryHandleLike, path: string, mode: OpenMode): Promise<void> {
  if (files.has(path)) {
    throw new Error(`OPFS file is already owned by this worker runtime: ${path}`);
  }
  const existing = await fileExists(root, path);
  if (mode === "createNew" && existing) {
    throw new Error(`OPFS file already exists: ${path}`);
  }
  if (mode === "openExisting" && !existing) {
    throw new Error(`OPFS file does not exist: ${path}`);
  }
  const handle = await root.getFileHandle(fileNameForPath(path), { create: true });
  const access = await handle.createSyncAccessHandle();
  if (mode === "createNew") {
    access.truncate(0);
    access.flush();
  }
  files.set(path, {
    path,
    exists: existing,
    handle,
    access,
  });
}

function recordFor(path: string): FileRecord {
  const record = files.get(path);
  if (!record) {
    throw new Error(`OPFS file was not prepared before engine access: ${path}`);
  }
  return record;
}

function bufferSource(bytes: Uint8Array): BufferSource {
  return bytes as Uint8Array<ArrayBuffer>;
}

export async function prepareDatabase(path: string, mode: "create" | "open" | "openOrCreate"): Promise<void> {
  assertOpfsAvailable();
  const root = await storage().getDirectory?.();
  if (!root) {
    throw new Error("OPFS root directory is unavailable.");
  }
  const openMode: OpenMode = mode === "create" ? "createNew" : mode === "open" ? "openExisting" : "openOrCreate";
  await prepareFile(root, path, openMode);
  await prepareFile(root, walPathForDb(path), "openOrCreate");
}

export async function replaceDatabaseBytes(path: string, bytes: Uint8Array): Promise<void> {
  assertOpfsAvailable();
  const root = await storage().getDirectory?.();
  if (!root) {
    throw new Error("OPFS root directory is unavailable.");
  }
  if (!files.has(path)) {
    await prepareFile(root, path, "openOrCreate");
  }
  const walPath = walPathForDb(path);
  if (!files.has(walPath)) {
    await prepareFile(root, walPath, "openOrCreate");
  }

  const db = recordFor(path);
  db.access.truncate(0);
  db.access.write(bufferSource(bytes), { at: 0 });
  db.access.flush();
  db.exists = true;

  const wal = recordFor(walPath);
  wal.access.truncate(0);
  wal.access.flush();
  wal.exists = true;
}

export function installOpfsHost(): void {
  const target = globalThis as typeof globalThis & Record<string, unknown>;

  target.__decentdb_opfs_open = (path: string, mode: OpenMode): void => {
    const record = recordFor(path);
    if (mode === "createNew" && record.exists) {
      throw new Error(`OPFS file already exists: ${path}`);
    }
    if (mode === "openExisting" && !record.exists) {
      throw new Error(`OPFS file does not exist: ${path}`);
    }
    record.exists = true;
  };

  target.__decentdb_opfs_exists = (path: string): boolean => {
    return files.get(path)?.exists ?? false;
  };

  target.__decentdb_opfs_remove = (path: string): void => {
    const record = files.get(path);
    if (!record) {
      return;
    }
    record.access.truncate(0);
    record.access.flush();
    record.exists = false;
  };

  target.__decentdb_opfs_read = (path: string, _kind: string, offset: number, len: number): Uint8Array => {
    const record = recordFor(path);
    const size = record.access.getSize();
    if (offset >= size) {
      return new Uint8Array(0);
    }
    const buffer = new Uint8Array(Math.min(len, size - offset));
    const read = record.access.read(buffer, { at: offset });
    return buffer.slice(0, read);
  };

  target.__decentdb_opfs_write = (path: string, _kind: string, offset: number, bytes: Uint8Array): number => {
    const record = recordFor(path);
    const written = record.access.write(bufferSource(bytes), { at: offset });
    record.exists = true;
    return written;
  };

  target.__decentdb_opfs_size = (path: string): number => {
    return recordFor(path).access.getSize();
  };

  target.__decentdb_opfs_set_len = (path: string, _kind: string, len: number): void => {
    const record = recordFor(path);
    record.access.truncate(len);
    record.exists = true;
  };

  target.__decentdb_opfs_flush = (path: string): void => {
    recordFor(path).access.flush();
  };

  target.__decentdb_opfs_close = (path: string): void => {
    const record = files.get(path);
    if (!record) {
      return;
    }
    record.access.close();
    files.delete(path);
  };

  target.__decentdb_opfs_export_db = (path: string): Uint8Array => {
    const record = recordFor(path);
    const size = record.access.getSize();
    const bytes = new Uint8Array(size);
    const read = record.access.read(bytes, { at: 0 });
    return bytes.slice(0, read);
  };

  target.__decentdb_opfs_import_db = (path: string, bytes: Uint8Array): void => {
    const db = recordFor(path);
    db.access.truncate(0);
    db.access.write(bufferSource(bytes), { at: 0 });
    db.access.flush();
    db.exists = true;

    const wal = files.get(walPathForDb(path));
    if (wal) {
      wal.access.truncate(0);
      wal.access.flush();
      wal.exists = true;
    }
  };
}

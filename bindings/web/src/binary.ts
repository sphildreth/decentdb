import type { QueryRow, QueryValue } from "./protocol.js";

export interface BinaryQueryResult {
  columns: string[];
  rows: QueryRow[];
  affectedRows: number;
}

const MAGIC = "DDBR";
const VERSION = 1;

const TAG_NULL = 0;
const TAG_INT64 = 1;
const TAG_FLOAT64 = 2;
const TAG_BOOL = 3;
const TAG_TEXT = 4;
const TAG_BYTES = 5;
const TAG_DECIMAL = 6;
const TAG_UUID = 7;
const TAG_TIMESTAMP_MICROS = 8;
const TAG_GEOMETRY = 9;
const TAG_GEOGRAPHY = 10;
const TAG_ENUM = 11;
const TAG_IPADDR = 12;
const TAG_CIDR = 13;
const TAG_MACADDR = 14;
const TAG_DATE_DAYS = 15;
const TAG_TIME_MICROS = 16;
const TAG_TIMESTAMPTZ_MICROS = 17;
const TAG_INTERVAL = 18;

export function decodeBinaryResult(bytes: Uint8Array): BinaryQueryResult {
  const reader = new BinaryReader(bytes);
  const magic = String.fromCharCode(reader.u8(), reader.u8(), reader.u8(), reader.u8());
  if (magic !== MAGIC) {
    throw new Error(`Invalid DecentDB binary result magic: ${magic}`);
  }
  const version = reader.u16();
  if (version !== VERSION) {
    throw new Error(`Unsupported DecentDB binary result version: ${version}`);
  }
  reader.u16();
  const affectedRows = Number(reader.u64());
  const columnCount = reader.u32();
  const rowCount = reader.u32();
  const columns: string[] = [];
  for (let index = 0; index < columnCount; index += 1) {
    columns.push(reader.string());
  }

  const rows: QueryRow[] = [];
  for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
    const row: QueryRow = {};
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
      row[columns[columnIndex] ?? `column_${columnIndex}`] = reader.value();
    }
    rows.push(row);
  }
  reader.done();
  return { columns, rows, affectedRows };
}

class BinaryReader {
  private readonly view: DataView;
  private offset = 0;
  private readonly decoder = new TextDecoder();

  constructor(private readonly bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  u8(): number {
    this.require(1);
    const value = this.view.getUint8(this.offset);
    this.offset += 1;
    return value;
  }

  u16(): number {
    this.require(2);
    const value = this.view.getUint16(this.offset, true);
    this.offset += 2;
    return value;
  }

  u32(): number {
    this.require(4);
    const value = this.view.getUint32(this.offset, true);
    this.offset += 4;
    return value;
  }

  i32(): number {
    this.require(4);
    const value = this.view.getInt32(this.offset, true);
    this.offset += 4;
    return value;
  }

  u64(): bigint {
    this.require(8);
    const value = this.view.getBigUint64(this.offset, true);
    this.offset += 8;
    return value;
  }

  i64(): bigint {
    this.require(8);
    const value = this.view.getBigInt64(this.offset, true);
    this.offset += 8;
    return value;
  }

  f64(): number {
    this.require(8);
    const value = this.view.getFloat64(this.offset, true);
    this.offset += 8;
    return value;
  }

  bytesValue(): Uint8Array {
    const len = this.u32();
    this.require(len);
    const value = this.bytes.slice(this.offset, this.offset + len);
    this.offset += len;
    return value;
  }

  fixedBytes(len: number): Uint8Array {
    this.require(len);
    const value = this.bytes.slice(this.offset, this.offset + len);
    this.offset += len;
    return value;
  }

  string(): string {
    return this.decoder.decode(this.bytesValue());
  }

  value(): QueryValue {
    const tag = this.u8();
    switch (tag) {
      case TAG_NULL:
        return null;
      case TAG_INT64:
        return int64ToQueryValue(this.i64());
      case TAG_FLOAT64:
        return this.f64();
      case TAG_BOOL:
        return this.u8() !== 0;
      case TAG_TEXT:
        return this.string();
      case TAG_BYTES:
        return this.bytesValue();
      case TAG_DECIMAL:
        return { kind: "decimal", scaled: this.i64().toString(), scale: this.u8() };
      case TAG_UUID:
        return { kind: "uuid", bytes: this.fixedBytes(16) };
      case TAG_TIMESTAMP_MICROS:
        return { kind: "timestampMicros", value: this.i64().toString() };
      case TAG_GEOMETRY:
        return { kind: "geometry", bytes: this.bytesValue() };
      case TAG_GEOGRAPHY:
        return { kind: "geography", bytes: this.bytesValue() };
      case TAG_ENUM:
        return {
          kind: "enum",
          enumTypeId: this.u64().toString(),
          labelId: this.u64().toString(),
        };
      case TAG_IPADDR:
        return { kind: "ipaddr", family: this.u8(), bytes: this.fixedBytes(16) };
      case TAG_CIDR:
        return {
          kind: "cidr",
          family: this.u8(),
          prefixLen: this.u8(),
          bytes: this.fixedBytes(16),
        };
      case TAG_MACADDR:
        return { kind: "macaddr", len: this.u8(), bytes: this.fixedBytes(8) };
      case TAG_DATE_DAYS:
        return { kind: "dateDays", value: this.i32() };
      case TAG_TIME_MICROS:
        return { kind: "timeMicros", value: this.i64().toString() };
      case TAG_TIMESTAMPTZ_MICROS:
        return { kind: "timestampTzMicros", value: this.i64().toString() };
      case TAG_INTERVAL:
        return {
          kind: "interval",
          months: this.i32(),
          days: this.i32(),
          micros: this.i64().toString(),
        };
      default:
        throw new Error(`Unknown DecentDB binary result value tag: ${tag}`);
    }
  }

  done(): void {
    if (this.offset !== this.bytes.byteLength) {
      throw new Error(`DecentDB binary result has ${this.bytes.byteLength - this.offset} trailing bytes`);
    }
  }

  private require(len: number): void {
    if (this.offset + len > this.bytes.byteLength) {
      throw new Error("Truncated DecentDB binary result");
    }
  }
}

function int64ToQueryValue(value: bigint): QueryValue {
  const numberValue = Number(value);
  if (Number.isSafeInteger(numberValue)) {
    return numberValue;
  }
  return { kind: "int64", value: value.toString() };
}

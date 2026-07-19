import fs from "fs/promises";
import os from "os";
import path from "path";
import { randomUUID } from "crypto";
import { ParquetReader, ParquetWriter, ParquetSchema, type SchemaDefinition, type ParquetType } from "@dsnp/parquetjs";
import { IObjectStorage } from "./IObjectStorage.js";
import { StoragePath } from "./StoragePath.js";

export interface ParquetRow {
  [key: string]: unknown;
}

export class ParquetEngine {
  /**
   * Convert values that parquetjs cannot write directly (objects, arrays, BigInt,
   * Long objects, Buffer/Uint8Array, Date) into safe scalar Parquet values.
   * Objects/arrays become JSON strings; Long-like objects become numbers.
   */
  static sanitizeValue(value: unknown, isRecord = false): unknown {
    if (value === null || value === undefined) return value;
    if (typeof value === "bigint") return Number(value);
    if (typeof value === "boolean" || typeof value === "number" || typeof value === "string") return value;
    if (value instanceof Date) return value;

    const anyValue = value as { toNumber?: () => number };
    if (typeof anyValue.toNumber === "function") {
      try {
        const n = anyValue.toNumber();
        if (Number.isFinite(n)) return n;
      } catch { /* fall through */ }
    }

    if (Buffer.isBuffer(value)) return value.toString("utf-8");
    if (value instanceof Uint8Array) return Buffer.from(value).toString("utf-8");
    if (Array.isArray(value)) return JSON.stringify(value);

    if (typeof value === "object" && isRecord) {
      const result: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(value)) {
        result[k] = ParquetEngine.sanitizeValue(v, false);
      }
      return result;
    }

    // Any remaining object (nested JSON, maps, etc.) becomes a JSON string.
    return JSON.stringify(value);
  }

  static buildSchema(rows: ParquetRow[]): ParquetSchema {
    const schemaObj: SchemaDefinition = {};
    for (const row of rows) {
      for (const [k, v] of Object.entries(row)) {
        if (!schemaObj[k]) {
          const value = ParquetEngine.sanitizeValue(v, false);
          const type: ParquetType =
            value === null || value === undefined
              ? "UTF8"
              : typeof value === "boolean"
              ? "BOOLEAN"
              : typeof value === "number"
              ? Number.isInteger(value) && Number.isSafeInteger(value)
                ? "INT64"
                : "DOUBLE"
              : value instanceof Date
              ? "TIMESTAMP_MILLIS"
              : "UTF8";
          schemaObj[k] = { type, optional: true };
        }
      }
    }
    return new ParquetSchema(schemaObj);
  }

  static async readRows(storage: IObjectStorage, storagePath: StoragePath): Promise<ParquetRow[]> {
    const buffer = await storage.read(storagePath);
    const reader = await ParquetReader.openBuffer(buffer);
    const cursor = reader.getCursor();
    const rows: ParquetRow[] = [];
    let row: ParquetRow | null;
    while ((row = await cursor.next() as ParquetRow | null)) {
      if (row) rows.push(ParquetEngine.sanitizeValue(row, true) as ParquetRow);
    }
    await reader.close();
    return rows;
  }

  static async writeRows(storage: IObjectStorage, storagePath: StoragePath, rows: ParquetRow[]): Promise<void> {
    if (rows.length === 0) {
      return;
    }

    const sanitizedRows = rows.map((row) => ParquetEngine.sanitizeValue(row, true) as ParquetRow);
    const tempFile = path.join(os.tmpdir(), `${randomUUID()}.parquet`);
    const writer = await ParquetWriter.openFile(ParquetEngine.buildSchema(sanitizedRows), tempFile);
    for (const row of sanitizedRows) {
      await writer.appendRow(row);
    }
    await writer.close();

    try {
      const buffer = await fs.readFile(tempFile);
      await storage.write(storagePath, buffer, "application/octet-stream");
    } finally {
      await fs.unlink(tempFile).catch(() => {});
    }
  }
}

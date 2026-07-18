import fs from "fs/promises";
import os from "os";
import path from "path";
import { randomUUID } from "crypto";
import { ParquetReader, ParquetWriter, ParquetSchema } from "@dsnp/parquetjs";
import { IObjectStorage } from "./IObjectStorage.js";
import { StoragePath } from "./StoragePath.js";

export interface ParquetRow {
  [key: string]: any;
}

export class ParquetEngine {
  static sanitizeBigInt(value: any): any {
    if (typeof value === "bigint") {
      return Number(value);
    }
    if (Array.isArray(value)) {
      return value.map(ParquetEngine.sanitizeBigInt);
    }
    if (value !== null && typeof value === "object") {
      const result: Record<string, any> = {};
      for (const [k, v] of Object.entries(value)) {
        result[k] = ParquetEngine.sanitizeBigInt(v);
      }
      return result;
    }
    return value;
  }

  static buildSchema(rows: ParquetRow[]): ParquetSchema {
    const schemaObj: any = {};
    for (const row of rows) {
      for (const [k, v] of Object.entries(row)) {
        if (!schemaObj[k]) {
          const value = ParquetEngine.sanitizeBigInt(v);
          const type =
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
    let row: any;
    while ((row = await cursor.next())) {
      rows.push(ParquetEngine.sanitizeBigInt(row));
    }
    await reader.close();
    return rows;
  }

  static async writeRows(storage: IObjectStorage, storagePath: StoragePath, rows: ParquetRow[]): Promise<void> {
    if (rows.length === 0) {
      return;
    }

    const tempFile = path.join(os.tmpdir(), `${randomUUID()}.parquet`);
    const writer = await ParquetWriter.openFile(ParquetEngine.buildSchema(rows), tempFile);
    for (const row of rows) {
      await writer.appendRow(ParquetEngine.sanitizeBigInt(row));
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

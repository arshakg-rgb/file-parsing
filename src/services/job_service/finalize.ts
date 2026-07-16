import { ParquetReader, ParquetWriter, ParquetSchema } from "@dsnp/parquetjs";
import { createReadStream } from "fs";
import fs from "fs/promises";
import { pipeline } from "node:stream/promises";
import os from "os";
import path from "path";
import { randomUUID } from "crypto";
import { gcsClient, parseGcsUrl, readFull, objectSize, putObject } from "../../shared/gcsUtils.js";
import { settings } from "../../shared/config.js";
import { getJob, pool } from "../../shared/db.js";

// Recursive BigInt sanitizer to handle nested structures
function sanitizeBigInt(value: any): any {
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (Array.isArray(value)) {
    return value.map(sanitizeBigInt);
  }
  if (value !== null && typeof value === "object") {
    const result: Record<string, any> = {};
    for (const [k, v] of Object.entries(value)) {
      result[k] = sanitizeBigInt(v);
    }
    return result;
  }
  return value;
}

function buildSchema(rows: Record<string, any>[]): any {
  const schemaObj: any = {};
  for (const row of rows) {
    for (const [k, v] of Object.entries(row)) {
      if (!schemaObj[k]) {
        // Handle BigInt values by converting to number for type checking
        const value = sanitizeBigInt(v);
        const type = value === null || value === undefined ? "UTF8" :
                    typeof value === "boolean" ? "BOOLEAN" :
                    typeof value === "number" ? (Number.isInteger(value) && Number.isSafeInteger(value) ? "INT64" : "DOUBLE") :
                    value instanceof Date ? "TIMESTAMP_MILLIS" :
                    "UTF8";
        schemaObj[k] = { type, optional: true };
      }
    }
  }
  return new ParquetSchema(schemaObj);
}

export interface FinalizeResult {
  failed: boolean;
  paths: string[];
  error?: string;
}

export async function finalizeOutput(
  jobId: string,
  partPaths: string[],
  bucket: string
): Promise<FinalizeResult> {
  if (!partPaths.length) {
    return { failed: false, paths: [] };
  }

  const groups = groupByTemplate(partPaths);
  const mergedPaths: string[] = [];

  for (const [templateId, paths] of groups) {
    if (paths.length === 1) {
      mergedPaths.push(paths[0]);
      continue;
    }

    const totalSize = await totalPartSize(paths);
    if (totalSize > settings.MAX_MERGED_PART_BYTES) {
      mergedPaths.push(...paths);
      continue;
    }

    try {
      const rows = await readAllRows(paths);
      if (!rows.length) {
        mergedPaths.push(...paths);
        continue;
      }

      // Put rows in source order and backfill missing line numbers.
      rows.sort((a, b) => (a._line_no ?? 0) - (b._line_no ?? 0));
      let nextLineNo = 1;
      for (const r of rows) {
        if (r._line_no === undefined || r._line_no === null || r._line_no === 0) {
          r._line_no = nextLineNo;
        }
        nextLineNo++;
      }

      const mergedId = randomUUID();
      const mergedKey = `outputs/${jobId}/merged/${templateId}/${mergedId}.parquet`;
      const tempFile = path.join(os.tmpdir(), `${mergedId}.parquet`);

      const schema = buildSchema(rows);
      const writer = await ParquetWriter.openFile(schema, tempFile);
      for (const row of rows) {
        await writer.appendRow(sanitizeBigInt(row));
      }
      await writer.close();

      const uploadStream = gcsClient()
        .bucket(bucket)
        .file(mergedKey)
        .createWriteStream({ contentType: "application/octet-stream" });
      const readStream = createReadStream(tempFile);
      const fileStat = await fs.stat(tempFile);

      try {
        await pipeline(readStream, uploadStream);
      } finally {
        await fs.unlink(tempFile).catch(() => {});
      }

      mergedPaths.push(`s3://${bucket}/${mergedKey}`);
    } catch (err) {
      console.error("finalize_merge_failed", { jobId, templateId, error: String(err) });
      return { failed: true, paths: partPaths, error: String(err) };
    }
  }

  await backfillLineNumbers(jobId, mergedPaths);

  return { failed: false, paths: mergedPaths };
}

function groupByTemplate(paths: string[]): Map<string, string[]> {
  const groups = new Map<string, string[]>();
  for (const p of paths) {
    const [, key] = parseGcsUrl(p);
    const filename = key.split("/").pop() || "";
    // Extract templateId from filename: <jobId>-<templateId>-<timestamp>.parquet
    const parts = filename.split("-");
    // Template ID is the second part (after jobId)
    const templateId = parts.length >= 2 ? parts[1] : "unknown";
    if (!groups.has(templateId)) groups.set(templateId, []);
    groups.get(templateId)!.push(p);
  }
  return groups;
}

async function totalPartSize(paths: string[]): Promise<number> {
  let total = 0;
  for (const p of paths) {
    const [bucket, key] = parseGcsUrl(p);
    total += await objectSize(bucket, key);
  }
  return total;
}

async function readAllRows(paths: string[]): Promise<Record<string, any>[]> {
  const rows: Record<string, any>[] = [];
  for (const p of paths) {
    const [bucket, key] = parseGcsUrl(p);
    const buffer = await readFull(bucket, key);
    const reader = await ParquetReader.openBuffer(buffer);
    const cursor = reader.getCursor();
    let row: any;
    while ((row = await cursor.next())) {
      rows.push(sanitizeBigInt(row));
    }
    await reader.close();
  }
  return rows;
}

async function backfillLineNumbers(jobId: string, mergedPaths: string[]): Promise<void> {
  const job = await getJob(jobId);
  if (!job?.s3_url) {
    console.log("backfill_skip_no_source", { jobId });
    return;
  }

  const timings = (job.timings as Record<string, any>) || {};
  const rubbishLogPath = timings._rubbish_log_path as string | undefined;

  let source: Buffer | undefined;
  try {
    const [srcBucket, srcKey] = parseGcsUrl(job.s3_url);
    source = await readFull(srcBucket, srcKey);
  } catch (e) {
    console.warn("backfill_source_read_failed", { jobId, error: String(e) });
    return;
  }

  const targetOffsets = new Set<number>();

  // Offsets from parsed output
  for (const p of mergedPaths) {
    const [bucket, key] = parseGcsUrl(p);
    try {
      const buffer = await readFull(bucket, key);
      const reader = await ParquetReader.openBuffer(buffer);
      const cursor = reader.getCursor();
      let row: any;
      while ((row = await cursor.next())) {
        if (row._byte_offset !== undefined && row._byte_offset !== null) {
          targetOffsets.add(Number(sanitizeBigInt(row._byte_offset)));
        }
      }
      await reader.close();
    } catch (e) {
      console.warn("backfill_parsed_read_failed", { jobId, path: p, error: String(e) });
    }
  }

  // Offsets from dead letters
  const dlqRows = await pool.query<{
    dlq_id: string;
    byte_offset: number;
  }>("SELECT dlq_id, byte_offset FROM dead_letters WHERE job_id = $1", [jobId]);
  for (const r of dlqRows.rows) {
    targetOffsets.add(Number(r.byte_offset));
  }

  // Offsets from rubbish log
  let rubbishEntries: Array<Record<string, any>> = [];
  if (rubbishLogPath) {
    try {
      const [bucket, key] = parseGcsUrl(rubbishLogPath);
      const raw = await readFull(bucket, key);
      const text = raw.toString("utf-8");
      rubbishEntries = text
        .split("\n")
        .filter((l) => l.trim())
        .map((l) => JSON.parse(l));
      for (const e of rubbishEntries) {
        if (typeof e.byte_offset === "number") targetOffsets.add(e.byte_offset);
      }
    } catch (e) {
      console.warn("backfill_rubbish_read_failed", { jobId, error: String(e) });
    }
  }

  const sortedOffsets = Array.from(targetOffsets).sort((a, b) => a - b);
  const lineMap = computeLineMap(source, sortedOffsets);

  // Update dead_letters
  for (const r of dlqRows.rows) {
    const line = lineMap.get(r.byte_offset);
    if (line !== undefined) {
      await pool.query("UPDATE dead_letters SET line_no = $1, updated_at = NOW() WHERE dlq_id = $2", [line, r.dlq_id]);
    }
  }

  // Rewrite rubbish log
  if (rubbishLogPath && rubbishEntries.length) {
    let changed = false;
    const updated = rubbishEntries.map((e) => {
      const line = lineMap.get(e.byte_offset);
      if (line !== undefined && e.line_no !== line) {
        changed = true;
        return { ...e, line_no: line };
      }
      return e;
    });
    if (changed) {
      const body = Buffer.from(updated.map((e) => JSON.stringify(e)).join("\n"));
      try {
        const [bucket, key] = parseGcsUrl(rubbishLogPath);
        await putObject(bucket, key, body, "application/x-ndjson");
        console.log("rubbish_log_backfilled", { jobId, entries: updated.length });
      } catch (e) {
        console.warn("backfill_rubbish_write_failed", { jobId, error: String(e) });
      }
    }
  }

  // Backfill merged parsed output files
  for (const p of mergedPaths) {
    try {
      const [bucket, key] = parseGcsUrl(p);
      const buffer = await readFull(bucket, key);
      const reader = await ParquetReader.openBuffer(buffer);
      const cursor = reader.getCursor();
      const rows: Record<string, any>[] = [];
      let row: any;
      while ((row = await cursor.next())) {
        rows.push(sanitizeBigInt(row));
      }
      await reader.close();

      let fileChanged = false;
      for (const r of rows) {
        const line = lineMap.get(r._byte_offset);
        if (line !== undefined && r._line_no !== line) {
          r._line_no = line;
          fileChanged = true;
        }
      }

      if (fileChanged) {
        const tempFile = path.join(os.tmpdir(), `${randomUUID()}.parquet`);
        const writer = await ParquetWriter.openFile(buildSchema(rows), tempFile);
        for (const r of rows) {
          await writer.appendRow(sanitizeBigInt(r));
        }
        await writer.close();

        const uploadStream = gcsClient().bucket(bucket).file(key).createWriteStream({ contentType: "application/octet-stream" });
        const readStream = createReadStream(tempFile);
        try {
          await pipeline(readStream, uploadStream);
        } finally {
          await fs.unlink(tempFile).catch(() => {});
        }
      }
    } catch (e) {
      console.warn("backfill_output_failed", { jobId, path: p, error: String(e) });
    }
  }
}

function computeLineMap(source: Buffer, offsets: number[]): Map<number, number> {
  const lineMap = new Map<number, number>();
  let sourcePos = 0;
  let nextOffsetIndex = 0;
  let newlineCount = 0;

  while (sourcePos < source.length && nextOffsetIndex < offsets.length) {
    while (nextOffsetIndex < offsets.length && offsets[nextOffsetIndex] <= sourcePos) {
      lineMap.set(offsets[nextOffsetIndex], newlineCount + 1);
      nextOffsetIndex++;
    }
    if (source[sourcePos] === 0x0a) {
      newlineCount++;
    }
    sourcePos++;
  }

  while (nextOffsetIndex < offsets.length && offsets[nextOffsetIndex] <= sourcePos) {
    lineMap.set(offsets[nextOffsetIndex], newlineCount + 1);
    nextOffsetIndex++;
  }

  return lineMap;
}

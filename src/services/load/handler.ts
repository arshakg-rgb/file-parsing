import { settings } from "../../shared/config.js";
import { EventType, JobEvent, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, LoadMessage } from "../../shared/models/job.js";
import { pool } from "../../shared/db.js";
import { receiveMessages, deleteMessage, publishEvent } from "../../shared/queueUtils.js";
import { parseS3Url, readFull } from "../../shared/s3Utils.js";
import { ParquetReader } from "@dsnp/parquetjs";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { startHealthCheckServer } from "../../shared/health.js";

const logger = createLogger("load");

if (process.env.HEALTH_CHECK_PORT) {
  startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
}

function emit(jobId: string, eventType: EventType, data: Record<string, any>) {
  publishEvent(makeJobEvent(eventType, jobId, "load", data));
}

export async function loadJob(msg: LoadMessage): Promise<void> {
  const jobId = msg.job_id;

  if (msg.recovered_row) {
    logger.info("load_recovered_row", { job_id: jobId, byte_offset: msg.byte_offset });
    metrics.increment("load.recovered_row", 1);
    const row = buildRecoveredRow(msg);
    await upsertRows(jobId, [row]);
    emit(jobId, EventType.LOADING_COMPLETED, { total_rows: 1 });
    return;
  }

  logger.info("load_start", { job_id: jobId, parts: (msg.merged_parquet_paths || []).length });
  metrics.increment("load.start", 1, { parts: String((msg.merged_parquet_paths || []).length) });

  let totalRows = 0;
  try {
    for (const s3Path of msg.merged_parquet_paths || []) {
      const rows = await readParquet(s3Path);
      if (!rows.length) continue;
      await upsertRows(jobId, rows);
      totalRows += rows.length;
    }

    logger.info("load_complete", { job_id: jobId, total_rows: totalRows });
    metrics.set("load.rows_loaded", totalRows);
    emit(jobId, EventType.LOADING_COMPLETED, { total_rows: totalRows });
  } catch (exc) {
    logger.error("load_failed", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
    metrics.increment("load.error", 1);
    emit(jobId, EventType.ERROR_OCCURRED, { error: String(exc) });
  }
}

function buildRecoveredRow(msg: LoadMessage): Record<string, any> {
  const now = new Date().toISOString();
  return {
    ...msg.recovered_row,
    _job_id: msg.job_id,
    _byte_offset: msg.byte_offset ?? 0,
    _byte_length: msg.byte_length ?? 0,
    _record_index: 0,
    _line_no: msg.line_no ?? 0,
    _template_id: msg.template_id ?? "unknown",
    _template_version: msg.template_version ?? 1,
    _checksum: "",
    _parsed_at: now,
    _part_id: "recovered",
  };
}

async function readParquet(s3Path: string): Promise<Record<string, any>[]> {
  const [bucket, key] = parseS3Url(s3Path);
  const raw = await readFull(bucket, key);
  const reader = await ParquetReader.openBuffer(raw);
  const cursor = reader.getCursor();
  const rows: Record<string, any>[] = [];
  let row: any;
  while ((row = await cursor.next())) {
    rows.push(row);
  }
  await reader.close();
  return rows;
}

const SYSTEM_COLS = [
  "_job_id", "_byte_offset", "_byte_length", "_record_index",
  "_line_no", "_template_id", "_template_version", "_checksum",
  "_parsed_at", "_part_id",
] as const;

const PARAMS_PER_ROW = SYSTEM_COLS.length + 1; // +1 for fields JSONB
const UPSERT_BATCH = Math.floor(60000 / PARAMS_PER_ROW); // stay well under 65535 limit

async function upsertRows(_jobId: string, rows: Record<string, any>[]): Promise<void> {
  if (!rows.length) return;

  for (let i = 0; i < rows.length; i += UPSERT_BATCH) {
    const batch = rows.slice(i, i + UPSERT_BATCH);
    const placeholders: string[] = [];
    const values: any[] = [];
    let idx = 1;

    for (const row of batch) {
      const rowPh: string[] = [];
      for (const col of SYSTEM_COLS) {
        rowPh.push(`$${idx++}`);
        let v = row[col] ?? null;
        if (col === "_parsed_at" && typeof v === "number") v = new Date(v);
        values.push(v);
      }
      // Collect all non-system fields into a JSONB payload
      const fields: Record<string, any> = {};
      for (const [k, v] of Object.entries(row)) {
        if (!k.startsWith("_")) fields[k] = v;
      }
      rowPh.push(`$${idx++}`);
      values.push(JSON.stringify(fields));
      placeholders.push(`(${rowPh.join(", ")})`);
    }

    const colList = [...SYSTEM_COLS, "fields"].map((c) => `"${c}"`).join(", ");
    const sql = `INSERT INTO parsed_records (${colList}) VALUES ${placeholders.join(", ")} ON CONFLICT ("_job_id", "_byte_offset") DO NOTHING`;
    await pool.query(sql, values);
    logger.debug("upsert_batch", { rows: batch.length, offset: i });
  }
}

export async function consumerLoop(): Promise<void> {
  logger.info("load_consumer_started");
  while (true) {
    const messages = await receiveMessages<LoadMessage>(
      settings.LOAD_QUEUE_URL,
      (body) => JSON.parse(body) as LoadMessage,
      1
    );
    for (const { payload, receiptHandle } of messages) {
      try {
        await loadJob(payload);
        await deleteMessage(settings.LOAD_QUEUE_URL, receiptHandle);
      } catch (exc) {
        logger.error("load_message_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
        metrics.increment("load.message_error", 1);
      }
    }
  }
}

consumerLoop();

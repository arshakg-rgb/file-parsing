import { pool, ParseJobRow } from "../../shared/db.js";
import { JobStatus, JobStatus as JS, VALID_TRANSITIONS, isTerminal } from "../../shared/models/job.js";
import { EventType, JobEvent, ParsingCompletedData } from "../../shared/models/events.js";
import { sendRaw, publishEvent } from "../../shared/queueUtils.js";
import { settings } from "../../shared/config.js";
import { randomUUID } from "crypto";
import { SourceType } from "../../shared/models/job.js";
import { finalizeOutput } from "./finalize.js";

export class TransitionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TransitionError";
  }
}

export async function getJob(jobId: string): Promise<ParseJobRow | undefined> {
  const result = await pool.query<ParseJobRow>("SELECT * FROM parse_jobs WHERE job_id = $1", [jobId]);
  return result.rows[0];
}

export async function transition(
  jobId: string,
  newStatus: JobStatus,
  error?: string,
  extraFields: Record<string, any> = {}
): Promise<ParseJobRow> {
  const row = await getJob(jobId);
  if (!row) throw new TransitionError(`Job ${jobId} not found`);

  const current = row.status as JobStatus;
  if (!VALID_TRANSITIONS[current]?.includes(newStatus)) {
    throw new TransitionError(`Job ${jobId}: cannot transition ${current} → ${newStatus}`);
  }

  const timingMap: Record<string, string> = {
    [JobStatus.INGESTING]: "ingesting_at",
    [JobStatus.DETECTING]: "detecting_at",
    [JobStatus.PARSING]: "parsing_at",
    [JobStatus.FINALIZING]: "finalizing_at",
    [JobStatus.LOADING]: "loading_at",
    [JobStatus.REPORTING]: "reporting_at",
  };

  const timings = { ...(row.timings || {}) };
  if (timingMap[newStatus]) {
    timings[timingMap[newStatus]] = new Date().toISOString();
  }
  if (isTerminal(newStatus)) {
    timings["completed_at"] = new Date().toISOString();
  }

  const updates: Record<string, any> = {
    status: newStatus,
    timings,
    updated_at: new Date(),
  };
  if (error) updates.error = error;
  Object.assign(updates, extraFields);

  const fields = Object.keys(updates);
  const values = Object.values(updates);
  const setClause = fields.map((f, i) => `${f} = $${i + 2}`).join(", ");

  await pool.query(`UPDATE parse_jobs SET ${setClause} WHERE job_id = $1`, [jobId, ...values]);
  return (await getJob(jobId))!;
}

export async function handleEvent(event: JobEvent): Promise<void> {
  const etype = event.event_type;

  if (etype === EventType.JOB_STATUS_CHANGED) {
    const newStatus = event.data.new_status as JobStatus;
    await transition(event.job_id, newStatus, event.data.error);
  } else if (etype === EventType.ENTRY_DISCOVERED) {
    await createChildJob(event);
  } else if (etype === EventType.PARSING_COMPLETED) {
    await onParsingCompleted(event);
  } else if (etype === EventType.LOADING_COMPLETED) {
    await transition(event.job_id, JobStatus.REPORTING);
    const row = await getJob(event.job_id);
    await sendRaw(settings.REPORT_QUEUE_URL, {
      job_id: event.job_id,
      status: row?.status,
      counts: row?.counts,
      output_paths: row?.output_paths,
      rubbish_log_path: (row?.timings as any)?._rubbish_log_path ?? null,
      dlq_count: (row?.timings as any)?._dlq_count ?? 0,
    });
  } else if (etype === EventType.ERROR_OCCURRED) {
    await transition(event.job_id, JobStatus.FAILED, event.data.error);
  }
}

async function createChildJob(event: JobEvent): Promise<void> {
  const data = event.data;
  const now = new Date().toISOString();
  const childId = randomUUID();

  await pool.query(
    `INSERT INTO parse_jobs
      (job_id, batch_id, parent_job_id, source_type, source_ref, s3_url, size, field_spec, exec_path, status, output_paths, counts, timings, error, created_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW())`,
    [
      childId,
      data.batch_id,
      data.parent_job_id,
      data.source_type || SourceType.ARCHIVE_ENTRY,
      data.entry_name,
      data.entry_s3_url,
      data.entry_size,
      JSON.stringify(data.field_spec),
      "stream",
      JobStatus.QUEUED,
      JSON.stringify([]),
      JSON.stringify({ parsed: 0, dropped_rubbish: 0, failed_by_class: {} }),
      JSON.stringify({ queued_at: now }),
      null,
    ]
  );

  await sendRaw(settings.CLASSIFY_QUEUE_URL, {
    job_id: childId,
    s3_url: data.entry_s3_url,
    size: data.entry_size,
    field_spec: data.field_spec,
  });

  console.log("child_job_created", { parent: data.parent_job_id, child: childId });
}

async function onParsingCompleted(event: JobEvent): Promise<void> {
  const data = event.data as ParsingCompletedData;
  const row = await getJob(event.job_id);
  if (!row) return;

  const counts = { ...(row.counts || { parsed: 0, dropped_rubbish: 0, failed_by_class: {} }) };
  counts.parsed = data.parsed;
  counts.dropped_rubbish = data.dropped_rubbish;

  // Stash rubbish_log_path and dlq_count in timings so the report step can read them later
  const timings = {
    ...(row.timings || {}),
    _rubbish_log_path: data.rubbish_log_path ?? null,
    _dlq_count: data.dlq_count ?? 0,
  };

  const totalLines = data.parsed + data.dropped_rubbish + data.failed;
  const failedRatio = totalLines > 0 ? data.failed / totalLines : 0;

  await transition(event.job_id, JobStatus.FINALIZING, undefined, { counts, timings });

  // Merge parts by template, backfill line numbers, and produce a final set of paths.
  const finalizeResult = await finalizeOutput(event.job_id, data.part_s3_paths, settings.DATA_BUCKET);
  if (finalizeResult.failed) {
    console.error("finalize_failed", { job_id: event.job_id, error: finalizeResult.error });
    await transition(event.job_id, JobStatus.FAILED, finalizeResult.error || "finalize_failed");
    return;
  }
  const mergedPaths = finalizeResult.paths;

  console.log("finalize_complete", { job_id: event.job_id, merged_paths_count: mergedPaths.length, merged_paths: mergedPaths });

  if (failedRatio > settings.FAILED_LINE_RATIO_THRESHOLD) {
    console.warn("quality_gate_held", { job_id: event.job_id, failed_ratio: failedRatio, threshold: settings.FAILED_LINE_RATIO_THRESHOLD });
    await transition(event.job_id, JobStatus.HELD, undefined, { output_paths: mergedPaths });
    return;
  }

  // If no output paths but we have parsed data, this is likely a template issue
  if (mergedPaths.length === 0 && data.parsed > 0) {
    console.warn("no_output_paths_with_parsed_data", { job_id: event.job_id, parsed: data.parsed });
    await transition(event.job_id, JobStatus.FAILED, "No output files generated despite parsed data");
    return;
  }

  // If no output paths and no parsed data, complete successfully
  if (mergedPaths.length === 0 && data.parsed === 0) {
    console.info("no_output_no_data", { job_id: event.job_id });
    await transition(event.job_id, JobStatus.DONE, undefined, { output_paths: [] });
    return;
  }

  await transition(event.job_id, JobStatus.LOADING, undefined, { output_paths: mergedPaths });
  await sendRaw(settings.LOAD_QUEUE_URL, {
    job_id: event.job_id,
    merged_parquet_paths: mergedPaths,
    field_spec: row.field_spec,
  });
}

import { repositories, ParseJobRow } from "@shared/DatabaseManager.js";
import { JobStatus, JobStatus as JS, VALID_TRANSITIONS, isTerminal, JobTimings, JobCounts } from "@shared/models/job.js";
import { EventType, JobEvent, ParsingCompletedData, EntryDiscoveredData, StatusChangedData } from "@shared/models/events.js";
import { sendRaw, publishEvent } from "@shared/QueueService.js";
import { settings } from "@shared/Settings.js";
import { randomUUID } from "crypto";
import { SourceType } from "@shared/models/job.js";
import { finalizeOutput } from "./FinalizationService.js";

/**
 * Class representing a transition error error.
 */
export class TransitionError extends Error {
    /**
   * Constructs a new TransitionError instance.
   * @param message - The message
   */
  constructor(message: string) {
    super(message);
    this.name = "TransitionError";
  }
}

/**
 * Gets job
 * @param jobId - The job identifier
 * @returns A promise that resolves to the result
 */
export async function getJob(jobId: string): Promise<ParseJobRow | undefined> {
  return repositories.jobs.findById(jobId) as Promise<ParseJobRow | undefined>;
}

/**
 * Performs the transition operation.
 * @param jobId - The job identifier
 * @param newStatus - The new status
 * @param error - The error that occurred
 * @param extraFields - The extra fields
 * @returns A promise that resolves to the result
 */
export async function transition(
  jobId: string,
  newStatus: JobStatus,
  error?: string,
  extraFields: Record<string, unknown> = {}
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

  const updates: Record<string, unknown> = {
    status: newStatus,
    timings,
    updated_at: new Date(),
  };
  if (error) updates.error = error;
  Object.assign(updates, extraFields);

  // Merge nested timing/count objects so callers don't accidentally drop fields like
  // _csv_output_path or previously recorded transition timestamps.
  if (extraFields.timings && typeof extraFields.timings === "object") {
    updates.timings = { ...timings, ...(extraFields.timings as Record<string, unknown>) };
  }
  if (extraFields.counts && typeof extraFields.counts === "object") {
    updates.counts = { ...(row.counts || { parsed: 0, dropped_rubbish: 0, failed_by_class: {} }), ...(extraFields.counts as Record<string, unknown>) };
  }

  await repositories.jobs.updateFields(jobId, updates);
  return (await getJob(jobId))!;
}

/**
 * Handles event
 * @param event - The event
 */
export async function handleEvent(event: JobEvent): Promise<void> {
  const etype = event.event_type;

  if (etype === EventType.JOB_STATUS_CHANGED) {
    const statusData = event.data as unknown as StatusChangedData;
    await transition(event.job_id, statusData.new_status, statusData.error);
  } else if (etype === EventType.ENTRY_DISCOVERED) {
    await createChildJob(event);
  } else if (etype === EventType.PARSING_COMPLETED) {
    await onParsingCompleted(event);
  } else if (etype === EventType.LOADING_COMPLETED) {
    const row = await getJob(event.job_id);
    await transition(event.job_id, JobStatus.REPORTING, undefined, { counts: row?.counts });
    await sendRaw(settings.REPORT_QUEUE_URL, {
      job_id: event.job_id,
      status: row?.status,
      counts: row?.counts,
      output_paths: Array.isArray(row?.output_paths) ? row.output_paths : [],
      rubbish_log_path: (row?.timings as JobTimings)?._rubbish_log_path ?? null,
      dlq_count: (row?.timings as JobTimings)?._dlq_count ?? 0,
      csv_output_path: (row?.timings as JobTimings)?._csv_output_path ?? null,
    });
  } else if (etype === EventType.REPORTING_COMPLETED) {
    const row = await getJob(event.job_id);
    // Use counts from event data if available, otherwise use database
    const counts = (event.data as Record<string, unknown>).counts as JobCounts || row?.counts;
    await transition(event.job_id, JobStatus.DONE, undefined, { counts });
  } else if (etype === EventType.ERROR_OCCURRED) {
    await transition(event.job_id, JobStatus.FAILED, (event.data as Record<string, unknown>).error as string);
  }
}

/**
 * Creates child job
 * @param event - The event
 */
async function createChildJob(event: JobEvent): Promise<void> {
  const data = event.data as unknown as EntryDiscoveredData;
  const now = new Date().toISOString();
  const childId = randomUUID();

  // Ensure field_spec is never null - fallback to empty array
  const rawFieldSpec = data.field_spec || [];
  const fieldSpec = Array.isArray(rawFieldSpec) ? rawFieldSpec : (typeof rawFieldSpec === "string" ? JSON.parse(rawFieldSpec) : []);

  await repositories.jobs.create({
    job_id: childId,
    batch_id: data.batch_id,
    parent_job_id: data.parent_job_id,
    source_type: SourceType.ARCHIVE_ENTRY,
    source_ref: data.entry_name,
    s3_url: data.entry_s3_url,
    size: data.entry_size,
    field_spec: fieldSpec,
    exec_path: "stream",
    status: JobStatus.QUEUED,
    output_paths: [],
    counts: { parsed: 0, dropped_rubbish: 0, failed_by_class: {} },
    timings: { queued_at: now },
    error: null,
  });

  await sendRaw(settings.CLASSIFY_QUEUE_URL, {
    job_id: childId,
    s3_url: data.entry_s3_url,
    size: data.entry_size,
    field_spec: data.field_spec,
  });

  console.log("child_job_created", { parent: data.parent_job_id, child: childId });
}

/**
 * Handles the parsing completed
 * @param event - The event
 */
async function onParsingCompleted(event: JobEvent): Promise<void> {
  const data = event.data as unknown as ParsingCompletedData;
  console.log("parsing_completed_received", { job_id: event.job_id, parsed: data.parsed, dropped: data.dropped_rubbish, failed: data.failed, part_count: data.part_s3_paths.length });
  
  const row = await getJob(event.job_id);
  if (!row) {
    console.error("job_not_found", { job_id: event.job_id });
    return;
  }

  const counts = { ...(row.counts || { parsed: 0, dropped_rubbish: 0, failed_by_class: {} }) };
  counts.parsed = data.parsed;
  counts.dropped_rubbish = data.dropped_rubbish;
  counts.failed_by_class = data.failed_by_class || {};
  counts.dlq_count = data.dlq_count ?? 0;

  // Stash rubbish_log_path, dlq_count, and csv_output_path in timings so the report step can read them later
  const timings = {
    ...(row.timings || {}),
    _rubbish_log_path: data.rubbish_log_path ?? null,
    _dlq_count: data.dlq_count ?? 0,
    _csv_output_path: data.csv_output_path ?? null,
  };

  const totalLines = data.parsed + data.dropped_rubbish + data.failed;
  const failedRatio = totalLines > 0 ? data.failed / totalLines : 0;

  console.log("transitioning_to_finalizing", { job_id: event.job_id, failed_ratio: failedRatio });
  await transition(event.job_id, JobStatus.FINALIZING, undefined, { counts, timings });

  // Merge parts by template, backfill line numbers, and produce a final set of paths.
  console.log("starting_finalization", { job_id: event.job_id, part_paths: data.part_s3_paths });
  
  try {
    const finalizeResult = await finalizeOutput(event.job_id, data.part_s3_paths, settings.DATA_BUCKET);
    console.log("finalization_result", { job_id: event.job_id, failed: finalizeResult.failed, paths_count: finalizeResult.paths.length, error: finalizeResult.error });
    
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
      console.warn("no_output_paths_with_parsed_data", { job_id: event.job_id, parsed: data.parsed, part_paths: data.part_s3_paths });
      await transition(event.job_id, JobStatus.FAILED, "No output files generated despite parsed data");
      return;
    }

    // If no output paths and no parsed data, complete successfully
    if (mergedPaths.length === 0 && data.parsed === 0) {
      console.info("no_output_no_data", { job_id: event.job_id });
      await transition(event.job_id, JobStatus.DONE, undefined, { output_paths: [] });
      return;
    }

    console.log("transitioning_to_loading", { job_id: event.job_id, merged_paths_count: mergedPaths.length });
    await transition(event.job_id, JobStatus.LOADING, undefined, { output_paths: mergedPaths, counts });
    await sendRaw(settings.LOAD_QUEUE_URL, {
      job_id: event.job_id,
      output_paths: mergedPaths,
      field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
    });
    console.log("loading_message_sent", { job_id: event.job_id });
  } catch (error) {
    console.error("finalization_exception", { job_id: event.job_id, error: String(error), stack: error instanceof Error ? error.stack : undefined });
    await transition(event.job_id, JobStatus.FAILED, `Finalization error: ${String(error)}`);
  }
}

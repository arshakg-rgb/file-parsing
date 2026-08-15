import pino from "pino";
import { randomUUID } from "crypto";
import { InstantiationError } from "@errors/InstantiationError.js";
import { TransitionError } from "@errors/TransitionError.js";
import {DatabaseService, ParseJobRow} from "@shared/DatabaseManager.js";
import {
  JobStatus,
  VALID_TRANSITIONS,
  isTerminal,
  JobTimings,
  JobCounts,
  SourceType,
} from "@shared/models/job.js";
import {
  EventType,
  JobEvent,
  ParsingCompletedData,
  EntryDiscoveredData,
  StatusChangedData,
} from "@shared/models/events.js";
import { settings } from "@shared/Settings.js";
import { createLogger } from "@utils/logger/Log.js";
import {finalizeOutput, FinalizeResult} from "./FinalizationService.js";
import { EMPTY_COUNTS, TIMING_FIELD_BY_STATUS } from "@service/job-service/io/IStateMachine.js";
import {StateMachine} from "@service/job-service/StateMachine";
import {IParseJob} from "@config/db/models";
import {QueueService} from "@shared/QueueService";

/**
 * Singleton implementation of the State Machine business layer.
 *
 * Orchestrates the lifecycle of parse jobs: status transitions, child-job
 * fan-out for archive entries, and the post-parsing finalize/load/report
 * pipeline.
 */
export class StateMachineImpl implements StateMachine
{
  private static instance: StateMachineImpl;
  private readonly logger: pino.Logger;
  private readonly jobsRepo;
  private readonly jobLogsRepo;
  private readonly finalize: typeof finalizeOutput;
  private readonly enqueue;

  /**
   * Private constructor to enforce a Singleton pattern.
   *
   * @param enforce - Function to enforce a Singleton pattern.
   * @param jobsRepo - The jobs repository instance.
   * @param finalize - The output-finalization function.
   * @param enqueue - The queue-send function.
   * @param logger - The logger instance.
   * @throws InstantiationError if instantiation is attempted directly.
   */

  private constructor(enforce: () => void, jobsRepo, jobLogsRepo, finalize: typeof finalizeOutput, enqueue, logger: pino.Logger)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate StateMachineImpl directly. Use getInstance()");
    }

    this.jobsRepo = jobsRepo;
    this.jobLogsRepo = jobLogsRepo;
    this.finalize = finalize;
    this.enqueue = enqueue;
    this.logger = logger;
  }

  /**
   * Gets the singleton instance of StateMachineImpl.
   *
   * @returns The singleton instance of StateMachineImpl.
   */

  public static getInstance(): StateMachineImpl
  {
    if (!StateMachineImpl.instance)
    {
      StateMachineImpl.instance = new StateMachineImpl(
          Enforce,
          DatabaseService.getInstance().repositories.jobs,
          DatabaseService.getInstance().repositories.jobLogs,
          finalizeOutput,
          (queueUrl: string, body: Record<string, unknown>, delaySeconds?: number) => QueueService.getInstance().sendRaw(queueUrl, body, delaySeconds),
          createLogger(module)
      );
    }

    return StateMachineImpl.instance;
  }

  /**
   * Fetches a job by id.
   *
   * @param jobId - The job identifier
   * @returns The job row, or undefined if it does not exist.
   */

  public async getJob(jobId: string): Promise<ParseJobRow | undefined>
  {
    return this.jobsRepo.findById(jobId) as Promise<ParseJobRow | undefined>;
  }

  /**
   * Moves a job to a new status, validating the transition, stamping the
   * relevant timing field, and merging any extra fields (with nested
   * `timings`/`counts` objects merged rather than overwritten).
   *
   * @param jobId - The job identifier
   * @param newStatus - The status to transition into
   * @param error - Optional error message to persist alongside the transition
   * @param extraFields - Additional row fields to merge into the update
   * @throws TransitionError if the job does not exist or the transition is invalid.
   */

  public async transition(jobId: string, newStatus: JobStatus, error?: string, extraFields: Record<string, unknown> = {}): Promise<ParseJobRow | null>
  {
    const row: IParseJob = await this.getJob(jobId);

    if (!row)
    {
      throw new TransitionError(`Job ${jobId} not found`);
    }

    const current = row.status as JobStatus;

    if (!VALID_TRANSITIONS[current]?.includes(newStatus))
    {
      throw new TransitionError(`Job ${jobId}: cannot transition ${current} → ${newStatus}`);
    }

    const timings = { ...(row.timings || {}) };
    const timingField: string = TIMING_FIELD_BY_STATUS[newStatus];

    if (timingField)
    {
      timings[timingField] = new Date().toISOString();
    }

    if (isTerminal(newStatus))
    {
      timings["completed_at"] = new Date().toISOString();
    }

    const changes: Partial<ParseJobRow> = {
      ...(extraFields as Partial<ParseJobRow>),
    };

    if (error !== undefined)
    {
      changes.error = error;
    }

    if (extraFields.timings && typeof extraFields.timings === "object")
    {
      changes.timings = { ...timings, ...(extraFields.timings as Record<string, unknown>) } as JobTimings;
    }
    else
    {
      changes.timings = timings as JobTimings;
    }

    if (extraFields.counts && typeof extraFields.counts === "object")
    {
      changes.counts = {
        ...(row.counts || EMPTY_COUNTS),
        ...(extraFields.counts as Record<string, unknown>),
      } as JobCounts;
    }

    const ok = await this.jobsRepo.tryTransitionStatus(jobId, newStatus, [current], changes);

    if (!ok)
    {
      this.logger.info({ job_id: jobId, attempted: newStatus, from: current }, "transition_lost_race");
      return null;
    }

    this.logger.info(
        {
          job_id: jobId,
          from: current,
          to: newStatus,
          terminal: isTerminal(newStatus),
        },
        "job_transition"
    );

    if (newStatus === JobStatus.FAILED)
    {
      await this.jobLogsRepo.log({
        job_id: jobId,
        event_type: "crashed",
        stage: current,
        message: error ?? null,
        metadata: { from: current, to: newStatus },
      });
    }
    else
    {
      await this.jobLogsRepo.log({
        job_id: jobId,
        event_type: "stage_completed",
        stage: newStatus,
        message: null,
        metadata: { from: current, to: newStatus, terminal: isTerminal(newStatus) },
      });
    }

    return (await this.getJob(jobId))!;
  }

  /**
   * Dispatches an incoming job-lifecycle event to the appropriate handler.
   *
   * @param event - The event
   */

  public async handleEvent(event: JobEvent): Promise<void>
  {
    switch (event.event_type)
    {
      case EventType.JOB_STATUS_CHANGED:
      {
        const statusData = event.data as unknown as StatusChangedData;
        const row = await this.getJob(event.job_id);

        if (!row)
        {
          this.logger.error({ job_id: event.job_id }, "job_status_changed_job_not_found");
          break;
        }

        const current = row.status as JobStatus;

        if (isTerminal(current))
        {
          this.logger.info({ job_id: event.job_id, current_status: current, requested: statusData.new_status }, "job_status_changed_ignored_terminal");
          break;
        }

        if (current === statusData.new_status)
        {
          break;
        }

        await this.transition(event.job_id, statusData.new_status, statusData.error);
        break;
      }
      case EventType.ENTRY_DISCOVERED:
        await this.createChildJob(event);
        break;
      case EventType.PARSING_COMPLETED:
        await this.onParsingCompleted(event);
        break;
      case EventType.LOADING_COMPLETED:
        await this.onLoadingCompleted(event);
        break;
      case EventType.REPORTING_COMPLETED:
        await this.onReportingCompleted(event);
        break;
      case EventType.ERROR_OCCURRED:
      {
        const row = await this.getJob(event.job_id);
        if (!row || isTerminal(row.status as JobStatus))
        {
          break;
        }

        await this.transition(
            event.job_id,
            JobStatus.FAILED,
            (event.data as Record<string, unknown>).error as string
        );
        break;
      }
    }
  }

  /**
   * Handles the LOADING_COMPLETED event: moves the job into REPORTING and
   * enqueues the report request.
   *
   * @param event - The event
   */

  private async onLoadingCompleted(event: JobEvent): Promise<void>
  {
    const row: IParseJob = await this.getJob(event.job_id);

    const current = row?.status as JobStatus | undefined;

    if (!row || current !== JobStatus.SAVING_TO_DATABASE)
    {
      this.logger.info({ job_id: event.job_id, status: current }, "loading_completed_ignored");
      return;
    }

    const updated = await this.transition(event.job_id, JobStatus.REPORTING, undefined, { counts: row.counts });

    if (!updated)
    {
      this.logger.info({ job_id: event.job_id }, "loading_completed_lost_race");
      return;
    }

    await this.enqueue(settings.REPORT_QUEUE_URL, {
      job_id: event.job_id,
      status: row.status,
      counts: row.counts,
      output_paths: Array.isArray(row.output_paths) ? row.output_paths : [],
      rubbish_log_path: (row.timings as JobTimings)?._rubbish_log_path ?? null,
      dlq_count: (row.timings as JobTimings)?._dlq_count ?? 0,
      csv_output_path: (row.timings as JobTimings)?._csv_output_path ?? null,
    });
  }

  /**
   * Handles the REPORTING_COMPLETED event: marks the job DONE, preferring
   * counts carried on the event over the persisted row.
   *
   * @param event - The event
   */

  private async onReportingCompleted(event: JobEvent): Promise<void>
  {
    const row: IParseJob = await this.getJob(event.job_id);

    const current = row?.status as JobStatus | undefined;

    if (!row || current !== JobStatus.REPORTING)
    {
      this.logger.info({ job_id: event.job_id, status: current }, "reporting_completed_ignored");
      return;
    }

    const counts: JobCounts = ((event.data as Record<string, unknown>).counts as JobCounts) || row.counts;
    const updated = await this.transition(event.job_id, JobStatus.COMPLETED, undefined, { counts });

    if (!updated)
    {
      this.logger.info({ job_id: event.job_id }, "reporting_completed_lost_race");
    }
  }

  /**
   * Creates a child job for a discovered archive entry and enqueues it for
   * classification.
   *
   * @param event - The event
   * @throws Error if the entry's source_ref or field_spec is invalid.
   */

  private async createChildJob(event: JobEvent): Promise<void>
  {
    const data = event.data as unknown as EntryDiscoveredData;
    const now: string = new Date().toISOString();
    const childId = randomUUID();

    if (!data.entry_s3_url || !/^gs:\/\/|^s3:\/\//i.test(data.entry_s3_url))
    {
      throw new Error(`entry_s3_url must be a gs:// or s3:// URL: ${data.entry_s3_url}`);
    }

    if (!Array.isArray(data.field_spec))
    {
      throw new Error("field_spec must be an array of field names");
    }

    const fieldSpec: string[] = data.field_spec;

    await this.jobsRepo.create({
      job_id: childId,
      batch_id: data.batch_id,
      parent_job_id: data.parent_job_id,
      source_type: SourceType.ARCHIVE_ENTRY,
      source_ref: data.entry_name,
      s3_url: data.entry_s3_url,
      size: data.entry_size,
      field_spec: fieldSpec,
      exec_path: "stream",
      status: JobStatus.CREATED,
      output_paths: [],
      counts: { ...EMPTY_COUNTS },
      timings: { queued_at: now },
      error: null,
    });

    await this.enqueue(settings.CLASSIFY_QUEUE_URL, {
      job_id: childId,
      s3_url: data.entry_s3_url,
      size: data.entry_size,
      field_spec: data.field_spec,
    });

    this.logger.info({ parent: data.parent_job_id, child: childId }, "child_job_created");
  }

  /**
   * Handles the PARSING_COMPLETED event: records parse counts, transitions
   * to FINALIZING, then drives the finalize → quality-gate → load pipeline.
   *
   * @param event - The event
   */

  private async onParsingCompleted(event: JobEvent): Promise<void>
  {
    const rawData = event.data as unknown as ParsingCompletedData;
    const data: ParsingCompletedData = {
      ...rawData,
      part_s3_paths: rawData.part_s3_paths ?? [],
    };

    this.logger.info(
        {
          job_id: event.job_id,
          parsed: data.parsed,
          dropped: data.dropped_rubbish,
          failed: data.failed,
          part_count: data.part_s3_paths.length,
        },
        "parsing_completed_received"
    );

    const row: IParseJob = await this.getJob(event.job_id);

    if (!row)
    {
      this.logger.error({ job_id: event.job_id }, "job_not_found");
      return;
    }

    const currentStatus = row.status as JobStatus;

    if (currentStatus !== JobStatus.PARSING)
    {
      this.logger.info({ job_id: event.job_id, status: currentStatus }, "parsing_completed_already_processed");
      return;
    }

    const counts = { ...(row.counts || EMPTY_COUNTS) };
    counts.parsed = data.parsed;
    counts.dropped_rubbish = data.dropped_rubbish;
    counts.failed_by_class = data.failed_by_class || {};
    counts.dlq_count = data.dlq_count ?? 0;

    const timings = {
      ...(row.timings || {}),
      _rubbish_log_path: data.rubbish_log_path ?? null,
      _dlq_count: data.dlq_count ?? 0,
      _csv_output_path: data.csv_output_path ?? null,
    };

    const totalLines: number = data.parsed + data.dropped_rubbish + data.failed;
    const failedRatio: number = totalLines > 0 ? data.failed / totalLines : 0;

    const [droppedSummary, failedSummary, templateUsage] = await Promise.all([
      DatabaseService.getInstance().repositories.rubbishLogs.getSummaryByJob(event.job_id).catch((err) => {
        this.logger.warn({ job_id: event.job_id, error: String(err) }, "dropped_summary_failed");
        return { count: data.dropped_rubbish, line_numbers: [], line_numbers_truncated: false, by_template: {} };
      }),
      DatabaseService.getInstance().repositories.deadLetters.getSummaryByJob(event.job_id).catch((err) => {
        this.logger.warn({ job_id: event.job_id, error: String(err) }, "failed_summary_failed");
        return { count: data.failed, line_numbers: [], line_numbers_truncated: false, by_class: data.failed_by_class || {} };
      }),
      DatabaseService.getInstance().repositories.parsedRecords.getTemplateUsageCounts(event.job_id).catch((err) => {
        this.logger.warn({ job_id: event.job_id, error: String(err) }, "template_usage_failed");
        return [];
      }),
    ]);

    await this.jobLogsRepo.log({
      job_id: event.job_id,
      event_type: "parsing_summary",
      stage: "parsing",
      message: null,
      metadata: {
        parsed: data.parsed,
        templates_used: templateUsage,
        dropped_rubbish: droppedSummary,
        failed: failedSummary,
        dlq_count: data.dlq_count ?? 0,
        ai_calls: data.ai_calls ?? 0,
        ai_recoveries: data.ai_recoveries ?? 0,
      },
    });

    this.logger.info({ job_id: event.job_id, failed_ratio: failedRatio }, "transitioning_to_finalizing");
    const updated = await this.transition(event.job_id, JobStatus.MERGING_OUTPUT, undefined, { counts, timings });

    if (!updated)
    {
      this.logger.info({ job_id: event.job_id }, "parsing_completed_lost_race");
      return;
    }

    this.logger.info({ job_id: event.job_id, part_paths: data.part_s3_paths }, "starting_finalization");

    try
    {
      await this.finalizeAndAdvance(event.job_id, row, data, counts, failedRatio);
    }
    catch (error)
    {
      this.logger.error({ job_id: event.job_id, error: String(error), stack: error instanceof Error ? error.stack : undefined }, "finalization_exception");
      await this.transition(event.job_id, JobStatus.FAILED, `Finalization error: ${String(error)}`);
    }
  }

  /**
   * Runs finalization for a completed parse job and routes it to HELD,
   * FAILED, DONE, or LOADING based on the outcome and quality gate.
   *
   * @param jobId - The job identifier
   * @param row - The persisted job row
   * @param data - The parsing-completed event payload
   * @param counts - The recorded parse counts
   * @param failedRatio - The proportion of failed lines
   */

  private async finalizeAndAdvance(jobId: string, row: ParseJobRow, data: ParsingCompletedData, counts: JobCounts, failedRatio: number): Promise<void>
  {
    const finalizeResult: FinalizeResult = await this.finalize(jobId, data.part_s3_paths, settings.DATA_BUCKET);

    this.logger.info(
        {
          job_id: jobId,
          failed: finalizeResult.failed,
          paths_count: finalizeResult.paths.length,
          error: finalizeResult.error,
        },
        "finalization_result"
    );

    if (finalizeResult.failed)
    {
      this.logger.error({ job_id: jobId, error: finalizeResult.error }, "finalize_failed");
      await this.transition(jobId, JobStatus.FAILED, finalizeResult.error || "finalize_failed");
      return;
    }

    const mergedPaths: string[] = finalizeResult.paths;

    this.logger.info({ job_id: jobId, merged_paths_count: mergedPaths.length, merged_paths: mergedPaths }, "finalize_complete");

    await this.jobsRepo.updateFields(jobId, { output_paths: mergedPaths });

    if (failedRatio > settings.FAILED_LINE_RATIO_THRESHOLD)
    {
      this.logger.warn({ job_id: jobId, failed_ratio: failedRatio, threshold: settings.FAILED_LINE_RATIO_THRESHOLD }, "quality_gate_held");
      await this.transition(jobId, JobStatus.ON_HOLD, undefined, { output_paths: mergedPaths });
      return;
    }

    if (mergedPaths.length === 0 && data.parsed > 0)
    {
      this.logger.warn(
          { job_id: jobId, parsed: data.parsed, part_paths: data.part_s3_paths },
          "no_output_paths_with_parsed_data"
      );
      await this.transition(jobId, JobStatus.FAILED, "No output files generated despite parsed data");
      return;
    }

    if (mergedPaths.length === 0)
    {
      this.logger.info({ job_id: jobId }, "no_output_no_data");
      await this.transition(jobId, JobStatus.SAVING_TO_DATABASE, undefined, { output_paths: [], counts });
    }
    else
    {
      this.logger.info({ job_id: jobId, merged_paths_count: mergedPaths.length }, "transitioning_to_loading");
      await this.transition(jobId, JobStatus.SAVING_TO_DATABASE, undefined, { output_paths: mergedPaths, counts });
    }

    const sizeMb = (row.size ?? 0) / (1024 * 1024);
    const queueUrl = sizeMb <= 25
      ? settings.LOAD_QUEUE_URL_SMALL
      : sizeMb > 100
        ? settings.LOAD_QUEUE_URL_LARGE
        : settings.LOAD_QUEUE_URL;
    this.logger.info({ job_id: jobId, size_mb: sizeMb, queue: queueUrl }, "routing_to_load_queue");

    await this.enqueue(queueUrl, {
      job_id: jobId,
      output_paths: mergedPaths,
      field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
    });

    this.logger.info({ job_id: jobId }, "loading_message_sent");
  }
}

function Enforce(): void {}

export const getJob = (jobId: string) => StateMachineImpl.getInstance().getJob(jobId);
export const transition = (
    jobId: string,
    newStatus: JobStatus,
    error?: string,
    extraFields?: Record<string, unknown>
) => StateMachineImpl.getInstance().transition(jobId, newStatus, error, extraFields);
export const handleEvent = (event: JobEvent) => StateMachineImpl.getInstance().handleEvent(event);

import { randomUUID } from "crypto";
import { InstantiationError } from "@errors/InstantiationError.js";
import { CustomError } from "@errors/CustomError.js";
import { ValidationError } from "@errors/ValidationError.js";
import { settings } from "@shared/Settings.js";
import { repositories, ParseJobRow } from "@shared/DatabaseManager.js";
import { SourceType, JobStatus, JobTimings, JobCounts } from "@shared/models/job.js";
import { sendRaw } from "@shared/QueueService.js";
import { presignedPutUrl } from "@shared/GcsUtils.js";
import { transition } from "@service/job_service/stateMachine.js";
import { JobServiceService } from "@service/job_service/services/JobServiceService.js";
import { ICreateJobRequest, ICreateJobResponse, IJobResponse, IStuckJobsResponse, IProvidePasswordRequest, IMarkFailedRequest, IRetryJobRequest } from "@service/job_service/io/IJob.js";

/**
 * Singleton implementation of the Job Service business layer.
 */
export class JobServiceServiceImpl implements JobServiceService {
  private static instance: JobServiceServiceImpl;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate JobServiceServiceImpl directly. Use getInstance()");
    }
  }

  public static getInstance(): JobServiceServiceImpl {
    if (!JobServiceServiceImpl.instance) {
      JobServiceServiceImpl.instance = new JobServiceServiceImpl(Enforce);
    }
    return JobServiceServiceImpl.instance;
  }

  public async createJob(request: ICreateJobRequest): Promise<ICreateJobResponse> {
    const { source_type, source_ref, field_spec, batch_id, column_map } = request;

    let columnMap: Record<string, number | number[]> | undefined;
    if (column_map) {
      const raw = typeof column_map === "string" ? (() => { try { return JSON.parse(column_map); } catch { return undefined; } })() : column_map;
      if (raw && typeof raw === "object" && !Array.isArray(raw)) {
        const cleaned: Record<string, number | number[]> = {};
        for (const [k, v] of Object.entries(raw)) {
          if (typeof v === "number" && Number.isInteger(v) && v >= 0) cleaned[k] = v;
          else if (Array.isArray(v)) {
            const idxs = v.filter((n: unknown) => typeof n === "number" && Number.isInteger(n) && n >= 0);
            if (idxs.length) cleaned[k] = idxs;
          }
        }
        if (Object.keys(cleaned).length) columnMap = cleaned;
      }
    }

    if ([SourceType.S3, SourceType.URL].includes(source_type) && !source_ref) {
      throw new ValidationError("source_ref is required for s3 and url sources");
    }

    const namesFromArray = (arr: unknown[]): string[] =>
      arr.map((f) => (typeof f === "string" ? f : (f as { name?: string } | undefined | null)?.name)).filter((x): x is string => typeof x === "string");

    let fieldNames: string[] = [];
    if (field_spec) {
      if (Array.isArray(field_spec)) {
        fieldNames = namesFromArray(field_spec);
      } else if (typeof field_spec === "string") {
        const s = field_spec.trim();
        let parsed: unknown;
        try { parsed = JSON.parse(s); } catch { parsed = undefined; }
        if (Array.isArray(parsed)) fieldNames = namesFromArray(parsed);
        else if (parsed && Array.isArray((parsed as Record<string, unknown>).fields)) fieldNames = namesFromArray((parsed as Record<string, unknown>).fields as unknown[]);
        else if (s) fieldNames = s.split(",").map((x) => x.trim()).filter(Boolean);
      } else if ((field_spec as Record<string, unknown>).fields && Array.isArray((field_spec as Record<string, unknown>).fields)) {
        fieldNames = namesFromArray((field_spec as Record<string, unknown>).fields as unknown[]);
      }
    }

    const jobId = randomUUID();
    const batchId = batch_id || randomUUID();
    let putUrl: string | undefined;
    let s3Url: string | undefined;

    if (source_type === SourceType.UPLOAD) {
      const uploadKey = `uploads/${jobId}/source`;
      putUrl = await presignedPutUrl(settings.DATA_BUCKET, uploadKey);
      s3Url = `gs://${settings.DATA_BUCKET}/${uploadKey}`;
    }

    const now = new Date().toISOString();
    const row: ParseJobRow = {
      job_id: jobId,
      batch_id: batchId,
      source_type,
      source_ref: source_ref || s3Url!,
      s3_url: s3Url,
      field_spec: fieldNames,
      exec_path: "stream",
      status: JobStatus.QUEUED,
      output_paths: [],
      counts: { parsed: 0, dropped_rubbish: 0, failed_by_class: {} },
      timings: { queued_at: now },
      error: undefined,
      created_at: new Date(),
      updated_at: new Date(),
    };

    await repositories.jobs.create(row);

    const messageId = await sendRaw(settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      source_type,
      source_ref: source_ref || s3Url,
      field_spec: fieldNames,
      column_map: columnMap,
      batch_id: batchId,
    });

    return { job_id: jobId, status: JobStatus.QUEUED, presigned_put_url: putUrl, message_id: messageId };
  }

  public async findStuckJobs(thresholdMinutes: number): Promise<IStuckJobsResponse> {
    const rows = await repositories.jobs.findStuckJobs(thresholdMinutes);
    return { stuck_jobs: rows, count: rows.length, threshold_minutes: thresholdMinutes };
  }

  public async getJob(jobId: string): Promise<IJobResponse | null> {
    const row = await repositories.jobs.findById(jobId);
    if (!row) return null;

    return {
      job_id: row.job_id,
      batch_id: row.batch_id,
      status: row.status,
      counts: row.counts as JobCounts,
      timings: row.timings as JobTimings,
      output_paths: row.output_paths,
      csv_output_path: ((row.timings as JobTimings)._csv_output_path as string | undefined) ?? null,
      error: row.error,
    };
  }

  public async getBatchJobs(batchId: string): Promise<IJobResponse[]> {
    const rows = await repositories.jobs.findByBatchId(batchId);
    return rows.map((row: ParseJobRow) => ({
      job_id: row.job_id,
      batch_id: row.batch_id,
      status: row.status,
      counts: row.counts as JobCounts,
      timings: row.timings as JobTimings,
      output_paths: row.output_paths,
      csv_output_path: ((row.timings as JobTimings)._csv_output_path as string | undefined) ?? null,
      error: row.error,
    }));
  }

  public async providePassword(jobId: string, request: IProvidePasswordRequest): Promise<void> {
    const row = await repositories.jobs.findById(jobId);
    if (!row) {
      throw new CustomError("Job not found", "NOT_FOUND", 404);
    }
    if (row.status !== JobStatus.AWAITING_PASSWORD) {
      throw new CustomError(`Job is not awaiting a password (status=${row.status})`, "CONFLICT", 409);
    }
    await sendRaw(settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      action: "provide_password",
      password: request.password,
    });
  }

  public async releaseHold(jobId: string): Promise<void> {
    try {
      await transition(jobId, JobStatus.LOADING);
    } catch (err) {
      throw new CustomError(err instanceof Error ? err.message : String(err), "CONFLICT", 409);
    }
    await sendRaw(settings.LOAD_QUEUE_URL, { job_id: jobId, manual_override: true });
  }

  public async markFailed(jobId: string, request: IMarkFailedRequest): Promise<void> {
    const row = await repositories.jobs.findById(jobId);
    if (!row) {
      throw new CustomError("Job not found", "NOT_FOUND", 404);
    }
    await repositories.jobs.markFailed(jobId, request.reason || "manually_failed");
  }

  public async retryJob(jobId: string, request: IRetryJobRequest): Promise<void> {
    const { target_status } = request;
    const row = await repositories.jobs.findById(jobId);
    if (!row) {
      throw new CustomError("Job not found", "NOT_FOUND", 404);
    }

    let queueUrl: string;
    let message: Record<string, unknown> = { job_id: jobId, manual_override: true };

    switch (target_status) {
      case JobStatus.INGESTING:
        queueUrl = settings.INGEST_QUEUE_URL;
        message = {
          job_id: jobId,
          source_type: row.source_type,
          source_ref: row.source_ref,
          field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
          batch_id: row.batch_id,
          manual_override: true,
        };
        break;
      case JobStatus.DETECTING:
        queueUrl = settings.CLASSIFY_QUEUE_URL;
        message = {
          job_id: jobId,
          s3_url: row.s3_url,
          size: row.size,
          field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
          manual_override: true,
        };
        break;
      case JobStatus.PARSING:
        queueUrl = settings.PARSE_QUEUE_URL;
        message = {
          job_id: jobId,
          s3_url: row.s3_url,
          field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
          manual_override: true,
        };
        break;
      case JobStatus.LOADING:
        queueUrl = settings.LOAD_QUEUE_URL;
        break;
      case JobStatus.REPORTING:
        queueUrl = settings.REPORT_QUEUE_URL;
        break;
      default:
        throw new ValidationError(`Invalid target_status: ${target_status}`);
    }

    try {
      await transition(jobId, target_status);
    } catch (err) {
      throw new CustomError(err instanceof Error ? err.message : String(err), "CONFLICT", 409);
    }
    await sendRaw(queueUrl, message);
  }
}

function Enforce(): void {}

import pino from "pino";
import { randomUUID } from "crypto";
import { InstantiationError } from "@errors/InstantiationError.js";
import { ValidationError } from "@errors/ValidationError.js";
import { settings } from "@shared/Settings.js";
import { DatabaseManager } from "@shared/DatabaseManager.js";
import type { ParseJobRow } from "@shared/DatabaseManager.js";
import { SourceType, JobStatus, JobTimings, JobCounts, totalFailed, ColumnMap } from "@shared/models/job.js";
import { transition } from "@service/job-service/StateMachineImpl.js";
import { createLogger } from "@utils/logger/Log.js";
import {JobService } from "@service/job-service/services/JobService.js";
import { ICreateJobRequest, ICreateJobResponse, IJobResponse, IJobHeadersResponse, IStuckJobsResponse, IStatusesResponse, IProvidePasswordRequest, IMarkFailedRequest, IRetryJobRequest, IJobLogEntry, IUploadCsvRequest, IUploadCsvResponse, IUploadAndCreateJobRequest, IDownloadCsvResponse } from "@service/job-service/io/IJob.js";
import {HttpError} from "@errors/HttpError.js";
import {ServerError} from "@errors/ServerError.js";
import type {IParseJob, ParseJobAttributes} from "@config/db/models";
import {GcsUtils} from "@shared/GcsUtils";
import {QueueService} from "@shared/QueueService";

/**
 * Singleton implementation of the Job Service business layer.
 */
export class JobServiceImpl implements JobService
{
  private static instance: JobServiceImpl;
  private readonly dbManager: DatabaseManager;
  private readonly logger: pino.Logger;
  private gcsUtils: GcsUtils;
  private queueService: QueueService;

  /**
   * Private constructor to enforce a Singleton pattern.
   *
   * @param enforce - Function to enforce a Singleton pattern.
   * @param dbManager
   * @param gcsUtisl
   * @param queueService
   * @throws Error if instantiation is attempted directly.
   */

  private constructor(enforce: () => void, dbManager: DatabaseManager, gcsUtisl: GcsUtils, queueService: QueueService)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE,"Cannot instantiate JobServiceImpl directly. Use getInstance()");
    }

    this.gcsUtils = gcsUtisl;
    this.queueService = queueService;
    this.dbManager = dbManager;
    this.logger = createLogger(module);
  }

  /**
   * Gets the singleton instance of JobServiceImpl.
   *
   * @returns The singleton instance of JobServiceImpl.
   */

  public static getInstance(): JobServiceImpl
  {
    if (!JobServiceImpl.instance)
    {
      JobServiceImpl.instance = new JobServiceImpl(Enforce, DatabaseManager.getInstance(), GcsUtils.getInstance(), QueueService.getInstance());
    }

    return JobServiceImpl.instance;
  }

  /**
   * Creates a new parsing job.
   *
   * @param request - The job creation request.
   * @returns The created job details, including upload URL (if applicable).
   * @throws ValidationError if the request contains invalid input.
   * @throws ServerError if job creation fails.
   */

  public async createJob(request: ICreateJobRequest): Promise<ICreateJobResponse>
  {
    const { source_type, source_ref, field_spec, batch_id, column_map, content_type, filename } = request;

    let columnMap: Record<string, number | number[]> | undefined;

    if (column_map)
    {
      const raw = typeof column_map === "string" ? (() => { try { return JSON.parse(column_map); } catch { return undefined; } })() : column_map;

      if (raw && typeof raw === "object" && !Array.isArray(raw))
      {
        const cleaned: Record<string, number | number[]> = {};

        for (const [k, v] of Object.entries(raw))
        {
          if (typeof v === "number" && Number.isInteger(v) && v >= 0)
          {
            cleaned[k] = v;
          }

          else if (Array.isArray(v))
          {
            const idxs = v.filter((n: unknown) => typeof n === "number" && Number.isInteger(n) && n >= 0);

            if (idxs.length)
            {
              cleaned[k] = idxs;
            }
          }
        }
        if (Object.keys(cleaned).length) columnMap = cleaned;
      }
    }

    if (!source_type || !Object.values(SourceType).includes(source_type as SourceType))
    {
      throw new ValidationError(ValidationError.INPUT, `Invalid source_type: ${source_type}`);
    }

    if ([SourceType.BUCKET, SourceType.URL, SourceType.ARCHIVE_ENTRY].includes(source_type) && !source_ref)
    {
      throw new ValidationError(ValidationError.INPUT, "source_ref is required for bucket, url and archive_entry sources");
    }

    if ((source_type === SourceType.BUCKET || source_type === SourceType.ARCHIVE_ENTRY) && source_ref)
    {
      if (!/^gs:\/\/|^s3:\/\//i.test(source_ref))
      {
        throw new ValidationError(ValidationError.INPUT, `source_ref must be a gs:// or s3:// URL: ${source_ref}`);
      }

      let bucket: string;
      let key: string;

      try
      {
        [bucket, key] = this.gcsUtils.parseGcsUrl(source_ref);
      }
      catch
      {
        throw new ValidationError(ValidationError.INPUT, `source_ref must be a gs:// or s3:// URL: ${source_ref}`);
      }

      let size: number;

      try
      {
        size = await this.gcsUtils.objectSize(bucket, key);

      }
      catch (err)
      {
        if (err instanceof ValidationError)
        {
          throw err;
        }

        throw new ValidationError(ValidationError.INPUT, `source_ref file not found or unreadable: ${source_ref}`);
      }

      if (size === 0)
      {
        throw new ValidationError(ValidationError.INPUT, `source_ref file is empty: ${source_ref}`);
      }
    }

    if (source_type === SourceType.URL && source_ref && !/^https?:\/\//i.test(source_ref))
    {
      throw new ValidationError(ValidationError.INPUT, `source_ref must be an http(s) URL for url sources: ${source_ref}`);
    }

    const namesFromArray = (arr: unknown[]): string[] =>
      arr.map((f) => (typeof f === "string" ? f : (f as { name?: string } | undefined | null)?.name)).filter((x): x is string => typeof x === "string");

    let fieldNames: string[] = [];

    if (field_spec)
    {
      if (Array.isArray(field_spec))
      {
        fieldNames = namesFromArray(field_spec);
      }
      else
      {
        throw new ValidationError(ValidationError.INPUT, "field_spec must be an array of field names, not a string");
      }
    }


    const jobId = randomUUID();
    const batchId: string = batch_id || randomUUID();
    let putUrl: string | undefined;
    let s3Url: string | undefined;
    const uploadContentType: string = content_type || "application/octet-stream";

    if (source_type === SourceType.UPLOAD)
    {
      const rawName: string = (filename || "source").trim();
      const baseName: string = (rawName.split(/[\\/]/).pop() || "source").replace(/[#\s]+/g, "_") || "source";
      const uploadKey = `uploads/${jobId}/${baseName}`;
      putUrl = await this.gcsUtils.presignedPutUrl(settings.DATA_BUCKET, uploadKey, 3600, uploadContentType);
      s3Url = `gs://${settings.DATA_BUCKET}/${uploadKey}`;
    }

    const now: string = new Date().toISOString();
    const row: ParseJobRow = {
      job_id: jobId,
      batch_id: batchId,
      source_type,
      source_ref: source_ref || s3Url!,
      s3_url: s3Url,
      field_spec: fieldNames,
      exec_path: "stream",
      status: JobStatus.CREATED,
      output_paths: [],
      counts: { parsed: 0, dropped_rubbish: 0, failed_by_class: {} },
      timings: { queued_at: now },
      error: undefined,
      created_at: new Date(),
      updated_at: new Date(),
    };

    this.logger.info("job_created", { job_id: jobId, queue_url: settings.INGEST_QUEUE_URL });
    await this.dbManager.repositories.jobs.create(row);

    const messageId: string = await this.queueService.sendRaw(settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      source_type,
      source_ref: source_ref || s3Url,
      field_spec: fieldNames,
      column_map: columnMap,
      batch_id: batchId,
    });

    this.logger.info("job_queued", { job_id: jobId, message_id: messageId });

    return {
      job_id: jobId,
      status: JobStatus.CREATED,
      presigned_put_url: putUrl,
      presigned_put_content_type: putUrl ? uploadContentType : undefined,
      message_id: messageId,
    };
  }

  /**
   * Creates an upload job from a supplied file buffer in a single call.
   * Uploads the buffer to GCS and immediately queues it for ingestion.
   *
   * @param request - The upload-and-create request.
   * @returns The created job details.
   * @throws ValidationError if the request contains invalid input.
   * @throws ServerError if the upload to GCS fails.
   */

  public async uploadAndCreateJob(request: IUploadAndCreateJobRequest): Promise<ICreateJobResponse>
  {
    const { source_buffer, mimetype, filename, field_spec, column_map, batch_id } = request;

    let columnMap: Record<string, number | number[]> | undefined;
    if (column_map)
    {
      const raw = typeof column_map === "string" ? (() => { try { return JSON.parse(column_map); } catch { return undefined; } })() : column_map;

      if (raw && typeof raw === "object" && !Array.isArray(raw))
      {
        const cleaned: Record<string, number | number[]> = {};

        for (const [k, v] of Object.entries(raw))
        {
          if (typeof v === "number" && Number.isInteger(v) && v >= 0)
          {
            cleaned[k] = v;
          }

          else if (Array.isArray(v))
          {
            const idxs = v.filter((n: unknown) => typeof n === "number" && Number.isInteger(n) && n >= 0);

            if (idxs.length)
            {
              cleaned[k] = idxs;
            }
          }
        }
        if (Object.keys(cleaned).length) columnMap = cleaned;
      }
    }

    const namesFromArray = (arr: unknown[]): string[] =>
      arr.map((f) => (typeof f === "string" ? f : (f as { name?: string } | undefined | null)?.name)).filter((x): x is string => typeof x === "string");

    let fieldNames: string[] = [];

    if (field_spec)
    {
      if (Array.isArray(field_spec))
      {
        fieldNames = namesFromArray(field_spec);
      }
      else
      {
        throw new ValidationError(ValidationError.INPUT, "field_spec must be an array of field names, not a string");
      }
    }


    const jobId = randomUUID();
    const batchId: string = batch_id || randomUUID();
    const now: string = new Date().toISOString();

    const rawName: string = (filename || "source").trim();
    const baseName: string = (rawName.split(/[\\/]/).pop() || "source").replace(/[#\s]+/g, "_") || "source";
    const uploadKey = `uploads/${jobId}/${baseName}`;
    const s3Url = `gs://${settings.DATA_BUCKET}/${uploadKey}`;

    try
    {
      await this.gcsUtils.putObject(settings.DATA_BUCKET, uploadKey, source_buffer, mimetype || "application/octet-stream");
    }
    catch (err)
    {
      throw new ServerError(ServerError.INTERNAL, `Failed to upload file to GCS: ${String(err)}`);
    }

    const row: ParseJobRow = {
      job_id: jobId,
      batch_id: batchId,
      source_type: SourceType.UPLOAD,
      source_ref: s3Url,
      s3_url: s3Url,
      field_spec: fieldNames,
      exec_path: "stream",
      status: JobStatus.CREATED,
      output_paths: [],
      counts: { parsed: 0, dropped_rubbish: 0, failed_by_class: {} },
      timings: { queued_at: now },
      error: undefined,
      created_at: new Date(),
      updated_at: new Date(),
    };

    this.logger.info("upload_job_created", { job_id: jobId, queue_url: settings.INGEST_QUEUE_URL, bytes: source_buffer.length });
    await this.dbManager.repositories.jobs.create(row);

    const messageId: string = await this.queueService.sendRaw(settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      source_type: SourceType.UPLOAD,
      source_ref: s3Url,
      field_spec: fieldNames,
      column_map: columnMap,
      batch_id: batchId,
    });

    this.logger.info("upload_job_queued", { job_id: jobId, message_id: messageId });

    return { job_id: jobId, status: JobStatus.CREATED, message_id: messageId };
  }

  /**
   * Retrieves jobs that have been stuck longer than the specified threshold.
   *
   * @param thresholdMinutes - The minimum number of minutes a job must be inactive to be considered stuck.
   * @returns The list of stuck jobs and summary information.
   */

  public async findStuckJobs(thresholdMinutes: number): Promise<IStuckJobsResponse>
  {
    const rows: ParseJobAttributes[] = await this.dbManager.repositories.jobs.findStuckJobs(thresholdMinutes);
    return { stuck_jobs: rows, count: rows.length, threshold_minutes: thresholdMinutes };
  }

  /**
   * Retrieves a job by its identifier.
   *
   * @param jobId - The unique identifier of the job.
   * @returns The job details, or null if the job does not exist.
   */

  public async getJobHeaders(jobId: string): Promise<IJobHeadersResponse | null>
  {
    const row: IParseJob = await this.dbManager.repositories.jobs.findById(jobId);

    if (!row)
    {
      return null;
    }

    return {
      job_id: row.job_id,
      headers: (row.headers as string[]) ?? [],
      field_spec: (row.field_spec as string[]) ?? [],
      column_map: row.column_map as ColumnMap | null | undefined,
    };
  }

  public async getJob(jobId: string): Promise<IJobResponse | null>
  {
    const row: IParseJob = await this.dbManager.repositories.jobs.findById(jobId);

    if (!row)
    {
      return null;
    }

    return await this.buildJobResponse(row as ParseJobAttributes);
  }

  private readonly DEFAULT_JOB_LIST_LIMIT = 100;
  private readonly MAX_JOB_LIST_LIMIT = 500;

  /**
   * Slice a result set down to a bounded page before any per-row enrichment
   * (e.g. presigned URL generation) runs on it. Caps even the unspecified
   * case, since callers that fetch "all jobs" with no limit are exactly the
   * ones that would otherwise trigger N concurrent GCS signing calls.
   *
   * @param rows - Full result set from the repository query
   * @param limit - Requested page size, clamped to MAX_JOB_LIST_LIMIT
   * @param offset - Requested page offset
   * @private
   */
  private paginateRows<T>(rows: T[], limit?: number, offset?: number): T[]
  {
    const effectiveLimit: number = Math.min(limit ?? this.DEFAULT_JOB_LIST_LIMIT, this.MAX_JOB_LIST_LIMIT);
    const effectiveOffset: number = Math.max(offset ?? 0, 0);

    return rows.slice(effectiveOffset, effectiveOffset + effectiveLimit);
  }

  /**
   * Retrieves all jobs belonging to the specified batch.
   *
   * @param batchId - The batch identifier.
   * @returns A list of jobs associated with the batch.
   */

  public async getBatchJobs(batchId: string, limit?: number, offset?: number): Promise<IJobResponse[]>
  {
    const rows: ParseJobAttributes[] = await this.dbManager.repositories.jobs.findByBatchId(batchId);
    const page: ParseJobAttributes[] = this.paginateRows(rows, limit, offset);

    return await Promise.all(page.map((row: ParseJobAttributes) => this.buildJobResponse(row)));
  }

  /**
   * Retrieves all jobs.
   *
   * @returns A list of all jobs.
   */
  public async getAllJobs(statuses?: string[], limit?: number, offset?: number): Promise<IJobResponse[]>
  {
    const rows: ParseJobAttributes[] = await this.dbManager.repositories.jobs.findAll(statuses);
    const page: ParseJobAttributes[] = this.paginateRows(rows, limit, offset);

    return await Promise.all(page.map((row: ParseJobAttributes) => this.buildJobResponse(row)));
  }

  /**
   * Returns all supported job statuses.
   *
   * @returns The list of status values.
   */
  public async getAllStatuses(): Promise<IStatusesResponse>
  {
    return { statuses: Object.values(JobStatus) };
  }

  /**
   * Builds job timings with a presigned download URL for the parsed CSV.
   */
  private async buildTimingsWithDownload(jobId: string, rawTimings: JobTimings): Promise<JobTimings>
  {
    const timings: JobTimings = { ...rawTimings };
    const csvOutputPath = rawTimings._csv_output_path as string | undefined;
    if (csvOutputPath)
    {
      try
      {
        const [bucket, key] = this.gcsUtils.parseGcsUrl(csvOutputPath);
        const filename = key.split("/").pop() ?? `${jobId}.csv`;
        timings.download_path = await this.gcsUtils.presignedGetUrl(bucket, key, 3600, filename);
      }
      catch (err)
      {
        this.logger.warn("download_url_generation_failed", { job_id: jobId, error: String(err) });
        timings.download_path = null;
      }
    }
    return timings;
  }

  /**
   * Builds a full job response including input, fields, derived timings and counts.
   */
  private async buildJobResponse(row: ParseJobAttributes): Promise<IJobResponse>
  {
    const counts = row.counts as JobCounts;
    const rawTimings = row.timings as JobTimings;
    const timings = await this.buildTimingsWithDownload(row.job_id, rawTimings);

    const startedAt = rawTimings.ingesting_at
        || rawTimings.parsing_at
        || rawTimings.created_at
        || row.created_at?.toISOString();

    const finishedAt = rawTimings.completed_at || undefined;

    let fileName: string | null = null;
    if (row.s3_url)
    {
      try
      {
        const [, key] = this.gcsUtils.parseGcsUrl(row.s3_url);
        fileName = key.split("/").pop() ?? null;
      }
      catch
      {
        fileName = null;
      }
    }

    return {
      job_id: row.job_id,
      batch_id: row.batch_id,
      status: row.status as string,
      counts,
      timings,
      error: row.error,
      input: {
        source_type: row.source_type,
        source_ref: row.source_ref,
        s3_url: row.s3_url,
        size: row.size,
        file_name: fileName,
      },
      fields: row.field_spec as string[],
      headers: row.headers as string[] | null | undefined,
      column_map: row.column_map as ColumnMap | null | undefined,
      started_at: startedAt,
      finished_at: finishedAt,
      parsed: counts.parsed,
      failed: totalFailed(counts),
    };
  }

  /**
   * Provides a password for a password-protected job.
   *
   * @param jobId - The unique identifier of the job.
   * @param request - The request containing the password.
   * @throws HttpError if the job does not exist.
   * @throws ServerError if the job is not awaiting a password.
   */

  public async providePassword(jobId: string, request: IProvidePasswordRequest): Promise<void>
  {
    const row: IParseJob = await this.dbManager.repositories.jobs.findById(jobId);

    if (!row)
    {
      throw new HttpError(HttpError.NOT_FOUND, "Job not found");
    }

    if (row.status !== JobStatus.NEEDS_PASSWORD)
    {
      throw new ServerError(ServerError.CONFLICT, `Job is not awaiting a password (status=${row.status})`);
    }

    await this.queueService.sendRaw(settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      action: "provide_password",
      password: request.password,
    });

    this.logger.info("job_password_provided", { job_id: jobId });
  }

  /**
   * Releases a job from hold and resumes processing.
   *
   * @param jobId - The unique identifier of the job.
   * @throws ServerError if the job state transition fails.
   */

  public async releaseHold(jobId: string): Promise<void>
  {
    try
    {
      await transition(jobId, JobStatus.SAVING_TO_DATABASE);
    }
    catch (err)
    {
      throw new ServerError(ServerError.CONFLICT, err instanceof Error ? err.message : String(err));
    }

    await this.queueService.sendRaw(settings.LOAD_QUEUE_URL, { job_id: jobId, manual_override: true });
  }

  /**
   * Marks a job as failed.
   *
   * @param jobId - The unique identifier of the job.
   * @param request - The request containing the failure reason.
   * @throws HttpError if the job does not exist.
   */

  public async markFailed(jobId: string, request: IMarkFailedRequest): Promise<void>
  {
    const row: IParseJob = await this.dbManager.repositories.jobs.findById(jobId);

    if (!row)
    {
      throw new HttpError(HttpError.NOT_FOUND, "Job not found");
    }

    const failureReason = (request.reason?.trim() || row.error || "manually_failed") as string;
    await this.dbManager.repositories.jobs.markFailed(jobId, failureReason);
    this.logger.info("job_marked_failed", { job_id: jobId, reason: failureReason });
  }

  /**
   * Retries a job from the specified processing stage.
   *
   * @param jobId - The unique identifier of the job.
   * @param request - The retry request containing the target status.
   * @throws ValidationError if the target status is invalid.
   * @throws HttpError if the job does not exist.
   * @throws ServerError if the job state transition fails.
   */

  public async retryJob(jobId: string, request: IRetryJobRequest): Promise<void>
  {
    const row: IParseJob = await this.dbManager.repositories.jobs.findById(jobId);

    if (!row)
    {
      throw new HttpError(HttpError.NOT_FOUND, "Job not found");
    }

    let queueUrl: string;
    let message: Record<string, unknown> = { job_id: jobId, manual_override: true };

    const target_status: JobStatus = request.target_status ?? JobStatus.INGESTING;
    switch (target_status)
    {
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

        let updatedFieldSpec: string[] = Array.isArray(request.field_spec)
          ? request.field_spec
          : (Array.isArray(row.field_spec) ? row.field_spec : []);

        const detectedHeaders: string[] = Array.isArray(row.headers) ? row.headers : [];
        const userSelected: boolean = Array.isArray(request.field_spec);

        let updatedColumnMap: Record<string, number | number[]> | undefined = userSelected
          ? {}
          : (request.column_map ?? row.column_map ?? undefined);

        if (userSelected)
        {
          const selected: Set<string> = new Set(updatedFieldSpec);
          const hasUnselected: boolean = detectedHeaders.length > 0 && detectedHeaders.some((h) => !selected.has(h));

          if (hasUnselected && !selected.has("meta"))
          {
            updatedFieldSpec = [...updatedFieldSpec, "meta"];
          }

          for (const field of updatedFieldSpec)
          {
            if (field === "meta") continue;
            const idx = detectedHeaders.indexOf(field);
            if (idx >= 0) (updatedColumnMap as Record<string, number | number[]>)[field] = idx;
          }

          await this.dbManager.repositories.jobs.updateFields(jobId, {
            field_spec: updatedFieldSpec,
            column_map: updatedColumnMap,
          });
        }

        message = {
          job_id: jobId,
          s3_url: row.s3_url,
          field_spec: updatedFieldSpec,
          column_map: updatedColumnMap,
          headers: detectedHeaders,
          manual_override: true,
        };
        break;
      case JobStatus.SAVING_TO_DATABASE:
        queueUrl = settings.LOAD_QUEUE_URL;
        break;
      case JobStatus.REPORTING:
        queueUrl = settings.REPORT_QUEUE_URL;
        break;
      default:
        throw new ValidationError(ValidationError.INPUT,`Invalid target_status: ${target_status}`);
    }

    try
    {
      await transition(jobId, target_status);
    }
    catch (err)
    {
      throw new ServerError(ServerError.CONFLICT, err instanceof Error ? err.message : String(err));
    }
    await this.queueService.sendRaw(queueUrl, message);
  }

  /**
   * Retrieves the audit-trail log entries for a job: crashes (with the stage
   * they occurred in), which templates were used, and drop/DLQ counts.
   *
   * @param jobId - The unique identifier of the job.
   * @returns The list of log entries for the job, oldest first.
   * @throws HttpError if the job does not exist.
   */

  public async getJobLogs(jobId: string): Promise<IJobLogEntry[]>
  {
    const row: IParseJob = await this.dbManager.repositories.jobs.findById(jobId);

    if (!row)
    {
      throw new HttpError(HttpError.NOT_FOUND, "Job not found");
    }

    const rows = await this.dbManager.repositories.jobLogs.findByJob(jobId);

    return rows.map((log) => ({
      event_type: log.event_type,
      stage: log.stage,
      template_id: log.template_id,
      message: log.message,
      metadata: log.metadata,
      created_at: log.created_at,
    }));
  }

  /**
   * Uploads the parsed CSV output for a completed job to a user-supplied
   * gs:// or s3:// destination URL.
   *
   * @param jobId - The unique identifier of the job.
   * @param request - The upload request containing the destination URL.
   * @returns The source CSV path, destination URL, and number of bytes written.
   * @throws HttpError if the job does not exist.
   * @throws ServerError if the parsed CSV is not yet available.
   * @throws ValidationError if the destination URL is invalid.
   */

  public async uploadCsv(jobId: string, request: IUploadCsvRequest): Promise<IUploadCsvResponse>
  {
    const row: IParseJob = await this.dbManager.repositories.jobs.findById(jobId);

    if (!row)
    {
      throw new HttpError(HttpError.NOT_FOUND, "Job not found");
    }

    const csvOutputPath = (row.timings as JobTimings)?._csv_output_path as string | undefined;
    if (!csvOutputPath)
    {
      throw new ServerError(ServerError.CONFLICT, "Parsed CSV output is not yet available");
    }

    if (!/^gs:\/\/|^s3:\/\//i.test(request.destination_url))
    {
      throw new ValidationError(ValidationError.INPUT, "destination_url must be a gs:// or s3:// URL");
    }

    let srcBucket: string;
    let srcKey: string;
    let dstBucket: string;
    let dstKey: string;
    try
    {
      [srcBucket, srcKey] = this.gcsUtils.parseGcsUrl(csvOutputPath);
      [dstBucket, dstKey] = this.gcsUtils.parseGcsUrl(request.destination_url);
    }
    catch
    {
      throw new ValidationError(ValidationError.INPUT, "Invalid source or destination URL");
    }

    const body: Buffer = await this.gcsUtils.readFull(srcBucket, srcKey);
    await this.gcsUtils.putObject(dstBucket, dstKey, body, "text/csv");

    this.logger.info("csv_uploaded", { job_id: jobId, from: csvOutputPath, to: request.destination_url, bytes: body.length });

    return {
      csv_output_path: csvOutputPath,
      destination_url: request.destination_url,
      bytes: body.length,
    };
  }

  /**
   * Returns a presigned GCS URL to download the parsed CSV output for a completed job.
   *
   * @param jobId - The unique identifier of the job.
   * @returns The GCS path, filename, and presigned download URL.
   * @throws HttpError if the job does not exist.
   * @throws ServerError if the parsed CSV is not yet available.
   */

  public async downloadCsv(jobId: string): Promise<IDownloadCsvResponse>
  {
    const row: IParseJob = await this.dbManager.repositories.jobs.findById(jobId);

    if (!row)
    {
      throw new HttpError(HttpError.NOT_FOUND, "Job not found");
    }

    const csvOutputPath = (row.timings as JobTimings)?._csv_output_path as string | undefined;

    if (!csvOutputPath)
    {
      throw new ServerError(ServerError.CONFLICT, "Parsed CSV output is not yet available");
    }

    let bucket: string;
    let key: string;

    try
    {
      [bucket, key] = this.gcsUtils.parseGcsUrl(csvOutputPath);
    }
    catch
    {
      throw new ServerError(ServerError.INTERNAL, "Invalid CSV output path");
    }

    const filename: string = key.split("/").pop() ?? `${jobId}.csv`;
    const downloadUrl: string = await this.gcsUtils.presignedGetUrl(bucket, key, 3600, filename);

    this.logger.info("csv_download_url_generated", { job_id: jobId, path: csvOutputPath, filename });

    return {
      csv_output_path: csvOutputPath,
      filename,
      download_url: downloadUrl,
    };
  }
}

function Enforce(): void {}

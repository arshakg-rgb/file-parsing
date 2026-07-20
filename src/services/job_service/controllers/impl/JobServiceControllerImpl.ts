import { Request, Response, NextFunction } from "express";
import { randomUUID } from "crypto";
import { InstantiationError } from "@errors/InstantiationError.js";
import { settings } from "@shared/Settings.js";
import { repositories, ParseJobRow } from "@shared/DatabaseManager.js";
import { SourceType, JobStatus, JobTimings } from "@shared/models/job.js";
import { sendRaw } from "@shared/QueueService.js";
import { presignedPutUrl } from "@shared/GcsUtils.js";
import { transition } from "@service/job_service/stateMachine.js";
import type { JobServiceController } from "@service/job_service/controllers/JobServiceController.js";

/**
 * Singleton implementation of the Job Service HTTP controller.
 *
 * No permission checks are performed here; the service endpoints are
 * intended to be internal or protected by upstream network controls.
 */
export class JobServiceControllerImpl implements JobServiceController {
  private static instance: JobServiceControllerImpl;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate JobServiceControllerImpl directly. Use getInstance()");
    }
  }

  public static getInstance(): JobServiceControllerImpl {
    if (!JobServiceControllerImpl.instance) {
      JobServiceControllerImpl.instance = new JobServiceControllerImpl(Enforce);
    }
    return JobServiceControllerImpl.instance;
  }

  public async createJob(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const { source_type, source_ref, field_spec, batch_id, column_map } = req.body;

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
        res.status(400).json({ detail: "source_ref is required for s3 and url sources" });
        return;
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
          fieldNames = namesFromArray(field_spec.fields);
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

      res.status(202).json({ job_id: jobId, status: JobStatus.QUEUED, presigned_put_url: putUrl, message_id: messageId });
    } catch (err) {
      next(err);
    }
  }

  public async findStuckJobs(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const thresholdMinutes = parseInt(req.query.minutes as string) || 15;
      const rows = await repositories.jobs.findStuckJobs(thresholdMinutes);
      res.json({ stuck_jobs: rows, count: rows.length, threshold_minutes: thresholdMinutes });
    } catch (err) {
      next(err);
    }
  }

  public async getJob(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const row = await repositories.jobs.findById(String(req.params.job_id));
      if (!row) {
        res.status(404).json({ detail: "Job not found" });
        return;
      }
      res.json({
        job_id: row.job_id,
        batch_id: row.batch_id,
        status: row.status,
        counts: row.counts,
        timings: row.timings,
        output_paths: row.output_paths,
        csv_output_path: (row.timings as JobTimings)?._csv_output_path ?? null,
        error: row.error,
      });
    } catch (err) {
      next(err);
    }
  }

  public async getBatchJobs(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const rows = await repositories.jobs.findByBatchId(String(req.params.batch_id));
      res.json(
        rows.map((row: ParseJobRow) => ({
          job_id: row.job_id,
          batch_id: row.batch_id,
          status: row.status,
          counts: row.counts,
          timings: row.timings,
          output_paths: row.output_paths,
          error: row.error,
        }))
      );
    } catch (err) {
      next(err);
    }
  }

  public async providePassword(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const row = await repositories.jobs.findById(String(req.params.job_id));
      if (!row) {
        res.status(404).json({ detail: "Job not found" });
        return;
      }
      if (row.status !== JobStatus.AWAITING_PASSWORD) {
        res.status(409).json({ detail: `Job is not awaiting a password (status=${row.status})` });
        return;
      }
      await sendRaw(settings.INGEST_QUEUE_URL, {
        job_id: req.params.job_id,
        action: "provide_password",
        password: req.body.password,
      });
      res.status(204).send();
    } catch (err) {
      next(err);
    }
  }

  public async releaseHold(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      await transition(req.params.job_id as string, JobStatus.LOADING);
      await sendRaw(settings.LOAD_QUEUE_URL, { job_id: req.params.job_id, manual_override: true });
      res.status(204).send();
    } catch (err) {
      res.status(409).json({ detail: String(err) });
    }
  }

  public async markFailed(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const { reason } = req.body;
      const row = await repositories.jobs.findById(String(req.params.job_id));
      if (!row) {
        res.status(404).json({ detail: "Job not found" });
        return;
      }
      await repositories.jobs.markFailed(String(req.params.job_id), reason || "manually_failed");
      res.status(204).send();
    } catch (err) {
      next(err);
    }
  }

  public async retryJob(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const { target_status } = req.body;
      const row = await repositories.jobs.findById(String(req.params.job_id));
      if (!row) {
        res.status(404).json({ detail: "Job not found" });
        return;
      }

      let queueUrl: string;
      let message: Record<string, unknown> = { job_id: req.params.job_id, manual_override: true };

      switch (target_status) {
        case JobStatus.INGESTING:
          queueUrl = settings.INGEST_QUEUE_URL;
          message = {
            job_id: req.params.job_id,
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
            job_id: req.params.job_id,
            s3_url: row.s3_url,
            size: row.size,
            field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
            manual_override: true,
          };
          break;
        case JobStatus.PARSING:
          queueUrl = settings.PARSE_QUEUE_URL;
          message = {
            job_id: req.params.job_id,
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
          res.status(400).json({ detail: `Invalid target_status: ${target_status}` });
          return;
      }

      await transition(req.params.job_id as string, target_status as JobStatus);
      await sendRaw(queueUrl, message);
      res.status(204).send();
    } catch (err) {
      res.status(409).json({ detail: String(err) });
    }
  }
}

function Enforce(): void {}

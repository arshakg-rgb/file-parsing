import { Router, Request, Response, NextFunction } from "express";
import { randomUUID } from "crypto";
import { settings } from "../../shared/config.js";
import { pool, ParseJobRow } from "../../shared/db.js";
import { SourceType, JobStatus, ParseJob } from "../../shared/models/job.js";
import { sendRaw } from "../../shared/queueUtils.js";
import { presignedPutUrl } from "../../shared/gcsUtils.js";
import { transition } from "./stateMachine.js";

export const router = Router({ mergeParams: true });

router.post("/jobs", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { source_type, source_ref, field_spec, batch_id } = req.body;
    if ([SourceType.S3, SourceType.URL].includes(source_type) && !source_ref) {
      res.status(400).json({ detail: "source_ref is required for s3 and url sources" });
      return;
    }

    // Extract field names from field_spec if it's in the new format
    let fieldNames: string[] = [];
    if (field_spec) {
      if (Array.isArray(field_spec)) {
        fieldNames = field_spec;
      } else if (field_spec.fields && Array.isArray(field_spec.fields)) {
        fieldNames = field_spec.fields.map((f: any) => f.name);
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
      field_spec: fieldNames, // Store as simple array of field names
      exec_path: "stream",
      status: JobStatus.QUEUED,
      output_paths: [],
      counts: { parsed: 0, dropped_rubbish: 0, failed_by_class: {} },
      timings: { queued_at: now },
      error: undefined,
      created_at: new Date(),
      updated_at: new Date(),
    };

    await pool.query(
      `INSERT INTO parse_jobs
        (job_id, batch_id, parent_job_id, source_type, source_ref, s3_url, size, field_spec, exec_path, status, output_paths, counts, timings, error, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8, '{}'::text[]), $9, $10, $11, $12, $13, $14, NOW(), NOW())`,
      [row.job_id, row.batch_id, row.parent_job_id, row.source_type, row.source_ref, row.s3_url, row.size, row.field_spec, row.exec_path, row.status, JSON.stringify(row.output_paths), JSON.stringify(row.counts), JSON.stringify(row.timings), row.error]
    );

    console.log("job_created_sending_queue", { job_id: jobId, queue_url: settings.INGEST_QUEUE_URL, queue_backend: settings.QUEUE_BACKEND });
    const messageId = await sendRaw(settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      source_type,
      source_ref: source_ref || s3Url,
      field_spec,
      batch_id: batchId,
    });
    console.log("job_queue_message_sent", { job_id: jobId, message_id: messageId });

    res.status(202).json({ job_id: jobId, status: JobStatus.QUEUED, presigned_put_url: putUrl });
  } catch (err) {
    next(err);
  }
});

router.get("/jobs/stuck", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const thresholdMinutes = parseInt(req.query.minutes as string) || 15;
    const result = await pool.query<ParseJobRow>(
      `SELECT job_id, status, timings, error, created_at FROM parse_jobs
       WHERE status NOT IN ('done', 'failed', 'partial', 'held')
       AND updated_at < NOW() - INTERVAL '${thresholdMinutes} minutes'
       ORDER BY updated_at ASC`
    );
    res.json({ stuck_jobs: result.rows, count: result.rows.length, threshold_minutes: thresholdMinutes });
  } catch (err) {
    next(err);
  }
});

router.get("/jobs/:job_id", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const result = await pool.query<ParseJobRow>("SELECT * FROM parse_jobs WHERE job_id = $1", [req.params.job_id]);
    const row = result.rows[0];
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
      error: row.error,
    });
  } catch (err) {
    next(err);
  }
});

router.get("/batches/:batch_id/jobs", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const result = await pool.query<ParseJobRow>("SELECT * FROM parse_jobs WHERE batch_id = $1", [req.params.batch_id]);
    res.json(
      result.rows.map((row: ParseJobRow) => ({
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
});

router.post("/jobs/:job_id/password", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const result = await pool.query<ParseJobRow>("SELECT * FROM parse_jobs WHERE job_id = $1", [req.params.job_id]);
    const row = result.rows[0];
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
});

router.post("/jobs/:job_id/release-hold", async (req: Request, res: Response, next: NextFunction) => {
  try {
    await transition(req.params.job_id as string, JobStatus.LOADING);
    await sendRaw(settings.LOAD_QUEUE_URL, { job_id: req.params.job_id, manual_override: true });
    res.status(204).send();
  } catch (err) {
    res.status(409).json({ detail: String(err) });
  }
});

router.post("/jobs/:job_id/fail", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { reason } = req.body;
    const result = await pool.query<ParseJobRow>("SELECT * FROM parse_jobs WHERE job_id = $1", [req.params.job_id]);
    const row = result.rows[0];
    if (!row) {
      res.status(404).json({ detail: "Job not found" });
      return;
    }
    await pool.query(
      `UPDATE parse_jobs SET status = 'failed', error = $1, updated_at = NOW(),
       timings = timings || jsonb_build_object('failed_at', to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
       WHERE job_id = $2`,
      [reason || "manually_failed", req.params.job_id]
    );
    res.status(204).send();
  } catch (err) {
    next(err);
  }
});

router.post("/jobs/:job_id/retry", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { target_status } = req.body;
    const result = await pool.query<ParseJobRow>("SELECT * FROM parse_jobs WHERE job_id = $1", [req.params.job_id]);
    const row = result.rows[0];
    if (!row) {
      res.status(404).json({ detail: "Job not found" });
      return;
    }

    // Determine appropriate queue based on target status
    let queueUrl: string;
    let message: any = { job_id: req.params.job_id, manual_override: true };

    switch (target_status) {
      case JobStatus.INGESTING:
        queueUrl = settings.INGEST_QUEUE_URL;
        message = {
          job_id: req.params.job_id,
          source_type: row.source_type,
          source_ref: row.source_ref,
          field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
          batch_id: row.batch_id,
          manual_override: true
        };
        break;
      case JobStatus.DETECTING:
        queueUrl = settings.CLASSIFY_QUEUE_URL;
        message = {
          job_id: req.params.job_id,
          s3_url: row.s3_url,
          size: row.size,
          field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
          manual_override: true
        };
        break;
      case JobStatus.PARSING:
        queueUrl = settings.PARSE_QUEUE_URL;
        message = {
          job_id: req.params.job_id,
          s3_url: row.s3_url,
          field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
          manual_override: true
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
});

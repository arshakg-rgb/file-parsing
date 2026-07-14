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
      field_spec,
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
        (job_id, batch_id, source_type, source_ref, s3_url, field_spec, exec_path, status, counts, timings, output_paths, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW())`,
      [row.job_id, row.batch_id, row.source_type, row.source_ref, row.s3_url, JSON.stringify(row.field_spec), row.exec_path, row.status, JSON.stringify(row.counts), JSON.stringify(row.timings), JSON.stringify(row.output_paths)]
    );

    await sendRaw(settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      source_type,
      source_ref: source_ref || s3Url,
      field_spec,
      batch_id: batchId,
    });

    res.status(202).json({ job_id: jobId, status: JobStatus.QUEUED, presigned_put_url: putUrl });
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

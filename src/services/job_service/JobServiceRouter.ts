import { Router, Request, Response, NextFunction } from "express";
import { randomUUID } from "crypto";
import { settings } from "../../shared/config.js";
import { repositories, ParseJobRow } from "../../shared/db.js";
import { SourceType, JobStatus, ParseJob, JobTimings } from "../../shared/models/job.js";
import { sendRaw } from "../../shared/queueUtils.js";
import { presignedPutUrl } from "../../shared/gcsUtils.js";
import { transition } from "./stateMachine.js";

export const router = Router({ mergeParams: true });

router.post("/jobs", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { source_type, source_ref, field_spec, batch_id, column_map } = req.body;

    // Optional explicit column map for headerless fixed-column files: { field: index | [indices] }.
    // Accept a JSON string or object; keep only numeric indices so a bad payload can't inject junk.
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

    // Normalize field_spec into a plain array of field names. Accept: an array; a JSON-array
    // string ('["email","name"]'); a JSON-object string ('{"fields":[{"name":"email"}]}'); a
    // { fields: [{name}] } object; or a plain comma-separated string ('email,name').
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
        else if (s) fieldNames = s.split(",").map((x) => x.trim()).filter(Boolean); // plain CSV string
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

    await repositories.jobs.create(row);

    console.log("job_created_sending_queue", { job_id: jobId, queue_url: settings.INGEST_QUEUE_URL, queue_backend: settings.QUEUE_BACKEND });
    const messageId = await sendRaw(settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      source_type,
      source_ref: source_ref || s3Url,
      field_spec: fieldNames, // normalized array, consistent with the stored spec
      column_map: columnMap,
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
    const rows = await repositories.jobs.findStuckJobs(thresholdMinutes);
    res.json({ stuck_jobs: rows, count: rows.length, threshold_minutes: thresholdMinutes });
  } catch (err) {
    next(err);
  }
});

router.get("/jobs/:job_id", async (req: Request, res: Response, next: NextFunction) => {
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
});

router.get("/batches/:batch_id/jobs", async (req: Request, res: Response, next: NextFunction) => {
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
});

router.post("/jobs/:job_id/password", async (req: Request, res: Response, next: NextFunction) => {
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
});

router.post("/jobs/:job_id/retry", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { target_status } = req.body;
    const row = await repositories.jobs.findById(String(req.params.job_id));
    if (!row) {
      res.status(404).json({ detail: "Job not found" });
      return;
    }

    // Determine appropriate queue based on target status
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

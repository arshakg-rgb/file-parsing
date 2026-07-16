import { settings } from "../../shared/config.js";
import { EventType, JobEvent, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, ReportMessage, JobCounts } from "../../shared/models/job.js";
import { pool, ParseJobRow, OutputPartRow, waitForDb } from "../../shared/db.js";
import { receiveMessages, deleteMessage, publishEvent } from "../../shared/queueUtils.js";
import { putJson } from "../../shared/gcsUtils.js";
import { QualityGate } from "../../shared/qualityGate.js";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { startHealthCheckServer } from "../../shared/health.js";

const logger = createLogger("report");

if (process.env.HEALTH_CHECK_PORT) {
  startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
}

function emit(jobId: string, eventType: EventType, data: Record<string, any>) {
  publishEvent(makeJobEvent(eventType, jobId, "report", data));
}

function totalFailed(counts: JobCounts): number {
  return Object.values(counts.failed_by_class || {}).reduce((a, b) => a + b, 0);
}

export async function generateReport(msg: ReportMessage): Promise<void> {
  const jobId = msg.job_id;
  logger.info("report_start", { job_id: jobId, status: msg.status });
  metrics.increment("report.start", 1, { status: msg.status });

  const jobRow = await getJob(jobId);
  if (!jobRow) {
    throw new Error(`Job ${jobId} not found`);
  }
  const parts = await getParts(jobId);
  const batchSiblings = jobRow.batch_id ? await getBatchJobs(jobRow.batch_id) : [];

  // Get quality metrics
  const qualityGate = new QualityGate();
  const qualityMetrics = await qualityGate.calculateMetrics(jobId);
  const qualityCheck = await qualityGate.passesQualityGate(jobId);

  const report = {
    job_id: jobId,
    batch_id: jobRow.batch_id,
    generated_at: new Date().toISOString(),
    status: msg.status,
    source: {
      type: jobRow.source_type,
      ref: jobRow.source_ref,
      s3_url: jobRow.s3_url,
      size_bytes: jobRow.size,
    },
    field_spec: jobRow.field_spec,
    counts: {
      parsed: msg.counts.parsed,
      dropped_rubbish: msg.counts.dropped_rubbish,
      failed_total: totalFailed(msg.counts),
      failed_by_class: msg.counts.failed_by_class,
    },
    quality: {
      total_lines: qualityMetrics.totalLines,
      parsed_lines: qualityMetrics.parsedLines,
      dropped_rubbish_lines: qualityMetrics.droppedRubbishLines,
      failed_lines: qualityMetrics.failedLines,
      failed_line_ratio: qualityMetrics.failedLineRatio,
      passed_quality_gate: qualityCheck.passes,
      quality_gate_reason: qualityCheck.reason,
    },
    output_parts: parts.map((p) => ({ s3_path: p.s3_path, rows: p.row_count, bytes: p.byte_size })),
    output_paths: msg.output_paths,
    rubbish_log_path: msg.rubbish_log_path,
    dlq_count: msg.dlq_count,
    timings: jobRow.timings,
  };

  const reportKey = `reports/${jobId}/report.json`;
  await putJson(settings.DATA_BUCKET, reportKey, report);
  logger.info("report_written", { job_id: jobId, s3_key: reportKey, quality_passed: qualityCheck.passes });
  metrics.increment("report.generated", 1);

  if (batchSiblings.length && jobRow.batch_id) {
    const allTerminal = batchSiblings.every((j) =>
      [JobStatus.DONE, JobStatus.PARTIAL, JobStatus.HELD, JobStatus.FAILED].includes(j.status as JobStatus)
    );
    if (allTerminal) {
      await writeBatchRollup(jobRow.batch_id, batchSiblings);
    }
  }

  // Emit REPORTING_COMPLETED to let job service handle DONE transition with counts preservation
  emit(jobId, EventType.REPORTING_COMPLETED, {});
}

async function getJob(jobId: string): Promise<ParseJobRow | undefined> {
  const result = await pool.query<ParseJobRow>("SELECT * FROM parse_jobs WHERE job_id = $1", [jobId]);
  return result.rows[0];
}

async function getParts(jobId: string): Promise<OutputPartRow[]> {
  const result = await pool.query<OutputPartRow>("SELECT * FROM output_parts WHERE job_id = $1", [jobId]);
  return result.rows;
}

async function getBatchJobs(batchId: string): Promise<ParseJobRow[]> {
  const result = await pool.query<ParseJobRow>("SELECT * FROM parse_jobs WHERE batch_id = $1", [batchId]);
  return result.rows;
}

async function writeBatchRollup(batchId: string, jobs: ParseJobRow[]): Promise<void> {
  const rollup = {
    batch_id: batchId,
    generated_at: new Date().toISOString(),
    total_jobs: jobs.length,
    done: jobs.filter((j) => j.status === JobStatus.DONE).length,
    partial: jobs.filter((j) => j.status === JobStatus.PARTIAL).length,
    held: jobs.filter((j) => j.status === JobStatus.HELD).length,
    failed: jobs.filter((j) => j.status === JobStatus.FAILED).length,
    total_parsed: jobs.reduce((a, j) => a + ((j.counts as any)?.parsed || 0), 0),
    total_dropped: jobs.reduce((a, j) => a + ((j.counts as any)?.dropped_rubbish || 0), 0),
    jobs: jobs.map((j) => ({ job_id: j.job_id, status: j.status, source: j.source_ref })),
  };
  await putJson(settings.DATA_BUCKET, `reports/batches/${batchId}/rollup.json`, rollup);
  logger.info("batch_rollup_written", { batch_id: batchId, total: jobs.length });
  metrics.increment("report.batch_rollup", 1);
}

export async function consumerLoop(): Promise<void> {
  await waitForDb();
  logger.info("report_consumer_started");
  while (true) {
    const messages = await receiveMessages<ReportMessage>(
      settings.REPORT_QUEUE_URL,
      (body) => JSON.parse(body) as ReportMessage,
      5
    );
    for (const { payload, receiptHandle } of messages) {
      try {
        await generateReport(payload);
        await deleteMessage(settings.REPORT_QUEUE_URL, receiptHandle);
      } catch (exc) {
        const errorStr = String(exc);
        // Ack bad messages to prevent infinite retry loop
        if (errorStr.includes("Job") && (errorStr.includes("not found") || errorStr.includes("cannot transition"))) {
          logger.error("report_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
          metrics.increment("report.error_ack", 1);
          await deleteMessage(settings.REPORT_QUEUE_URL, receiptHandle);
        } else {
          logger.error("report_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
          metrics.increment("report.error", 1);
        }
      }
    }
  }
}

consumerLoop();

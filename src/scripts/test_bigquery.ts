import { randomUUID } from "crypto";
import { BigQueryManager } from "@config/db/BigQueryManager.js";
import { DatabaseManager } from "@shared/DatabaseManager.js";
import { JobStatus } from "@shared/models/job.js";
import type { ParseJobCreationAttributes } from "@config/db/models/ParseJob.js";

/**
 * Standalone smoke test for the BigQuery-backed repositories.
 * Run with: npx tsx -r tsconfig-paths/register src/scripts/test_bigquery.ts
 * or via the "test:bigquery" npm script.
 */
async function main(): Promise<void> {
  console.log("1) Verifying BigQuery dataset connectivity...");
  await BigQueryManager.getInstance().initialize();
  console.log("   OK: dataset reachable.\n");

  const db = DatabaseManager.getInstance();
  const jobId = randomUUID();
  const batchId = randomUUID();

  console.log(`2) Creating test job ${jobId}...`);
  const jobData: ParseJobCreationAttributes = {
    job_id: jobId,
    batch_id: batchId,
    source_type: "upload",
    source_ref: "gs://test-bucket/smoke-test.csv",
    field_spec: ["name", "email"],
    exec_path: "stream",
    status: JobStatus.CREATED,
    output_paths: [],
    counts: { parsed: 0, dropped_rubbish: 0, failed_by_class: {} },
    timings: { queued_at: new Date().toISOString() },
  };
  await db.repositories.jobs.create(jobData);
  console.log("   OK: insert succeeded.\n");

  console.log("3) Reading it back via findById...");
  const fetched = await db.repositories.jobs.findById(jobId);
  if (!fetched) {
    throw new Error("FAIL: job was not found after insert (findById returned null)");
  }
  console.log(`   OK: found job with status=${fetched.status}\n`);

  console.log("4) Updating status via tryTransitionStatus...");
  const transitioned = await db.repositories.jobs.tryTransitionStatus(
    jobId,
    JobStatus.INGESTING,
    [JobStatus.CREATED]
  );
  if (!transitioned) {
    throw new Error("FAIL: tryTransitionStatus did not affect any rows");
  }
  console.log("   OK: transition succeeded.\n");

  console.log("5) Verifying updated status...");
  const afterUpdate = await db.repositories.jobs.findById(jobId);
  if (afterUpdate?.status !== JobStatus.INGESTING) {
    throw new Error(`FAIL: expected status=${JobStatus.INGESTING}, got ${afterUpdate?.status}`);
  }
  console.log("   OK: status updated correctly.\n");

  console.log("6) Testing findByBatchId...");
  const batchJobs = await db.repositories.jobs.findByBatchId(batchId);
  if (batchJobs.length !== 1) {
    throw new Error(`FAIL: expected 1 job in batch, got ${batchJobs.length}`);
  }
  console.log("   OK: batch lookup returned the correct job.\n");

  console.log("7) Cleaning up test row...");
  await BigQueryManager.getInstance().execute(
    "DELETE FROM `data-etl-499916.file_parsing.parse_jobs` WHERE job_id = @job_id",
    { job_id: jobId }
  );
  console.log("   OK: cleanup complete.\n");

  console.log("8) Testing PendingArchiveEntryRepository (nullable error field)...");
  const entryId = randomUUID();
  await db.repositories.pendingArchiveEntries.create({
    id: entryId,
    job_id: jobId,
    entry_name: "smoke-test.txt",
    entry_size: 123,
    status: "pending",
  });
  await db.repositories.pendingArchiveEntries.markStatus(entryId, "failed", "smoke test error");
  const entry = await db.repositories.pendingArchiveEntries.findById(entryId);
  if (entry?.status !== "failed") {
    throw new Error(`FAIL: expected pending_archive_entries status=failed, got ${entry?.status}`);
  }
  await BigQueryManager.getInstance().execute(
    "DELETE FROM `data-etl-499916.file_parsing.pending_archive_entries` WHERE id = @id",
    { id: entryId }
  );
  console.log("   OK: pending_archive_entries create/markStatus/find/cleanup succeeded.\n");

  console.log("9) Testing JobLogRepository (nullable stage/template_id/message)...");
  await db.repositories.jobLogs.log({
    job_id: jobId,
    event_type: "smoke_test",
    metadata: { note: "smoke test" },
  });
  const logs = await db.repositories.jobLogs.findByJob(jobId);
  if (logs.length < 1) {
    throw new Error("FAIL: expected at least 1 job_log entry");
  }
  await BigQueryManager.getInstance().execute(
    "DELETE FROM `data-etl-499916.file_parsing.job_logs` WHERE job_id = @job_id",
    { job_id: jobId }
  );
  console.log("   OK: job_logs insert with null optional fields succeeded.\n");

  console.log("10) Testing TemplateRepository (nullable field_map/structure/signature/confidence)...");
  const templateFingerprint = `smoke-test-${randomUUID()}`;
  await db.repositories.templates.saveTemplate(
    {
      template_id: randomUUID(),
      fingerprint: templateFingerprint,
      version: 0,
      source: "smoke_test",
      signature: "smoke-signature",
      confidence: 0.5,
    } as any,
    "rubbish"
  );
  const savedTemplate = await db.repositories.templates.findByFingerprint(templateFingerprint);
  if (!savedTemplate) {
    throw new Error("FAIL: template was not saved");
  }
  await BigQueryManager.getInstance().execute(
    "DELETE FROM `data-etl-499916.file_parsing.templates` WHERE fingerprint = @fingerprint",
    { fingerprint: templateFingerprint }
  );
  console.log("   OK: templates saveTemplate/find/cleanup succeeded.\n");

  console.log("11) Testing OutputPartRepository...");
  const partId = randomUUID();
  await db.repositories.outputParts.create({
    part_id: partId,
    job_id: jobId,
    template_id: randomUUID(),
    s3_path: "gs://test-bucket/smoke-test.parquet",
    row_count: 10,
    byte_size: 1024,
  });
  const parts = await db.repositories.outputParts.findByJob(jobId);
  if (parts.length < 1) {
    throw new Error("FAIL: expected at least 1 output_part");
  }
  await BigQueryManager.getInstance().execute(
    "DELETE FROM `data-etl-499916.file_parsing.output_parts` WHERE part_id = @part_id",
    { part_id: partId }
  );
  console.log("   OK: output_parts create/find/cleanup succeeded.\n");

  console.log("12) Testing DeadLetterRepository...");
  const dlqId = randomUUID();
  await db.repositories.deadLetters.create({
    dlq_id: dlqId,
    job_id: jobId,
    byte_offset: 0,
    byte_length: 10,
    line_no: 1,
    raw_bytes: "smoke-test",
    failure_class: "smoke_test",
    error: "smoke test error",
    attempts: 0,
    status: "pending",
  });
  const dlqRows = await db.repositories.deadLetters.findByJob(jobId);
  if (dlqRows.length < 1) {
    throw new Error("FAIL: expected at least 1 dead_letter");
  }
  await BigQueryManager.getInstance().execute(
    "DELETE FROM `data-etl-499916.file_parsing.dead_letters` WHERE dlq_id = @dlq_id",
    { dlq_id: dlqId }
  );
  console.log("   OK: dead_letters create/find/cleanup succeeded.\n");

  console.log("13) Testing RubbishLogRepository...");
  await db.repositories.rubbishLogs.create({
    job_id: jobId,
    byte_offset: 0,
    line_no: 1,
    raw_bytes: "smoke-test-rubbish",
    matched_template_id: randomUUID(),
  });
  const rubbishRows = await db.repositories.rubbishLogs.findByJob(jobId);
  if (rubbishRows.length < 1) {
    throw new Error("FAIL: expected at least 1 rubbish_log row");
  }
  await BigQueryManager.getInstance().execute(
    "DELETE FROM `data-etl-499916.file_parsing.rubbish_log` WHERE job_id = @job_id",
    { job_id: jobId }
  );
  console.log("   OK: rubbish_log create/find/cleanup succeeded.\n");

  console.log("14) Testing ParsedRecordRepository (create + JSON 'fields' column)...");
  await db.repositories.parsedRecords.create({
    _job_id: jobId,
    _byte_offset: 0,
    _byte_length: 10,
    _record_index: 0,
    _line_no: 1,
    _template_id: randomUUID(),
    _template_version: 1,
    _checksum: "smoke-checksum",
    _parsed_at: new Date(),
    _part_id: partId,
    fields: { name: "smoke test", email: "smoke@test.com" },
  });
  const parsedRows = await db.repositories.parsedRecords.findByJob(jobId);
  if (parsedRows.length < 1) {
    throw new Error("FAIL: expected at least 1 parsed_record");
  }
  if (parsedRows[0].fields?.name !== "smoke test") {
    throw new Error(`FAIL: expected fields.name='smoke test', got ${JSON.stringify(parsedRows[0].fields)}`);
  }
  await BigQueryManager.getInstance().execute(
    "DELETE FROM `data-etl-499916.file_parsing.parsed_records` WHERE _job_id = @job_id",
    { job_id: jobId }
  );
  console.log("   OK: parsed_records create/find/JSON-fields/cleanup succeeded.\n");

  console.log("ALL CHECKS PASSED.");
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("SMOKE TEST FAILED:", err);
    process.exit(1);
  });

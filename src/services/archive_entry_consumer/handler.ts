import crypto from "crypto";
import fs from "fs/promises";
import { createWriteStream } from "fs";
import { pipeline } from "stream/promises";
import os from "os";
import path from "path";
import { spawn } from "child_process";
import { settings } from "../../shared/config.js";
import { parseGcsUrl, objectSize, readFull, readRange, putObject, gcsClient } from "../../shared/gcsUtils.js";
import { receiveMessages, deleteMessage, publishEvent, sendRaw } from "../../shared/queueUtils.js";
import { EventType, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus } from "../../shared/models/job.js";
import { markPendingEntryCompleted, markPendingEntryFailed, markPendingEntryProcessing, getPendingEntryCount, getPendingEntryTotalSize, getJob, pool, waitForDb, createPendingArchiveEntry } from "../../shared/db.js";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { detectArchiveType, extractArchiveToS3, BombError } from "../ingest/normalizer.js";
import { startHealthCheckServer } from "../../shared/health.js";

const logger = createLogger("archive-entry-consumer");

if (process.env.HEALTH_CHECK_PORT) {
  startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
}

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5 * 60 * 1000; // 5 minutes between retries
const MAX_TOTAL_UNCOMPRESSED = 10 * 1024 * 1024 * 1024; // 10GB total limit per job
const CONCURRENT_MESSAGES = 3; // Process up to 3 messages concurrently

interface ArchiveEntryMessage {
  job_id: string;
  batch_id: string;
  archive_s3_url: string;
  entry_name: string;
  entry_size: number;
  field_spec: string[];
  password?: string;
  archive_type: string;
  nesting_depth?: number; // Track archive nesting depth
}

async function extractSingleRarEntry(
  jobId: string,
  archiveS3Url: string,
  entryName: string,
  password: string | undefined,
  fieldSpec: string[]
): Promise<{ s3Url: string; size: number }> {
  const [bucket, archiveKey] = parseGcsUrl(archiveS3Url);
  const mountPath = process.env.RAR_TEMP_MOUNT || '/mnt/scratch';
  const tmpPath = path.join(mountPath, `${crypto.randomUUID()}.rar`);
  
  logger.info("archive_entry_download_start", { job_id: jobId, archive_s3_url: archiveS3Url, tmp_path: tmpPath });
  
  // Download archive to local mount
  const fileStream = gcsClient().bucket(bucket).file(archiveKey).createReadStream();
  const writeStream = createWriteStream(tmpPath);
  
  fileStream.on('error', (err) => {
    logger.error("archive_entry_download_stream_error", { job_id: jobId, error: err.message });
  });
  
  writeStream.on('error', (err) => {
    logger.error("archive_entry_download_write_error", { job_id: jobId, error: err.message });
  });
  
  await pipeline(fileStream, writeStream);
  logger.info("archive_entry_download_complete", { job_id: jobId, tmp_path: tmpPath });
  
  try {
    // Extract single entry using unrar
    // Use different path for async extraction to avoid conflicts with synchronous path
    const safeEntryName = entryName.replace(/[#\s]+/g, "_");
    const entryKey = `ingested/${jobId}/entries/${safeEntryName}`;
    const entryFile = gcsClient().bucket(bucket).file(entryKey);
    const writeStream = entryFile.createWriteStream();
    
    const extractArgs = ['p', '-inul', tmpPath, entryName];
    if (password) {
      extractArgs.push('-p' + password);
    }
    
    logger.info("archive_entry_extract_start", { job_id: jobId, entry_name: entryName, extract_args: extractArgs });
    const extractProcess = spawn('unrar', extractArgs);
    
    extractProcess.stdout.pipe(writeStream);
    
    // Capture stderr for debugging
    let stderrOutput = '';
    extractProcess.stderr.on('data', (data) => {
      stderrOutput += data.toString();
      logger.error("archive_entry_extract_stderr", { job_id: jobId, entry_name: entryName, stderr: data.toString() });
    });
    
    await new Promise<void>((resolve, reject) => {
      writeStream.on('finish', resolve);
      writeStream.on('error', (err) => {
        logger.error("archive_entry_extract_write_error", { job_id: jobId, entry_name: entryName, error: err.message });
        reject(err);
      });
      extractProcess.on('error', (err) => {
        logger.error("archive_entry_extract_spawn_error", { job_id: jobId, entry_name: entryName, error: err.message });
        reject(err);
      });
      extractProcess.on('close', (code) => {
        if (code !== 0) {
          logger.error("archive_entry_extract_failed", { job_id: jobId, entry_name: entryName, code, stderr: stderrOutput });
          reject(new Error(`unrar extraction failed with code ${code}: ${stderrOutput}`));
        } else {
          resolve();
        }
      });
    });
    
    const size = await objectSize(bucket, entryKey);
    const s3Url = `gs://${bucket}/${entryKey}`;
    
    logger.info("archive_entry_extract_complete", { job_id: jobId, entry_name: entryName, size, s3_url: s3Url });
    
    return { s3Url, size };
  } finally {
    // Cleanup temp archive file
    await fs.unlink(tmpPath).catch(() => {});
  }
}

async function handleArchiveEntry(msg: ArchiveEntryMessage, attempt: number): Promise<void> {
  const { job_id, batch_id, archive_s3_url, entry_name, entry_size, field_spec, password, archive_type, nesting_depth = 0 } = msg;
  
  // Idempotency check: if entry is already completed, skip processing
  const existingEntry = await pool.query(
    "SELECT status FROM pending_archive_entries WHERE job_id = $1 AND entry_name = $2",
    [job_id, entry_name]
  );
  const existingStatus = existingEntry.rows[0]?.status;
  
  if (existingStatus === "completed") {
    logger.info("archive_entry_already_completed", { job_id, entry_name });
    return;
  }
  
  if (existingStatus === "failed") {
    logger.info("archive_entry_already_failed", { job_id, entry_name });
    return;
  }
  
  logger.info("archive_entry_processing_start", { job_id, entry_name, entry_size, attempt, nesting_depth });
  metrics.increment("archive_entry.processing", 1, { attempt: attempt.toString() });
  
  
  try {
    if (archive_type !== "rar") {
      throw new Error(`Unsupported archive type: ${archive_type}`);
    }
    
    // Check nesting depth to prevent infinite recursion
    if (nesting_depth >= settings.ARCHIVE_MAX_NESTING_DEPTH) {
      throw new Error(`Archive nesting depth ${nesting_depth} exceeds maximum ${settings.ARCHIVE_MAX_NESTING_DEPTH}`);
    }
    
    // Mark as processing BEFORE extraction to close race condition
    await markPendingEntryProcessing(job_id, entry_name);
    logger.info("archive_entry_marked_processing", { job_id, entry_name });
    // Check total uncompressed size limit to prevent archive bomb attacks via queue
    const currentTotalSize = await getPendingEntryTotalSize(job_id);
    if (currentTotalSize + entry_size > MAX_TOTAL_UNCOMPRESSED) {
      throw new Error(`Entry size ${entry_size} would exceed total uncompressed limit ${MAX_TOTAL_UNCOMPRESSED} (current: ${currentTotalSize})`);
    }
    
    const { s3Url, size } = await extractSingleRarEntry(job_id, archive_s3_url, entry_name, password || undefined, field_spec);
    
    // Detect if extracted file is itself an archive (nested archive handling)
    const [bucket, key] = parseGcsUrl(s3Url);
    const header = await readRange(bucket, key, 0, 511);
    const detectedArchiveType = detectArchiveType(header);
    
    if (detectedArchiveType) {
      logger.info("archive_entry_nested_archive_detected", { job_id, entry_name, detected_type: detectedArchiveType, nesting_depth });
      metrics.increment("archive_entry.nested_archive", 1, { type: detectedArchiveType });
      
      // Extract nested archive recursively
      const nestedEntries = await extractArchiveToS3(
        job_id,
        s3Url,
        detectedArchiveType,
        field_spec,
        batch_id,
        password,
        nesting_depth + 1
      );
      
      // Register nested pending entries BEFORE marking parent is completed
      // This prevents race condition where job gets marked DONE before children are registered
      for (const nestedEntry of nestedEntries) {
        if (nestedEntry.pending) {
          await createPendingArchiveEntry(job_id, nestedEntry.entry_name, nestedEntry.entry_size);
          logger.info("archive_entry_nested_pending", { job_id, entry_name, nested_entry_name: nestedEntry.entry_name });
        } else {
          await publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, job_id, "archive-entry-consumer", {
            parent_job_id: job_id,
            batch_id: batch_id,
            entry_s3_url: nestedEntry.entry_s3_url,
            entry_name: nestedEntry.entry_name,
            entry_size: nestedEntry.entry_size,
            field_spec: field_spec || [],
          }));
        }
      }
      
      // Delete the intermediate nested archive file after extraction
      try {
        await gcsClient().bucket(bucket).file(key).delete();
        logger.info("archive_entry_nested_archive_deleted", { job_id, entry_name });
      } catch (deleteError) {
        logger.warn("archive_entry_nested_archive_delete_failed", { job_id, entry_name, error: String(deleteError) });
      }
    } else {
      // Not an archive - publish ENTRY_DISCOVERED for parsing
      await publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, job_id, "archive-entry-consumer", {
        parent_job_id: job_id,
        batch_id: batch_id,
        entry_s3_url: s3Url,
        entry_name: entry_name,
        entry_size: size,
        field_spec: field_spec || [],
      }));
    }
    
    // Mark entry as completed in database (after nested entries are registered)
    await markPendingEntryCompleted(job_id, entry_name);
    logger.info("archive_entry_marked_completed", { job_id, entry_name });
    
    logger.info("archive_entry_success", { job_id, entry_name, s3_url: s3Url });
    metrics.increment("archive_entry.success", 1);
    
    // Check if all pending entries are now complete
    const counts = await getPendingEntryCount(job_id);
    logger.info("archive_entry_completion_check", { job_id, pending: counts.pending, completed: counts.completed, failed: counts.failed });
    
    if (counts.pending === 0) {
      // All entries processed - check job status and transition to DONE
      const job = await getJob(job_id);
      if (job && job.status === JobStatus.INGESTING) {
        logger.info("archive_entry_all_complete_transitioning_to_done", { job_id });
        // Publish event to transition job to DONE
        await publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, job_id, "archive-entry-consumer", {
          new_status: JobStatus.DONE,
        }));
      }
    }
  } catch (error) {
    logger.error("archive_entry_processing_failed", { job_id, entry_name, attempt, error: String(error) }, error instanceof Error ? error : new Error(String(error)));
    metrics.increment("archive_entry.error", 1, { attempt: attempt.toString() });
    
    if (attempt >= MAX_RETRIES) {
      // Max retries exhausted - mark as failed
      await markPendingEntryFailed(job_id, entry_name, String(error));
      logger.error("archive_entry_max_retries_exhausted", { job_id, entry_name });
      metrics.increment("archive_entry.exhausted", 1);
      
      // Check if this was the last pending entry
      const counts = await getPendingEntryCount(job_id);
      if (counts.pending === 0) {
        const job = await getJob(job_id);
        if (job && job.status === JobStatus.INGESTING) {
          logger.info("archive_entry_final_failed_transitioning_to_done", { job_id, failed: counts.failed });
          // Still transition to DONE even with failures - job has partial success
          await publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, job_id, "archive-entry-consumer", {
            new_status: JobStatus.DONE,
          }));
        }
      }
    } else {
      // Retry with delay - let Pub/Sub redeliver instead of blocking
      logger.info("archive_entry_retry_scheduled", { job_id, entry_name, attempt, delay_ms: RETRY_DELAY_MS });
      // Don't retry via recursion - let Pub/Sub's own retry/backoff redeliver the message
      // This avoids blocking the consumer loop and prevents duplicate delivery
      throw error; // This will cause the message to be nacked and redelivered by Pub/Sub
    }
  }
}

export async function consumerLoop(): Promise<void> {
  while (true) {
    try {
      await waitForDb();
      logger.info("archive_entry_consumer_started", { queue_url: settings.ARCHIVE_ENTRY_QUEUE_URL, queue_backend: settings.QUEUE_BACKEND });
      
      while (true) {
        logger.info("archive_entry_consumer_waiting_for_messages");
        const messages = await receiveMessages<ArchiveEntryMessage>(
          settings.ARCHIVE_ENTRY_QUEUE_URL,
          (body) => JSON.parse(body) as ArchiveEntryMessage,
          5
        );
        logger.info("archive_entry_consumer_messages_received", { count: messages.length });
        
        // Group messages by job_id to prevent race condition on total-size cap
        // Entries from same job processed serially, different jobs processed in parallel
        const messagesByJob = new Map<string, Array<{ payload: ArchiveEntryMessage; receiptHandle: string }>>();
        for (const msg of messages) {
          const jobKey = msg.payload.job_id;
          if (!messagesByJob.has(jobKey)) {
            messagesByJob.set(jobKey, []);
          }
          messagesByJob.get(jobKey)!.push(msg);
        }
        
        // Process each job's messages serially, but process different jobs in parallel
        const jobProcessingPromises = Array.from(messagesByJob.entries()).map(async ([jobId, jobMessages]) => {
          // Process messages for this job serially to avoid race on total-size cap
          for (const { payload, receiptHandle } of jobMessages) {
            try {
              await handleArchiveEntry(payload, 1);
              // Only delete message on success to prevent redelivery on ack failure
              await deleteMessage(settings.ARCHIVE_ENTRY_QUEUE_URL, receiptHandle);
            } catch (exc) {
              logger.error("archive_entry_consumer_message_failed", { job_id: payload.job_id, entry_name: payload.entry_name }, exc instanceof Error ? exc : new Error(String(exc)));
              metrics.increment("archive_entry.message_error", 1);
              // Don't delete message on failure - let it retry for transient errors
              // But check if this is a permanent error that should be acked
              const errorStr = String(exc);
              if (errorStr.includes("max retries exhausted") || errorStr.includes("exceeds maximum")) {
                // Permanent error - ack to prevent infinite retry
                await deleteMessage(settings.ARCHIVE_ENTRY_QUEUE_URL, receiptHandle);
              }
            }
          }
        });
        
        // Limit concurrency across jobs
        const jobChunks = [];
        for (let i = 0; i < jobProcessingPromises.length; i += CONCURRENT_MESSAGES) {
          jobChunks.push(jobProcessingPromises.slice(i, i + CONCURRENT_MESSAGES));
        }
        
        for (const chunk of jobChunks) {
          await Promise.allSettled(chunk);
        }
      }
    } catch (dbError) {
      logger.error("archive_entry_consumer_database_connection_lost", { error: String(dbError) }, dbError instanceof Error ? dbError : new Error(String(dbError)));
      metrics.increment("archive_entry.db_connection_lost", 1);
      await waitForDb();
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

consumerLoop();

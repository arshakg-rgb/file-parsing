import { settings } from "../shared/Settings.js";
import { getPendingEntryCount, repositories, waitForDb } from "../shared/DatabaseManager.js";
import { publishEvent } from "../shared/QueueService.js";
import { EventType, makeJobEvent } from "../shared/models/events.js";
import { JobStatus } from "../shared/models/job.js";
import { createLogger } from "../utils/logger/logger.js";

const logger = createLogger("reconciler");

// Threshold: jobs stuck in INGESTING for more than 2 hours are considered stuck
const STUCK_THRESHOLD_MS = 2 * 60 * 60 * 1000;

async function reconcileStuckJobs(): Promise<void> {
  await waitForDb();
  logger.info("reconciler_start");
  
  const rows = await repositories.jobs.findStuckIngesting(2);
  
  logger.info("reconciler_found_stuck_jobs", { count: rows.length });
  
  for (const row of rows) {
    const jobId = row.job_id;
    const stuckDuration = Date.now() - new Date(row.updated_at ?? Date.now()).getTime();
    
    logger.info("reconciler_processing_stuck_job", { job_id: jobId, stuck_duration_ms: stuckDuration });
    
    try {
      // Check if this job has pending entries
      const counts = await getPendingEntryCount(jobId);
      
      logger.info("reconciler_job_pending_counts", { job_id: jobId, pending: counts.pending, completed: counts.completed, failed: counts.failed });
      
      if (counts.pending === 0 && counts.completed === 0 && counts.failed === 0) {
        // No pending entries at all - job is genuinely stuck, mark as FAILED
        logger.warn("reconciler_job_no_pending_entries_marking_failed", { job_id: jobId });
        await publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, jobId, "reconciler", {
          new_status: JobStatus.FAILED,
          error: "Job stuck in INGESTING with no pending entries",
        }));
      } else if (counts.pending === 0) {
        // All pending entries processed (completed or failed) - transition to DONE
        logger.info("reconciler_job_all_pending_processed_transitioning_to_done", { job_id: jobId, completed: counts.completed, failed: counts.failed });
        await publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, jobId, "reconciler", {
          new_status: JobStatus.DONE,
        }));
      } else {
        // Still has pending entries - check if they're stale
        const staleThreshold = Date.now() - (3 * 60 * 60 * 1000); // 3 hours
        
        const staleEntries = await repositories.pendingArchiveEntries.findStaleEntries(jobId, 3, ["pending", "processing"]);
        
        if (staleEntries.length > 0) {
          logger.warn("reconciler_job_has_stale_pending_entries", { job_id: jobId, stale_count: staleEntries.length });
          
          // Mark stale pending entries as failed
          for (const pendingRow of staleEntries) {
            await repositories.pendingArchiveEntries.markStatus(pendingRow.id, "failed", "Stale pending entry - reconciler cleanup");
            logger.info("reconciler_marked_stale_entry_failed", { job_id: jobId, entry_name: pendingRow.entry_name });
          }
          
          // Re-check if this resolves the job
          const newCounts = await getPendingEntryCount(jobId);
          if (newCounts.pending === 0) {
            logger.info("reconciler_job_resolved_after_stale_cleanup_transitioning_to_done", { job_id: jobId });
            await publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, jobId, "reconciler", {
              new_status: JobStatus.DONE,
            }));
          }
        } else {
          logger.info("reconciler_job_has_active_pending_entries", { job_id: jobId, pending: counts.pending });
          // Job has active pending entries - leave in INGESTING, but log for monitoring
        }
      }
    } catch (error) {
      logger.error("reconciler_job_processing_failed", { job_id: jobId, error: String(error) }, error instanceof Error ? error : new Error(String(error)));
    }
  }
  
  logger.info("reconciler_complete");
}

// Run if executed directly
if (require.main === module) {
  reconcileStuckJobs()
    .then(() => {
      console.log("Reconciler completed successfully");
      process.exit(0);
    })
    .catch((error) => {
      console.error("Reconciler failed:", error);
      process.exit(1);
    });
}

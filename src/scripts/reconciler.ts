import { DatabaseService } from "@shared/DatabaseManager.js";
import { QueueService } from "@shared/QueueService.js";
import { EventType, makeJobEvent } from "@shared/models/events.js";
import { JobStatus } from "@shared/models/job.js";
import { createLogger } from "@utils/logger/Log.js";

const logger = createLogger(module);

/**
 * Performs the reconcile stuck jobs operation.
 */
async function reconcileStuckJobs(): Promise<void> {
  const db = DatabaseService.getInstance();
  await db.waitForDb();
  logger.info("reconciler_start");

  const rows = await db.repositories.jobs.findStuckIngesting(2);

  logger.info("reconciler_found_stuck_jobs", { count: rows.length });

  for (const row of rows) {
    const jobId = row.job_id;
    const stuckDuration = Date.now() - new Date(row.updated_at ?? Date.now()).getTime();

    logger.info("reconciler_processing_stuck_job", { job_id: jobId, stuck_duration_ms: stuckDuration });

    try {
      const counts = await db.repositories.pendingArchiveEntries.getCountByJob(jobId);

      logger.info("reconciler_job_pending_counts", { job_id: jobId, pending: counts.pending, completed: counts.completed, failed: counts.failed });

      if (counts.pending === 0 && counts.completed === 0 && counts.failed === 0) {
        logger.warn("reconciler_job_no_pending_entries_marking_failed", { job_id: jobId });
        await QueueService.getInstance().publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, jobId, "reconciler", {
          new_status: JobStatus.FAILED,
          error: "Job stuck in INGESTING with no pending entries",
        }));
      } else if (counts.pending === 0) {
        logger.info("reconciler_job_all_pending_processed_transitioning_to_done", { job_id: jobId, completed: counts.completed, failed: counts.failed });
        await QueueService.getInstance().publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, jobId, "reconciler", {
          new_status: counts.failed > 0 ? JobStatus.FAILED : JobStatus.COMPLETED,
        }));
      } else {
        const staleEntries = await db.repositories.pendingArchiveEntries.findStaleEntries(jobId, 3, ["pending", "processing"]);

        if (staleEntries.length > 0) {
          logger.warn("reconciler_job_has_stale_pending_entries", { job_id: jobId, stale_count: staleEntries.length });

          for (const pendingRow of staleEntries) {
            await db.repositories.pendingArchiveEntries.markStatus(pendingRow.id, "failed", "Stale pending entry - reconciler cleanup");
            logger.info("reconciler_marked_stale_entry_failed", { job_id: jobId, entry_name: pendingRow.entry_name });
          }

          const newCounts = await db.repositories.pendingArchiveEntries.getCountByJob(jobId);
          if (newCounts.pending === 0) {
            logger.info("reconciler_job_resolved_after_stale_cleanup_transitioning_to_done", { job_id: jobId });
            await QueueService.getInstance().publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, jobId, "reconciler", {
              new_status: newCounts.failed > 0 ? JobStatus.FAILED : JobStatus.COMPLETED,
            }));
          }
        } else {
          logger.info("reconciler_job_has_active_pending_entries", { job_id: jobId, pending: counts.pending });
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

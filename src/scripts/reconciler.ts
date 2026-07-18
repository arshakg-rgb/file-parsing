import { getPendingEntryCount, pool, waitForDb } from "../shared/db.js";
import { publishEvent } from "../shared/queueUtils.js";
import { EventType, makeJobEvent } from "../shared/models/events.js";
import { JobStatus } from "../shared/models/job.js";
import { createLogger } from "../utils/logger/logger.js";

const logger = createLogger("reconciler");


async function reconcileStuckJobs(): Promise<void> 
{
  await waitForDb();
  logger.info("reconciler_start");
  
  const result = await pool.query(
    `SELECT job_id, status, created_at, updated_at 
     FROM parse_jobs 
     WHERE status = $1 
     AND updated_at < NOW() - INTERVAL '2 hours'`,
    [JobStatus.INGESTING]
  );
  
  logger.info("reconciler_found_stuck_jobs", { count: result.rows.length });
  
  for (const row of result.rows) 
{
    const jobId = row.job_id;
    const stuckDuration = Date.now() - new Date(row.updated_at).getTime();
    
    logger.info("reconciler_processing_stuck_job", { job_id: jobId, stuck_duration_ms: stuckDuration });
    
    try 
{
      const counts = await getPendingEntryCount(jobId);
      
      logger.info("reconciler_job_pending_counts", { job_id: jobId, pending: counts.pending, completed: counts.completed, failed: counts.failed });
      
      if (counts.pending === 0 && counts.completed === 0 && counts.failed === 0) 
{
        logger.warn("reconciler_job_no_pending_entries_marking_failed", { job_id: jobId });
        await publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, jobId, "reconciler", {
          new_status: JobStatus.FAILED,
          error: "Job stuck in INGESTING with no pending entries",
        }));
      }
 else if (counts.pending === 0) 
{
        logger.info("reconciler_job_all_pending_processed_transitioning_to_done", { job_id: jobId, completed: counts.completed, failed: counts.failed });
        await publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, jobId, "reconciler", {
          new_status: JobStatus.DONE,
        }));
      }
 else 
{
        const _staleThreshold = Date.now() - (3 * 60 * 60 * 1000);
        
        const pendingResult = await pool.query(
          `SELECT id, entry_name, created_at 
           FROM pending_archive_entries 
           WHERE job_id = $1 AND status IN ('pending', 'processing') 
           AND updated_at < NOW() - INTERVAL '3 hours'`,
          [jobId]
        );
        
        if (pendingResult.rows.length > 0) 
{
          logger.warn("reconciler_job_has_stale_pending_entries", { job_id: jobId, stale_count: pendingResult.rows.length });
          
          for (const pendingRow of pendingResult.rows) 
{
            await pool.query(
              `UPDATE pending_archive_entries 
               SET status = 'failed', error = 'Stale pending entry - reconciler cleanup', updated_at = NOW() 
               WHERE id = $1`,
              [pendingRow.id]
            );
            logger.info("reconciler_marked_stale_entry_failed", { job_id: jobId, entry_name: pendingRow.entry_name });
          }
          
          const newCounts = await getPendingEntryCount(jobId);
          if (newCounts.pending === 0) 
{
            logger.info("reconciler_job_resolved_after_stale_cleanup_transitioning_to_done", { job_id: jobId });
            await publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, jobId, "reconciler", {
              new_status: JobStatus.DONE,
            }));
          }
        }
 else 
{
          logger.info("reconciler_job_has_active_pending_entries", { job_id: jobId, pending: counts.pending });
        }
      }
    }
 catch (error) 
{
      logger.error("reconciler_job_processing_failed", { job_id: jobId, error: String(error) }, error instanceof Error ? error : new Error(String(error)));
    }
  }
  
  logger.info("reconciler_complete");
}

if (require.main === module) 
{
  reconcileStuckJobs()
    .then(() => 
{
      console.log("Reconciler completed successfully");
      process.exit(0);
    })
    .catch((error) => 
{
      console.error("Reconciler failed:", error);
      process.exit(1);
    });
}

import type { ParseJobRow } from "@shared/DatabaseManager.js";
import type { JobStatus } from "@shared/models/job.js";
import type { JobEvent } from "@shared/models/events.js";

/**
 * Contract for the job state machine: status transitions and job-event
 * dispatch for the parse-job lifecycle.
 */
export interface StateMachine
{
    /**
     * Fetches a job by id.
     * @param jobId - The job identifier
     */
    getJob(jobId: string): Promise<ParseJobRow | undefined>;

    /**
     * Moves a job to a new status, validating the transition, stamping the
     * relevant timing field, and merging any extra fields.
     *
     * @param jobId - The job identifier
     * @param newStatus - The status to transition into
     * @param error - Optional error message to persist alongside the transition
     * @param extraFields - Additional row fields to merge into the update
     */
    transition(
        jobId: string,
        newStatus: JobStatus,
        error?: string,
        extraFields?: Record<string, unknown>
    ): Promise<ParseJobRow>;

    /**
     * Dispatches an incoming job-lifecycle event to the appropriate handler.
     * @param event - The event
     */
    handleEvent(event: JobEvent): Promise<void>;
}

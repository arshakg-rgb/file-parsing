import { JobStatus } from "@common/enum/JobStatus";
import {JobCounts} from "@shared/models/job";

/** Maps a target status to the timings field that should be stamped on entry. */
export const TIMING_FIELD_BY_STATUS: Partial<Record<JobStatus, string>> = {
    [JobStatus.CREATED]: "created_at",
    [JobStatus.INGESTING]: "ingesting_at",
    [JobStatus.NEEDS_PASSWORD]: "needs_password_at",
    [JobStatus.DETECTING]: "detecting_at",
    [JobStatus.PARSING]: "parsing_at",
    [JobStatus.MERGING_OUTPUT]: "merging_output_at",
    [JobStatus.SAVING_TO_DATABASE]: "saving_to_database_at",
    [JobStatus.REPORTING]: "reporting_at",
    [JobStatus.COMPLETED]: "completed_at",
    [JobStatus.PARTIAL]: "partial_at",
    [JobStatus.ON_HOLD]: "on_hold_at",
    [JobStatus.FAILED]: "failed_at",
};

export const EMPTY_COUNTS: JobCounts = { parsed: 0, dropped_rubbish: 0, failed_by_class: {} };

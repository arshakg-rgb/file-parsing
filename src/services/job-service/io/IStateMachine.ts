import { JobStatus } from "@common/enum/JobStatus";
import {JobCounts} from "@shared/models/job";

/** Maps a target status to the timings field that should be stamped on entry. */
export const TIMING_FIELD_BY_STATUS: Partial<Record<JobStatus, string>> = {
    [JobStatus.INGESTING]: "ingesting_at",
    [JobStatus.DETECTING]: "detecting_at",
    [JobStatus.PARSING]: "parsing_at",
    [JobStatus.MERGING_OUTPUT]: "finalizing_at",
    [JobStatus.SAVING_TO_DATABASE]: "loading_at",
    [JobStatus.REPORTING]: "reporting_at",
};

export const EMPTY_COUNTS: JobCounts = { parsed: 0, dropped_rubbish: 0, failed_by_class: {} };

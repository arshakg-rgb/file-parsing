import { SourceType } from "@common/enum/SourceType.js";
import { JobStatus } from "@common/enum/JobStatus.js";
import { FailureClass } from "@common/enum/FailureClass.js";
export { SourceType } from "@common/enum/SourceType.js";
export { ExecPath } from "@common/enum/ExecPath.js";
export { JobStatus } from "@common/enum/JobStatus.js";

export { FailureClass } from "@common/enum/FailureClass.js";
export { DLQStatus } from "@common/enum/DLQStatus.js";

/**
 * The v a l i d_ t r a n s i t i o n s
 */
export const VALID_TRANSITIONS: Record<JobStatus, JobStatus[]> = {
  [JobStatus.CREATED]: [JobStatus.INGESTING, JobStatus.DETECTING, JobStatus.FAILED],
  [JobStatus.INGESTING]: [JobStatus.NEEDS_PASSWORD, JobStatus.DETECTING, JobStatus.COMPLETED, JobStatus.FAILED],
  [JobStatus.NEEDS_PASSWORD]: [JobStatus.DETECTING, JobStatus.FAILED],
  [JobStatus.DETECTING]: [JobStatus.DETECTING, JobStatus.PARSING, JobStatus.FAILED],
  [JobStatus.PARSING]: [JobStatus.MERGING_OUTPUT, JobStatus.FAILED],
  [JobStatus.MERGING_OUTPUT]: [JobStatus.SAVING_TO_DATABASE, JobStatus.ON_HOLD, JobStatus.FAILED],
  [JobStatus.SAVING_TO_DATABASE]: [JobStatus.REPORTING, JobStatus.FAILED],
  [JobStatus.REPORTING]: [JobStatus.COMPLETED, JobStatus.PARTIAL, JobStatus.FAILED],
  [JobStatus.COMPLETED]: [],
  [JobStatus.PARTIAL]: [],
  [JobStatus.ON_HOLD]: [JobStatus.SAVING_TO_DATABASE, JobStatus.FAILED],
  [JobStatus.FAILED]: [
    JobStatus.INGESTING,
    JobStatus.DETECTING,
    JobStatus.PARSING,
    JobStatus.SAVING_TO_DATABASE,
    JobStatus.REPORTING,
  ],
};

/**
 * The t e r m i n a l_ s t a t u s e s
 */
export const TERMINAL_STATUSES = new Set([
  JobStatus.COMPLETED,
  JobStatus.PARTIAL,
  JobStatus.ON_HOLD,
  JobStatus.FAILED,
]);

export interface JobCounts {
  parsed: number;
  dropped_rubbish: number;
  failed_by_class: Record<string, number>;
  dlq_count?: number;
  rubbish_log_path?: string;
}

/**
 * Performs the total failed operation.
 * @param counts - The counts
 * @returns The numeric result
 */
export function totalFailed(counts: JobCounts): number {
  return Object.values(counts.failed_by_class).reduce((a, b) => a + b, 0);
}

export interface JobTimings {
  created_at?: string;
  ingesting_at?: string;
  needs_password_at?: string;
  detecting_at?: string;
  parsing_at?: string;
  merging_output_at?: string;
  saving_to_database_at?: string;
  reporting_at?: string;
  completed_at?: string;
  partial_at?: string;
  on_hold_at?: string;
  failed_at?: string;
  [key: string]: unknown;
}


/**
 * Checks whether terminal
 * @param status - The status
 * @returns True if the condition is met, false otherwise
 */
export function isTerminal(status: JobStatus): boolean {
  return TERMINAL_STATUSES.has(status);
}

export interface TraceRecord {
  s3_url: string;
  byte_offset: number;
  byte_length: number;
  record_index: number;
  line_no: number;
  job_id: string;
  part_id: string;
  template_id: string;
  template_version: number;
  checksum: string;
  parsed_at: string;
}

/**
 * Optional explicit column layout for headerless, fixed-column delimited files.
 * Maps a field_spec name to a 0-based column index, or an array of indices whose
 * non-empty cells are joined (e.g. a multi-column address). Threaded from job creation
 * through to the parser, which trusts it for delimited lines instead of guessing.
 */
export type ColumnMap = Record<string, number | number[]>;

export interface IngestMessage {
  job_id: string;
  source_type: SourceType;
  source_ref: string;
  field_spec: string[];
  column_map?: ColumnMap;
  batch_id?: string;
  password?: string;
}

export interface ClassifyMessage {
  job_id: string;
  s3_url: string;
  size: number;
  field_spec: string[];
  column_map?: ColumnMap;
}

export interface ParseMessage {
  job_id: string;
  s3_url: string;
  size: number;
  field_spec: string[];
  column_map?: ColumnMap;
  headers?: string[];
  seed_template_ids: string[];
}

export interface DLQMessage {
  dlq_id?: string;
  job_id: string;
  byte_offset: number;
  byte_length: number;
  line_no: number;
  raw_bytes: string;
  failure_class: FailureClass;
  error: string;
  attempts: number;
  status?: string;
}

export interface LoadMessage {
  job_id: string;
  output_paths?: string[];
  field_spec?: string[];
  recovered_row?: Record<string, unknown>;
  byte_offset?: number;
  byte_length?: number;
  line_no?: number;
  template_id?: string;
  template_version?: number;
}

export interface ReportMessage {
  job_id: string;
  status: JobStatus;
  counts: JobCounts;
  output_paths: string[];
  rubbish_log_path?: string;
  dlq_count: number;
  csv_output_path?: string | null;
}

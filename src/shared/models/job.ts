import { randomUUID } from "crypto";

export enum SourceType {
  S3 = "s3",
  UPLOAD = "upload",
  URL = "url",
  ARCHIVE_ENTRY = "archive_entry",
}

export enum ExecPath {
  STREAM = "stream",
  PARALLEL = "parallel",
}

export enum JobStatus {
  QUEUED = "queued",
  INGESTING = "ingesting",
  AWAITING_PASSWORD = "awaiting_password",
  DETECTING = "detecting",
  PARSING = "parsing",
  FINALIZING = "finalizing",
  LOADING = "loading",
  REPORTING = "reporting",
  DONE = "done",
  PARTIAL = "partial",
  HELD = "held",
  FAILED = "failed",
}

export const VALID_TRANSITIONS: Record<JobStatus, JobStatus[]> = {
  [JobStatus.QUEUED]: [JobStatus.INGESTING, JobStatus.DETECTING, JobStatus.FAILED],
  [JobStatus.INGESTING]: [JobStatus.AWAITING_PASSWORD, JobStatus.DETECTING, JobStatus.DONE, JobStatus.FAILED],
  [JobStatus.AWAITING_PASSWORD]: [JobStatus.DETECTING, JobStatus.FAILED],
  [JobStatus.DETECTING]: [JobStatus.DETECTING, JobStatus.PARSING, JobStatus.FAILED],
  [JobStatus.PARSING]: [JobStatus.FINALIZING, JobStatus.FAILED],
  [JobStatus.FINALIZING]: [JobStatus.LOADING, JobStatus.HELD, JobStatus.FAILED],
  [JobStatus.LOADING]: [JobStatus.REPORTING, JobStatus.FAILED],
  [JobStatus.REPORTING]: [JobStatus.DONE, JobStatus.PARTIAL, JobStatus.FAILED],
  [JobStatus.DONE]: [],
  [JobStatus.PARTIAL]: [],
  [JobStatus.HELD]: [JobStatus.LOADING],
  [JobStatus.FAILED]: [],
};

export const TERMINAL_STATUSES = new Set([
  JobStatus.DONE,
  JobStatus.PARTIAL,
  JobStatus.HELD,
  JobStatus.FAILED,
]);

export interface JobCounts {
  parsed: number;
  dropped_rubbish: number;
  failed_by_class: Record<string, number>;
  dlq_count?: number;
  rubbish_log_path?: string;
}

export function totalFailed(counts: JobCounts): number {
  return Object.values(counts.failed_by_class).reduce((a, b) => a + b, 0);
}

export interface JobTimings {
  queued_at?: string;
  ingesting_at?: string;
  detecting_at?: string;
  parsing_at?: string;
  finalizing_at?: string;
  loading_at?: string;
  reporting_at?: string;
  completed_at?: string;
}

export interface ParseJob {
  job_id: string;
  batch_id?: string;
  parent_job_id?: string;
  source_type: SourceType;
  source_ref: string;
  s3_url?: string;
  size?: number;
  field_spec: string[];
  exec_path: ExecPath;
  status: JobStatus;
  output_paths: string[];
  counts: JobCounts;
  timings: JobTimings;
  error?: string;
  created_at: string;
  updated_at: string;
}

export function defaultParseJob(): ParseJob {
  const now = new Date().toISOString();
  return {
    job_id: randomUUID(),
    source_type: SourceType.S3,
    source_ref: "",
    field_spec: [],
    exec_path: ExecPath.STREAM,
    status: JobStatus.QUEUED,
    output_paths: [],
    counts: { parsed: 0, dropped_rubbish: 0, failed_by_class: {} },
    timings: {},
    created_at: now,
    updated_at: now,
  };
}

export function canTransitionTo(current: JobStatus, next: JobStatus): boolean {
  return VALID_TRANSITIONS[current]?.includes(next) ?? false;
}

export function isTerminal(status: JobStatus): boolean {
  return TERMINAL_STATUSES.has(status);
}

export interface OutputPart {
  part_id: string;
  job_id: string;
  template_id: string;
  s3_path: string;
  row_count: number;
  byte_size: number;
  created_at: string;
}

export interface RubbishLogEntry {
  job_id: string;
  byte_offset: number;
  line_no: number;
  raw_bytes: string;
  matched_template_id: string;
  logged_at: string;
}

export enum FailureClass {
  UNCERTAIN = "uncertain",
  TRANSFORM_ERROR = "transform_error",
  TYPE_MISMATCH = "type_mismatch",
  ENCODING_ERROR = "encoding_error",
  EXTRACTION_ERROR = "extraction_error",
}

export enum DLQStatus {
  PENDING = "pending",
  RETRY = "retry",
  REVIEW = "review",
  RESOLVED = "resolved",
}

export interface DeadLetterEntry {
  dlq_id: string;
  job_id: string;
  byte_offset: number;
  byte_length: number;
  line_no: number;
  raw_bytes: string;
  failure_class: FailureClass;
  error: string;
  attempts: number;
  status: DLQStatus;
  created_at: string;
  updated_at: string;
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
  recovered_row?: Record<string, any>;
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
}

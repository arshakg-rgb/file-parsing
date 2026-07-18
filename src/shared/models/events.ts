import { randomUUID } from "crypto";
import { JobStatus } from "./job.js";

export enum EventType {
  JOB_STATUS_CHANGED = "job_status_changed",
  ENTRY_DISCOVERED = "entry_discovered",
  PARSING_COMPLETED = "parsing_completed",
  LOADING_COMPLETED = "loading_completed",
  REPORTING_COMPLETED = "reporting_completed",
  ERROR_OCCURRED = "error_occurred",
}

export interface JobEvent {
  event_id: string;
  event_type: EventType;
  job_id: string;
  source_service: string;
  timestamp: string;
  data: Record<string, unknown>;
}

export function makeJobEvent(
  event_type: EventType,
  job_id: string,
  source_service: string,
  data: Record<string, unknown> = {}
): JobEvent {
  return {
    event_id: randomUUID(),
    event_type,
    job_id,
    source_service,
    timestamp: new Date().toISOString(),
    data,
  };
}

export interface StatusChangedData {
  old_status: JobStatus;
  new_status: JobStatus;
  error?: string;
}

export interface EntryDiscoveredData {
  parent_job_id: string;
  batch_id: string;
  entry_s3_url: string | null;
  entry_name: string;
  entry_size: number;
  field_spec: string[];
  pending?: boolean;
}

export interface PendingEntryData {
  parent_job_id: string;
  batch_id: string;
  entry_s3_url: null;
  entry_name: string;
  entry_size: number;
  field_spec: string[];
  pending: true;
}

export interface ParsingCompletedData {
  job_id: string;
  parsed: number;
  dropped_rubbish: number;
  failed: number;
  part_s3_paths: string[];
  dlq_count: number;
  rubbish_log_path?: string;
  csv_output_path?: string | null;
}

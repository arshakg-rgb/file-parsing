export interface ArchiveEntryRequest {
  job_id: string;
  batchId: string;
  archive_s3_url: string;
  entry_name: string;
  entry_size: number;
  field_spec: string[];
  password?: string;
  archive_type: string;
  nesting_depth: number;
}

export interface ArchiveEntryResponse {
  success: boolean;
  error?: string;
}

export interface IArchiveEntryConsumer {
  processEntry(req: ArchiveEntryRequest): Promise<ArchiveEntryResponse>;
}

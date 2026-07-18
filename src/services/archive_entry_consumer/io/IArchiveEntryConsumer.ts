export interface ArchiveEntryRequest {
  job_id: string;
  entry_path: string;
  s3_url: string;
  password?: Buffer;
}

export interface ArchiveEntryResponse {
  success: boolean;
  error?: string;
}

export interface IArchiveEntryConsumer {
  processEntry(req: ArchiveEntryRequest): Promise<ArchiveEntryResponse>;
}

export interface ReportRequest {
  job_id: string;
  [key: string]: unknown;
}

export interface ReportResponse {
  success: boolean;
  error?: string;
  data?: unknown;
}

export interface IReport {
  processReport(req: ReportRequest): Promise<ReportResponse>;
}

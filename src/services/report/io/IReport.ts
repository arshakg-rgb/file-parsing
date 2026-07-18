export interface ReportRequest {
  job_id: string;
  [key: string]: any;
}

export interface ReportResponse {
  success: boolean;
  error?: string;
  data?: any;
}

export interface IReport {
  processReport(req: ReportRequest): Promise<ReportResponse>;
}

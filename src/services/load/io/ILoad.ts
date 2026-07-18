export interface LoadRequest {
  job_id: string;
  s3_url: string;
  field_spec: string[];
  [key: string]: any;
}

export interface LoadResponse {
  success: boolean;
  error?: string;
  records_loaded?: number;
}

export interface ILoad {
  processLoad(req: LoadRequest): Promise<LoadResponse>;
}

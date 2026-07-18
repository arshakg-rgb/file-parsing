export interface ErrorInfo {
  message: string;
  code?: string;
  statusCode?: number;
  details?: unknown;
}

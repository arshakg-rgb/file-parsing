/**
 * Error information object.
 */
export interface ErrorInfo
{
  name?: string;
  code?: string;
  service?: string;
  timestamp?: number;
  forwarded?: string[];
  message?: string | null;
  fields?: string[];
  info?: Record<string, any>;
  stack?: string | string[];
}


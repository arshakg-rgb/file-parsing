import { FailureClass } from "@shared/models/job.js";
import { RecordTemplate, RubbishTemplate } from "@shared/TemplateRegistryService.js";

export interface ClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines?: string[];
  job_id?: string;
}

export interface ClassifyResponse {
  kind: "record-template" | "rubbish-signature" | "uncertain";
  template?: RecordTemplate | RubbishTemplate;
}

export interface ClassifyResult {
  verdict: "parsed" | "rubbish" | "uncertain";
  row?: Record<string, unknown>;
  template_id?: string;
  template_version?: number;
  failure_class?: FailureClass;
  ai_calls_used?: number;
}

export interface IClassifier
{
  classify(line: string, byteOffset: number, byteLength: number): ClassifyResult;

  classifyWithAI(line: string, contextLines: string[], remainingBudget?: number): Promise<ClassifyResult>;

  classifyWithTimeout(line: string, contextLines: string[], timeoutMs: number, remainingBudget?: number): Promise<ClassifyResult>;
}


export interface AiRateLimiter
{
  acquire(): Promise<void>;
}

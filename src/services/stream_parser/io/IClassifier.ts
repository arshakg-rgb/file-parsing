import { _FailureClass } from "../../../shared/models/job.js";
import { RecordTemplate, RubbishTemplate } from "../../../shared/templateRegistry.js";

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
  row?: Record<string, any>;
  template_id?: string;
  template_version?: number;
  failure_class?: _FailureClass;
}

export interface IClassifier {
  classify(line: string, byteOffset: number, byteLength: number): ClassifyResult;
  classifyWithAI(line: string, contextLines: string[]): Promise<ClassifyResult>;
  classifyWithTimeout(line: string, contextLines: string[], timeoutMs: number): Promise<ClassifyResult>;
}

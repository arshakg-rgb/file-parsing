import { RecordTemplate, RubbishTemplate } from "../../../shared/templateRegistry.js";

export interface ClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines?: string[];
  job_id?: string;
}

export enum AIVerdict {
  RECORD_TEMPLATE = "record-template",
  RUBBISH_SIGNATURE = "rubbish-signature",
  UNCERTAIN = "uncertain",
}

export interface ClassifyResponse {
  kind: AIVerdict;
  template?: RecordTemplate | RubbishTemplate;
}

export interface FieldLocator {
  index?: number;
  regex?: string;
  key?: string;
}

export interface CSVParseResult {
  success: boolean;
  delimiter: string;
  fields: string[];
}

export interface IAiClassifier {
  classifyAi(req: ClassifyRequest): Promise<ClassifyResponse>;
}

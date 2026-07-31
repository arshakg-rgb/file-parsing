
export enum TemplateKind {
  RECORD = "record",
  RUBBISH = "rubbish",
}

export enum TemplateSource {
  AI = "ai",
  BOOTSTRAP = "bootstrap",
  USER = "user",
}

export enum LineStructure {
  CSV = "csv",
  JSON = "json",
  KV = "kv",
  FIXED = "fixed",
  REGEX = "regex",
}

export interface FieldLocator {
  index?: number;
  regex?: string;
  key?: string;
}

export interface RecordTemplateData {
  structure: LineStructure;
  delimiter?: string;
  quote_char?: string;
  field_map: Record<string, FieldLocator>;
  length_hint_min?: number;
  length_hint_max?: number;
  has_header: boolean;
}

export interface RubbishTemplateData {
  signature: string;
  confidence: number;
  description?: string;
}

export interface Template {
  template_id: string;
  kind: TemplateKind;
  fingerprint: string;
  version: number;
  record?: RecordTemplateData;
  rubbish?: RubbishTemplateData;
  source: TemplateSource;
  match_count: number;
  created_at: string;
  updated_at: string;
}


export enum AIVerdict {
  RECORD_TEMPLATE = "record-template",
  RUBBISH_SIGNATURE = "rubbish-signature",
  UNCERTAIN = "uncertain",
}

export interface ClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines: string[];
  job_id?: string;
}

export interface ClassifyResponse {
  kind: AIVerdict;
  template?: Template;
  reasoning?: string;
}

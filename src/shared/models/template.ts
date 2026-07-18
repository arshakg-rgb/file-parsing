import { randomUUID } from "crypto";

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

export function validateFieldLocator(loc: FieldLocator): void 
{
  const set = [loc.index, loc.regex, loc.key].filter((v) => v !== null && v !== undefined);
  if (set.length !== 1) 
{
    throw new Error("Exactly one of index, regex, key must be set in FieldLocator");
  }
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

export function validateTemplate(t: Template): void 
{
  if (t.kind === TemplateKind.RECORD && !t.record) 
{
    throw new Error("Template with kind=record must have record data");
  }
  if (t.kind === TemplateKind.RUBBISH && !t.rubbish) 
{
    throw new Error("Template with kind=rubbish must have rubbish data");
  }
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

export function makeRecordTemplate(
  record: RecordTemplateData,
  fingerprint: string,
  source: TemplateSource = TemplateSource.AI
): Template 
{
  return {
    template_id: randomUUID(),
    kind: TemplateKind.RECORD,
    fingerprint,
    version: 1,
    record,
    source,
    match_count: 0,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
}

export function makeRubbishTemplate(
  rubbish: RubbishTemplateData,
  fingerprint: string,
  source: TemplateSource = TemplateSource.AI
): Template 
{
  if (rubbish.confidence < 0.9) 
{
    throw new Error(`Rubbish template confidence ${rubbish.confidence} is below minimum 0.90`);
  }
  return {
    template_id: randomUUID(),
    kind: TemplateKind.RUBBISH,
    fingerprint,
    version: 1,
    rubbish,
    source,
    match_count: 0,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
}

export function isRecord(t: Template): boolean 
{
  return t.kind === TemplateKind.RECORD;
}

export function isRubbish(t: Template): boolean 
{
  return t.kind === TemplateKind.RUBBISH;
}

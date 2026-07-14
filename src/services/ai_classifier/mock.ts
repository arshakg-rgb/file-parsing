import crypto from "crypto";
import { ClassifyRequest, ClassifyResponse, AIVerdict, FieldLocator, LineStructure, RecordTemplateData, RubbishTemplateData, Template, TemplateKind, TemplateSource } from "../../shared/models/template.js";

function mockFingerprint(line: string): string {
  return crypto.createHash("sha256").update(line).digest("hex").slice(0, 24);
}

export function mockClassify(req: ClassifyRequest): ClassifyResponse {
  const line = req.unknown_line;

  for (const delim of [",", ";", "\t", "|"]) {
    const parts = line.split(delim);
    if (parts.length >= 3) {
      const fieldMap: Record<string, FieldLocator> = {};
      for (let i = 0; i < req.field_spec.length; i++) {
        fieldMap[req.field_spec[i]] = { index: Math.min(i, parts.length - 1) };
      }
      const tmpl: Template = {
        template_id: crypto.randomUUID(),
        kind: TemplateKind.RECORD,
        fingerprint: mockFingerprint(line),
        version: 1,
        record: {
          structure: LineStructure.CSV,
          delimiter: delim,
          quote_char: '"',
          field_map: fieldMap,
          length_hint_min: Math.floor(line.length / 2),
          length_hint_max: line.length * 2,
          has_header: false,
        } as RecordTemplateData,
        source: TemplateSource.AI,
        match_count: 0,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
      return { kind: AIVerdict.RECORD_TEMPLATE, template: tmpl };
    }
  }

  if (/^(ERROR|WARNING|DEBUG|INFO|TRACE)/.test(line)) {
    const tmpl: Template = {
      template_id: crypto.randomUUID(),
      kind: TemplateKind.RUBBISH,
      fingerprint: mockFingerprint(line),
      version: 1,
      rubbish: {
        signature: "^(ERROR|WARNING|DEBUG|INFO|TRACE).*",
        confidence: 0.95,
        description: "Log line prefix",
      } as RubbishTemplateData,
      source: TemplateSource.AI,
      match_count: 0,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
    return { kind: AIVerdict.RUBBISH_SIGNATURE, template: tmpl };
  }

  return { kind: AIVerdict.UNCERTAIN };
}

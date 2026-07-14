import crypto from "crypto";
import { RecordTemplate, RubbishTemplate } from "../../shared/templateRegistry.js";

interface ClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines?: string[];
  job_id?: string;
}

interface ClassifyResponse {
  kind: "record-template" | "rubbish-signature" | "uncertain";
  template?: RecordTemplate | RubbishTemplate;
}

function mockFingerprint(line: string): string {
  return crypto.createHash("sha256").update(line).digest("hex").slice(0, 24);
}

export function mockClassify(req: ClassifyRequest): ClassifyResponse {
  const line = req.unknown_line;

  for (const delim of [",", ";", "\t", "|"]) {
    const parts = line.split(delim);
    if (parts.length >= 3) {
      const fieldMap: Record<string, { locator: string; type: string }> = {};
      for (let i = 0; i < req.field_spec.length; i++) {
        fieldMap[req.field_spec[i]] = {
          locator: `index:${Math.min(i, parts.length - 1)}`,
          type: "string"
        };
      }
      const tmpl: RecordTemplate = {
        template_id: crypto.randomUUID(),
        fingerprint: mockFingerprint(line),
        version: 1,
        field_map: fieldMap,
        structure: "csv",
        length_hint: Math.floor(line.length / 2),
        source: "ai" as const,
        created_at: new Date(),
      };
      return { kind: "record-template", template: tmpl };
    }
  }

  if (/^(ERROR|WARNING|DEBUG|INFO|TRACE)/.test(line)) {
    const tmpl: RubbishTemplate = {
      template_id: crypto.randomUUID(),
      fingerprint: mockFingerprint(line),
      version: 1,
      signature: "^(ERROR|WARNING|DEBUG|INFO|TRACE).*",
      confidence: 0.95,
      source: "ai" as const,
      created_at: new Date(),
    };
    return { kind: "rubbish-signature", template: tmpl };
  }

  return { kind: "uncertain" };
}

import crypto from "crypto";
import { RecordTemplate, RubbishTemplate } from "../../../shared/templateRegistry.js";

export interface MockClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines?: string[];
  job_id?: string;
}

export type MockClassifyKind = "record-template" | "rubbish-signature" | "uncertain";

export interface MockClassifyResponse {
  kind: MockClassifyKind;
  template?: RecordTemplate | RubbishTemplate;
}

export class MockClassifier {
  private static instance: MockClassifier;

  private constructor() {}

  static getInstance(): MockClassifier {
    if (!MockClassifier.instance) {
      MockClassifier.instance = new MockClassifier();
    }
    return MockClassifier.instance;
  }

  classify(req: MockClassifyRequest): MockClassifyResponse {
    const line = req.unknown_line;

    for (const delim of [",", ";", "\t", "|"]) {
      const parts = line.split(delim);
      if (parts.length >= 3) {
        const fieldMap: Record<string, { locator: string; type: string }> = {};
        for (let i = 0; i < req.field_spec.length; i++) {
          fieldMap[req.field_spec[i]] = {
            locator: `index:${Math.min(i, parts.length - 1)}`,
            type: "string",
          };
        }
        const tmpl: RecordTemplate = {
          template_id: crypto.randomUUID(),
          fingerprint: MockClassifier.fingerprint(line),
          version: 1,
          field_map: fieldMap,
          structure: "csv",
          delimiter: delim,
          length_hint: Math.floor(line.length / 2),
          source: "ai",
          created_at: new Date(),
        };
        return { kind: "record-template", template: tmpl };
      }
    }

    if (/^(ERROR|WARNING|DEBUG|INFO|TRACE)/.test(line)) {
      const tmpl: RubbishTemplate = {
        template_id: crypto.randomUUID(),
        fingerprint: MockClassifier.fingerprint(line),
        version: 1,
        signature: "^(ERROR|WARNING|DEBUG|INFO|TRACE).*",
        confidence: 0.95,
        source: "ai",
        created_at: new Date(),
      };
      return { kind: "rubbish-signature", template: tmpl };
    }

    return { kind: "uncertain" };
  }

  static fingerprint(line: string): string {
    return crypto.createHash("sha256").update(line).digest("hex").slice(0, 24);
  }
}

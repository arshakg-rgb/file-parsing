
export interface RecordTemplate {
  template_id: string;
  fingerprint: string;
  version: number;
  field_map: Record<string, { locator: string; type: string }>;
  structure: string;
  delimiter?: string;
  length_hint: number;
  source: "ai" | "bootstrap" | "user";
  created_at: Date;
}

export interface RubbishTemplate {
  template_id: string;
  fingerprint: string;
  signature: string;
  confidence: number;
  version: number;
  source: "ai" | "bootstrap" | "user";
  created_at: Date;
}

export type Template = RecordTemplate | RubbishTemplate;
export type TemplateKind = "record" | "rubbish";

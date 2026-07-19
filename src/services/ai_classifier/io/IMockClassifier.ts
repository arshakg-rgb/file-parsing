import { RecordTemplate, RubbishTemplate } from "@shared/TemplateRegistryService.js";

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

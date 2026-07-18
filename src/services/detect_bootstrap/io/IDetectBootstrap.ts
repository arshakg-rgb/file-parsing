export interface ClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines?: string[];
  job_id?: string;
}

export interface ClassifyResponse {
  kind: "record-template" | "rubbish-signature" | "uncertain";
  template?: unknown;
}

export interface IDetectBootstrap {
  detectBootstrap(req: ClassifyRequest): Promise<ClassifyResponse>;
}

import { Template, TemplateKind } from "@shared/models/template.js";

export interface ITemplateRegistry
{
  getByFingerprint(fingerprint: string): Template[];
  getLatest(fingerprint: string, kind?: TemplateKind): Template | null;
  save(tmpl: Template): Promise<Template>;
  incrementMatchCount(templateId: string, fingerprint: string): void;
  listAll(kind?: TemplateKind): Template[];
  warmCache(): Promise<void>;
  ensureTableExists(): void;
}

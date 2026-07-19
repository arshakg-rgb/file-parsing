import { Template, TemplateKind } from "@shared/models/template.js";
import { TemplateRegistryService } from "@service/ai_classifier/templateRegistry/TemplateRegistryService.js";

const registry = TemplateRegistryService.getInstance();

export function getByFingerprint(fingerprint: string): Template[] {
  return registry.getByFingerprint(fingerprint);
}

export function getLatest(fingerprint: string, kind?: TemplateKind): Template | null {
  return registry.getLatest(fingerprint, kind);
}

export function save(tmpl: Template): Promise<Template> {
  return registry.save(tmpl);
}

export function incrementMatchCount(templateId: string, fingerprint: string): void {
  registry.incrementMatchCount(templateId, fingerprint);
}

export function listAll(kind?: TemplateKind): Template[] {
  return registry.listAll(kind);
}

export function warmCache(): Promise<void> {
  return registry.warmCache();
}

export function ensureTableExists(): void {
  registry.ensureTableExists();
}

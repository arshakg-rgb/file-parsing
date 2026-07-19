import { Template, TemplateKind } from "@shared/models/template.js";
import { TemplateRegistryService } from "@service/ai_classifier/templateRegistry/TemplateRegistryService.js";

/**
 * The registry
 */
const registry = TemplateRegistryService.getInstance();

/**
 * Gets by fingerprint
 * @param fingerprint - The fingerprint
 * @returns The list of results
 */
export function getByFingerprint(fingerprint: string): Template[] {
  return registry.getByFingerprint(fingerprint);
}

/**
 * Gets latest
 * @param fingerprint - The fingerprint
 * @param kind - The kind
 * @returns The template | null result
 */
export function getLatest(fingerprint: string, kind?: TemplateKind): Template | null {
  return registry.getLatest(fingerprint, kind);
}

/**
 * Saves the operation
 * @param tmpl - The tmpl
 * @returns A promise that resolves to the result
 */
export function save(tmpl: Template): Promise<Template> {
  return registry.save(tmpl);
}

/**
 * Performs the increment match count operation.
 * @param templateId - The template id
 * @param fingerprint - The fingerprint
 */
export function incrementMatchCount(templateId: string, fingerprint: string): void {
  registry.incrementMatchCount(templateId, fingerprint);
}

/**
 * Performs the list all operation.
 * @param kind - The kind
 * @returns The list of results
 */
export function listAll(kind?: TemplateKind): Template[] {
  return registry.listAll(kind);
}

/**
 * Performs the warm cache operation.
 */
export function warmCache(): Promise<void> {
  return registry.warmCache();
}

/**
 * Ensures table exists
 */
export function ensureTableExists(): void {
  registry.ensureTableExists();
}

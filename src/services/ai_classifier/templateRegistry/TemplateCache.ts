import { Template, TemplateKind } from "@shared/models/template.js";

/**
 * TemplateCache is responsible for template cache operations.
 */
export class TemplateCache {
    /**
   * Templates
   * @private
   */
  private readonly templates: Template[] = [];
    /**
   * Warmed
   * @private
   */
  private warmed = false;

    /**
   * Checks whether warmed
   * @returns True if the condition is met, false otherwise
   */
  isWarmed(): boolean {
    return this.warmed;
  }

    /**
   * Sets warmed
   * @param value - The value to use
   */
  setWarmed(value: boolean): void {
    this.warmed = value;
  }

    /**
   * Gets by fingerprint
   * @param fingerprint - The fingerprint
   * @returns The list of results
   */
  getByFingerprint(fingerprint: string): Template[] {
    return this.templates.filter((t) => t.fingerprint === fingerprint);
  }

    /**
   * Gets latest
   * @param fingerprint - The fingerprint
   * @param kind - The kind
   * @returns The template | null result
   */
  getLatest(fingerprint: string, kind?: TemplateKind): Template | null {
    const matches = this.getByFingerprint(fingerprint).filter((t) => (kind ? t.kind === kind : true));
    if (!matches.length) return null;
    return matches.reduce((max, t) => (t.version > max.version ? t : max));
  }

    /**
   * Adds the operation
   * @param template - The template
   */
  add(template: Template): void {
    this.templates.push(template);
  }

    /**
   * Finds the operation
   * @param templateId - The template id
   * @param fingerprint - The fingerprint
   * @returns The template | undefined result
   */
  find(templateId: string, fingerprint: string): Template | undefined {
    return this.templates.find((t) => t.template_id === templateId && t.fingerprint === fingerprint);
  }

    /**
   * Performs the list all operation.
   * @param kind - The kind
   * @returns The list of results
   */
  listAll(kind?: TemplateKind): Template[] {
    if (!kind) return [...this.templates];
    return this.templates.filter((t) => t.kind === kind);
  }

    /**
   * Resets the operation
   * @param templates - The templates
   */
  reset(templates: Template[]): void {
    this.templates.length = 0;
    this.templates.push(...templates);
  }
}

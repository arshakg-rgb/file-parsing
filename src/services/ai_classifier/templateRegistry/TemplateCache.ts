import { Template, TemplateKind } from "@shared/models/template.js";

export class TemplateCache {
  private readonly templates: Template[] = [];
  private warmed = false;

  isWarmed(): boolean {
    return this.warmed;
  }

  setWarmed(value: boolean): void {
    this.warmed = value;
  }

  getByFingerprint(fingerprint: string): Template[] {
    return this.templates.filter((t) => t.fingerprint === fingerprint);
  }

  getLatest(fingerprint: string, kind?: TemplateKind): Template | null {
    const matches = this.getByFingerprint(fingerprint).filter((t) => (kind ? t.kind === kind : true));
    if (!matches.length) return null;
    return matches.reduce((max, t) => (t.version > max.version ? t : max));
  }

  add(template: Template): void {
    this.templates.push(template);
  }

  find(templateId: string, fingerprint: string): Template | undefined {
    return this.templates.find((t) => t.template_id === templateId && t.fingerprint === fingerprint);
  }

  listAll(kind?: TemplateKind): Template[] {
    if (!kind) return [...this.templates];
    return this.templates.filter((t) => t.kind === kind);
  }

  reset(templates: Template[]): void {
    this.templates.length = 0;
    this.templates.push(...templates);
  }
}

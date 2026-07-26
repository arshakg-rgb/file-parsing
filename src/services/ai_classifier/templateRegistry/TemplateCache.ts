import { Template, TemplateKind } from "@shared/models/template.js";

/**
 * TemplateCache is responsible for template cache operations.
 */
export class TemplateCache
{
    /**
   * Templates
   * @private
   */

  private readonly templates: Template[] = [];
    /**
   * Warmed
   * @private
   */

  private warmed: boolean = false;

    /**
   * Checks whether warmed
   * @returns True if the condition is met, false otherwise
   */

  public isWarmed(): boolean
  {
    return this.warmed;
  }

    /**
   * Sets warmed
   * @param value - The value to use
   */

  public setWarmed(value: boolean): void
  {
    this.warmed = value;
  }

    /**
   * Gets by fingerprint
   * @param fingerprint - The fingerprint
   * @returns The list of results
   */

  public getByFingerprint(fingerprint: string): Template[]
  {
    return this.templates.filter((template) => template.fingerprint === fingerprint);
  }

    /**
   * Gets latest
   * @param fingerprint - The fingerprint
   * @param kind - The kind
   * @returns The template | null result
   */

  public getLatest(fingerprint: string, kind?: TemplateKind): Template | null
  {
    const matches: Template[] = this.getByFingerprint(fingerprint).filter((template) => (kind ? template.kind === kind : true));

    if (!matches.length)
    {
      return null;
    }

    return matches.reduce((max, template) => (template.version > max.version ? template : max));
  }

    /**
   * Adds the operation
   * @param template - The template
   */

  public add(template: Template): void
  {
    this.templates.push(template);
  }

    /**
   * Finds the operation
   * @param templateId - The template id
   * @param fingerprint - The fingerprint
   * @returns The template | undefined result
   */

  public find(templateId: string, fingerprint: string): Template | undefined
  {
    return this.templates.find((template) => template.template_id === templateId && template.fingerprint === fingerprint);
  }

    /**
   * Performs the list all operation.
   * @param kind - The kind
   * @returns The list of results
   */

  public listAll(kind?: TemplateKind): Template[]
  {
    if (!kind)
    {
      return [...this.templates];
    }

    return this.templates.filter((template) => template.kind === kind);
  }

    /**
   * Resets the operation
   * @param templates - The templates
   */

  public reset(templates: Template[]): void
  {
    this.templates.length = 0;
    this.templates.push(...templates);
  }
}

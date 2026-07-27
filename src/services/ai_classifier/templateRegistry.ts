import { Template, TemplateKind } from "@shared/models/template.js";
import { TemplateRegistryService } from "@service/ai_classifier/templateRegistry/TemplateRegistryService.js";
import { InstantiationError } from "@errors/InstantiationError.js";


export class TemplateRegistryFacade
{
  /**
   * Singleton instance of TemplateRegistryFacade.
   */
  private static instance: TemplateRegistryFacade;

  /**
   * Underlying registry service instance.
   * @private
   */
  private registry: TemplateRegistryService = TemplateRegistryService.getInstance();

  /**
   * Private constructor to enforce a Singleton pattern.
   *
   * @param enforce - Function to enforce a Singleton pattern.
   * @throws Error if instantiation is attempted directly.
   */
  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use TemplateRegistryFacade.getInstance() instead of new.");
    }
  }

  /**
   * Gets the singleton instance of TemplateRegistryFacade.
   *
   * @returns The singleton instance of TemplateRegistryFacade.
   */
  public static getInstance(): TemplateRegistryFacade
  {
    if (!TemplateRegistryFacade.instance)
    {
      TemplateRegistryFacade.instance = new TemplateRegistryFacade(Enforce);
    }

    return TemplateRegistryFacade.instance;
  }

  /**
   * Gets templates by fingerprint.
   * @param fingerprint - The fingerprint
   * @returns The list of matching templates
   */
  public getByFingerprint(fingerprint: string): Template[]
  {
    return this.registry.getByFingerprint(fingerprint);
  }

  /**
   * Gets the latest template for a fingerprint/kind.
   * @param fingerprint - The fingerprint
   * @param kind - The kind
   * @returns The template, or null if none found
   */
  public getLatest(fingerprint: string, kind?: TemplateKind): Template | null
  {
    return this.registry.getLatest(fingerprint, kind);
  }

  /**
   * Saves a template.
   * @param tmpl - The template to save
   * @returns A promise that resolves to the saved template
   */
  public save(tmpl: Template): Promise<Template>
  {
    return this.registry.save(tmpl);
  }

  /**
   * Increments the match count for a template.
   * @param templateId - The template id
   * @param fingerprint - The fingerprint
   */
  public incrementMatchCount(templateId: string, fingerprint: string): void
  {
    this.registry.incrementMatchCount(templateId, fingerprint);
  }

  /**
   * Lists all templates, optionally filtered by kind.
   * @param kind - The kind
   * @returns The list of templates
   */
  public listAll(kind?: TemplateKind): Template[]
  {
    return this.registry.listAll(kind);
  }

  /**
   * Warms the template cache.
   * @returns A promise that resolves when the cache is warmed
   */
  public warmCache(): Promise<void>
  {
    return this.registry.warmCache();
  }
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void {}

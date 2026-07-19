import { randomUUID } from "crypto";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { Template, TemplateKind } from "@shared/models/template.js";
import { ITemplateRegistry } from "@service/ai_classifier/io/ITemplateRegistry.js";
import { TemplateCache } from "./TemplateCache.js";
import { FirestoreTemplateRepository } from "./FirestoreTemplateRepository.js";

/**
 * TemplateRegistryService is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
export class TemplateRegistryService extends ServiceManager implements ITemplateRegistry {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: TemplateRegistryService;
    /**
   * Cache
   * @private
   */
  private readonly cache: TemplateCache;
    /**
   * Repository
   * @private
   */
  private readonly repository: FirestoreTemplateRepository;
    /**
   * Warming
   * @private
   */
  private warming: Promise<void> | null = null;

    /**
   * Constructs a new TemplateRegistryService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @param cache - The cache
   * @param repository - The repository
   * @throws Error if instantiated directly
   */
  private constructor(
    enforce: () => void,
    cache?: TemplateCache,
    repository?: FirestoreTemplateRepository
  ) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate TemplateRegistryService directly. Use getInstance()");
    }
    super(enforce);
    this.cache = cache ?? new TemplateCache();
    this.repository = repository ?? new FirestoreTemplateRepository();
  }

    /**
   * Gets the single instance of the TemplateRegistryService class.
   * @returns The single instance of the class
   */
  static getInstance(): TemplateRegistryService {
    if (!TemplateRegistryService.instance) {
      TemplateRegistryService.instance = new TemplateRegistryService(Enforce);
    }
    return TemplateRegistryService.instance;
  }

    /**
   * Ensures warmed
   */
  ensureWarmed(): void {
    if (!this.cache.isWarmed()) {
      this.warmCache().catch(() => {});
    }
  }

    /**
   * Gets by fingerprint
   * @param fingerprint - The fingerprint
   * @returns The list of results
   */
  getByFingerprint(fingerprint: string): Template[] {
    this.ensureWarmed();
    return this.cache.getByFingerprint(fingerprint);
  }

    /**
   * Gets latest
   * @param fingerprint - The fingerprint
   * @param kind - The kind
   * @returns The template | null result
   */
  getLatest(fingerprint: string, kind?: TemplateKind): Template | null {
    this.ensureWarmed();
    return this.cache.getLatest(fingerprint, kind);
  }

    /**
   * Saves the operation
   * @param tmpl - The tmpl
   * @returns A promise that resolves to the result
   */
  async save(tmpl: Template): Promise<Template> {
    await this.warmCache();
    const existing = this.cache.getLatest(tmpl.fingerprint, tmpl.kind);
    if (existing) {
      tmpl = { ...tmpl, version: existing.version + 1, template_id: randomUUID() };
    }
    tmpl.updated_at = new Date().toISOString();
    this.cache.add(tmpl);
    try {
      await this.repository.save(tmpl);
    } catch (e) {
      console.error("firestore_save_error", { template_id: tmpl.template_id, error: String(e) });
    }
    return tmpl;
  }

    /**
   * Performs the increment match count operation.
   * @param templateId - The template id
   * @param fingerprint - The fingerprint
   */
  incrementMatchCount(templateId: string, fingerprint: string): void {
    const t = this.cache.find(templateId, fingerprint);
    if (t) {
      t.match_count += 1;
      t.updated_at = new Date().toISOString();
      this.repository.updateMatchCount(templateId, t.match_count, t.updated_at).catch(() => {});
    }
  }

    /**
   * Performs the list all operation.
   * @param kind - The kind
   * @returns The list of results
   */
  listAll(kind?: TemplateKind): Template[] {
    this.ensureWarmed();
    return this.cache.listAll(kind);
  }

    /**
   * Performs the warm cache operation.
   */
  async warmCache(): Promise<void> {
    if (this.cache.isWarmed()) return;
    if (this.warming) return this.warming;
    this.warming = this.loadCache();
    try {
      await this.warming;
    } finally {
      this.cache.setWarmed(true);
      this.warming = null;
    }
  }

    /**
   * Loads cache
   */
  private async loadCache(): Promise<void> {
    try {
      const templates = await this.repository.findAll();
      this.cache.reset(templates);
      console.log("template_cache_warmed", { count: this.cache.listAll().length });
    } catch (e) {
      console.warn("template_cache_warm_failed", { error: String(e) });
    }
  }

    /**
   * Ensures table exists
   */
  ensureTableExists(): void {
    // Firestore is schemaless — no setup needed
  }
}

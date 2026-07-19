import { randomUUID } from "crypto";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { Template, TemplateKind } from "@shared/models/template.js";
import { ITemplateRegistry } from "@service/ai_classifier/io/ITemplateRegistry.js";
import { TemplateCache } from "./TemplateCache.js";
import { FirestoreTemplateRepository } from "./FirestoreTemplateRepository.js";

export class TemplateRegistryService extends ServiceManager implements ITemplateRegistry {
  protected static instance: TemplateRegistryService;
  private readonly cache: TemplateCache;
  private readonly repository: FirestoreTemplateRepository;
  private warming: Promise<void> | null = null;

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

  static getInstance(): TemplateRegistryService {
    if (!TemplateRegistryService.instance) {
      TemplateRegistryService.instance = new TemplateRegistryService(Enforce);
    }
    return TemplateRegistryService.instance;
  }

  ensureWarmed(): void {
    if (!this.cache.isWarmed()) {
      this.warmCache().catch(() => {});
    }
  }

  getByFingerprint(fingerprint: string): Template[] {
    this.ensureWarmed();
    return this.cache.getByFingerprint(fingerprint);
  }

  getLatest(fingerprint: string, kind?: TemplateKind): Template | null {
    this.ensureWarmed();
    return this.cache.getLatest(fingerprint, kind);
  }

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

  incrementMatchCount(templateId: string, fingerprint: string): void {
    const t = this.cache.find(templateId, fingerprint);
    if (t) {
      t.match_count += 1;
      t.updated_at = new Date().toISOString();
      this.repository.updateMatchCount(templateId, t.match_count, t.updated_at).catch(() => {});
    }
  }

  listAll(kind?: TemplateKind): Template[] {
    this.ensureWarmed();
    return this.cache.listAll(kind);
  }

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

  private async loadCache(): Promise<void> {
    try {
      const templates = await this.repository.findAll();
      this.cache.reset(templates);
      console.log("template_cache_warmed", { count: this.cache.listAll().length });
    } catch (e) {
      console.warn("template_cache_warm_failed", { error: String(e) });
    }
  }

  ensureTableExists(): void {
    // Firestore is schemaless — no setup needed
  }
}

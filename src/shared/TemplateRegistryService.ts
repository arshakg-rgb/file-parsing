import crypto from "crypto";
import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import MySqlManager from "@config/db/MySqlManager.js";

export interface RecordTemplate {
  template_id: string;
  fingerprint: string;
  version: number;
  field_map: Record<string, { locator: string; type: string }>;
  structure: string;
  delimiter?: string; // for structure "csv": the field delimiter (defaults to "," when absent)
  length_hint: number;
  source: "ai" | "bootstrap" | "user";
  created_at: Date;
}

export interface RubbishTemplate {
  template_id: string;
  fingerprint: string;
  signature: string;
  confidence: number;
  version: number;
  source: "ai" | "bootstrap" | "user";
  created_at: Date;
}

export type Template = RecordTemplate | RubbishTemplate;
export type TemplateKind = "record" | "rubbish";


/**
 * TemplateRegistryService is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
export class TemplateRegistryService extends ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: TemplateRegistryService;
    /**
   * Record Cache
   * @private
   */
  private recordCache = new Map<string, RecordTemplate>();
    /**
   * Rubbish Cache
   * @private
   */
  private rubbishCache = new Map<string, RubbishTemplate>();
    /**
   * Match Rate History
   * @private
   */
  private matchRateHistory: number[] = [];
    /**
   * M A T C H_ R A T E_ W I N D O W
   * @private
   */
  private readonly MATCH_RATE_WINDOW = 1000;
    /**
   * M A T C H_ R A T E_ F L O O R
   * @private
   */
  private readonly MATCH_RATE_FLOOR = 0.1;
    /**
   * L O A D_ T T L_ M S
   * @private
   */
  private readonly LOAD_TTL_MS = 30000; // 30s
    /**
   * Last Loaded At
   * @private
   */
  private lastLoadedAt = 0;
    /**
   * Db Manager
   * @private
   */
  private dbManager: MySqlManager;

    /**
   * Constructs a new TemplateRegistryService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate TemplateRegistryService directly. Use getInstance()");
    }
    super(enforce);
    
    this.dbManager = MySqlManager.getInstance();
  }

    /**
   * Gets the single instance of the TemplateRegistryService class.
   * @returns The single instance of the class
   */
  public static getInstance(): TemplateRegistryService {
    if (!TemplateRegistryService.instance) {
      TemplateRegistryService.instance = new TemplateRegistryService(Enforce);
    }
    return TemplateRegistryService.instance;
  }

    /**
   * Performs the generate fingerprint operation.
   * @param line - The line to process
   * @returns The string result
   */
  static generateFingerprint(line: string): string {
    const normalized = line.trim().toLowerCase();
    return crypto.createHash("sha256").update(normalized).digest("hex");
  }

    /**
   * Performs the generate structure fingerprint operation.
   * @param fields - The fields
   * @returns The string result
   */
  static generateStructureFingerprint(fields: string[]): string {
    return crypto.createHash("sha256").update(fields.join(",")).digest("hex");
  }

    /**
   * Performs the passes length gate operation.
   * @param line - The line to process
   * @param fieldSpec - The field spec
   * @returns True if the operation succeeds, false otherwise
   */
  static passesLengthGate(line: string, fieldSpec: string[]): boolean {
    const lineLength = line.length;
    if (lineLength === 0) return false;
    
    const fieldCount = fieldSpec.length;
    const minExpectedLength = fieldCount * 2;
    const maxExpectedLength = fieldCount * 1000;
    
    return lineLength >= minExpectedLength && lineLength <= maxExpectedLength;
  }

    /**
   * Performs the match record template operation.
   * @param line - The line to process
   * @param fieldSpec - The field spec
   * @returns The record template | null result
   */
  matchRecordTemplate(line: string, fieldSpec: string[]): RecordTemplate | null {
    const fingerprint = TemplateRegistryService.generateFingerprint(line);
    const template = this.recordCache.get(fingerprint);
    
    if (template) {
      this.updateMatchRate(true);
      return template;
    }
    
    this.updateMatchRate(false);
    return null;
  }

    /**
   * Performs the match rubbish template operation.
   * @param line - The line to process
   * @returns The rubbish template | null result
   */
  matchRubbishTemplate(line: string): RubbishTemplate | null {
    const fingerprint = TemplateRegistryService.generateFingerprint(line);
    const template = this.rubbishCache.get(fingerprint);
    
    if (template && template.confidence > 0.9) {
      this.updateMatchRate(true);
      return template;
    }
    
    this.updateMatchRate(false);
    return null;
  }

    /**
   * Adds record template
   * @param template - The template
   */
  addRecordTemplate(template: RecordTemplate): void {
    this.recordCache.set(template.fingerprint, template);
  }

    /**
   * Adds rubbish template
   * @param template - The template
   */
  addRubbishTemplate(template: RubbishTemplate): void {
    this.rubbishCache.set(template.fingerprint, template);
  }

    /**
   * Updates match rate
   * @param matched - The matched
   */
  private updateMatchRate(matched: boolean): void {
    this.matchRateHistory.push(matched ? 1 : 0);
    if (this.matchRateHistory.length > this.MATCH_RATE_WINDOW) {
      this.matchRateHistory.shift();
    }
  }

    /**
   * Gets match rate
   * @returns The numeric result
   */
  getMatchRate(): number {
    if (this.matchRateHistory.length === 0) return 1.0;
    const sum = this.matchRateHistory.reduce((a, b) => a + b, 0);
    return sum / this.matchRateHistory.length;
  }

    /**
   * Checks whether match rate collapsed
   * @returns True if the condition is met, false otherwise
   */
  hasMatchRateCollapsed(): boolean {
    return this.getMatchRate() < this.MATCH_RATE_FLOOR;
  }

    /**
   * Gets by fingerprint
   * @param fingerprint - The fingerprint
   * @returns The template | null result
   */
  getByFingerprint(fingerprint: string): Template | null {
    const record = this.recordCache.get(fingerprint);
    if (record) return record;
    const rubbish = this.rubbishCache.get(fingerprint);
    if (rubbish) return rubbish;
    return null;
  }

    /**
   * Gets all record templates
   * @returns The list of results
   */
  getAllRecordTemplates(): RecordTemplate[] {
    return Array.from(this.recordCache.values());
  }

    /**
   * Gets all rubbish templates
   * @returns The list of results
   */
  getAllRubbishTemplates(): RubbishTemplate[] {
    return Array.from(this.rubbishCache.values());
  }

    /**
   * Loads from database
   */
  async loadFromDatabase(): Promise<void> {
    if (Date.now() - this.lastLoadedAt < this.LOAD_TTL_MS) {
      return;
    }
    try {
      const recordRows = await this.dbManager.repositories.templates.findByKind("record");
      
      for (const row of recordRows) {
        let fieldMap;
        if (typeof row.field_map === "string") {
          fieldMap = JSON.parse(row.field_map);
        } else if (typeof row.field_map === "object" && row.field_map !== null) {
          fieldMap = row.field_map;
        } else {
          fieldMap = {};
        }
        
        this.recordCache.set(row.fingerprint, {
          template_id: row.template_id,
          fingerprint: row.fingerprint,
          version: row.version,
          field_map: fieldMap,
          structure: row.structure as string,
          length_hint: row.length_hint as number,
          source: row.source as "ai" | "bootstrap" | "user",
          created_at: row.created_at as Date,
        });
      }

      const rubbishRows = await this.dbManager.repositories.templates.findByKind("rubbish");
      
      for (const row of rubbishRows) {
        this.rubbishCache.set(row.fingerprint, {
          template_id: row.template_id,
          fingerprint: row.fingerprint,
          signature: row.signature as string,
          confidence: Number(row.confidence),
          version: row.version,
          source: row.source as "ai" | "bootstrap" | "user",
          created_at: row.created_at as Date,
        });
      }
      this.lastLoadedAt = Date.now();
    } catch (error) {
      console.error("Failed to load templates from database:", error);
    }
  }

    /**
   * Saves template
   * @param template - The template
   * @param kind - The kind
   */
  async saveTemplate(template: Template, kind: TemplateKind): Promise<void> {
    try {
      await this.dbManager.repositories.templates.saveTemplate(template, kind);
    } catch (error) {
      console.error("Failed to save template to database:", error);
    }
  }
}

/**
 * The template registry
 */
export const templateRegistry = TemplateRegistryService.getInstance();

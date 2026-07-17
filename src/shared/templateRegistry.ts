import crypto from "crypto";
import Config from "../config/system-config/Config.js";
import ServiceManager from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import MySqlManager from "../config/db/MySqlManager.js";

export interface RecordTemplate {
  template_id: string;
  fingerprint: string;
  version: number;
  field_map: Record<string, { locator: string; type: string }>;
  structure: string;
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

function Enforce(): void {}

export class TemplateRegistryService extends ServiceManager {
  protected static instance: TemplateRegistryService;
  private recordCache = new Map<string, RecordTemplate>();
  private rubbishCache = new Map<string, RubbishTemplate>();
  private matchRateHistory: number[] = [];
  private readonly MATCH_RATE_WINDOW = 1000;
  private readonly MATCH_RATE_FLOOR = 0.1;
  private dbManager: MySqlManager;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate TemplateRegistryService directly. Use getInstance()");
    }
    super(enforce);
    
    this.dbManager = MySqlManager.getInstance();
  }

  public static getInstance(): TemplateRegistryService {
    if (!ServiceManager.instance) {
      ServiceManager.instance = new TemplateRegistryService(Enforce);
    }
    return ServiceManager.instance as TemplateRegistryService;
  }

  static generateFingerprint(line: string): string {
    const normalized = line.trim().toLowerCase();
    return crypto.createHash("sha256").update(normalized).digest("hex");
  }

  static generateStructureFingerprint(fields: string[]): string {
    return crypto.createHash("sha256").update(fields.join(",")).digest("hex");
  }

  static passesLengthGate(line: string, fieldSpec: string[]): boolean {
    const lineLength = line.length;
    if (lineLength === 0) return false;
    
    const fieldCount = fieldSpec.length;
    const minExpectedLength = fieldCount * 2;
    const maxExpectedLength = fieldCount * 1000;
    
    return lineLength >= minExpectedLength && lineLength <= maxExpectedLength;
  }

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

  addRecordTemplate(template: RecordTemplate): void {
    this.recordCache.set(template.fingerprint, template);
  }

  addRubbishTemplate(template: RubbishTemplate): void {
    this.rubbishCache.set(template.fingerprint, template);
  }

  private updateMatchRate(matched: boolean): void {
    this.matchRateHistory.push(matched ? 1 : 0);
    if (this.matchRateHistory.length > this.MATCH_RATE_WINDOW) {
      this.matchRateHistory.shift();
    }
  }

  getMatchRate(): number {
    if (this.matchRateHistory.length === 0) return 1.0;
    const sum = this.matchRateHistory.reduce((a, b) => a + b, 0);
    return sum / this.matchRateHistory.length;
  }

  hasMatchRateCollapsed(): boolean {
    return this.getMatchRate() < this.MATCH_RATE_FLOOR;
  }

  getByFingerprint(fingerprint: string): Template | null {
    const record = this.recordCache.get(fingerprint);
    if (record) return record;
    const rubbish = this.rubbishCache.get(fingerprint);
    if (rubbish) return rubbish;
    return null;
  }

  getAllRecordTemplates(): RecordTemplate[] {
    return Array.from(this.recordCache.values());
  }

  getAllRubbishTemplates(): RubbishTemplate[] {
    return Array.from(this.rubbishCache.values());
  }

  async loadFromDatabase(): Promise<void> {
    try {
      const recordResult = await this.dbManager.pool.query(
        "SELECT * FROM templates WHERE kind = 'record'"
      );
      
      for (const row of recordResult.rows) {
        let fieldMap;
        if (typeof row.field_map === 'string') {
          fieldMap = JSON.parse(row.field_map);
        } else if (typeof row.field_map === 'object' && row.field_map !== null) {
          fieldMap = row.field_map;
        } else {
          fieldMap = {};
        }
        
        this.recordCache.set(row.fingerprint, {
          template_id: row.template_id,
          fingerprint: row.fingerprint,
          version: row.version,
          field_map: fieldMap,
          structure: row.structure,
          length_hint: row.length_hint,
          source: row.source,
          created_at: row.created_at,
        });
      }

      const rubbishResult = await this.dbManager.pool.query(
        "SELECT * FROM templates WHERE kind = 'rubbish'"
      );
      
      for (const row of rubbishResult.rows) {
        this.rubbishCache.set(row.fingerprint, {
          template_id: row.template_id,
          fingerprint: row.fingerprint,
          signature: row.signature,
          confidence: row.confidence,
          version: row.version,
          source: row.source,
          created_at: row.created_at,
        });
      }
    } catch (error) {
      console.error("Failed to load templates from database:", error);
    }
  }

  async saveTemplate(template: Template, kind: TemplateKind): Promise<void> {
    try {
      if (kind === "record") {
        const recordTemplate = template as RecordTemplate;
        await this.dbManager.pool.query(
          `INSERT INTO templates (template_id, fingerprint, version, field_map, structure, length_hint, kind, source, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT (fingerprint) DO UPDATE SET
             version = EXCLUDED.version + 1,
             field_map = EXCLUDED.field_map,
             structure = EXCLUDED.structure,
             length_hint = EXCLUDED.length_hint`,
          [
            recordTemplate.template_id,
            recordTemplate.fingerprint,
            recordTemplate.version,
            JSON.stringify(recordTemplate.field_map),
            recordTemplate.structure,
            recordTemplate.length_hint,
            kind,
            recordTemplate.source,
            recordTemplate.created_at,
          ]
        );
      } else {
        const rubbishTemplate = template as RubbishTemplate;
        await this.dbManager.pool.query(
          `INSERT INTO templates (template_id, fingerprint, version, signature, confidence, kind, source, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
           ON CONFLICT (fingerprint) DO UPDATE SET
             version = EXCLUDED.version + 1,
             signature = EXCLUDED.signature,
             confidence = EXCLUDED.confidence`,
          [
            rubbishTemplate.template_id,
            rubbishTemplate.fingerprint,
            rubbishTemplate.version,
            rubbishTemplate.signature,
            rubbishTemplate.confidence,
            kind,
            rubbishTemplate.source,
            rubbishTemplate.created_at,
          ]
        );
      }
    } catch (error) {
      console.error("Failed to save template to database:", error);
    }
  }
}

export const templateRegistry = TemplateRegistryService.getInstance();

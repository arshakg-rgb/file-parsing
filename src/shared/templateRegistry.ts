import crypto from "crypto";
import { pool } from "./db.js";

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

export class TemplateRegistry {
  private recordCache = new Map<string, RecordTemplate>();
  private rubbishCache = new Map<string, RubbishTemplate>();
  private matchRateHistory: number[] = [];
  private readonly MATCH_RATE_WINDOW = 1000;
  private readonly MATCH_RATE_FLOOR = 0.1;

  /**
   * Generate fingerprint from line content
   */
  static generateFingerprint(line: string): string {
    // Normalize line for fingerprinting
    const normalized = line.trim().toLowerCase();
    return crypto.createHash("sha256").update(normalized).digest("hex");
  }

  /**
   * Generate fingerprint from field structure
   */
  static generateStructureFingerprint(fields: string[]): string {
    return crypto.createHash("sha256").update(fields.join(",")).digest("hex");
  }

  /**
   * Check if line matches length gate
   */
  static passesLengthGate(line: string, fieldSpec: string[]): boolean {
    const lineLength = line.length;
    if (lineLength === 0) return false;
    
    // Basic length validation - line should be reasonable for the field count
    const fieldCount = fieldSpec.length;
    const minExpectedLength = fieldCount * 2; // Minimum 2 chars per field
    const maxExpectedLength = fieldCount * 1000; // Maximum 1000 chars per field
    
    return lineLength >= minExpectedLength && lineLength <= maxExpectedLength;
  }

  /**
   * Try to match against record templates
   */
  matchRecordTemplate(line: string, fieldSpec: string[]): RecordTemplate | null {
    const fingerprint = TemplateRegistry.generateFingerprint(line);
    const template = this.recordCache.get(fingerprint);
    
    if (template) {
      this.updateMatchRate(true);
      return template;
    }
    
    this.updateMatchRate(false);
    return null;
  }

  /**
   * Try to match against rubbish templates (high confidence only)
   */
  matchRubbishTemplate(line: string): RubbishTemplate | null {
    const fingerprint = TemplateRegistry.generateFingerprint(line);
    const template = this.rubbishCache.get(fingerprint);
    
    // Only return high-confidence rubbish matches
    if (template && template.confidence > 0.9) {
      this.updateMatchRate(true);
      return template;
    }
    
    this.updateMatchRate(false);
    return null;
  }

  /**
   * Add record template to cache
   */
  addRecordTemplate(template: RecordTemplate): void {
    this.recordCache.set(template.fingerprint, template);
  }

  /**
   * Add rubbish template to cache
   */
  addRubbishTemplate(template: RubbishTemplate): void {
    this.rubbishCache.set(template.fingerprint, template);
  }

  /**
   * Update match rate for AI cost bounding
   */
  private updateMatchRate(matched: boolean): void {
    this.matchRateHistory.push(matched ? 1 : 0);
    if (this.matchRateHistory.length > this.MATCH_RATE_WINDOW) {
      this.matchRateHistory.shift();
    }
  }

  /**
   * Get current match rate
   */
  getMatchRate(): number {
    if (this.matchRateHistory.length === 0) return 1.0;
    const sum = this.matchRateHistory.reduce((a, b) => a + b, 0);
    return sum / this.matchRateHistory.length;
  }

  /**
   * Check if match rate has collapsed (AI cost bounding)
   */
  hasMatchRateCollapsed(): boolean {
    return this.getMatchRate() < this.MATCH_RATE_FLOOR;
  }

  /**
   * Get template by fingerprint
   */
  getByFingerprint(fingerprint: string): Template | null {
    const record = this.recordCache.get(fingerprint);
    if (record) return record;
    const rubbish = this.rubbishCache.get(fingerprint);
    if (rubbish) return rubbish;
    return null;
  }

  /**
   * Get all record templates
   */
  getAllRecordTemplates(): RecordTemplate[] {
    return Array.from(this.recordCache.values());
  }

  /**
   * Get all rubbish templates
   */
  getAllRubbishTemplates(): RubbishTemplate[] {
    return Array.from(this.rubbishCache.values());
  }

  /**
   * Load templates from database
   */
  async loadFromDatabase(): Promise<void> {
    try {
      // Load record templates
      const recordResult = await pool.query(
        "SELECT * FROM templates WHERE kind = 'record'"
      );
      
      for (const row of recordResult.rows) {
        // Handle field_map - check if it's already an object or needs parsing
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

      // Load rubbish templates
      const rubbishResult = await pool.query(
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

  /**
   * Save template to database
   */
  async saveTemplate(template: Template, kind: TemplateKind): Promise<void> {
    try {
      if (kind === "record") {
        const recordTemplate = template as RecordTemplate;
        await pool.query(
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
        await pool.query(
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

// Global registry instance
export const templateRegistry = new TemplateRegistry();

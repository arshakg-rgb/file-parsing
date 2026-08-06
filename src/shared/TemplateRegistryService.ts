import crypto from "crypto";
import ServiceManager from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { DatabaseManager } from "@shared/DatabaseManager.js";
import { RecordTemplate, RubbishTemplate } from "./io/ITemplateRegistryService";
import { TemplateAttributes} from "@config/db/models";
import {createLogger} from "@utils/logger/Log";
import {PersistKind} from "@service/ai-classifier/io/IClassifierStats";
const logger = createLogger(module);

/**
 * TemplateRegistryService is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */

export class TemplateRegistryService extends ServiceManager
{
    /**
   * Singleton instance
   * @private
   */

  protected static instance: TemplateRegistryService;

    /**
   * Record Cache
   * @private
   */

  private recordCache: Map<string, RecordTemplate> = new Map<string, RecordTemplate>();

    /**
   * Rubbish Cache
   * @private
   */

  private rubbishCache: Map<string, RubbishTemplate> = new Map<string, RubbishTemplate>();

    /**
   * Match Rate History
   * @private
   */

  private matchRateHistory: number[] = [];

    /**
   * M A T C H_ R A T E_ W I N D O W
   * @private
   */

  private readonly MATCH_RATE_WINDOW: number = 1000;

    /**
   * LOAD TTL MS
   * @private
   */

  private readonly LOAD_TTL_MS: number = 30000;

    /**
   * Last Loaded At
   * @private
   */

  private lastLoadedAt: number = 0;

    /**
   * Db Manager
   * @private
   */

  private dbManager: DatabaseManager;

    /**
   * Constructs a new TemplateRegistryService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */

  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE,"Cannot instantiate TemplateRegistryService directly. Use getInstance()");
    }

    super(enforce);

    this.dbManager = DatabaseManager.getInstance();
  }

    /**
   * Gets the single instance of the TemplateRegistryService class.
   * @returns The single instance of the class
   */

  public static getInstance(): TemplateRegistryService
  {
    if (!TemplateRegistryService.instance)
    {
      TemplateRegistryService.instance = new TemplateRegistryService(Enforce);
    }

    return TemplateRegistryService.instance;
  }

    /**
   * Performs the generate fingerprint operation.
   * @param line - The line to process
   * @returns The string result
   */

  static generateFingerprint(line: string): string
  {
    const normalized: string = line.trim().toLowerCase();
    return crypto.createHash("sha256").update(normalized).digest("hex");
  }

    /**
   * Performs the generate structure fingerprint operation.
   * @param fields - The fields
   * @returns The string result
   */
  static generateStructureFingerprint(fields: string[]): string
  {
    return crypto.createHash("sha256").update(fields.join(",")).digest("hex");
  }

    /**
   * Performs the passes length gate operation.
   * @param line - The line to process
   * @param fieldSpec - The field spec
   * @returns True if the operation succeeds, false otherwise
   */

  static passesLengthGate(line: string, fieldSpec: string[]): boolean
  {
    const lineLength: number = line.length;

    if (lineLength === 0)
    {
      return false;
    }

    const fieldCount: number = fieldSpec.length;
    const minExpectedLength: number = fieldCount * 2;
    const maxExpectedLength: number = fieldCount * 1000;

    return lineLength >= minExpectedLength && lineLength <= maxExpectedLength;
  }

    /**
   * Performs the match record template operation.
   * @param line - The line to process
   * @returns The record template | null result
   */
  public matchRecordTemplate(line: string): RecordTemplate | null
  {
    const fingerprint: string = TemplateRegistryService.generateFingerprint(line);
    const template: RecordTemplate = this.recordCache.get(fingerprint);

    if (template)
    {
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

  public matchRubbishTemplate(line: string): RubbishTemplate | null
  {
    const fingerprint: string = TemplateRegistryService.generateFingerprint(line);
    const template: RubbishTemplate = this.rubbishCache.get(fingerprint);

    if (template && template.confidence > 0.9)
    {
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

  public addRecordTemplate(template: RecordTemplate): void
  {
    this.recordCache.set(template.fingerprint, template);
  }

    /**
   * Adds rubbish template
   * @param template - The template
   */

  public addRubbishTemplate(template: RubbishTemplate): void
  {
    this.rubbishCache.set(template.fingerprint, template);
  }

    /**
   * Updates match rate
   * @param matched - The matched
   */

  private updateMatchRate(matched: boolean): void
  {
    this.matchRateHistory.push(matched ? 1 : 0);

    if (this.matchRateHistory.length > this.MATCH_RATE_WINDOW)
    {
      this.matchRateHistory.shift();
    }
  }


    /**
   * Gets by fingerprint
   * @param fingerprint - The fingerprint
   * @returns The template | null result
   */

  public getByFingerprint(fingerprint: string): RubbishTemplate | RecordTemplate
  {
    const record: RecordTemplate = this.recordCache.get(fingerprint);

    if (record)
    {
      return record;
    }

    const rubbish: RubbishTemplate = this.rubbishCache.get(fingerprint);

    if (rubbish)
    {
      return rubbish;
    }

    return null;
  }

    /**
   * Gets all record templates
   * @returns The list of results
   */

  public getAllRecordTemplates(): RecordTemplate[]
  {
    return Array.from(this.recordCache.values());
  }

    /**
   * Gets all rubbish templates
   * @returns The list of results
   */

  public getAllRubbishTemplates(): RubbishTemplate[]
  {
    return Array.from(this.rubbishCache.values());
  }

    /**
   * Loads from database
   */

  public async loadFromDatabase(): Promise<void>
  {
    if (Date.now() - this.lastLoadedAt < this.LOAD_TTL_MS)
    {
      return;
    }

    try
    {
      const recordRows: TemplateAttributes[] = await this.dbManager.repositories.templates.findByKind("record");

      for (const row of recordRows)
      {
        let fieldMap;

        if (typeof row.field_map === "string")
        {
          fieldMap = JSON.parse(row.field_map);
        }
        else if (typeof row.field_map === "object" && row.field_map !== null)
        {
          fieldMap = row.field_map;
        }
        else
        {
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

      const rubbishRows: TemplateAttributes[] = await this.dbManager.repositories.templates.findByKind("rubbish");

      for (const row of rubbishRows)
      {
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
    }
    catch (error)
    {
      logger.error("Failed to load templates from database:", error);
    }
  }

    /**
   * Saves template
   * @param template - The template
   * @param kind - The kind
   */

  public async saveTemplate(template: RecordTemplate | RubbishTemplate, kind: PersistKind): Promise<void>
  {
    try
    {
      await this.dbManager.repositories.templates.saveTemplate(template, kind);
    }
    catch (error)
    {
      logger.error("Failed to save template to database:", error);
    }
  }
}

/**
 * The template registry
 */
export const templateRegistry = TemplateRegistryService.getInstance();

function Enforce(): void {}

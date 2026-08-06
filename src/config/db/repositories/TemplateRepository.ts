import { BigQueryManager, toDate } from "../BigQueryManager.js";
import { settings } from "@shared/Settings.js";
import type {
  TemplateAttributes,
  TemplateCreationAttributes,
} from "../models/Template.js";
import type { RecordTemplate, RubbishTemplate } from "@shared/io/ITemplateRegistryService";
import type { FieldLocator } from "@shared/models/template.js";

const TABLE = "templates";
const FULL_TABLE = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${TABLE}\``;

/**
 * BigQuery-backed repository for templates.
 */
export class TemplateRepository
{
  constructor() {}

  private fromRow(row: Record<string, unknown>): TemplateAttributes
  {
    return {
      template_id: row.template_id as string,
      fingerprint: row.fingerprint as string,
      version: Number(row.version ?? 1),
      kind: row.kind as string,
      field_map: (row.field_map == null ? null : typeof row.field_map === "string" ? JSON.parse(row.field_map) : row.field_map) as Record<string, FieldLocator> | null,
      structure: (row.structure as string | null) ?? null,
      length_hint: (row.length_hint as number | null) ?? null,
      signature: (row.signature as string | null) ?? null,
      confidence: (row.confidence as number | null) ?? null,
      source: row.source as string,
      created_at: toDate(row.created_at),
    };
  }

  /**
   * Finds all templates of a kind.
   */
  async findByKind(kind: string): Promise<TemplateAttributes[]>
  {
    const rows = await BigQueryManager.getInstance().queryMany<Record<string, unknown>>(
      TABLE,
      { kind },
      { column: "created_at", direction: "DESC" }
    );

    return rows.map((r) => this.fromRow(r));
  }

  /**
   * Finds a template by its fingerprint.
   */
  async findByFingerprint(fingerprint: string): Promise<TemplateAttributes | null>
  {
    const row = await BigQueryManager.getInstance().queryOne<Record<string, unknown>>(TABLE, { fingerprint });
    return row ? this.fromRow(row) : null;
  }

  /**
   * Creates or updates a template.
   */
  async saveTemplate(template: RecordTemplate | RubbishTemplate, kind: "record" | "rubbish"): Promise<void>
  {
    const existing = await this.findByFingerprint(template.fingerprint);
    const now = new Date();

    const base = {
      fingerprint: template.fingerprint,
      version: (existing?.version || template.version || 0) + 1,
      kind,
      source: template.source,
    };

    let data: Record<string, unknown>;

    if (kind === "record")
    {
      const rt = template as RecordTemplate;
      data = {
        ...base,
        template_id: existing ? existing.template_id : rt.template_id,
        field_map: rt.field_map ?? {},
        structure: rt.structure ?? null,
        length_hint: rt.length_hint ?? null,
        signature: null,
        confidence: null,
      };
    }
    else
    {
      const rt = template as RubbishTemplate;
      data = {
        ...base,
        template_id: existing ? existing.template_id : rt.template_id,
        field_map: null,
        structure: null,
        length_hint: null,
        signature: rt.signature ?? null,
        confidence: rt.confidence ?? null,
      };
    }

    if (existing)
    {
      await BigQueryManager.getInstance().execute(
        `UPDATE ${FULL_TABLE} SET
          template_id = @template_id,
          version = @version,
          kind = @kind,
          field_map = @field_map,
          structure = @structure,
          length_hint = @length_hint,
          signature = @signature,
          confidence = @confidence,
          source = @source
        WHERE fingerprint = @fingerprint`,
        data,
        await BigQueryManager.getInstance().inferTypes(data, TABLE)
      );
    }
    else
    {
      const insertParams = { ...data, created_at: now };

      await BigQueryManager.getInstance().insertOne(TABLE, insertParams);
    }
  }
}

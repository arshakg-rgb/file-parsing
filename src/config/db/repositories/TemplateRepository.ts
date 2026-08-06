import { BigQueryManager, paramTypes } from "../BigQueryManager.js";
import { settings } from "@shared/Settings.js";
import type {
  TemplateAttributes,
  TemplateCreationAttributes,
} from "../models/Template.js";
import type { RecordTemplate, RubbishTemplate } from "@shared/io/ITemplateRegistryService";
import type { FieldLocator } from "@shared/models/template.js";

const TABLE = "templates";
const FULL_TABLE = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${TABLE}\``;

const NULLABLE_TYPES: Record<string, string> = {
  field_map: "STRING",
  structure: "STRING",
  length_hint: "INT64",
  signature: "STRING",
  confidence: "FLOAT64",
};

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
      created_at: new Date(row.created_at as string),
    };
  }

  /**
   * Finds all templates of a kind.
   */
  async findByKind(kind: string): Promise<TemplateAttributes[]>
  {
    const rows = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE} WHERE kind = @kind ORDER BY created_at DESC`,
      { kind }
    );

    return rows.map((r) => this.fromRow(r));
  }

  /**
   * Finds a template by its fingerprint.
   */
  async findByFingerprint(fingerprint: string): Promise<TemplateAttributes | null>
  {
    const [row] = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE} WHERE fingerprint = @fingerprint LIMIT 1`,
      { fingerprint }
    );

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
        { ...paramTypes(data, NULLABLE_TYPES), field_map: "JSON" }
      );
    }
    else
    {
      const insertParams = { ...data, created_at: now };

      await BigQueryManager.getInstance().execute(
        `INSERT INTO ${FULL_TABLE} (
          template_id, fingerprint, version, kind, field_map, structure,
          length_hint, signature, confidence, source, created_at
        ) VALUES (
          @template_id, @fingerprint, @version, @kind, @field_map, @structure,
          @length_hint, @signature, @confidence, @source, @created_at
        )`,
        insertParams,
        { ...paramTypes(insertParams, NULLABLE_TYPES), field_map: "JSON" }
      );
    }
  }
}

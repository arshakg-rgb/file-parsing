import { DataTypes, Model, Optional } from "sequelize";
import type { Sequelize } from "sequelize";

export interface TemplateAttributes {
  template_id: string;
  fingerprint: string;
  version: number;
  kind: string;
  field_map?: any;
  structure?: string | null;
  length_hint?: number | null;
  signature?: string | null;
  confidence?: number | null;
  source: string;
  created_at?: Date;
}

export type TemplateCreationAttributes = Optional<TemplateAttributes, "version" | "created_at">;

export class Template extends Model<TemplateAttributes, TemplateCreationAttributes> implements TemplateAttributes {
  declare template_id: string;
  declare fingerprint: string;
  declare version: number;
  declare kind: string;
  declare field_map: any;
  declare structure: string | null;
  declare length_hint: number | null;
  declare signature: string | null;
  declare confidence: number | null;
  declare source: string;
  declare created_at: Date;
}

export function initTemplateModel(sequelize: Sequelize): typeof Template {
  Template.init(
    {
      template_id: { type: DataTypes.STRING(36), primaryKey: true },
      fingerprint: { type: DataTypes.STRING(64), allowNull: false, unique: true },
      version: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 1 },
      kind: { type: DataTypes.STRING(16), allowNull: false },
      field_map: { type: DataTypes.JSONB, allowNull: true },
      structure: { type: DataTypes.TEXT, allowNull: true },
      length_hint: { type: DataTypes.INTEGER, allowNull: true },
      signature: { type: DataTypes.TEXT, allowNull: true },
      confidence: { type: DataTypes.DECIMAL, allowNull: true },
      source: { type: DataTypes.STRING(16), allowNull: false },
      created_at: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW },
    },
    {
      sequelize,
      tableName: "templates",
      timestamps: false,
      indexes: [{ fields: ["kind"] }, { fields: ["fingerprint"] }],
    }
  );
  return Template;
}

import {
  Table,
  Column,
  DataType,
  Model,
  PrimaryKey,
} from "sequelize-typescript";
import type { FieldLocator } from "../../../shared/models/template.js";

export interface ITemplate {
  template_id: string;
  fingerprint: string;
  version: number;
  kind: string;
  field_map?: Record<string, FieldLocator> | null;
  structure?: string | null;
  length_hint?: number | null;
  signature?: string | null;
  confidence?: number | null;
  source: string;
  created_at?: Date;
}

export type TemplateAttributes = ITemplate;

export interface TemplateCreationAttributes extends Omit<
  ITemplate,
  "created_at"
> {}

@Table({
  tableName: "templates",
  timestamps: false,
  indexes: [{ fields: ["kind"] }, { fields: ["fingerprint"] }],
})
export default class Template extends Model<ITemplate, TemplateCreationAttributes> {
  @PrimaryKey
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare template_id: string;

  @Column({ type: DataType.STRING(64), allowNull: false, unique: true })
  declare fingerprint: string;

  @Column({ type: DataType.INTEGER, allowNull: false, defaultValue: 1 })
  declare version: number;

  @Column({ type: DataType.STRING(16), allowNull: false })
  declare kind: string;

  @Column({ type: DataType.JSONB, allowNull: true })
  declare field_map: Record<string, FieldLocator> | null;

  @Column({ type: DataType.TEXT, allowNull: true })
  declare structure: string | null;

  @Column({ type: DataType.INTEGER, allowNull: true })
  declare length_hint: number | null;

  @Column({ type: DataType.TEXT, allowNull: true })
  declare signature: string | null;

  @Column({ type: DataType.DECIMAL, allowNull: true })
  declare confidence: number | null;

  @Column({ type: DataType.STRING(16), allowNull: false })
  declare source: string;

  @Column({
    type: DataType.DATE,
    allowNull: false,
    defaultValue: DataType.NOW,
    get() {
      return this.getDataValue("created_at");
    },
  })
  declare created_at: Date;
}

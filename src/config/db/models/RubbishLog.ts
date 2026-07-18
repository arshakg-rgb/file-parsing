import { DataTypes, Model, Optional } from "sequelize";
import type { Sequelize } from "sequelize";

export interface RubbishLogAttributes {
  id?: number;
  job_id: string;
  byte_offset: number;
  line_no: number;
  raw_bytes: string;
  matched_template_id: string;
  logged_at?: Date;
}

export type RubbishLogCreationAttributes = Optional<RubbishLogAttributes, "id" | "logged_at">;

export class RubbishLog extends Model<RubbishLogAttributes, RubbishLogCreationAttributes> implements RubbishLogAttributes {
  declare id: number;
  declare job_id: string;
  declare byte_offset: number;
  declare line_no: number;
  declare raw_bytes: string;
  declare matched_template_id: string;
  declare logged_at: Date;
}

export function initRubbishLogModel(sequelize: Sequelize): typeof RubbishLog {
  RubbishLog.init(
    {
      id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: true },
      job_id: { type: DataTypes.STRING(36), allowNull: false },
      byte_offset: { type: DataTypes.BIGINT, allowNull: false },
      line_no: { type: DataTypes.BIGINT, allowNull: false },
      raw_bytes: { type: DataTypes.TEXT, allowNull: false },
      matched_template_id: { type: DataTypes.STRING(36), allowNull: false },
      logged_at: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW },
    },
    {
      sequelize,
      tableName: "rubbish_log",
      timestamps: false,
      indexes: [{ fields: ["job_id", "byte_offset"] }],
    }
  );
  return RubbishLog;
}

import { DataTypes, Model, Optional } from "sequelize";
import type { Sequelize } from "sequelize";

export interface ParseJobAttributes {
  job_id: string;
  batch_id?: string | null;
  parent_job_id?: string | null;
  source_type: string;
  source_ref: string;
  s3_url?: string | null;
  size?: number | null;
  field_spec: any;
  exec_path: string;
  status: string;
  output_paths: any;
  counts: any;
  timings: any;
  error?: string | null;
  created_at?: Date;
  updated_at?: Date;
}

export type ParseJobCreationAttributes = Optional<ParseJobAttributes, "created_at" | "updated_at">;

export class ParseJob extends Model<ParseJobAttributes, ParseJobCreationAttributes> implements ParseJobAttributes {
  declare job_id: string;
  declare batch_id: string | null;
  declare parent_job_id: string | null;
  declare source_type: string;
  declare source_ref: string;
  declare s3_url: string | null;
  declare size: number | null;
  declare field_spec: any;
  declare exec_path: string;
  declare status: string;
  declare output_paths: any;
  declare counts: any;
  declare timings: any;
  declare error: string | null;
  declare created_at: Date;
  declare updated_at: Date;
}

export function initParseJobModel(sequelize: Sequelize): typeof ParseJob {
  ParseJob.init(
    {
      job_id: { type: DataTypes.STRING(36), primaryKey: true },
      batch_id: { type: DataTypes.STRING(36), allowNull: true },
      parent_job_id: { type: DataTypes.STRING(36), allowNull: true },
      source_type: { type: DataTypes.STRING(32), allowNull: false },
      source_ref: { type: DataTypes.TEXT, allowNull: false },
      s3_url: { type: DataTypes.TEXT, allowNull: true },
      size: { type: DataTypes.BIGINT, allowNull: true },
      field_spec: { type: DataTypes.JSONB, allowNull: false, defaultValue: [] },
      exec_path: { type: DataTypes.STRING(16), allowNull: false, defaultValue: "stream" },
      status: { type: DataTypes.STRING(32), allowNull: false, defaultValue: "queued" },
      output_paths: { type: DataTypes.JSONB, allowNull: false, defaultValue: [] },
      counts: { type: DataTypes.JSONB, allowNull: false, defaultValue: {} },
      timings: { type: DataTypes.JSONB, allowNull: false, defaultValue: {} },
      error: { type: DataTypes.TEXT, allowNull: true },
      created_at: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW },
      updated_at: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW },
    },
    {
      sequelize,
      tableName: "parse_jobs",
      timestamps: false,
      indexes: [{ fields: ["batch_id"] }, { fields: ["status"] }],
    }
  );
  return ParseJob;
}

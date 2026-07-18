import {
  Table,
  Column,
  DataType,
  Model,
  PrimaryKey,
} from "sequelize-typescript";
import type { JobCounts, JobTimings } from "../../../shared/models/job.js";

export interface IParseJob {
  job_id: string;
  batch_id?: string | null;
  parent_job_id?: string | null;
  source_type: string;
  source_ref: string;
  s3_url?: string | null;
  size?: number | null;
  field_spec: string[];
  exec_path: string;
  status: string;
  output_paths: string[];
  counts: JobCounts;
  timings: JobTimings;
  error?: string | null;
  created_at?: Date;
  updated_at?: Date;
}

export type ParseJobAttributes = IParseJob;

export interface ParseJobCreationAttributes extends Omit<
  IParseJob,
  "created_at" | "updated_at"
> {}

@Table({
  tableName: "parse_jobs",
  timestamps: false,
  indexes: [{ fields: ["batch_id"] }, { fields: ["status"] }],
})
export default class ParseJob extends Model<IParseJob, ParseJobCreationAttributes> {
  @PrimaryKey
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare job_id: string;

  @Column({ type: DataType.STRING(36), allowNull: true })
  declare batch_id: string | null;

  @Column({ type: DataType.STRING(36), allowNull: true })
  declare parent_job_id: string | null;

  @Column({ type: DataType.STRING(32), allowNull: false })
  declare source_type: string;

  @Column({ type: DataType.TEXT, allowNull: false })
  declare source_ref: string;

  @Column({ type: DataType.TEXT, allowNull: true })
  declare s3_url: string | null;

  @Column({ type: DataType.BIGINT, allowNull: true })
  declare size: number | null;

  @Column({ type: DataType.JSONB, allowNull: false, defaultValue: [] })
  declare field_spec: string[];

  @Column({ type: DataType.STRING(16), allowNull: false, defaultValue: "stream" })
  declare exec_path: string;

  @Column({ type: DataType.STRING(32), allowNull: false, defaultValue: "queued" })
  declare status: string;

  @Column({ type: DataType.JSONB, allowNull: false, defaultValue: [] })
  declare output_paths: string[];

  @Column({ type: DataType.JSONB, allowNull: false, defaultValue: {} })
  declare counts: JobCounts;

  @Column({ type: DataType.JSONB, allowNull: false, defaultValue: {} })
  declare timings: JobTimings;

  @Column({ type: DataType.TEXT, allowNull: true })
  declare error: string | null;

  @Column({
    type: DataType.DATE,
    allowNull: false,
    defaultValue: DataType.NOW,
    get() {
      return this.getDataValue("created_at");
    },
  })
  declare created_at: Date;

  @Column({
    type: DataType.DATE,
    allowNull: false,
    defaultValue: DataType.NOW,
  })
  declare updated_at: Date;
}

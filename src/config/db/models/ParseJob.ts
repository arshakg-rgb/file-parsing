import {
  Table,
  Column,
  DataType,
  Model,
  PrimaryKey,
} from "sequelize-typescript";
import type { JobCounts, JobTimings } from "@shared/models/job.js";

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

/**
 * ParseJob is responsible for parse job operations.
 */
@Table({
  tableName: "parse_jobs",
  timestamps: false,
  indexes: [{ fields: ["batch_id"] }, { fields: ["status"] }],
})
export default class ParseJob extends Model<IParseJob, ParseJobCreationAttributes> {
    /**
   * Job_id
   */
  @PrimaryKey
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare job_id: string;

    /**
   * Batch_id
   */
  @Column({ type: DataType.STRING(36), allowNull: true })
  declare batch_id: string | null;

    /**
   * Parent_job_id
   */
  @Column({ type: DataType.STRING(36), allowNull: true })
  declare parent_job_id: string | null;

    /**
   * Source_type
   */
  @Column({ type: DataType.STRING(32), allowNull: false })
  declare source_type: string;

    /**
   * Source_ref
   */
  @Column({ type: DataType.TEXT, allowNull: false })
  declare source_ref: string;

    /**
   * S3_url
   */
  @Column({ type: DataType.TEXT, allowNull: true })
  declare s3_url: string | null;

    /**
   * Size
   */
  @Column({ type: DataType.BIGINT, allowNull: true })
  declare size: number | null;

    /**
   * Field_spec
   */
  @Column({ type: DataType.JSONB, allowNull: false, defaultValue: [] })
  declare field_spec: string[];

    /**
   * Exec_path
   */
  @Column({ type: DataType.STRING(16), allowNull: false, defaultValue: "stream" })
  declare exec_path: string;

    /**
   * Status
   */
  @Column({ type: DataType.STRING(32), allowNull: false, defaultValue: "queued" })
  declare status: string;

    /**
   * Output_paths
   */
  @Column({ type: DataType.JSONB, allowNull: false, defaultValue: [] })
  declare output_paths: string[];

    /**
   * Counts
   */
  @Column({ type: DataType.JSONB, allowNull: false, defaultValue: {} })
  declare counts: JobCounts;

    /**
   * Timings
   */
  @Column({ type: DataType.JSONB, allowNull: false, defaultValue: {} })
  declare timings: JobTimings;

    /**
   * Error
   */
  @Column({ type: DataType.TEXT, allowNull: true })
  declare error: string | null;

    /**
   * Created_at
   */
  @Column({
    type: DataType.DATE,
    allowNull: false,
    defaultValue: DataType.NOW,
    get() {
      return this.getDataValue("created_at");
    },
  })
  declare created_at: Date;

    /**
   * Updated_at
   */
  @Column({
    type: DataType.DATE,
    allowNull: false,
    defaultValue: DataType.NOW,
  })
  declare updated_at: Date;
}

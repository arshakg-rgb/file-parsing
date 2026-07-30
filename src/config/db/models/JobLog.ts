import {
  Table,
  Column,
  DataType,
  Model,
  PrimaryKey,
  AutoIncrement,
} from "sequelize-typescript";

export interface IJobLog {
  id?: number;
  job_id: string;
  event_type: string;
  stage?: string | null;
  template_id?: string | null;
  message?: string | null;
  metadata?: Record<string, unknown> | null;
  created_at?: Date;
}

export type JobLogAttributes = IJobLog;

export interface JobLogCreationAttributes extends Omit<
  IJobLog,
  "id" | "created_at"
> {}

/**
 * JobLog is an append-only audit trail of everything that happened to a job:
 * crashes (with the stage they occurred in), which template was matched,
 * how many lines were dropped/rubbish, how many went to the DLQ, etc.
 */
@Table({
  tableName: "job_logs",
  timestamps: false,
  indexes: [{ fields: ["job_id"] }, { fields: ["event_type"] }],
})
export default class JobLog extends Model<IJobLog, JobLogCreationAttributes> {
    /**
   * Id
   */
  @PrimaryKey
  @AutoIncrement
  @Column({ type: DataType.BIGINT, allowNull: false })
  declare id: number;

    /**
   * Job_id
   */
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare job_id: string;

    /**
   * Event_type - e.g. crashed, parsing_summary, template_used
   */
  @Column({ type: DataType.STRING(32), allowNull: false })
  declare event_type: string;

    /**
   * Stage - which pipeline stage the event occurred in (ingest, detect, parse, finalize, load, report)
   */
  @Column({ type: DataType.STRING(32), allowNull: true })
  declare stage: string | null;

    /**
   * Template_id - which record/rubbish template was involved, if any
   */
  @Column({ type: DataType.STRING(36), allowNull: true })
  declare template_id: string | null;

    /**
   * Message - human-readable summary (e.g. the error message)
   */
  @Column({ type: DataType.TEXT, allowNull: true })
  declare message: string | null;

    /**
   * Metadata - free-form details (dropped_count, dlq_count, error stack, etc.)
   */
  @Column({ type: DataType.JSONB, allowNull: true, defaultValue: {} })
  declare metadata: Record<string, unknown> | null;

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
}

import {
  Table,
  Column,
  DataType,
  Model,
  PrimaryKey,
} from "sequelize-typescript";

export interface IRubbishLog {
  id?: number;
  job_id: string;
  byte_offset: number;
  line_no: number;
  raw_bytes: string;
  matched_template_id: string;
  logged_at?: Date;
}

export type RubbishLogAttributes = IRubbishLog;

export interface RubbishLogCreationAttributes extends Omit<
  IRubbishLog,
  "id" | "logged_at"
> {}

/**
 * RubbishLog is responsible for rubbish log operations.
 */
@Table({
  tableName: "rubbish_log",
  timestamps: false,
  indexes: [{ fields: ["job_id", "byte_offset"] }],
})
export default class RubbishLog extends Model<IRubbishLog, RubbishLogCreationAttributes> {
    /**
   * Id
   */
  @PrimaryKey
  @Column({ type: DataType.BIGINT, autoIncrement: true, allowNull: false })
  declare id: number;

    /**
   * Job_id
   */
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare job_id: string;

    /**
   * Byte_offset
   */
  @Column({ type: DataType.BIGINT, allowNull: false })
  declare byte_offset: number;

    /**
   * Line_no
   */
  @Column({ type: DataType.BIGINT, allowNull: false })
  declare line_no: number;

    /**
   * Raw_bytes
   */
  @Column({ type: DataType.TEXT, allowNull: false })
  declare raw_bytes: string;

    /**
   * Matched_template_id
   */
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare matched_template_id: string;

    /**
   * Logged_at
   */
  @Column({
    type: DataType.DATE,
    allowNull: false,
    defaultValue: DataType.NOW,
    get() {
      return this.getDataValue("logged_at");
    },
  })
  declare logged_at: Date;
}

import {
  Table,
  Column,
  DataType,
  Model,
  PrimaryKey,
} from "sequelize-typescript";

export interface IDeadLetter {
  dlq_id: string;
  job_id: string;
  byte_offset: number;
  byte_length: number;
  line_no: number;
  raw_bytes: string;
  failure_class: string;
  error: string;
  attempts: number;
  status: string;
  created_at?: Date;
  updated_at?: Date;
}

export type DeadLetterAttributes = IDeadLetter;

export interface DeadLetterCreationAttributes extends Omit<
  IDeadLetter,
  "created_at" | "updated_at"
> {}

/**
 * DeadLetter is responsible for dead letter operations.
 */
@Table({
  tableName: "dead_letters",
  timestamps: false,
  indexes: [{ fields: ["job_id", "byte_offset"] }, { fields: ["status"] }],
})
export default class DeadLetter extends Model<IDeadLetter, DeadLetterCreationAttributes> {
    /**
   * Dlq_id
   */
  @PrimaryKey
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare dlq_id: string;

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
   * Byte_length
   */
  @Column({ type: DataType.INTEGER, allowNull: false })
  declare byte_length: number;

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
   * Failure_class
   */
  @Column({ type: DataType.STRING(32), allowNull: false })
  declare failure_class: string;

    /**
   * Error
   */
  @Column({ type: DataType.TEXT, allowNull: false })
  declare error: string;

    /**
   * Attempts
   */
  @Column({ type: DataType.INTEGER, allowNull: false, defaultValue: 0 })
  declare attempts: number;

    /**
   * Status
   */
  @Column({ type: DataType.STRING(16), allowNull: false, defaultValue: "pending" })
  declare status: string;

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

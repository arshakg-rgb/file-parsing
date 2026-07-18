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

@Table({
  tableName: "rubbish_log",
  timestamps: false,
  indexes: [{ fields: ["job_id", "byte_offset"] }],
})
export default class RubbishLog extends Model<IRubbishLog, RubbishLogCreationAttributes> {
  @PrimaryKey
  @Column({ type: DataType.BIGINT, autoIncrement: true, allowNull: false })
  declare id: number;

  @Column({ type: DataType.STRING(36), allowNull: false })
  declare job_id: string;

  @Column({ type: DataType.BIGINT, allowNull: false })
  declare byte_offset: number;

  @Column({ type: DataType.BIGINT, allowNull: false })
  declare line_no: number;

  @Column({ type: DataType.TEXT, allowNull: false })
  declare raw_bytes: string;

  @Column({ type: DataType.STRING(36), allowNull: false })
  declare matched_template_id: string;

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

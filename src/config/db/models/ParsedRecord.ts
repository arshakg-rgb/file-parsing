import {
  Table,
  Column,
  DataType,
  Model,
  PrimaryKey,
} from "sequelize-typescript";

export interface IParsedRecord {
  id?: number;
  _job_id: string;
  _byte_offset: number;
  _byte_length: number;
  _record_index: number;
  _line_no: number;
  _template_id: string;
  _template_version: number;
  _checksum: string;
  _parsed_at: Date;
  _part_id: string;
  fields: Record<string, unknown>;
}

export type ParsedRecordAttributes = IParsedRecord;

export interface ParsedRecordCreationAttributes extends Omit<
  IParsedRecord,
  "id"
> {}

@Table({
  tableName: "parsed_records",
  timestamps: false,
  indexes: [
    { fields: ["_job_id"] },
    { fields: ["_job_id", "_byte_offset"], unique: true },
    { fields: ["fields"], using: "gin" },
  ],
})
export default class ParsedRecord extends Model<IParsedRecord, ParsedRecordCreationAttributes> {
  @PrimaryKey
  @Column({ type: DataType.BIGINT, autoIncrement: true, allowNull: false })
  declare id: number;

  @Column({ type: DataType.STRING(36), allowNull: false })
  declare _job_id: string;

  @Column({ type: DataType.BIGINT, allowNull: false })
  declare _byte_offset: number;

  @Column({ type: DataType.INTEGER, allowNull: false })
  declare _byte_length: number;

  @Column({ type: DataType.INTEGER, allowNull: false })
  declare _record_index: number;

  @Column({ type: DataType.BIGINT, allowNull: false })
  declare _line_no: number;

  @Column({ type: DataType.STRING(36), allowNull: false })
  declare _template_id: string;

  @Column({ type: DataType.INTEGER, allowNull: false })
  declare _template_version: number;

  @Column({ type: DataType.STRING(64), allowNull: false })
  declare _checksum: string;

  @Column({ type: DataType.DATE, allowNull: false })
  declare _parsed_at: Date;

  @Column({ type: DataType.STRING(36), allowNull: false })
  declare _part_id: string;

  @Column({ type: DataType.JSONB, allowNull: false, defaultValue: {} })
  declare fields: Record<string, unknown>;
}

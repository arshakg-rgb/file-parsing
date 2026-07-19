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

/**
 * ParsedRecord is responsible for parsed record operations.
 */
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
    /**
   * Id
   */
  @PrimaryKey
  @Column({ type: DataType.BIGINT, autoIncrement: true, allowNull: false })
  declare id: number;

    /**
   * _job_id
   */
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare _job_id: string;

    /**
   * _byte_offset
   */
  @Column({ type: DataType.BIGINT, allowNull: false })
  declare _byte_offset: number;

    /**
   * _byte_length
   */
  @Column({ type: DataType.INTEGER, allowNull: false })
  declare _byte_length: number;

    /**
   * _record_index
   */
  @Column({ type: DataType.INTEGER, allowNull: false })
  declare _record_index: number;

    /**
   * _line_no
   */
  @Column({ type: DataType.BIGINT, allowNull: false })
  declare _line_no: number;

    /**
   * _template_id
   */
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare _template_id: string;

    /**
   * _template_version
   */
  @Column({ type: DataType.INTEGER, allowNull: false })
  declare _template_version: number;

    /**
   * _checksum
   */
  @Column({ type: DataType.STRING(64), allowNull: false })
  declare _checksum: string;

    /**
   * _parsed_at
   */
  @Column({ type: DataType.DATE, allowNull: false })
  declare _parsed_at: Date;

    /**
   * _part_id
   */
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare _part_id: string;

    /**
   * Fields
   */
  @Column({ type: DataType.JSONB, allowNull: false, defaultValue: {} })
  declare fields: Record<string, unknown>;
}

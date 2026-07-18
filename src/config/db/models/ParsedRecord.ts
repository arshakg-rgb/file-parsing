import { DataTypes, Model, Optional } from "sequelize";
import type { Sequelize } from "sequelize";

export interface ParsedRecordAttributes {
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
  fields: any;
}

export type ParsedRecordCreationAttributes = Optional<ParsedRecordAttributes, "id">;

export class ParsedRecord extends Model<ParsedRecordAttributes, ParsedRecordCreationAttributes> implements ParsedRecordAttributes {
  declare id: number;
  declare _job_id: string;
  declare _byte_offset: number;
  declare _byte_length: number;
  declare _record_index: number;
  declare _line_no: number;
  declare _template_id: string;
  declare _template_version: number;
  declare _checksum: string;
  declare _parsed_at: Date;
  declare _part_id: string;
  declare fields: any;
}

export function initParsedRecordModel(sequelize: Sequelize): typeof ParsedRecord {
  ParsedRecord.init(
    {
      id: { type: DataTypes.BIGINT, primaryKey: true, autoIncrement: true },
      _job_id: { type: DataTypes.STRING(36), allowNull: false },
      _byte_offset: { type: DataTypes.BIGINT, allowNull: false },
      _byte_length: { type: DataTypes.INTEGER, allowNull: false },
      _record_index: { type: DataTypes.INTEGER, allowNull: false },
      _line_no: { type: DataTypes.BIGINT, allowNull: false },
      _template_id: { type: DataTypes.STRING(36), allowNull: false },
      _template_version: { type: DataTypes.INTEGER, allowNull: false },
      _checksum: { type: DataTypes.STRING(64), allowNull: false },
      _parsed_at: { type: DataTypes.DATE, allowNull: false },
      _part_id: { type: DataTypes.STRING(36), allowNull: false },
      fields: { type: DataTypes.JSONB, allowNull: false, defaultValue: {} },
    },
    {
      sequelize,
      tableName: "parsed_records",
      timestamps: false,
      indexes: [{ fields: ["_job_id"] }, { fields: ["_job_id", "_byte_offset"], unique: true }, { fields: ["fields"], using: "gin" }],
    }
  );
  return ParsedRecord;
}

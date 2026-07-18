import { DataTypes, Model, Optional } from "sequelize";
import type { Sequelize } from "sequelize";

export interface OutputPartAttributes {
  part_id: string;
  job_id: string;
  template_id: string;
  s3_path: string;
  row_count: number;
  byte_size: number;
  created_at?: Date;
}

export type OutputPartCreationAttributes = Optional<OutputPartAttributes, "created_at">;

export class OutputPart extends Model<OutputPartAttributes, OutputPartCreationAttributes> implements OutputPartAttributes {
  declare part_id: string;
  declare job_id: string;
  declare template_id: string;
  declare s3_path: string;
  declare row_count: number;
  declare byte_size: number;
  declare created_at: Date;
}

export function initOutputPartModel(sequelize: Sequelize): typeof OutputPart {
  OutputPart.init(
    {
      part_id: { type: DataTypes.STRING(36), primaryKey: true },
      job_id: { type: DataTypes.STRING(36), allowNull: false },
      template_id: { type: DataTypes.STRING(36), allowNull: false },
      s3_path: { type: DataTypes.TEXT, allowNull: false },
      row_count: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
      byte_size: { type: DataTypes.BIGINT, allowNull: false, defaultValue: 0 },
      created_at: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW },
    },
    {
      sequelize,
      tableName: "output_parts",
      timestamps: false,
      indexes: [{ fields: ["job_id"] }],
    }
  );
  return OutputPart;
}

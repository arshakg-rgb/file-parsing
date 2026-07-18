import { DataTypes, Model, Optional } from "sequelize";
import type { Sequelize } from "sequelize";

export interface DeadLetterAttributes {
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

export type DeadLetterCreationAttributes = Optional<DeadLetterAttributes, "created_at" | "updated_at" | "attempts">;

export class DeadLetter extends Model<DeadLetterAttributes, DeadLetterCreationAttributes> implements DeadLetterAttributes {
  declare dlq_id: string;
  declare job_id: string;
  declare byte_offset: number;
  declare byte_length: number;
  declare line_no: number;
  declare raw_bytes: string;
  declare failure_class: string;
  declare error: string;
  declare attempts: number;
  declare status: string;
  declare created_at: Date;
  declare updated_at: Date;
}

export function initDeadLetterModel(sequelize: Sequelize): typeof DeadLetter {
  DeadLetter.init(
    {
      dlq_id: { type: DataTypes.STRING(36), primaryKey: true },
      job_id: { type: DataTypes.STRING(36), allowNull: false },
      byte_offset: { type: DataTypes.BIGINT, allowNull: false },
      byte_length: { type: DataTypes.INTEGER, allowNull: false },
      line_no: { type: DataTypes.BIGINT, allowNull: false },
      raw_bytes: { type: DataTypes.TEXT, allowNull: false },
      failure_class: { type: DataTypes.STRING(32), allowNull: false },
      error: { type: DataTypes.TEXT, allowNull: false },
      attempts: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
      status: { type: DataTypes.STRING(16), allowNull: false, defaultValue: "pending" },
      created_at: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW },
      updated_at: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW },
    },
    {
      sequelize,
      tableName: "dead_letters",
      timestamps: false,
      indexes: [{ fields: ["job_id", "byte_offset"] }, { fields: ["status"] }],
    }
  );
  return DeadLetter;
}

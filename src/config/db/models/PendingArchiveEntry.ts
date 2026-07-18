import { DataTypes, Model, Optional } from "sequelize";
import type { Sequelize } from "sequelize";

export interface PendingArchiveEntryAttributes {
  id: string;
  job_id: string;
  entry_name: string;
  entry_size: number;
  status: string;
  error?: string | null;
  created_at?: Date;
  updated_at?: Date;
}

export type PendingArchiveEntryCreationAttributes = Optional<PendingArchiveEntryAttributes, "created_at" | "updated_at">;

export class PendingArchiveEntry extends Model<PendingArchiveEntryAttributes, PendingArchiveEntryCreationAttributes> implements PendingArchiveEntryAttributes {
  declare id: string;
  declare job_id: string;
  declare entry_name: string;
  declare entry_size: number;
  declare status: string;
  declare error: string | null;
  declare created_at: Date;
  declare updated_at: Date;
}

export function initPendingArchiveEntryModel(sequelize: Sequelize): typeof PendingArchiveEntry {
  PendingArchiveEntry.init(
    {
      id: { type: DataTypes.STRING(36), primaryKey: true },
      job_id: { type: DataTypes.STRING(36), allowNull: false },
      entry_name: { type: DataTypes.TEXT, allowNull: false },
      entry_size: { type: DataTypes.BIGINT, allowNull: false },
      status: { type: DataTypes.STRING(16), allowNull: false, defaultValue: "pending" },
      error: { type: DataTypes.TEXT, allowNull: true },
      created_at: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW },
      updated_at: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW },
    },
    {
      sequelize,
      tableName: "pending_archive_entries",
      timestamps: false,
      indexes: [{ fields: ["job_id"] }, { fields: ["status"] }],
    }
  );
  return PendingArchiveEntry;
}

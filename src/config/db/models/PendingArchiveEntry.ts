import {
  Table,
  Column,
  DataType,
  Model,
  PrimaryKey,
} from "sequelize-typescript";

export interface IPendingArchiveEntry {
  id: string;
  job_id: string;
  entry_name: string;
  entry_size: number;
  status: string;
  error?: string | null;
  created_at?: Date;
  updated_at?: Date;
}

export type PendingArchiveEntryAttributes = IPendingArchiveEntry;

export interface PendingArchiveEntryCreationAttributes extends Omit<
  IPendingArchiveEntry,
  "created_at" | "updated_at"
> {}

@Table({
  tableName: "pending_archive_entries",
  timestamps: false,
  indexes: [{ fields: ["job_id"] }, { fields: ["status"] }],
})
export default class PendingArchiveEntry extends Model<IPendingArchiveEntry, PendingArchiveEntryCreationAttributes> {
  @PrimaryKey
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare id: string;

  @Column({ type: DataType.STRING(36), allowNull: false })
  declare job_id: string;

  @Column({ type: DataType.TEXT, allowNull: false })
  declare entry_name: string;

  @Column({ type: DataType.BIGINT, allowNull: false })
  declare entry_size: number;

  @Column({ type: DataType.STRING(16), allowNull: false, defaultValue: "pending" })
  declare status: string;

  @Column({ type: DataType.TEXT, allowNull: true })
  declare error: string | null;

  @Column({
    type: DataType.DATE,
    allowNull: false,
    defaultValue: DataType.NOW,
    get() {
      return this.getDataValue("created_at");
    },
  })
  declare created_at: Date;

  @Column({
    type: DataType.DATE,
    allowNull: false,
    defaultValue: DataType.NOW,
  })
  declare updated_at: Date;
}

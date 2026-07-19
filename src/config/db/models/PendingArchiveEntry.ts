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

/**
 * PendingArchiveEntry is responsible for pending archive entry operations.
 */
@Table({
  tableName: "pending_archive_entries",
  timestamps: false,
  indexes: [{ fields: ["job_id"] }, { fields: ["status"] }],
})
export default class PendingArchiveEntry extends Model<IPendingArchiveEntry, PendingArchiveEntryCreationAttributes> {
    /**
   * Id
   */
  @PrimaryKey
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare id: string;

    /**
   * Job_id
   */
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare job_id: string;

    /**
   * Entry_name
   */
  @Column({ type: DataType.TEXT, allowNull: false })
  declare entry_name: string;

    /**
   * Entry_size
   */
  @Column({ type: DataType.BIGINT, allowNull: false })
  declare entry_size: number;

    /**
   * Status
   */
  @Column({ type: DataType.STRING(16), allowNull: false, defaultValue: "pending" })
  declare status: string;

    /**
   * Error
   */
  @Column({ type: DataType.TEXT, allowNull: true })
  declare error: string | null;

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

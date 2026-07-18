import {
  Table,
  Column,
  DataType,
  Model,
  PrimaryKey,
} from "sequelize-typescript";

export interface ISchemaMigration {
  version: number;
  applied_at?: Date;
  description?: string | null;
}

export type SchemaMigrationAttributes = ISchemaMigration;

export interface SchemaMigrationCreationAttributes extends Omit<
  ISchemaMigration,
  "applied_at"
> {}

@Table({
  tableName: "schema_migrations",
  timestamps: false,
})
export default class SchemaMigration extends Model<ISchemaMigration, SchemaMigrationCreationAttributes> {
  @PrimaryKey
  @Column({ type: DataType.INTEGER, allowNull: false })
  declare version: number;

  @Column({
    type: DataType.DATE,
    allowNull: false,
    defaultValue: DataType.NOW,
    get() {
      return this.getDataValue("applied_at");
    },
  })
  declare applied_at: Date;

  @Column({ type: DataType.TEXT, allowNull: true })
  declare description: string | null;
}

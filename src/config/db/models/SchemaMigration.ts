import { DataTypes, Model, Optional } from "sequelize";
import type { Sequelize } from "sequelize";

export interface SchemaMigrationAttributes {
  version: number;
  applied_at?: Date;
  description?: string | null;
}

export type SchemaMigrationCreationAttributes = Optional<SchemaMigrationAttributes, "applied_at">;

export class SchemaMigration extends Model<SchemaMigrationAttributes, SchemaMigrationCreationAttributes> implements SchemaMigrationAttributes {
  declare version: number;
  declare applied_at: Date;
  declare description: string | null;
}

export function initSchemaMigrationModel(sequelize: Sequelize): typeof SchemaMigration {
  SchemaMigration.init(
    {
      version: { type: DataTypes.INTEGER, primaryKey: true },
      applied_at: { type: DataTypes.DATE, allowNull: false, defaultValue: DataTypes.NOW },
      description: { type: DataTypes.TEXT, allowNull: true },
    },
    {
      sequelize,
      tableName: "schema_migrations",
      timestamps: false,
    }
  );
  return SchemaMigration;
}

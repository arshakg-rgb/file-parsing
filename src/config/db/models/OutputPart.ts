import {
  Table,
  Column,
  DataType,
  Model,
  PrimaryKey,
} from "sequelize-typescript";

export interface IOutputPart {
  part_id: string;
  job_id: string;
  template_id: string;
  s3_path: string;
  row_count: number;
  byte_size: number;
  created_at?: Date;
}

export type OutputPartAttributes = IOutputPart;

export interface OutputPartCreationAttributes extends Omit<
  IOutputPart,
  "created_at"
> {}

/**
 * OutputPart is responsible for output part operations.
 */
@Table({
  tableName: "output_parts",
  timestamps: false,
  indexes: [{ fields: ["job_id"] }],
})
export default class OutputPart extends Model<IOutputPart, OutputPartCreationAttributes> {
    /**
   * Part_id
   */
  @PrimaryKey
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare part_id: string;

    /**
   * Job_id
   */
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare job_id: string;

    /**
   * Template_id
   */
  @Column({ type: DataType.STRING(36), allowNull: false })
  declare template_id: string;

    /**
   * S3_path
   */
  @Column({ type: DataType.TEXT, allowNull: false })
  declare s3_path: string;

    /**
   * Row_count
   */
  @Column({ type: DataType.INTEGER, allowNull: false, defaultValue: 0 })
  declare row_count: number;

    /**
   * Byte_size
   */
  @Column({ type: DataType.BIGINT, allowNull: false, defaultValue: 0 })
  declare byte_size: number;

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
}

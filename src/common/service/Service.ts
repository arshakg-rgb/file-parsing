import { FindOptions } from "sequelize";

/**
 * Base service interface.
 *
 * Domain services extend this interface and implement CRUD operations
 * plus any feature-specific business methods.
 */
export interface Service {
  create(body: unknown): Promise<unknown>;
  update(body: unknown): Promise<unknown>;
  delete(params: unknown): Promise<{ id: string; deleteCount?: number }>;
  fetchAll(params: unknown, options?: FindOptions): Promise<unknown>;
  fetchById(params: unknown, options?: FindOptions): Promise<unknown>;
  validateOutput(params: unknown): unknown;
}

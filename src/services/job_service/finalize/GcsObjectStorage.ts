import { readFull, objectSize, putObject } from "@shared/GcsUtils.js";
import { IObjectStorage } from "./IObjectStorage.js";
import { StoragePath } from "./StoragePath.js";

/**
 * GcsObjectStorage is responsible for gcs object storage operations.
 */
export class GcsObjectStorage implements IObjectStorage {
    /**
   * Reads the operation
   * @param storagePath - The storage path
   * @returns A promise that resolves to the result
   */
  async read(storagePath: StoragePath): Promise<Buffer> {
    return readFull(storagePath.bucket, storagePath.key);
  }

    /**
   * Writes the operation
   * @param storagePath - The storage path
   * @param body - The body
   * @param contentType - The content type
   */
  async write(storagePath: StoragePath, body: Buffer, contentType = "application/octet-stream"): Promise<void> {
    return putObject(storagePath.bucket, storagePath.key, body, contentType);
  }

    /**
   * Performs the size operation.
   * @param storagePath - The storage path
   * @returns A promise that resolves to the result
   */
  async size(storagePath: StoragePath): Promise<number> {
    return objectSize(storagePath.bucket, storagePath.key);
  }
}

import { readFull, objectSize, putObject } from "../../../shared/GcsUtils.js";
import { IObjectStorage } from "./IObjectStorage.js";
import { StoragePath } from "./StoragePath.js";

export class GcsObjectStorage implements IObjectStorage {
  async read(storagePath: StoragePath): Promise<Buffer> {
    return readFull(storagePath.bucket, storagePath.key);
  }

  async write(storagePath: StoragePath, body: Buffer, contentType = "application/octet-stream"): Promise<void> {
    return putObject(storagePath.bucket, storagePath.key, body, contentType);
  }

  async size(storagePath: StoragePath): Promise<number> {
    return objectSize(storagePath.bucket, storagePath.key);
  }
}

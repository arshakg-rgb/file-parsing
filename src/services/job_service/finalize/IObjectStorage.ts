import { StoragePath } from "./StoragePath.js";

export interface IObjectStorage {
  read(storagePath: StoragePath): Promise<Buffer>;
  write(storagePath: StoragePath, body: Buffer, contentType?: string): Promise<void>;
  size(storagePath: StoragePath): Promise<number>;
}

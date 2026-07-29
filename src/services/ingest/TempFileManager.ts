import { randomUUID } from "crypto";
import fs from "fs/promises";
import os from "os";
import path from "path";

/**
 * Creates and cleans up scratch temp files/directories under the OS tmpdir.
 *
 * Stateless — used only through its static methods, so no getInstance() is
 * needed here.
 */

export class TempFileManager
{
    static async createTempFile(data: Buffer, ext: string): Promise<string>
    {
        const tmp: string = path.join(os.tmpdir(), `${randomUUID()}${ext}`);
        await fs.writeFile(tmp, data);
        return tmp;
    }

    static async createTempDir(): Promise<string>
    {
        const dir: string = path.join(os.tmpdir(), randomUUID());
        await fs.mkdir(dir, { recursive: true });
        return dir;
    }

    static async removeFile(filePath: string): Promise<void>
    {
        await fs.unlink(filePath).catch(() => {});
    }

    static async removeDir(dirPath: string): Promise<void>
    {
        await fs.rm(dirPath, { recursive: true, force: true });
    }
}

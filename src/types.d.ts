declare module "node-7z" {
  function extractFull(
    archive: string,
    output: string,
    options?: { password?: string }
  ): NodeJS.ReadableStream;

  const Seven: { extractFull: typeof extractFull };
  export default Seven;
}

declare module "unrar-async" {
  import { Readable } from "node:stream";

  export interface FileHeader {
    name: string;
    flags: {
      encrypted: boolean;
      solid: boolean;
      directory: boolean;
    };
    packSize: number;
    unpSize: number;
    crc: number;
    time: string;
    unpVer: string;
    method: string;
    comment: string;
  }

  export type ArcFile<withContent = never> = {
    fileHeader: FileHeader;
    extraction?: withContent;
  };

  export interface ExtractResult<withContent = never> {
    arcHeader: any;
    fileHeaders: FileHeader[];
    fileCount: number;
    totalSize: number;
    files: AsyncGenerator<ArcFile<withContent>>;
  }

  export interface RARExtractorOptions {
    password?: string;
    idleTimeoutMs?: number;
    outputSizeLimitFactor?: number;
    debug?: boolean;
  }

  export class RARExtractor 
{
    static fromFile(path: string, options?: RARExtractorOptions): Promise<RARExtractor>;
    static fromBuffer(buffer: Uint8Array, options?: RARExtractorOptions): Promise<RARExtractor>;
    extract(options?: RARExtractorOptions): Promise<ExtractResult<Readable>>;
    close(): void;
  }
}

declare module "node-stream-zip" {
  interface StreamZipOptions {
    file: string;
    password?: string;
  }

  interface AsyncEntry {
    isDirectory: boolean;
  }

  class AsyncStreamZip 
{
    constructor(options: StreamZipOptions);
    entries(): Promise<Record<string, AsyncEntry>>;
    entryData(name: string): Promise<Buffer>;
    close(): Promise<void>;
  }

  namespace NodeStreamZip {
    export { AsyncStreamZip as async };
  }

  export default NodeStreamZip;
}

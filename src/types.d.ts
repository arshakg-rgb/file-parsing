declare module "node-7z" {
    /**
   * Extracts full
   * @param archive - The archive
   * @param output - The output
   * @param options - The options object
   * @returns The node j s. readable stream result
   */
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
    arcHeader: unknown;
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

    /**
   * RARExtractor is responsible for r a r extractor operations.
   */
  export class RARExtractor {
        /**
     * Performs the from file operation.
     * @param path - The file path
     * @param options - The options object
     * @returns A promise that resolves to the result
     */
    static fromFile(path: string, options?: RARExtractorOptions): Promise<RARExtractor>;
        /**
     * Performs the from buffer operation.
     * @param buffer - The buffer to process
     * @param options - The options object
     * @returns A promise that resolves to the result
     */
    static fromBuffer(buffer: Uint8Array, options?: RARExtractorOptions): Promise<RARExtractor>;
        /**
     * Extracts the operation
     * @param options - The options object
     * @returns A promise that resolves to the result
     */
    extract(options?: RARExtractorOptions): Promise<ExtractResult<Readable>>;
        /**
     * Closes the operation
     */
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

    /**
   * AsyncStreamZip is responsible for async stream zip operations.
   */
  class AsyncStreamZip {
        /**
     * Constructs a new AsyncStreamZip instance.
     * @param options - The options object
     */
    constructor(options: StreamZipOptions);
        /**
     * Performs the entries operation.
     * @returns A promise that resolves to the result
     */
    entries(): Promise<Record<string, AsyncEntry>>;
        /**
     * Performs the entry data operation.
     * @param name - The name value
     * @returns A promise that resolves to the result
     */
    entryData(name: string): Promise<Buffer>;
        /**
     * Closes the operation
     */
    close(): Promise<void>;
  }

  namespace NodeStreamZip {
    export { AsyncStreamZip as async };
  }

  export default NodeStreamZip;
}

declare module "json-bigint" {
  interface JSONBigOptions {
    strict?: boolean;
    storeAsString?: boolean;
    alwaysParseAsBig?: boolean;
    useNativeBigInt?: boolean;
    protoAction?: "error" | "ignore" | "preserve";
    constructorAction?: "error" | "ignore" | "preserve";
  }

  interface JSONBig {
    parse(text: string, reviver?: (key: string, value: unknown) => unknown): any;
    stringify(value: unknown, replacer?: unknown, space?: string | number): string;
  }

  function JSONBigFactory(options?: JSONBigOptions): JSONBig;

  namespace JSONBigFactory {
    function parse(text: string, reviver?: (key: string, value: unknown) => unknown): any;
    function stringify(value: unknown, replacer?: unknown, space?: string | number): string;
  }

  export = JSONBigFactory;
}

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

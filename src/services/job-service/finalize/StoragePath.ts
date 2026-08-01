import {GcsUtils} from "@shared/GcsUtils";

export type GcsProtocol = "gs" | "s3";

/**
 * StoragePath is responsible for storage path operations.
 */
export class StoragePath
{
    /**
   * Constructs a new StoragePath instance.
   * @param protocol - The protocol
   * @param bucket - The bucket
   * @param key - The key
   */
  constructor(public readonly protocol: GcsProtocol, public readonly bucket: string, public readonly key: string) {}

    /**
   * Parses the operation
   * @param url - The URL to process
   * @returns The storage path result
   */

  static parse(url: string): StoragePath
  {
    const protocol: "gs" | "s3" = url.startsWith("gs://") ? "gs" : url.startsWith("s3://") ? "s3" : null;

    if (!protocol)
    {
      throw new Error(`Expected gs:// or s3:// URL, got: ${url}`);
    }

    const [bucket, key] = GcsUtils.getInstance().parseGcsUrl(url);
    return new StoragePath(protocol, bucket, key);
  }

    /**
   * function toString() { [native code] } the operation
   * @returns The string result
   */

  toString(): string
  {
    return `${this.protocol}://${this.bucket}/${this.key}`;
  }
}

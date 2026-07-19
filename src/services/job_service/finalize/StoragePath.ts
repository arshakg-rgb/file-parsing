import { parseGcsUrl } from "@shared/GcsUtils.js";

export type GcsProtocol = "gs" | "s3";

export class StoragePath {
  constructor(
    public readonly protocol: GcsProtocol,
    public readonly bucket: string,
    public readonly key: string
  ) {}

  static parse(url: string): StoragePath {
    const protocol = url.startsWith("gs://") ? "gs" : url.startsWith("s3://") ? "s3" : null;
    if (!protocol) {
      throw new Error(`Expected gs:// or s3:// URL, got: ${url}`);
    }
    const [bucket, key] = parseGcsUrl(url);
    return new StoragePath(protocol, bucket, key);
  }

  toString(): string {
    return `${this.protocol}://${this.bucket}/${this.key}`;
  }

  withKey(key: string): StoragePath {
    return new StoragePath(this.protocol, this.bucket, key);
  }
}

// All storage is now on Google Cloud Storage.
// This shim re-exports from gcsUtils.ts so all existing imports continue to compile unchanged.
export {
  gcsClient as s3Client,
  parseGcsUrl as parseS3Url,
  objectSize,
  readRange,
  readFull,
  putObject,
  putJson,
  putParquet,
  copyObject,
  presignedPutUrl,
  streamLines,
  sha256Hex,
  listObjects,
} from "./GcsUtils.js";

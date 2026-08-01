// This shim re-exports from gcsUtils.ts so all existing imports continue to compile unchanged.
export {
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

import assert from "assert";
import { GcsUtils } from "@shared/GcsUtils.js";

type Line = [string, number, number];

function buildFixture(): Buffer {
  const parts: string[] = [];
  parts.push("id,name,city\r\n");
  parts.push("1,Alice,Paris\n");
  parts.push("2,\"Bob\nNewline\",Berlin\n");
  parts.push("3,Ünïcödé-Ω-😀,München\n");
  for (let i = 4; i < 400; i++) {
    parts.push(`${i},name-${i},city-${i}\n`);
  }
  parts.push("999,tail-no-newline,end");
  return Buffer.from(parts.join(""), "utf-8");
}

async function collect(
  gcs: GcsUtils,
  chunkSize: number,
  shortReadEvery: number
): Promise<Line[]> {
  const out: Line[] = [];
  for await (const l of gcs.streamLines("b", "k", chunkSize, "utf-8")) {
    out.push([l[0], l[1], l[2]]);
  }
  void shortReadEvery;
  return out;
}

async function run(): Promise<void> {
  const data = buildFixture();
  const gcs = GcsUtils.getInstance();

  let reads = 0;
  let shortReadEvery = 0;

  (gcs as unknown as Record<string, unknown>).objectSize = async () => data.length;
  (gcs as unknown as Record<string, unknown>).readRange = async (
    _b: string,
    _k: string,
    start: number,
    end: number
  ) => {
    reads++;
    let stop = end + 1;
    if (shortReadEvery > 0 && reads % shortReadEvery === 0) {
      const truncated = Math.max(start + 1, stop - 7);
      stop = Math.min(stop, truncated);
    }
    return data.subarray(start, stop);
  };

  // Reference: force the single-GET path, which uses splitBytesToLines directly.
  (gcs as unknown as Record<string, unknown>).readFull = async () => data;
  process.env.SMALL_FILE_SINGLE_GET_THRESHOLD = String(data.length + 1000);
  const { default: Config } = await import("@config/system-config/Config.js");
  (Config.getInstance() as unknown as { settings: Record<string, unknown> }).settings.SMALL_FILE_SINGLE_GET_THRESHOLD =
    data.length + 1000;
  const reference = await collect(gcs, 64, 0);

  // Chunked + prefetch path across many chunk sizes.
  (Config.getInstance() as unknown as { settings: Record<string, unknown> }).settings.SMALL_FILE_SINGLE_GET_THRESHOLD = 1;

  for (const chunkSize of [7, 13, 64, 100, 512, 4096, data.length - 1]) {
    reads = 0;
    shortReadEvery = 0;
    const got = await collect(gcs, chunkSize, 0);
    assert.deepStrictEqual(
      got,
      reference,
      `chunked output differs from reference at chunkSize=${chunkSize}`
    );
  }
  console.log("PASS: chunked+prefetch === single-GET reference for all chunk sizes");

  // Short-read correction path.
  for (const every of [2, 3, 5]) {
    for (const chunkSize of [16, 64, 256]) {
      reads = 0;
      shortReadEvery = every;
      const got = await collect(gcs, chunkSize, every);
      assert.deepStrictEqual(
        got,
        reference,
        `short-read output differs (chunkSize=${chunkSize}, every=${every})`
      );
    }
  }
  console.log("PASS: short-read correction preserves exact lines and byte offsets");

  // Byte offsets must point at the real line start in the source buffer.
  for (const [text, offset, length] of reference) {
    const slice = data.subarray(offset, offset + length).toString("utf-8");
    assert.ok(
      slice.startsWith(text.slice(0, Math.min(8, text.length))),
      `offset ${offset} does not point at line start for ${JSON.stringify(text.slice(0, 40))}`
    );
  }
  console.log(`PASS: ${reference.length} byte offsets verified against source`);
}

run().catch((e) => {
  console.error("FAIL", e);
  process.exit(1);
});

import fs from "fs";
import { settings } from "@shared/Settings.js";

const NL = 0x0a;
const CR = 0x0d;
const QUOTE = 0x22;

interface ScanResult {
  lines: Array<{ text: string; lineNo: number; byteOffset: number }>;
  uncertainLines: Array<{ lineNo: number; text: string; reason: string }>;
}

function* scanLines(
  data: Buffer,
  baseOffset: number,
  state: { inQuote: boolean; quotedNewlines: number },
  maxQuotedNewlines: number = 100
): Generator<{ text: string; lineNo: number; byteOffset: number }, { lineStart: number; endedAtBoundary: boolean }, void> {
  let lineNo = 0;
  let pos = 0;
  let lineStart = 0;
  let endedAtBoundary = false;
  let quotedNewlines = state.quotedNewlines;

  const makeLine = (endExclusive: number) => {
    const raw = data.slice(lineStart, endExclusive);
    const text = raw.toString("utf-8").replace(/\r\n$|\n$/, "");
    const result = { text, lineNo, byteOffset: baseOffset + lineStart };
    lineNo++;
    lineStart = endExclusive;
    quotedNewlines = 0;
    state.quotedNewlines = 0;
    return result;
  };

  while (pos < data.length) {
    const b = data[pos];
    if (b === QUOTE) {
      if (state.inQuote) {
        if (pos + 1 === data.length) {
          endedAtBoundary = true;
          break;
        }
        if (pos + 1 < data.length && data[pos + 1] === QUOTE) {
          pos += 2;
          continue;
        }
      }
      state.inQuote = !state.inQuote;
      pos++;
      continue;
    }

    if (b === NL) {
      if (!state.inQuote) {
        yield makeLine(pos + 1);
      } else {
        quotedNewlines++;
        state.quotedNewlines = quotedNewlines;
        if (pos + 1 - lineStart >= 65536) {
          state.inQuote = false;
          yield makeLine(pos + 1);
        }
      }
    }

    pos++;

    if (pos - lineStart >= 65536) {
      state.inQuote = false;
      yield makeLine(pos);
    }
  }

  return { lineStart, endedAtBoundary };
}

async function testChunkedStreaming(filePath: string, chunkSize: number): Promise<ScanResult> {
  const fileHandle = await fs.promises.open(filePath, "r");
  const stats = await fileHandle.stat();
  const total = stats.size;

  const result: ScanResult = { lines: [], uncertainLines: [] };
  const state = { inQuote: false, quotedNewlines: 0 };
  let fetchOffset = 0;
  let remainder = Buffer.alloc(0);
  let remainderStart = 0;
  let lineNo = 0;

  while (fetchOffset < total) {
    const end = Math.min(fetchOffset + chunkSize - 1, total - 1);
    const chunkBuffer = Buffer.alloc(end - fetchOffset + 1);
    const { bytesRead } = await fileHandle.read(chunkBuffer, 0, chunkBuffer.length, fetchOffset);
    const chunk = chunkBuffer.slice(0, bytesRead);
    
    const data = Buffer.concat([remainder, chunk]);
    const dataBase = remainderStart;

    let scanResult;
    for (const line of scanLines(data, dataBase, state, settings.MAX_QUOTED_NEWLINES)) {
      lineNo = line.lineNo + 1;

      // Check for malformed lines (simplified check)
      if (line.text.split(",").length < 5 && line.text.length > 0) {
        result.uncertainLines.push({
          lineNo: line.lineNo,
          text: line.text.slice(0, 200),
          reason: "Too few fields"
        });
      }

      // Log state around line 185k
      if (lineNo >= 185500 && lineNo <= 186500) {
        console.log(`Line ${lineNo}: inQuote=${state.inQuote}, quotedNewlines=${state.quotedNewlines}, remainder=${remainder.length}`);
      }
    }

    scanResult = scanLines(data, dataBase, state, settings.MAX_QUOTED_NEWLINES).next().value as { lineStart: number; endedAtBoundary: boolean };
    
    remainder = data.slice(scanResult.lineStart);
    remainderStart = dataBase + scanResult.lineStart;
    fetchOffset += chunk.length;

  }

  await fileHandle.close();
  return result;
}

async function main() {
  const filePath = "/tmp/twitter_users_000.csv";
  const chunkSize = 64 * 1024; // 64KB

  console.log(`Testing chunked streaming with ${chunkSize / 1024}KB chunks...`);
  console.log(`File: ${filePath}`);

  const result = await testChunkedStreaming(filePath, chunkSize);

  console.log(`\nTotal lines: ${result.lines.length}`);
  console.log(`Uncertain lines: ${result.uncertainLines.length}`);
  
  if (result.uncertainLines.length > 0) {
    console.log("\nUncertain lines:");
    result.uncertainLines.slice(0, 20).forEach(u => {
      console.log(`  Line ${u.lineNo}: ${u.reason} - "${u.text}"`);
    });
  }

  // Check for clustering around line 185k
  const cluster = result.uncertainLines.filter(u => u.lineNo >= 185000 && u.lineNo <= 187000);
  if (cluster.length > 5) {
    console.log(`\n⚠️  CLUSTER DETECTED around line 185k: ${cluster.length} uncertain lines`);
  }
}

main().catch(console.error);

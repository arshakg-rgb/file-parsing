import fs from "fs";
import { settings } from "@shared/Settings.js";
import { splitAllLines } from "@shared/GcsUtils.js";
import { LineClassifier, parseCsvLine } from "@service/stream_parser/LineClassifier.js";
import { MockClassifier } from "@service/ai_classifier/mock/MockClassifier.js";

const HEADER_LABEL_RE = /^[A-Za-z][A-Za-z0-9 _.\-]*$/;

function isLabelLike(v: string): boolean {
  return v !== "" && !v.includes("@") && v.replace(/\D/g, "").length < 7 && HEADER_LABEL_RE.test(v);
}

function inferFieldSpec(firstLine: string, secondLine: string | undefined): string[] | null {
  let best: string[] | null = null;
  for (const delim of [",", ";", "\t", "|"]) {
    const parts = parseCsvLine(firstLine, delim, "\"");
    if (parts.length < 2) continue;
    if (secondLine !== undefined) {
      const secondParts = parseCsvLine(secondLine, delim, "\"");
      if (secondParts.length !== parts.length) continue;
    }
    const sanitized = parts.map((p, i) => {
      const v = p.trim();
      return isLabelLike(v) ? v : `col_${i}`;
    });
    if (!best || sanitized.length > best.length) best = sanitized;
  }
  return best;
}

function usage(): void {
  console.error("Usage: npx tsx src/scripts/testParseLocal.ts <path-to-file> [field_spec_json] [--mock-ai]");
  process.exit(1);
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  if (args.length === 0) usage();

  const filePath = args[0];
  const mockAi = args.includes("--mock-ai");
  const fieldSpecArg = args.find((a) => a.startsWith("[") || a.startsWith('"'));

  let fieldSpec: string[] = [];
  if (fieldSpecArg) {
    try {
      fieldSpec = JSON.parse(fieldSpecArg);
      if (!Array.isArray(fieldSpec)) {
        console.error("field_spec_json must be a JSON array of strings");
        process.exit(1);
      }
    } catch (e) {
      console.error("Invalid field_spec JSON:", e instanceof Error ? e.message : String(e));
      process.exit(1);
    }
  }

  const data = fs.readFileSync(filePath);
  const lines = splitAllLines(data, "utf-8", settings.CSV_MAX_QUOTED_NEWLINES);
  console.log(`Total lines after quote-aware split: ${lines.length}`);

  if (fieldSpec.length === 0 && lines.length > 0) {
    const first = lines[0][0];
    const second = lines[1]?.[0];
    const inferred = inferFieldSpec(first, second);
    if (inferred) {
      fieldSpec = inferred;
      console.log(`Inferred field_spec from header: ${JSON.stringify(fieldSpec)}`);
    } else {
      console.log("No field_spec supplied and header inference failed.");
    }
  } else if (fieldSpec.length > 0) {
    console.log(`Using supplied field_spec: ${JSON.stringify(fieldSpec)}`);
  }

  const classifier = new LineClassifier("local-test", fieldSpec, [], [], null);
  const mockClassifier = MockClassifier.getInstance();

  const counts = { parsed: 0, rubbish: 0, uncertain: 0, aiResolved: 0 };
  const uncertainSamples: string[] = [];
  const aiResolvedSamples: Array<{ line: string; kind: string }> = [];

  for (const [line] of lines) {
    let result;
    try {
      result = classifier.classify(line, 0, line.length);
    } catch {
      counts.uncertain++;
      continue;
    }

    if (result.verdict === "parsed") {
      counts.parsed++;
      continue;
    }
    if (result.verdict === "rubbish") {
      counts.rubbish++;
      continue;
    }

    counts.uncertain++;
    if (uncertainSamples.length < 10) uncertainSamples.push(line.slice(0, 200));

    if (mockAi) {
      const aiResult = mockClassifier.classify({ unknown_line: line, field_spec: fieldSpec, context_lines: [] });
      if (aiResult.kind !== "uncertain") {
        counts.aiResolved++;
        if (aiResolvedSamples.length < 5) {
          aiResolvedSamples.push({ line: line.slice(0, 200), kind: aiResult.kind });
        }
      }
    }
  }

  const total = counts.parsed + counts.rubbish + counts.uncertain;
  const uncertainRate = total > 0 ? counts.uncertain / total : 0;

  console.log("\n--- Local parse result ---");
  console.log(counts);
  console.log(`Uncertain rate: ${(uncertainRate * 100).toFixed(1)}%`);

  if (uncertainRate === 0) {
    console.log("\n✅ Local classifier handles this file with no AI.");
  } else if (uncertainRate < 0.2) {
    console.log("\n✅ Local classifier handles most of this file; only a small fraction needs AI.");
    console.log("Sample uncertain lines:");
    uncertainSamples.forEach((s, i) => console.log(`  [${i}] ${s}`));
  } else {
    console.log("\n⚠️  High uncertain rate. Sample uncertain lines:");
    uncertainSamples.forEach((s, i) => console.log(`  [${i}] ${s}`));
  }

  if (mockAi) {
    console.log(`\n--- Mock AI resolution ---`);
    console.log(`AI resolved ${counts.aiResolved} of ${counts.uncertain} uncertain lines`);
    aiResolvedSamples.forEach((s, i) => console.log(`  [${i}] ${s.kind}: ${s.line}`));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

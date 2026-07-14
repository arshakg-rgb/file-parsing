import crypto from "crypto";
import { BedrockRuntimeClient, ConverseCommand } from "@aws-sdk/client-bedrock-runtime";
import { settings } from "../../shared/config.js";
import { ClassifyRequest, ClassifyResponse, AIVerdict, FieldLocator, LineStructure, RecordTemplateData, RubbishTemplateData, Template, TemplateKind, TemplateSource } from "../../shared/models/template.js";
import * as templateRegistry from "./templateRegistry.js";

const SYSTEM_PROMPT = `You are a data-parsing assistant embedded in a production file-parsing pipeline.
A streaming parser has encountered a line that matches NO known template.

Your task: classify the line and generate a REUSABLE declarative template.

== CRITICAL RULES ==
1. Output is ALWAYS a JSON object — never prose, never code.
2. You have exactly three possible verdicts:
   a) record-template  — the line is parseable structured data
   b) rubbish-signature — the line is definitely junk (confidence ≥ 0.90)
   c) uncertain          — you cannot safely decide
3. When in doubt → uncertain. NEVER guess. A wrong drop is unrecoverable.
4. Rubbish confidence must be ≥ 0.90. Anything lower → uncertain.
5. Templates are declarative specs interpreted by the engine — never code.
6. Every column name in field_map MUST come from the detected structure, not invented.
7. Validate your template against the triggering line before responding.

== OUTPUT FORMAT ==

If record-template:
{
  "kind": "record-template",
  "template": {
    "structure": "csv" | "json" | "kv" | "fixed" | "regex",
    "delimiter": "," | ";" | "\\t" | "|" | null,
    "quote_char": "\\"" | "'" | null,
    "field_map": {
      "<target_field>": {"index": 0}
                      | {"regex": "capture-group-pattern"}
                      | {"key": "json_key_name"}
    },
    "length_hint_min": <int or null>,
    "length_hint_max": <int or null>
  }
}

If rubbish-signature:
{
  "kind": "rubbish-signature",
  "template": {
    "signature": "<tight regex that identifies this junk class>",
    "confidence": 0.95,
    "description": "<brief reason this is junk>"
  }
}

If uncertain:
{"kind": "uncertain"}`;

function buildUserPrompt(req: ClassifyRequest): string {
  return `Target fields to extract: ${req.field_spec.join(", ")}\n\nUnknown line:\n${req.unknown_line}\n\nSurrounding context lines:\n${req.context_lines?.join("\n") || "(none)"}`;
}

function extractJson(text: string): any {
  const fence = /\`\`\`(?:json)?\s*(\{[\s\S]*?\})\s*\`\`\`/.exec(text);
  if (fence) return JSON.parse(fence[1]);
  const brace = /\{[\s\S]*\}/.exec(text);
  if (brace) return JSON.parse(brace[0]);
  throw new Error("No JSON found in model output");
}

function fingerprint(line: string, raw: any): string {
  const t = raw.template || {};
  const parts = [raw.kind || "unknown", t.structure || "", t.delimiter || "", t.quote_char || ""];
  if (t.field_map) parts.push(Object.keys(t.field_map).sort().join(","));
  if (t.signature) parts.push(t.signature.slice(0, 64));
  return crypto.createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 24);
}

function quickFingerprint(line: string): string {
  try {
    const parsed = JSON.parse(line);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return crypto.createHash("sha256").update(`json|${Object.keys(parsed).sort().join(",")}`).digest("hex").slice(0, 24);
    }
  } catch {}
  for (const delim of [",", ";", "\t", "|"]) {
    const parts = line.split(delim);
    if (parts.length >= 3) {
      return crypto.createHash("sha256").update(`csv|${delim}|${parts.length}`).digest("hex").slice(0, 24);
    }
  }
  if (line.includes("=") && line.split("=").length >= 2) {
    return crypto.createHash("sha256").update("kv|=").digest("hex").slice(0, 24);
  }
  return crypto.createHash("sha256").update(`text|${line.length}`).digest("hex").slice(0, 24);
}

function buildTemplateFromRaw(raw: any, kindStr: string, line: string): Template | null {
  try {
    const fp = fingerprint(line, raw);
    if (kindStr === "record-template") {
      const t = raw.template || {};
      const fieldMap: Record<string, FieldLocator> = {};
      for (const [field, loc] of Object.entries(t.field_map || {})) {
        fieldMap[field] = loc as FieldLocator;
      }
      return {
        template_id: crypto.randomUUID(),
        kind: TemplateKind.RECORD,
        fingerprint: fp,
        version: 1,
        record: {
          structure: (t.structure || "csv") as LineStructure,
          delimiter: t.delimiter,
          quote_char: t.quote_char,
          field_map: fieldMap,
          length_hint_min: t.length_hint_min,
          length_hint_max: t.length_hint_max,
          has_header: false,
        },
        source: TemplateSource.AI,
        match_count: 0,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
    }
    if (kindStr === "rubbish-signature") {
      const t = raw.template || {};
      return {
        template_id: crypto.randomUUID(),
        kind: TemplateKind.RUBBISH,
        fingerprint: fp,
        version: 1,
        rubbish: {
          signature: t.signature,
          confidence: parseFloat(t.confidence),
          description: t.description,
        } as RubbishTemplateData,
        source: TemplateSource.AI,
        match_count: 0,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
    }
  } catch (err) {
    console.warn("template_build_error", { error: String(err), raw: JSON.stringify(raw).slice(0, 200) });
  }
  return null;
}

async function callBedrock(prompt: string): Promise<any> {
  const client = new BedrockRuntimeClient({ region: process.env.AWS_REGION ?? "us-east-1" });
  const resp = await client.send(
    new ConverseCommand({
      modelId: settings.BEDROCK_MODEL_ID,
      system: [{ text: SYSTEM_PROMPT }],
      messages: [{ role: "user", content: [{ text: prompt }] }],
      inferenceConfig: { temperature: 0.0, maxTokens: 1024 },
    })
  );
  const content = resp.output?.message?.content || [];
  for (const item of content) {
    if (item.text) return extractJson(item.text);
  }
  throw new Error("No text content in Bedrock response");
}

export async function classifyAi(req: ClassifyRequest): Promise<ClassifyResponse> {
  await templateRegistry.warmCache();

  const lineFp = quickFingerprint(req.unknown_line);
  const existing = templateRegistry.getLatest(lineFp);
  if (existing) {
    templateRegistry.incrementMatchCount(existing.template_id, existing.fingerprint);
    const kind = existing.kind === TemplateKind.RECORD ? AIVerdict.RECORD_TEMPLATE : AIVerdict.RUBBISH_SIGNATURE;
    return { kind, template: existing };
  }

  const userPrompt = buildUserPrompt(req);
  try {
    const raw = await callBedrock(userPrompt);
    const kindStr = raw.kind || "uncertain";
    if (kindStr === "uncertain") return { kind: AIVerdict.UNCERTAIN };
    const tmpl = buildTemplateFromRaw(raw, kindStr, req.unknown_line);
    if (!tmpl) return { kind: AIVerdict.UNCERTAIN };
    if (tmpl.kind === TemplateKind.RECORD && !(await validateTemplate(req, tmpl))) {
      console.log("ai_template_validation_failed", { job_id: req.job_id, fingerprint: tmpl.fingerprint });
      return { kind: AIVerdict.UNCERTAIN };
    }
    const saved = await templateRegistry.save(tmpl);
    const verdict = kindStr === "record-template" ? AIVerdict.RECORD_TEMPLATE : AIVerdict.RUBBISH_SIGNATURE;
    console.log("ai_classified", { job_id: req.job_id, verdict, template_id: saved.template_id, fingerprint: saved.fingerprint });
    return { kind: verdict, template: saved };
  } catch (err) {
    console.error("bedrock_call_failed", { job_id: req.job_id, error: String(err) });
    return { kind: AIVerdict.UNCERTAIN };
  }
}

async function validateTemplate(req: ClassifyRequest, tmpl: Template): Promise<boolean> {
  try {
    const { LineClassifier } = await import("../stream_parser/classifier.js");
    const classifier = new LineClassifier(req.job_id || "", req.field_spec, [tmpl], []);
    const result = classifier.classify(req.unknown_line, 0, 0);
    return result.verdict === "parsed";
  } catch (err) {
    console.warn("template_validation_error", { job_id: req.job_id, error: String(err) });
    return false;
  }
}

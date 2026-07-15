import crypto from "crypto";
import Anthropic from "@anthropic-ai/sdk";
import { settings } from "../../shared/config.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "../../shared/templateRegistry.js";

interface ClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines?: string[];
  job_id?: string;
}

interface ClassifyResponse {
  kind: "record-template" | "rubbish-signature" | "uncertain";
  template?: RecordTemplate | RubbishTemplate;
}

interface FieldLocator {
  index?: number;
  regex?: string;
  key?: string;
}

enum AIVerdict {
  RECORD_TEMPLATE = "record-template",
  RUBBISH_SIGNATURE = "rubbish-signature",
  UNCERTAIN = "uncertain"
}

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

function buildTemplateFromRaw(raw: any, kindStr: string, line: string): RecordTemplate | RubbishTemplate | null {
  try {
    const fp = fingerprint(line, raw);
    if (kindStr === "record-template") {
      const t = raw.template || {};
      const fieldMap: Record<string, { locator: string; type: string }> = {};
      for (const [field, loc] of Object.entries(t.field_map || {})) {
        const locator = loc as FieldLocator;
        fieldMap[field] = {
          locator: locator.index !== undefined ? `index:${locator.index}` : 
                    locator.regex ? `regex:${locator.regex}` : 
                    locator.key ? `key:${locator.key}` : "unknown",
          type: "string"
        };
      }
      return {
        template_id: crypto.randomUUID(),
        fingerprint: fp,
        version: 1,
        field_map: fieldMap,
        structure: t.structure || "csv",
        length_hint: t.length_hint_min || 0,
        source: "ai" as const,
        created_at: new Date(),
      };
    }
    if (kindStr === "rubbish-signature") {
      const t = raw.template || {};
      return {
        template_id: crypto.randomUUID(),
        fingerprint: fp,
        version: 1,
        signature: t.signature,
        confidence: parseFloat(t.confidence) || 0.95,
        source: "ai" as const,
        created_at: new Date(),
      };
    }
  } catch (err) {
    console.warn("template_build_error", { error: String(err), raw: JSON.stringify(raw).slice(0, 200) });
  }
  return null;
}

async function callAnthropic(prompt: string): Promise<any> {
  const anthropic = new Anthropic({
    apiKey: settings.ANTHROPIC_API_KEY,
  });
  
  const resp = await anthropic.messages.create({
    model: settings.ANTHROPIC_MODEL || "claude-3-sonnet-20240229",
    max_tokens: 1024,
    system: SYSTEM_PROMPT,
    messages: [{ role: "user", content: prompt }],
    temperature: 0.0,
  });
  
  const content = resp.content[0];
  if (content.type === "text") {
    return extractJson(content.text);
  }
  throw new Error("No text content in Anthropic response");
}

export async function classifyAi(req: ClassifyRequest): Promise<ClassifyResponse> {
  await templateRegistry.loadFromDatabase();

  const lineFp = quickFingerprint(req.unknown_line);
  const existing = templateRegistry.getByFingerprint(lineFp);
  if (existing) {
    const kind = (existing as RecordTemplate).field_map ? AIVerdict.RECORD_TEMPLATE : AIVerdict.RUBBISH_SIGNATURE;
    return { kind, template: existing };
  }

  const userPrompt = buildUserPrompt(req);
  try {
    const raw = await callAnthropic(userPrompt);
    const kindStr = raw.kind || "uncertain";
    if (kindStr === "uncertain") return { kind: AIVerdict.UNCERTAIN };
    const tmpl = buildTemplateFromRaw(raw, kindStr, req.unknown_line);
    if (!tmpl) return { kind: AIVerdict.UNCERTAIN };
    
    // Save to database and cache
    const kind = kindStr === "record-template" ? "record" : "rubbish";
    await templateRegistry.saveTemplate(tmpl, kind);
    templateRegistry.addRecordTemplate(tmpl as RecordTemplate);
    
    const verdict = kindStr === "record-template" ? AIVerdict.RECORD_TEMPLATE : AIVerdict.RUBBISH_SIGNATURE;
    console.log("ai_classified", { job_id: req.job_id, verdict, template_id: tmpl.template_id, fingerprint: tmpl.fingerprint });
    return { kind: verdict, template: tmpl };
  } catch (err) {
    console.error("anthropic_call_failed", { job_id: req.job_id, error: String(err) });
    return { kind: AIVerdict.UNCERTAIN };
  }
}

async function validateTemplate(req: ClassifyRequest, tmpl: RecordTemplate): Promise<boolean> {
  try {
    // Basic validation: ensure template can extract fields from the line
    const line = req.unknown_line;
    const fieldMap = tmpl.field_map;
    
    // Simple validation: check if we can at least parse the structure
    if (tmpl.structure === "csv") {
      const parts = line.split(",");
      return parts.length >= Object.keys(fieldMap).length;
    }
    if (tmpl.structure === "json") {
      try {
        const parsed = JSON.parse(line);
        return typeof parsed === "object" && parsed !== null;
      } catch {
        return false;
      }
    }
    return true;
  } catch (err) {
    console.warn("template_validation_error", { job_id: req.job_id, error: String(err) });
    return false;
  }
}

import crypto from "crypto";
import { GoogleGenAI } from "@google/genai";
import { settings } from "../../shared/config.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "../../shared/templateRegistry.js";

// Vertex AI integration using local implementation pattern
async function askVertexAI(prompt: string): Promise<string> {
  const PROJECT_ID = settings.GCP_PROJECT_ID || 'data-etl-499916';
  const LOCATION = settings.VERTEX_LOCATION || 'us-central1';
  const MODEL = settings.VERTEX_MODEL || 'gemini-2.5-flash';

  const ai = new GoogleGenAI({
    vertexai: true,
    project: PROJECT_ID,
    location: LOCATION,
  });

  const response = await ai.models.generateContent({
    model: MODEL,
    contents: [
      {
        role: 'user',
        parts: [{ text: prompt }],
      },
    ],
    config: {
      responseModalities: ['TEXT'],
      temperature: 0.2,
      maxOutputTokens: 1024,
    },
  });

  return response.text
    ?? response.candidates?.[0]?.content?.parts?.map((part: any) => part.text).join('')
    ?? '';
}

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
  return `Target fields to extract: ${req.field_spec.join(", ")}\n\nUnknown line to classify:\n${req.unknown_line}\n\nSurrounding context lines:\n${req.context_lines?.join("\n") || "(none)"}

IMPORTANT: You must respond with a template definition (kind, template.field_map, etc.) as specified in the system prompt. Do NOT extract the data from this line - create a reusable template that can parse this line and similar lines.`;
}

function extractJson(text: string): any {
  // Try markdown code fence first (```json or ```)
  const fence = /\`\`\`(?:json)?\s*(\{[\s\S]*?\})\s*\`\`\`/.exec(text);
  if (fence) return JSON.parse(fence[1]);
  
  // Try bare JSON object
  const brace = /\{[\s\S]*\}/.exec(text);
  if (brace) {
    try {
      return JSON.parse(brace[0]);
    } catch {}
  }
  
  // Try to find JSON by looking for first { and last }
  const firstBrace = text.indexOf('{');
  const lastBrace = text.lastIndexOf('}');
  if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
    try {
      return JSON.parse(text.substring(firstBrace, lastBrace + 1));
    } catch {}
  }
  
  throw new Error(`No JSON found in model output. Response: ${text.slice(0, 200)}...`);
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

async function callVertexAI(prompt: string): Promise<any> {
  try {
    console.log("vertex_ai_request_start", { promptLength: prompt.length });
    const text = await askVertexAI(prompt);
    console.log("vertex_ai_response_raw", { response: text.slice(0, 500) });
    const parsed = extractJson(text);
    console.log("vertex_ai_response_parsed", { parsed: JSON.stringify(parsed).slice(0, 500) });
    return parsed;
  } catch (error) {
    console.error("vertex_ai_request_failed", { error: String(error) });
    throw error;
  }
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
    const raw = await callVertexAI(userPrompt);
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
    console.error("vertex_ai_call_failed", { job_id: req.job_id, error: String(err) });
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

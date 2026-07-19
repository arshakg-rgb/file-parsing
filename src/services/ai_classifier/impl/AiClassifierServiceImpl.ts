import crypto from "crypto";
import { GoogleGenAI } from "@google/genai";
import Config from "../../../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../../../config/ServiceManager.js";
import { InstantiationError } from "../../../errors/InstantiationError.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "../../../shared/TemplateRegistryService.js";
import { AiClassifierService } from "../AiClassifierService.js";
import {
  IAiClassifier,
  ClassifyRequest,
  ClassifyResponse,
  FieldLocator,
  CSVParseResult,
  AIVerdict,
} from "../io/IAiClassifier.js";

type RawClassifyResponse = Record<string, unknown>;

const SYSTEM_PROMPT = `You are a data-parsing assistant embedded in a production file-parsing pipeline.
A streaming parser has encountered a line that matches NO known template.

Your task: classify the line and generate a REUSABLE declarative template.

== CRITICAL RULES ==
1. Output is ALWAYS a JSON object — never prose, never code, never YAML.
2. You have exactly three possible verdicts:
   a) record-template  — the line is parseable structured data
   b) rubbish-signature — the line is definitely junk (confidence ≥ 0.90)
   c) uncertain          — you cannot safely decide
3. When in doubt → uncertain. NEVER guess. A wrong drop is unrecoverable.
4. Rubbish confidence must be ≥ 0.90. Anything lower → uncertain.
5. Templates are declarative specs interpreted by the engine — never code.
6. Every column name in field_map MUST come from the detected structure, not invented.
7. Validate your template against the triggering line before responding.
8. MUST return valid JSON format only - no YAML, no markdown code blocks.
9. The "kind" field MUST be exactly one of: "record-template", "rubbish-signature", or "uncertain" - no other values are accepted.

== OUTPUT FORMAT (JSON ONLY) ==

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

class AiClassifierServiceImpl extends ServiceManager implements AiClassifierService {
  protected static instance: AiClassifierServiceImpl;
  private ai: GoogleGenAI;

  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate AiClassifierServiceImpl directly. Use getInstance()");
    }
    super(enforce);
    
    const config = this.getConfig();
    const PROJECT_ID = config.settings.GCP_PROJECT_ID || "data-etl-499916";
    const LOCATION = config.settings.VERTEX_LOCATION || "us-central1";

    this.ai = new GoogleGenAI({
      vertexai: true,
      project: PROJECT_ID,
      location: LOCATION,
    });
  }

  public static getInstance(): AiClassifierServiceImpl {
    if (!AiClassifierServiceImpl.instance) {
      AiClassifierServiceImpl.instance = new AiClassifierServiceImpl(Enforce);
    }
    return AiClassifierServiceImpl.instance;
  }

  /**
   * Vertex AI integration using local implementation pattern
   */
  public async askVertexAI(prompt: string): Promise<string> {
    const config = this.getConfig();
    const MODEL = config.settings.VERTEX_MODEL || "gemini-2.5-flash";

    const response = await this.ai.models.generateContent({
      model: MODEL,
      contents: [
        {
          role: "user",
          parts: [{ text: prompt }],
        },
      ],
      config: {
        responseModalities: ["TEXT"],
        temperature: 0.2,
        maxOutputTokens: 1024,
      },
    });

    return response.text
      ?? response.candidates?.[0]?.content?.parts?.map((part: unknown) => (part as { text?: string }).text).join("")
      ?? "";
  }

  public buildUserPrompt(req: ClassifyRequest): string {
    return `Target fields to extract: ${req.field_spec.join(", ")}\n\nUnknown line to classify:\n${req.unknown_line}\n\nSurrounding context lines:\n${req.context_lines?.join("\n") || "(none)"}

IMPORTANT: You must respond with a template definition (kind, template.field_map, etc.) as specified in the system prompt. Do NOT extract the data from this line - create a reusable template that can parse this line and similar lines.`;
  }

  public extractJson(text: string): RawClassifyResponse {
    // Try markdown code fence first (```json or ```)
    const fence = /\`\`\`(?:json)?\s*(\{[\s\S]*?\})\s*\`\`\`/.exec(text);
    if (fence) return JSON.parse(fence[1]) as RawClassifyResponse;
    
    // Try bare JSON object
    const brace = /\{[\s\S]*\}/.exec(text);
    if (brace) {
      try {
        return JSON.parse(brace[0]) as RawClassifyResponse;
      } catch {}
    }
    
    // Try to find JSON by looking for first { and last }
    const firstBrace = text.indexOf("{");
    const lastBrace = text.lastIndexOf("}");
    if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
      try {
        return JSON.parse(text.substring(firstBrace, lastBrace + 1)) as RawClassifyResponse;
      } catch {}
    }
    
    throw new Error(`No JSON found in model output. Response: ${text.slice(0, 200)}...`);
  }

  public fingerprint(line: string, raw: RawClassifyResponse): string {
    const t = (raw.template || {}) as Record<string, unknown>;
    const parts = [(raw.kind as string) || "unknown", (t.structure as string) || "", (t.delimiter as string) || "", (t.quote_char as string) || ""];
    if (t.field_map) parts.push(Object.keys(t.field_map as Record<string, unknown>).sort().join(","));
    if (t.signature) parts.push((t.signature as string).slice(0, 64));
    return crypto.createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 24);
  }

  public quickFingerprint(line: string): string {
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

  public buildTemplateFromRaw(raw: RawClassifyResponse, kindStr: string, line: string): RecordTemplate | RubbishTemplate | null {
    try {
      const fp = this.fingerprint(line, raw);
      if (kindStr === "record-template") {
        const t = (raw.template || {}) as Record<string, unknown>;
        const fieldMap: Record<string, { locator: string; type: string }> = {};
        for (const [field, loc] of Object.entries((t.field_map || {}) as Record<string, FieldLocator>)) {
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
          structure: (t.structure as string) || "csv",
          length_hint: (t.length_hint_min as number) || 0,
          source: "ai" as const,
          created_at: new Date(),
        };
      }
      if (kindStr === "rubbish-signature") {
        const t = (raw.template || {}) as Record<string, unknown>;
        return {
          template_id: crypto.randomUUID(),
          fingerprint: fp,
          version: 1,
          signature: t.signature as string,
          confidence: parseFloat(t.confidence as string) || 0.95,
          source: "ai" as const,
          created_at: new Date(),
        };
      }
    } catch (err) {
      console.warn("template_build_error", { error: String(err), raw: JSON.stringify(raw).slice(0, 200) });
    }
    return null;
  }

  public async callVertexAI(prompt: string): Promise<RawClassifyResponse> {
    try {
      console.log("vertex_ai_request_start", { promptLength: prompt.length });
      const text = await this.askVertexAI(prompt);
      console.log("vertex_ai_response_raw", { response: text.slice(0, 500) });
      const parsed = this.extractJson(text);
      console.log("vertex_ai_response_parsed", { parsed: JSON.stringify(parsed).slice(0, 500) });
      return parsed as RawClassifyResponse;
    } catch (error) {
      console.error("vertex_ai_request_failed", { error: String(error) });
      throw error;
    }
  }

  public tryParseAsCSV(line: string, fieldSpec: string[]): CSVParseResult {
    const delimiters = [",", ";", "\t", "|"];
    
    // Ensure fieldSpec is an array
    const fieldSpecArray = Array.isArray(fieldSpec) ? fieldSpec : 
      (typeof fieldSpec === "string" ? JSON.parse(fieldSpec) : []);
    
    console.log("csv_parser_start", { line, fieldSpec: fieldSpecArray, delimiterCount: delimiters.length });
    
    for (const delimiter of delimiters) {
      const parts = line.split(delimiter);
      console.log("csv_parser_try_delimiter", { delimiter, partCount: parts.length, expectedCount: fieldSpecArray.length });
      
      if (parts.length === fieldSpecArray.length) {
        // Check if all parts are non-empty (basic validation)
        const allNonEmpty = parts.every(part => part.trim().length > 0);
        console.log("csv_parser_validation", { delimiter, allNonEmpty, parts });
        
        if (allNonEmpty) {
          console.log("csv_parser_success", { delimiter, fields: parts });
          return { success: true, delimiter, fields: parts };
        }
      }
    }
    
    console.log("csv_parser_failed", { reason: "no_delimiter_matched" });
    return { success: false, delimiter: "", fields: [] };
  }

  public createTemplateFromCSV(line: string, fieldSpec: string[], delimiter: string): RecordTemplate {
    const fieldMap: Record<string, { locator: string; type: string }> = {};
    
    fieldSpec.forEach((field, index) => {
      fieldMap[field] = { locator: `index:${index}`, type: "string" };
    });
    
    const template = {
      template_id: crypto.randomBytes(16).toString("hex"),
      fingerprint: this.quickFingerprint(line),
      version: 1,
      field_map: fieldMap,
      structure: "csv",
      length_hint: line.length,
      source: "ai" as const,
      created_at: new Date()
    };
    
    console.log("csv_template_created", { 
      template_id: template.template_id, 
      fieldMap, 
      structure: template.structure,
      delimiter 
    });
    
    return template;
  }

  public async classifyAi(req: ClassifyRequest): Promise<ClassifyResponse> {
    await templateRegistry.loadFromDatabase();

    // Step 1: Try CSV parsing with common delimiters before template matching
    const csvResult = this.tryParseAsCSV(req.unknown_line, req.field_spec);
    if (csvResult.success) {
      console.log("ai_classifier_csv_parse_success", { job_id: req.job_id, delimiter: csvResult.delimiter });
      // Create a template from the CSV parse result
      const template = this.createTemplateFromCSV(req.unknown_line, req.field_spec, csvResult.delimiter);
      await templateRegistry.saveTemplate(template, "record");
      templateRegistry.addRecordTemplate(template);
      return { kind: AIVerdict.RECORD_TEMPLATE, template };
    }

    // Step 2: Try to match by fingerprint (fast path)
    const lineFp = this.quickFingerprint(req.unknown_line);
    const existing = templateRegistry.getByFingerprint(lineFp);
    if (existing) {
      const kind = (existing as RecordTemplate).field_map ? AIVerdict.RECORD_TEMPLATE : AIVerdict.RUBBISH_SIGNATURE;
      return { kind, template: existing };
    }

    // Step 3: Try to match against existing record templates by attempting to parse
    const recordMatch = templateRegistry.matchRecordTemplate(req.unknown_line, req.field_spec);
    if (recordMatch) {
      console.log("ai_classifier_local_match", { job_id: req.job_id, template_id: recordMatch.template_id });
      return { kind: AIVerdict.RECORD_TEMPLATE, template: recordMatch };
    }

    // Step 4: Try to match against rubbish templates
    const rubbishMatch = templateRegistry.matchRubbishTemplate(req.unknown_line);
    if (rubbishMatch) {
      console.log("ai_classifier_rubbish_match", { job_id: req.job_id, template_id: rubbishMatch.template_id });
      return { kind: AIVerdict.RUBBISH_SIGNATURE, template: rubbishMatch };
    }

    // Step 5: No local match found, fall back to Vertex AI
    console.log("ai_classifier_fallback_to_ai", { job_id: req.job_id, reason: "no_local_template_match" });
    
    const userPrompt = this.buildUserPrompt(req);
    try {
      const raw = await this.callVertexAI(userPrompt);
      let kindStr: string = (raw.kind as string) || "uncertain";
      
      // Handle structure names (csv, json, etc.) as record-template
      const structureNames = ["csv", "json", "kv", "fixed", "regex"];
      if (structureNames.includes(kindStr)) {
        kindStr = "record-template";
      }
      
      if (kindStr === "uncertain") return { kind: AIVerdict.UNCERTAIN };
      const tmpl = this.buildTemplateFromRaw(raw, kindStr, req.unknown_line);
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

  public async validateTemplate(req: ClassifyRequest, tmpl: RecordTemplate): Promise<boolean> {
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
}

export default AiClassifierServiceImpl;

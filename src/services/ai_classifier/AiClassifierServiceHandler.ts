import crypto from "crypto";
import { GoogleGenAI } from "@google/genai";
import { settings } from "@shared/Settings.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "@shared/TemplateRegistryService.js";
import { ClassifyRequest, ClassifyResponse, FieldLocator, CSVParseResult, AIVerdict } from "@service/ai_classifier/io/IAiClassifier.js";

/**
 * Extract JSON from a string that may be wrapped in markdown code fences
 * or contain explanatory text around the JSON object.
 */
function extractJsonFromMarkdown(raw: string): string {
  const trimmed = raw.trim();

  // Match fenced code blocks marked as json
  const fenceMatch = trimmed.match(/```(?:json)?\s*\n?([\s\S]*?)```/);
  if (fenceMatch) {
    return fenceMatch[1].trim();
  }

  // Find the first { and last } to extract a JSON object
  const firstBrace = trimmed.indexOf("{");
  const lastBrace = trimmed.lastIndexOf("}");
  if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
    return trimmed.slice(firstBrace, lastBrace + 1);
  }

  // Find the first [ and last ]
  const firstBracket = trimmed.indexOf("[");
  const lastBracket = trimmed.lastIndexOf("]");
  if (firstBracket !== -1 && lastBracket !== -1 && lastBracket > firstBracket) {
    return trimmed.slice(firstBracket, lastBracket + 1);
  }

  return trimmed;
}

/**
 * AI Classifier Service - Senior Level ORM-Style Implementation
 * 
 * This service provides AI-powered classification for unknown lines in the parsing pipeline.
 * It integrates with Vertex AI for template generation and includes local caching
 * to minimize API calls. Follows ORM-style patterns with:
 * - Class-based architecture with instance state
 * - Dependency injection for services
 * - Lifecycle management (initialize, start, stop)
 * - Repository-style methods for data operations
 * - Clean separation of concerns
 * 
 * @class AiClassifierService
 */
export class AiClassifierService {
  private static instance: AiClassifierService;
  
  // Instance state
  private running: boolean = false;
  private totalClassifications: number = 0;
  private cacheHits: number = 0;
  private cacheMisses: number = 0;
  private vertexAiCalls: number = 0;
  private mockClassifications: number = 0;
  private csvParseSuccesses: number = 0;
  private csvParseFailures: number = 0;
  private genAIClient: GoogleGenAI | null = null;
  
  /**
   * Private constructor for singleton pattern
   */
  private constructor() {
    // Initialization happens in initialize() method
  }
  
  /**
   * Get singleton instance
   */
  static getInstance(): AiClassifierService {
    if (!AiClassifierService.instance) {
      AiClassifierService.instance = new AiClassifierService();
    }
    return AiClassifierService.instance;
  }
  
  /**
   * Initialize the service
   */
  async initialize(): Promise<void> {
    await templateRegistry.loadFromDatabase();
    console.log("ai_classifier_initialized");
  }
  
  /**
   * Start the service
   */
  async start(): Promise<void> {
    if (this.running) {
      console.warn("ai_classifier_already_running");
      return;
    }
    
    this.running = true;
    await this.initialize();
    console.log("ai_classifier_started");
  }
  
  /**
   * Stop the service gracefully
   */
  async stop(): Promise<void> {
    this.running = false;
    console.log("ai_classifier_stopped");
  }
  
  /**
   * Get service statistics
   */
  getStats() {
    return {
      totalClassifications: this.totalClassifications,
      cacheHits: this.cacheHits,
      cacheMisses: this.cacheMisses,
      vertexAiCalls: this.vertexAiCalls,
      mockClassifications: this.mockClassifications,
      csvParseSuccesses: this.csvParseSuccesses,
      csvParseFailures: this.csvParseFailures
    };
  }

  /**
   * Call Vertex AI with a prompt and return the response text
   * 
   * @param prompt - The prompt to send to Vertex AI
   * @returns The response text from the model
   * @throws Error if the API call fails
   */
  private getGenAIClient(): GoogleGenAI {
    if (!this.genAIClient) {
      this.genAIClient = new GoogleGenAI({
        vertexai: true,
        project: settings.GCP_PROJECT_ID || "data-etl-499916",
        location: settings.VERTEX_LOCATION || "us-central1",
      });
    }
    return this.genAIClient;
  }

  private async askVertexAI(prompt: string, timeoutMs: number = 30000): Promise<string> {
    const MODEL = settings.VERTEX_MODEL || "gemini-2.5-flash";
    const ai = this.getGenAIClient();

    const generatePromise = ai.models.generateContent({
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

    const response = await Promise.race([
      generatePromise,
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error(`Vertex AI call timed out after ${timeoutMs}ms`)), timeoutMs)
      ),
    ]);

    const resp = response as { text?: string; candidates?: { content?: { parts?: { text?: string }[] } }[] };
    return resp.text
      ?? resp.candidates?.[0]?.content?.parts?.map((part) => part.text).join("")
      ?? "";
  }

  /**
   * System prompt for Vertex AI classification
   * Defines the rules and output format for the AI model
   */
  private readonly SYSTEM_PROMPT = `You are a data-parsing assistant embedded in a production file-parsing pipeline.
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

  /**
   * Try to parse a line as CSV with common delimiters
   * This is a fast path to avoid AI calls for simple CSV data
   * 
   * @param line - The line to parse
   * @param fieldSpec - Expected field specification
   * @returns Parse result with success status and delimiter
   */
  private tryParseAsCSV(line: string, fieldSpec: string[]): CSVParseResult {
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
          this.csvParseSuccesses++;
          return { success: true, delimiter, fields: parts };
        }
      }
    }
    
    console.log("csv_parser_failed", { reason: "no_delimiter_matched" });
    this.csvParseFailures++;
    return { success: false, delimiter: "", fields: [] };
  }

  /**
   * Create a template from a successful CSV parse
   * 
   * @param line - The line that was parsed
   * @param fieldSpec - Field specification
   * @param delimiter - The delimiter that matched
   * @returns Template object
   */
  private createTemplateFromCSV(line: string, fieldSpec: string[], delimiter: string): RecordTemplate {
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
      delimiter,
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

  /**
   * Validate that a template can successfully parse the given line
   * 
   * @param req - Classification request containing the line
   * @param tmpl - Template to validate
   * @returns True if template can parse the line, false otherwise
   */
  private async validateTemplate(req: ClassifyRequest, tmpl: RecordTemplate): Promise<boolean> {
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

  /**
   * Main AI classification function
   * 
   * This function implements a multi-step classification strategy:
   * 1. Try CSV parsing with common delimiters (fast path)
   * 2. Check fingerprint cache for existing templates
   * 3. Try to match against existing record templates
   * 4. Try to match against rubbish templates
   * 5. Use mock mode if enabled (for testing)
   * 6. Fall back to Vertex AI for new patterns
   * 
   * @param req - Classification request
   * @returns Classification response with template if successful
   */
  async classifyAi(req: ClassifyRequest): Promise<ClassifyResponse> {
    this.totalClassifications++;
    
    await templateRegistry.loadFromDatabase();

    // Step 1: Try CSV parsing with common delimiters before template matching
    const csvResult = this.tryParseAsCSV(req.unknown_line, req.field_spec);
    if (csvResult.success) {
      console.log("ai_classifier_csv_parse_success", { job_id: req.job_id, delimiter: csvResult.delimiter });
      // Create a template from the CSV parse result
      const template = this.createTemplateFromCSV(req.unknown_line, req.field_spec, csvResult.delimiter);
      await templateRegistry.saveTemplate(template, "record");
      templateRegistry.addRecordTemplate(template as RecordTemplate);
      return { kind: AIVerdict.RECORD_TEMPLATE, template };
    }

    // Step 2: Try to match by fingerprint (fast path)
    const lineFp = this.quickFingerprint(req.unknown_line);
    const existing = templateRegistry.getByFingerprint(lineFp);
    if (existing) {
      this.cacheHits++;
      const kind = (existing as RecordTemplate).field_map ? AIVerdict.RECORD_TEMPLATE : AIVerdict.RUBBISH_SIGNATURE;
      return { kind, template: existing };
    }
    
    this.cacheMisses++;

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

    // Step 5a: Mock mode — deterministic classifier, zero model cost. Used to validate the
    // inline-AI flow (learning + caching + budget) before switching to the real model.
    if (settings.AI_INLINE_MODE === "mock" || settings.BEDROCK_MODEL_ID === "mock") {
      this.mockClassifications++;
      const { mockClassify } = await import("./mock.js");
      const resp = mockClassify(req) as ClassifyResponse;
      if (resp.template) {
        const isRecord = "field_map" in resp.template;
        await templateRegistry.saveTemplate(resp.template, isRecord ? "record" : "rubbish");
        if (isRecord) templateRegistry.addRecordTemplate(resp.template as RecordTemplate);
        else templateRegistry.addRubbishTemplate(resp.template as RubbishTemplate);
        console.log("ai_classified_mock", { job_id: req.job_id, kind: resp.kind, template_id: resp.template.template_id });
      }
      return resp;
    }

    // Step 5: No local match found, fall back to Vertex AI
    console.log("ai_classifier_fallback_to_ai", { job_id: req.job_id, reason: "no_local_template_match" });

    const userPrompt = this.buildUserPrompt(req);
    try {
      this.vertexAiCalls++;
      const rawText = await this.askVertexAI(userPrompt);
      const raw = JSON.parse(extractJsonFromMarkdown(rawText)) as Record<string, unknown>;
      let kindStr = (raw.kind as string) || "uncertain";
      
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

  /**
   * Quick fingerprint for line matching
   * @param line - The line to fingerprint
   * @returns Fingerprint hash
   */
  private quickFingerprint(line: string): string {
    return crypto.createHash("md5").update(line).digest("hex");
  }

  /**
   * Build user prompt for Vertex AI classification
   * @param req - Classification request
   * @returns Formatted prompt string
   */
  private buildUserPrompt(req: ClassifyRequest): string {
    return `${this.SYSTEM_PROMPT}

Unknown line: ${req.unknown_line}
Field spec: ${req.field_spec.join(", ")}
${req.context_lines ? `Context lines:\n${req.context_lines.join("\n")}` : ""}`;
  }

  /**
   * Build template from raw AI response
   * @param raw - Raw AI response
   * @param kind - Template kind
   * @param line - Original line
   * @returns Template object or null
   */
  private buildTemplateFromRaw(raw: Record<string, unknown>, kind: string, line: string): RecordTemplate | RubbishTemplate | null {
    try {
      const template = raw.template as Record<string, unknown> | undefined;
      if (!template) return null;

      const base = {
        template_id: crypto.randomBytes(16).toString("hex"),
        fingerprint: this.quickFingerprint(line),
        version: 1,
        source: "ai" as const,
        created_at: new Date(),
      };
      if (kind === "record-template") {
        const fieldMap = template.field_map as Record<string, { locator: string; type: string }> | undefined;
        if (!fieldMap) return null;
        return {
          ...base,
          field_map: fieldMap,
          structure: (template.structure as string) || "csv",
          delimiter: template.delimiter as string | undefined,
          quote_char: template.quote_char as string | undefined,
          length_hint: (template.length_hint as number) ?? line.length,
        } as RecordTemplate;
      }
      return {
        ...base,
        signature: (template.signature as string) || "",
        confidence: (template.confidence as number) ?? 1,
      } as RubbishTemplate;
    } catch {
      return null;
    }
  }
}

// Backward compatibility: export singleton instance and function wrappers
const aiClassifierService = AiClassifierService.getInstance();

// Backward compatibility wrappers
export async function classifyAi(req: ClassifyRequest): Promise<ClassifyResponse> {
  return aiClassifierService.classifyAi(req);
}

// Export interfaces for external use
export type { ClassifyRequest, ClassifyResponse, FieldLocator, CSVParseResult, AIVerdict } from "@service/ai_classifier/io/IAiClassifier.js";

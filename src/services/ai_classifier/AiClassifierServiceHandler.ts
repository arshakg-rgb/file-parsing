import crypto from "crypto";
import {GenerateContentResponse, GoogleGenAI} from "@google/genai";
import { settings } from "@shared/Settings.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "@shared/TemplateRegistryService.js";
import { ClassifyRequest, ClassifyResponse, CSVParseResult, AIVerdict } from "@service/ai_classifier/io/IAiClassifier.js";
import {IClassifierStats, PersistKind} from "@service/ai_classifier/io/IClassifierStats.js";
import {Constants} from "@common/io/Constants.js";

export class AiClassifierService
{
  /**
   * Singleton instance holder.
   * @private
   */

  private static instance: AiClassifierService;

  /**
   * Whether the service has been started via {@link start}.
   * @private
   */

  private running: boolean = false;

  /**
   * Lazily-initialized Vertex AI client, created on first use by {@link getGenAIClient}.
   * @private
   */

  private genAIClient: GoogleGenAI | null = null;

  /**
   * Structured logger scoped to this service.
   * @public
   */

  public readonly logger: Logger;

  /**
   * Running counters surfaced via {@link getStats}.
   * @private
   */

  private readonly stats: IClassifierStats = {
    totalClassifications: 0,
    cacheHits: 0,
    cacheMisses: 0,
    vertexAiCalls: 0,
    mockClassifications: 0,
    csvParseSuccesses: 0,
    csvParseFailures: 0,
  };

  /**
   * Private constructor to enforce the singleton pattern. Use {@link getInstance} instead.
   */

  private constructor()
  {
    this.logger = createLogger("AiClassifierServiceHandler");
  }

  /**
   * Get the singleton instance, creating it on first call.
   * @returns The shared {@link AiClassifierService} instance.
   */
  public static getInstance(): AiClassifierService
  {
    if (!AiClassifierService.instance)
    {
      AiClassifierService.instance = new AiClassifierService();
    }
    return AiClassifierService.instance;
  }

  /**
   * Load templates from the database so the registry is warm before use.
   * @returns A promise that resolves once the template registry has loaded.
   */

  public async initialize(): Promise<void>
  {
    await templateRegistry.loadFromDatabase();
    this.logger.info("ai_classifier_initialized");
  }

  /**
   * Start the service. No-op (with a warning) if already running.
   * @returns A promise that resolves once startup (including {@link initialize}) completes.
   */

  public async start(): Promise<void>
  {
    if (this.running)
    {
      this.logger.warn("ai_classifier_already_running");
      return;
    }

    this.running = true;
    await this.initialize();
    this.logger.info("ai_classifier_started");
  }

  /**
   * Get a snapshot copy of the current classifier statistics.
   * @returns A shallow copy of the internal {@link IClassifierStats} counters.
   */

  public getStats(): IClassifierStats
  {
    return { ...this.stats };
  }

  /**
   * Classify an unknown line, escalating through fast paths before falling
   * back to Vertex AI. Wraps {@link runClassification} with entry/exit logging.
   *
   * @param req - Classification request containing the unknown line, job id,
   *              expected field spec, and optional surrounding context lines.
   * @returns A promise resolving to the classification verdict and, when
   *          applicable, the matched or newly-derived template.
   */

  public async classifyAi(req: ClassifyRequest): Promise<ClassifyResponse>
  {
    this.logger.info("classify_ai_handler_invoked", {
      job_id: req.job_id,
      line_length: req.unknown_line.length,
      context_lines: (req.context_lines || []).length,
    });

    const result: ClassifyResponse = await this.runClassification(req);

    this.logger.info("classify_ai_handler_done", { job_id: req.job_id, kind: result.kind, has_template: !!result.template });
    return result;
  }

  /**
   * Ask the model which source fields/paths correspond to the requested
   * target columns for a JSON record sample.
   *
   * @param jsonSamples - One or more raw JSON record samples to inspect.
   * @param targetColumns - Optional list of target column names to resolve against the sample's fields/paths.
   * @returns A promise resolving to the matched source field/path names (an empty array if none matched or the call failed).
   */

  public async discoverJsonFieldSpec(jsonSamples: string[], targetColumns?: string[]): Promise<string[]> {
    const joined = jsonSamples.map((s, i) => `Sample ${i + 1}: ${s}`).join("\n\n");
    const targetHint = targetColumns?.length
        ? ` Only return field/path names from the sample that correspond to one of these target columns: ${targetColumns.join(", ")}. If none match, return [].`
        : "";
    const prompt = `You are a data-parsing assistant. Given the following JSON record sample, suggest which fields to extract as the requested target columns. Output ONLY a JSON array of the exact source field/path names that match, no prose.${targetHint}\n\n${joined}\n\nOutput:`;

    try {
      const raw = await this.askVertexAI(prompt, 8000);
      const parsed = JSON.parse(AiClassifierService.extractJsonFromMarkdown(raw));
      return Array.isArray(parsed)
          ? parsed.filter((x): x is string => typeof x === "string" && x.trim() !== "")
          : [];
    } catch (err) {
      this.logger.warn("json_field_discovery_failed", { error: String(err) });
      return [];
    }
  }

  /**
   * Ask the model to parse a JSON record into the requested target columns,
   * plus a "meta" column holding any unmapped fields as a JSON string.
   *
   * @param jsonLine - The raw JSON record line to parse.
   * @param targetColumns - Target column names to extract; defaults to an
   *                        empty array, in which case all source keys are
   *                        returned as columns.
   * @returns A promise resolving to the parsed record keyed by target column
   *          (with a stringified "meta" field for unmapped data), or null on
   *          invalid JSON or a model failure.
   */

  public async parseJsonLine(jsonLine: string, targetColumns: string[] = []): Promise<Record<string, unknown> | null> {
    const targetHint = targetColumns.length
        ? `Target columns: ${targetColumns.join(", ")}.`
        : "No target columns were specified; return the most reasonable top-level/flattened fields as columns.";
    const prompt = `You are a JSON parsing assistant. Given the JSON record below, extract values and return ONLY a JSON object.
${targetHint}
For each target column, use the source field or path that best matches it; if nothing matches, use null.
Add a "meta" key containing a JSON-string of all source fields/paths NOT represented by the target columns.
If target columns are empty, return all source keys as columns and put any remaining nested/unmapped data in "meta".
Do not wrap the output in markdown; return raw JSON only.

JSON record:
${jsonLine}

Output:`;

    try {
      const raw = await this.askVertexAI(prompt, 8000);
      const parsed = JSON.parse(AiClassifierService.extractJsonFromMarkdown(raw));
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;

      if ("meta" in parsed && parsed.meta != null && typeof parsed.meta !== "string") {
        parsed.meta = JSON.stringify(parsed.meta);
      }
      return parsed as Record<string, unknown>;
    } catch (err) {
      this.logger.warn("json_line_parse_failed", { error: String(err) });
      return null;
    }
  }

  /**
   * Core classification pipeline, wrapped by {@link classifyAi} for entry/exit
   * logging. Escalates through CSV fast-path parsing, cache/registry
   * matching, mock mode, and finally Vertex AI.
   *
   * @param req - Classification request being processed.
   * @returns A promise resolving to the classification verdict/template.
   */
  private async runClassification(req: ClassifyRequest): Promise<ClassifyResponse>
  {
    this.logger.info("ai_classifier_build_marker", { marker: "v2-json-mode" });
    this.stats.totalClassifications++;
    this.logger.info("classify_ai_start", {
      job_id: req.job_id,
      line_length: req.unknown_line.length,
      field_spec: req.field_spec,
    });

    await templateRegistry.loadFromDatabase();

    const csvResponse: ClassifyResponse | null = await this.tryCsvFastPath(req);

    if (csvResponse)
    {
      return csvResponse;
    }

    const cachedResponse: ClassifyResponse | null = this.tryCacheAndRegistryMatch(req);

    if (cachedResponse)
    {
      return cachedResponse;
    }

    if (settings.AI_INLINE_MODE === "mock" || settings.BEDROCK_MODEL_ID === "mock")
    {
      return this.classifyWithMock(req);
    }

    return this.classifyWithVertexAI(req);
  }

  /**
   * Step 1: attempt fast-path CSV parsing against the expected field spec,
   * persisting a new record template on success.
   *
   * @param req - Classification request being processed.
   * @returns A promise resolving to a record-template response on success, or null if the line did not parse as CSV.
   */

  private async tryCsvFastPath(req: ClassifyRequest): Promise<ClassifyResponse | null>
  {
    const csvResult: CSVParseResult = this.tryParseAsCSV(req.unknown_line, req.field_spec);

    if (!csvResult.success)
    {
      return null;
    }

    this.logger.info("ai_classifier_csv_parse_success", { job_id: req.job_id, delimiter: csvResult.delimiter });
    const template: RecordTemplate = this.createTemplateFromCSV(req.unknown_line, req.field_spec, csvResult.delimiter);
    await this.persistAndRegisterTemplate(template, "record", req.job_id, "csv_fast_path");
    return { kind: AIVerdict.RECORD_TEMPLATE, template };
  }

  /**
   * Steps 2–4: check the fingerprint cache, then attempt to match the line
   * against known record templates, then known rubbish templates.
   *
   * @param req - Classification request being processed.
   * @returns The matched classification response, or null if nothing matched.
   */

  private tryCacheAndRegistryMatch(req: ClassifyRequest): ClassifyResponse | null
  {
    const fingerprint: string = this.quickFingerprint(req.unknown_line);
    const existing: RecordTemplate | RubbishTemplate | null = templateRegistry.getByFingerprint(fingerprint);

    if (existing)
    {
      this.stats.cacheHits++;
      const kind = (existing as RecordTemplate).field_map ? AIVerdict.RECORD_TEMPLATE : AIVerdict.RUBBISH_SIGNATURE;
      this.logger.info("ai_classifier_fingerprint_match", {
        job_id: req.job_id,
        fingerprint,
        template_id: existing.template_id,
        kind,
      });

      return { kind, template: existing };
    }

    this.stats.cacheMisses++;
    this.logger.info("ai_classifier_fingerprint_miss", { job_id: req.job_id, fingerprint });

    const recordMatch: RecordTemplate | null = templateRegistry.matchRecordTemplate(req.unknown_line, req.field_spec);

    if (recordMatch)
    {
      this.logger.info("ai_classifier_local_match", { job_id: req.job_id, template_id: recordMatch.template_id });
      return { kind: AIVerdict.RECORD_TEMPLATE, template: recordMatch };
    }

    const rubbishMatch: RubbishTemplate | null = templateRegistry.matchRubbishTemplate(req.unknown_line);

    if (rubbishMatch)
    {
      this.logger.info("ai_classifier_rubbish_match", { job_id: req.job_id, template_id: rubbishMatch.template_id });
      return { kind: AIVerdict.RUBBISH_SIGNATURE, template: rubbishMatch };
    }

    return null;
  }

  /**
   * Step 5a: deterministic mock classifier used to validate the pipeline
   * (learning, caching, budget accounting) without incurring model cost.
   *
   * @param req - Classification request being processed.
   * @returns A promise resolving to the mock classifier's response.
   */

  private async classifyWithMock(req: ClassifyRequest): Promise<ClassifyResponse>
  {
    const fingerprint: string = this.quickFingerprint(req.unknown_line);
    this.stats.mockClassifications++;
    this.logger.info("ai_classifier_mock_mode", { job_id: req.job_id, fingerprint });

    const { mockClassify } = await import("./mock.js");
    const response = mockClassify(req) as ClassifyResponse;

    if (response.template)
    {
      const kind: PersistKind = "field_map" in response.template ? "record" : "rubbish";
      await this.persistAndRegisterTemplate(response.template, kind, req.job_id, "mock");
      this.logger.info("ai_classified_mock", { job_id: req.job_id, kind: response.kind, template_id: response.template.template_id });
    }

    return response;
  }

  /**
   * Step 5: fall back to Vertex AI for genuinely novel lines. Parses and
   * validates the model's response, builds a template from it, persists the
   * result, and returns the final verdict.
   *
   * @param req - Classification request being processed.
   * @returns A promise resolving to the classification verdict/template, or an uncertain verdict on malformed output or a call failure.
   */

  private async classifyWithVertexAI(req: ClassifyRequest): Promise<ClassifyResponse>
  {
    const fingerprint: string = this.quickFingerprint(req.unknown_line);
    this.logger.info("ai_classifier_fallback_to_ai", { job_id: req.job_id, fingerprint, reason: "no_local_template_match" });

    const userPrompt: string = this.buildUserPrompt(req);

    try
    {
      this.stats.vertexAiCalls++;
      this.logger.info("vertex_ai_request_start", {
        job_id: req.job_id,
        fingerprint,
        prompt_length: userPrompt.length,
        model: settings.VERTEX_MODEL || "gemini-2.5-flash",
      });

      const rawText: string = await this.askVertexAI(userPrompt);
      this.logger.info("vertex_ai_response_raw", {
        job_id: req.job_id,
        fingerprint,
        response_length: rawText.length,
        response: rawText.slice(0, 2000),
      });

      const raw = this.parseVertexResponse(rawText, req.job_id, fingerprint);

      if (!raw)
      {
        return {kind: AIVerdict.UNCERTAIN};
      }

      let kindStr: string = (raw.kind as string) || "uncertain";

      if (Constants.STRUCTURE_NAMES.has(kindStr))
      {
        kindStr = "record-template";
      }

      if (kindStr === "uncertain")
      {
        this.logger.info("ai_classifier_uncertain", { job_id: req.job_id, fingerprint });
        return { kind: AIVerdict.UNCERTAIN };
      }

      const template: RecordTemplate | RubbishTemplate | null = this.buildTemplateFromRaw(raw, kindStr, req.unknown_line);

      if (!template)
      {
        this.logger.warn("ai_classifier_template_build_failed", { job_id: req.job_id, raw_kind: kindStr });
        return { kind: AIVerdict.UNCERTAIN };
      }

      const persistKind: PersistKind = kindStr === "record-template" ? "record" : "rubbish";
      await this.persistAndRegisterTemplate(template, persistKind, req.job_id, "ai_call");

      const verdict = kindStr === "record-template" ? AIVerdict.RECORD_TEMPLATE : AIVerdict.RUBBISH_SIGNATURE;
      this.logger.info("ai_classified", { job_id: req.job_id, verdict, template_id: template.template_id, fingerprint: template.fingerprint });
      return { kind: verdict, template };
    }
    catch (err)
    {
      this.logger.error("vertex_ai_call_failed", {
        job_id: req.job_id,
        error: err instanceof Error ? err.message : typeof err === "string" ? err : JSON.stringify(err),
        ...(err instanceof Error && err.stack ? { stack: err.stack } : {}),
      });

      return { kind: AIVerdict.UNCERTAIN };
    }
  }

  /**
   * Parse and log the raw Vertex AI response text as JSON.
   *
   * @param rawText - Raw response text returned by the model.
   * @param jobId - Job id associated with the originating request, forcorrelated logging.
   * @param fingerprint - Fingerprint of the line being classified, for correlated logging.
   * @returns The parsed response object, or null if the response was not valid JSON.
   */

  private parseVertexResponse(rawText: string, jobId: string | undefined, fingerprint: string): Record<string, unknown> | null {
    try
    {
      const parsed = JSON.parse(AiClassifierService.extractJsonFromMarkdown(rawText)) as Record<string, unknown>;

      this.logger.info("vertex_ai_response_parsed", {
        job_id: jobId,
        fingerprint,
        parsedKind: parsed.kind,
        parsedKeys: Object.keys(parsed).length,
      });

      return parsed;
    }
    catch (err)
    {
      this.logger.error("vertex_ai_json_parse_failed", {
        job_id: jobId,
        fingerprint,
        error: String(err),
        raw_response: rawText.slice(0, 500),
      });

      this.logger.info("ai_classifier_uncertain", { job_id: jobId, fingerprint, reason: "malformed_json" });
      return null;
    }
  }

  /**
   * Persist a newly-derived template to the registry's backing store and
   * warm the corresponding in-memory cache.
   *
   * @param template - The record or rubbish template to persist.
   * @param kind - Whether the template is a "record" or "rubbish" template.
   * @param jobId - Job id associated with the originating request, forcorrelated logging.
   * @param source - Where the template came from (e.g. "csv_fast_path","mock", "ai_call"), for correlated logging.
   * @returns A promise that resolves once the template has been saved and registered in memory.
   */

  private async persistAndRegisterTemplate(template: RecordTemplate | RubbishTemplate, kind: PersistKind, jobId: string | undefined, source: string): Promise<void>
  {
    await templateRegistry.saveTemplate(template, kind);
    this.logger.info("ai_template_saved", { job_id: jobId, template_id: template.template_id, kind, source });

    if (kind === "record")
    {
      templateRegistry.addRecordTemplate(template as RecordTemplate);
    }
    else
    {
      templateRegistry.addRubbishTemplate(template as RubbishTemplate);
    }
  }

  /**
   * Get the lazily-initialized Vertex AI client, creating it on first call.
   *
   * @returns The shared {@link GoogleGenAI} client instance.
   */

  private getGenAIClient(): GoogleGenAI
  {
    if (!this.genAIClient)
    {
      this.genAIClient = new GoogleGenAI({
        vertexai: true,
        project: settings.GCP_PROJECT_ID || "data-etl-499916",
        location: settings.VERTEX_LOCATION || "us-central1",
      });
    }

    return this.genAIClient;
  }

  /**
   * Call Vertex AI with a prompt and return the response text, racing the
   * call against a timeout so callers never hang indefinitely.
   *
   * @param prompt - The prompt to send to Vertex AI.
   * @param timeoutMs - Timeout in milliseconds before the call is aborted;defaults to 30000.
   * @returns A promise resolving to the model's response text.
   * @throws Error if the call times out or the underlying request fails.
   */

  private async askVertexAI(prompt: string, timeoutMs = 30000): Promise<string>
  {
    const model: string = settings.VERTEX_MODEL || "gemini-2.5-flash";
    const ai: GoogleGenAI = this.getGenAIClient();

    let timer: NodeJS.Timeout;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Vertex AI call timed out after ${timeoutMs}ms`)), timeoutMs);
    });

    try
    {
      const response: GenerateContentResponse = await Promise.race([
        ai.models.generateContent({
          model,
          contents: [{ role: "user", parts: [{ text: prompt }] }],
          config: {
            responseModalities: ["TEXT"],
            temperature: 0.2,
            maxOutputTokens: 2048,
            responseMimeType: "application/json",
          },
        }),
        timeout,
      ]);

      const resp = response as { text?: string; candidates?: { content?: { parts?: { text?: string }[] } }[] };
      return resp.text ?? resp.candidates?.[0]?.content?.parts?.map((part) => part.text).join("") ?? "";
    }
    finally
    {
      clearTimeout(timer!);
    }
  }

  /**
   * Build the full user prompt (system prompt + line + field spec + context)
   * sent to Vertex AI for classification.
   *
   * @param req - Classification request supplying the line, field spec, and optional context lines.
   * @returns The formatted prompt string.
   */

  private buildUserPrompt(req: ClassifyRequest): string
  {
    return `${Constants.SYSTEM_PROMPT}

Unknown line: ${req.unknown_line}
Field spec: ${req.field_spec.join(", ")}
${req.context_lines ? `Context lines:\n${req.context_lines.join("\n")}` : ""}`;
  }

  /**
   * Try to parse a line as CSV using each of the common delimiters, matching
   * against the expected number of fields.
   *
   * @param line - The line to parse.
   * @param fieldSpec - Expected field names; only the count is used to validate a candidate delimiter.
   * @returns The parse result, including the matched delimiter and split fields on success.
   */

  private tryParseAsCSV(line: string, fieldSpec: string[]): CSVParseResult
  {
    for (const delimiter of Constants.CSV_DELIMITERS)
    {
      const parts: string[] = line.split(delimiter);

      if (parts.length === fieldSpec.length && parts.every((part) => part.trim().length > 0))
      {
        this.logger.info("csv_parser_success", { delimiter, fields: parts });
        this.stats.csvParseSuccesses++;
        return { success: true, delimiter, fields: parts };
      }
    }

    this.logger.debug("csv_parser_failed", { line, fieldSpec });
    this.stats.csvParseFailures++;
    return { success: false, delimiter: "", fields: [] };
  }

  /**
   * Create a record template from a successful CSV parse.
   *
   * @param line - The line that was parsed.
   * @param fieldSpec - Field names, in order, matching the split CSV columns.
   * @param delimiter - The delimiter that matched during {@link tryParseAsCSV}.
   * @returns The newly-created {@link RecordTemplate}.
   */
  private createTemplateFromCSV(line: string, fieldSpec: string[], delimiter: string): RecordTemplate
  {
    const fieldMap: Record<string, { locator: string; type: string }> = {};
    fieldSpec.forEach((field, index) => {fieldMap[field] = {locator: `index:${index}`, type: "string"};});

    const template: RecordTemplate = {
      template_id: crypto.randomBytes(16).toString("hex"),
      fingerprint: this.quickFingerprint(line),
      version: 1,
      field_map: fieldMap,
      structure: "csv",
      delimiter,
      length_hint: line.length,
      source: "ai",
      created_at: new Date(),
    };

    this.logger.info("csv_template_created", {
      template_id: template.template_id,
      fieldMap,
      structure: template.structure,
      delimiter,
    });

    return template;
  }

  /**
   * Build a record or rubbish template from a raw, already-parsed Vertex AI
   * response object.
   *
   * @param raw - The parsed JSON response object from Vertex AI.
   * @param kind - The classification kind string (e.g. "record-template", "rubbish-signature") used to decide how to shape the template.
   * @param line - The original line, used for the fingerprint and as a fallback length hint.
   * @returns The constructed template, or null if the response was missing required fields or otherwise invalid.
   */

  private buildTemplateFromRaw(raw: Record<string, unknown>, kind: string, line: string): RecordTemplate | RubbishTemplate | null
  {
    const template = raw.template as Record<string, unknown> | undefined;

    if (!template)
    {
      return null;
    }

    const base = {
      template_id: crypto.randomBytes(16).toString("hex"),
      fingerprint: this.quickFingerprint(line),
      version: 1,
      source: "ai" as const,
      created_at: new Date(),
    };

    try
    {
      if (kind === "record-template")
      {
        const fieldMap = template.field_map as Record<string, { locator: string; type: string }> | undefined;

        if (!fieldMap)
        {
          return null;
        }

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
    }
    catch
    {
      return null;
    }
  }

  /**
   * Compute a quick fingerprint for line matching.
   *
   * @param line - The line to fingerprint.
   * @returns The MD5 hash of the line, as a hex string.
   */

  private quickFingerprint(line: string): string
  {
    return crypto.createHash("md5").update(line).digest("hex");
  }

  /**
   * Extract a JSON object/array from a model response that may be wrapped in
   * markdown code fences, contain surrounding prose, or use invalid escape
   * sequences.
   *
   * @param raw - The raw response text returned by the model.
   * @returns A best-effort, JSON-parseable string extracted from `raw`.
   */

  private static extractJsonFromMarkdown(raw: string): string
  {
    let trimmed: string = raw.trim();

    trimmed = trimmed.replace(/^```(?:json)?\s*\n?/, "").trim();
    trimmed = trimmed.replace(/\n?```\s*$/, "").trim();

    const firstBrace: number = trimmed.indexOf("{");
    const lastBrace: number = trimmed.lastIndexOf("}");

    if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace)
    {
      trimmed = trimmed.slice(firstBrace, lastBrace + 1);
    }
    else
    {
      const firstBracket: number = trimmed.indexOf("[");
      const lastBracket: number = trimmed.lastIndexOf("]");

      if (firstBracket !== -1 && lastBracket !== -1 && lastBracket > firstBracket)
      {
        trimmed = trimmed.slice(firstBracket, lastBracket + 1);
      }
    }

    return trimmed.replace(/\\(u[0-9a-fA-F]{4}|["\\/bfnrt])|\\/g, (match) => (match.length > 1 ? match : "\\\\"));
  }
}

export const aiClassifierService = AiClassifierService.getInstance();

export type { ClassifyRequest, ClassifyResponse, FieldLocator, CSVParseResult, AIVerdict } from "@service/ai_classifier/io/IAiClassifier.js";

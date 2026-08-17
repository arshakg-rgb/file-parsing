import pino from "pino";
import crypto from "crypto";
import {GenerateContentResponse, GoogleGenAI} from "@google/genai";
import { settings } from "@shared/Settings.js";
import { createLogger } from "@utils/logger/Log.js";
import { templateRegistry } from "@shared/TemplateRegistryService.js";
import { ClassifyRequest, ClassifyResponse, CSVParseResult, AIVerdict } from "@service/ai-classifier/io/IAiClassifier.js";
import {IClassifierStats, PersistKind} from "@service/ai-classifier/io/IClassifierStats.js";
import {Constants} from "@common/io/Constants.js";
import {RecordTemplate, RubbishTemplate} from "@shared/io/ITemplateRegistryService";
import {ColumnMap} from "@shared/models/job.js";
import SafeRegexUtils from "@utils/validator/SafeRegex";

export class AiClassifierServiceImpl
{
  /**
   * Singleton instance holder.
   * @private
   */

  private static instance: AiClassifierServiceImpl;

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

  public readonly logger: pino.Logger;

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
    this.logger = createLogger(module);
  }

  /**
   * Get the singleton instance, creating it on first call.
   * @returns The shared {@link AiClassifierService} instance.
   */

  public static getInstance(): AiClassifierServiceImpl
  {
    if (!AiClassifierServiceImpl.instance)
    {
      AiClassifierServiceImpl.instance = new AiClassifierServiceImpl();
    }

    return AiClassifierServiceImpl.instance;
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
      const parsed = JSON.parse(AiClassifierServiceImpl.extractJsonFromMarkdown(raw));
      return Array.isArray(parsed)
          ? parsed.filter((x): x is string => typeof x === "string" && x.trim() !== "")
          : [];
    } catch (err) {
      this.logger.warn("json_field_discovery_failed", { error: String(err) });
      return [];
    }
  }

  /**
   * Extract target field values from a JSON meta payload. The AI returns a
   * JSON object with the best-matching source values for each requested
   * target field. The returned object also includes a cleaned "meta" string
   * with the extracted fields removed, or null if no relevant data exists.
   *
   * @param metaJson - The raw meta JSON string to extract from.
   * @param targetColumns - Target field names to extract into.
   * @param jobId - Job id for logging.
   * @returns A promise resolving to an object with extracted values and a
   *          cleaned meta string, or null on failure/irrelevant data.
   */

  public async extractFromMeta(metaJson: string, targetColumns: string[], jobId: string): Promise<{ row: Record<string, unknown>; cleanedMeta: string | null } | null>
  {
    this.logger.info("ai_meta_extraction_start", { job_id: jobId, target_columns: targetColumns, meta_preview: metaJson.slice(0, 200) });

    const exampleFields = targetColumns.map((col) => `  "${col}": "value or null"`).join(",\n");
    const prompt = `You are a data extraction assistant. Extract relevant values from the following JSON object for these target fields: ${targetColumns.join(", ")}.

The meta JSON may contain any fields. Map each source field to the most semantically appropriate target field. For example:
- Postcode / postal code / zip values enrich the field that represents location or address
- CountryName / country / nation values enrich the field that represents location, address, or country
- first_name, last_name, full_name map to the corresponding name fields
- Any source field should be mapped to the most appropriate target field only once
- If a target field cannot be filled from the meta, set it to null

Return ONLY valid JSON in this exact shape (no markdown, no explanation):
{
${exampleFields},
  "extracted_meta_keys": ["sourceKey1", "sourceKey2"]
}

The "extracted_meta_keys" array must list the source JSON keys that were used for extraction, so they can be removed from the original meta. Include only keys that were actually used.

Meta JSON:
${metaJson}

Output:`;

    try
    {
      const raw = await this.askVertexAI(prompt);
      const jsonStr = AiClassifierServiceImpl.extractJsonFromMarkdown(raw);
      const parsed = JSON.parse(jsonStr) as Record<string, unknown>;

      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
      {
        this.logger.warn("ai_meta_extraction_invalid_response", { job_id: jobId, raw });
        return null;
      }

      const row: Record<string, unknown> = {};
      for (const col of targetColumns)
      {
        if (col in parsed)
        {
          row[col] = parsed[col] === undefined ? null : parsed[col];
        }
      }

      const extractedKeys: string[] = Array.isArray(parsed.extracted_meta_keys)
          ? (parsed.extracted_meta_keys as unknown[]).filter((k): k is string => typeof k === "string")
          : [];

      let cleanedMeta: string | null;
      try
      {
        const metaObj = JSON.parse(metaJson) as Record<string, unknown>;
        for (const k of extractedKeys)
        {
          delete metaObj[k];
        }
        cleanedMeta = Object.keys(metaObj).length > 0 ? JSON.stringify(metaObj) : null;
      }
      catch
      {
        cleanedMeta = metaJson;
      }

      this.logger.info("ai_meta_extraction_success", { job_id: jobId, extracted_keys: extractedKeys, cleaned_meta: cleanedMeta });
      return { row, cleanedMeta };
    }
    catch (err)
    {
      const errorMessage = err instanceof Error ? err.message : (typeof err === "string" ? err : JSON.stringify(err));
      this.logger.error("ai_meta_extraction_failed", { job_id: jobId, error: errorMessage });
      return null;
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
    const targetHint: string = targetColumns.length
        ? `Target columns: ${targetColumns.join(", ")}.`
        : "No target columns were specified; return the most reasonable top-level/flattened fields as columns.";
    const prompt = `You are a JSON parsing assistant. Given the JSON record below, extract values and return ONLY a JSON object.
${targetHint}
For each target column, choose the single best-matching source field or path; if nothing matches, use null.
If a logical field is split across multiple source keys (e.g. address1/address2, or street+city+zip for "location"), COMBINE them into one string in the order they would naturally appear, joined by ", ".
If two source keys are equally plausible for the same target (e.g. "phone" and "phone_number" both present), prefer the more complete/valid-looking value, and put the other in "meta".
If nothing matches, use null.
Add a "meta" key containing a JSON-string of all source fields/paths NOT represented by the target columns.
If target columns are empty, return all source keys as columns and put any remaining nested/unmapped data in "meta".
Do not wrap the output in markdown; return raw JSON only.

JSON record:
${jsonLine}

Output:`;

    try
    {
      const raw: string = await this.askVertexAI(prompt, 8000);
      const parsed = JSON.parse(AiClassifierServiceImpl.extractJsonFromMarkdown(raw));

      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
      {
        return null;
      }

      if ("meta" in parsed && parsed.meta != null && typeof parsed.meta !== "string")
      {
        parsed.meta = JSON.stringify(parsed.meta);
      }

      return parsed as Record<string, unknown>;
    }
    catch (err)
    {
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

    const recordMatch: RecordTemplate | null = templateRegistry.matchRecordTemplate(req.unknown_line);

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

    const { mockClassify } = await import("../mock.js");
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

      const rawText: string = await this.askVertexAI(userPrompt, Math.max(1000, settings.AI_CLASSIFY_TIMEOUT_MS - 2500));
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

  private parseVertexResponse(rawText: string, jobId: string | undefined, fingerprint: string): Record<string, unknown> | null
  {
    try
    {
      const parsed = JSON.parse(AiClassifierServiceImpl.extractJsonFromMarkdown(rawText)) as Record<string, unknown>;

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

  private static csvQuoteFor(delim: string): string
  {
    return delim === "\t" ? "" : "\"";
  }

  private static parseCsvLine(line: string, delim: string, quoteChar: string = "\""): string[]
  {
    const quote: string | null = quoteChar || null;
    const parts: string[] = [];
    let current: string = "";
    let inQuote: boolean = false;

    for (let i = 0; i < line.length; i++)
    {
      const c: string = line[i];
      const next: string = line[i + 1];

      if (quote && c === quote)
      {
        if (inQuote && next === quote)
        {
          current += quote;
          i++;
        }
        else
        {
          inQuote = !inQuote;
        }
      }
      else if (c === delim && !inQuote)
      {
        parts.push(current.trim());
        current = "";
      }
      else
      {
        current += c;
      }
    }

    parts.push(current.trim());

    return parts;
  }

  /**
   * Ask Vertex AI to map CSV header columns to the target field_spec.
   * Returns an object where keys are field_spec fields and values are arrays
   * of 0-based column indices. Unmapped columns are ignored.
   *
   * @param headerLine - The CSV header line (e.g. "email,phone,address_1,address_2,city").
   * @param fieldSpec - Ordered target field names (e.g. ["email","phone","address","location"]).
   * @param jobId - Job id for logging.
   * @returns A promise resolving to a column mapping, or null if the model fails or refuses.
   */

  public async mapHeaderColumns(headerLine: string, fieldSpec: string[], jobId: string): Promise<Record<string, number[]> | null>
  {
    this.logger.info("ai_header_mapping_start", { job_id: jobId, header_line: headerLine, field_spec: fieldSpec });

    const best: { delim: string; parts: string[] } | undefined = Constants.CSV_DELIMITERS
      .map((d) => ({
        delim: d,
        parts: AiClassifierServiceImpl.parseCsvLine(headerLine, d, AiClassifierServiceImpl.csvQuoteFor(d)),
      }))
      .sort((a, b) => b.parts.length - a.parts.length)[0];

    const headerParts: string[] = best?.parts.map((h) => h.trim()) ?? [headerLine.trim()];

    const headerList: string = headerParts.map((h, i) => `${i}: ${h}`).join("\n");

    const prompt = `You are a data mapping assistant.
Given the source column names (with 0-based indices, in any language/script) and the target field specification, return a JSON mapping of target field names to arrays of source column indices.

Guidelines:
- A source column must map to exactly one target field, or to none at all.
- Evaluate every column independently by its own meaning. Never assign a column to a field just because it sits next to (before/after) a column that DID match.
- Only group multiple source columns into one target field when they are OBVIOUSLY split parts of the SAME value (e.g. address_1 + address_2, or first_name + last_name). Never group columns of different semantic types together (e.g. do not combine a name column with a birthdate, ID, or passport column just because they are adjacent).
  - "address" = street, house number, address1, address2, road, lane, block, unit, etc.
  - "location" = city, town, county, state, province, postcode, zip, country, country_name.
  - "name" = first_name, last_name, given_name, surname, full_name, etc.
  - "phone" = telephone, mobile, cell, contact_number (NOT fax).
  - "email" = email, email_address, mail.
- NEVER map any of the following to a target field, in ANY language: national ID / insurance numbers (e.g. SSN, SNILS, СНИЛС), tax IDs (e.g. TIN, INN, ИНН), passport or other document numbers (e.g. ПАСПОРТ), dates of birth (e.g. ДАТА_РОЖДЕНИЯ), employer/company/insurer names (e.g. РАБОТОДАТЕЛИ, insurer, insurer_inns, insurer_names), or patronymic. These are unrelated ID/admin/metadata columns and must be omitted from every field's mapping so they fall through to the meta column automatically.
- Do NOT include a "meta" mapping.

Source columns:
${headerList}

Target field specification: ${fieldSpec.join(", ")}

Return ONLY valid JSON in this exact shape (no markdown, no explanation):
{
  "mappings": {
    "fieldName": [0, 1]
  }
}

If a field has no matching source columns, omit it from "mappings".`;

    try
    {
      const raw: string = await this.askVertexAI(prompt);
      const jsonStr: string = AiClassifierServiceImpl.extractJsonFromMarkdown(raw);
      const parsed = JSON.parse(jsonStr) as Record<string, unknown>;

      if (!parsed || typeof parsed !== "object" || !parsed.mappings || typeof parsed.mappings !== "object")
      {
        this.logger.warn("ai_header_mapping_invalid_response", { job_id: jobId, raw });
        return null;
      }

      const mappings = parsed.mappings as Record<string, unknown>;
      const out: Record<string, number[]> = {};

      for (const [field, idxs] of Object.entries(mappings))
      {
        if (!fieldSpec.includes(field))
        {
          continue;
        }

        if (Array.isArray(idxs) && idxs.every((n) => typeof n === "number" && n >= 0))
        {
          const filtered: number[] = (idxs as number[]).filter((i) => {
            const label: string | undefined = headerParts[i];
            return label === undefined || !AiClassifierServiceImpl.isDenylistedHeader(label);
          });

          if (filtered.length > 0)
          {
            out[field] = filtered;
          }
        }
      }

      this.logger.info("ai_header_mapping_success", { job_id: jobId, mapping: out });
      return out;
    }
    catch (err)
    {
      const errorMessage = err instanceof Error ? err.message : (typeof err === "string" ? err : JSON.stringify(err));
      this.logger.error("ai_header_mapping_failed", { job_id: jobId, error: errorMessage });
      return null;
    }
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
        const rawFieldMap = template.field_map as Record<string, Record<string, unknown>> | undefined;

        if (!rawFieldMap)
        {
          return null;
        }

        let parsedJson: Record<string, unknown> | null = null;
        const trimmed = line.trim();
        let structure = (template.structure as string) || "csv";

        if (trimmed.startsWith("{") || trimmed.startsWith("["))
        {
          try
          {
            const parsed = JSON.parse(trimmed);
            if (parsed && typeof parsed === "object" && !Array.isArray(parsed))
            {
              parsedJson = parsed;
              structure = "json";
            }
          }
          catch
          {
            // leave structure as-is
          }
        }

        const findBestKey = (target: string, keys: string[]): string | undefined =>
        {
          const normalizedTarget = target.toLowerCase();

          for (const key of keys)
          {
            if (key.toLowerCase() === normalizedTarget)
            {
              return key;
            }
          }

          for (const key of keys)
          {
            const lower = key.toLowerCase();
            if (lower.includes(normalizedTarget) || normalizedTarget.includes(lower))
            {
              return key;
            }
          }

          return undefined;
        };

        const jsonKeys = parsedJson ? Object.keys(parsedJson) : [];
        const fieldMap: Record<string, { locator: string; type: string }> = {};

        for (const [field, rawLoc] of Object.entries(rawFieldMap))
        {
          let locString = "";
          let type = "string";

          if (typeof rawLoc === "string")
          {
            locString = rawLoc;
          }
          else if (rawLoc && typeof rawLoc === "object")
          {
            const locObj = rawLoc as Record<string, unknown>;

            if (typeof locObj.locator === "string")
            {
              locString = locObj.locator;
              if (typeof locObj.type === "string")
              {
                type = locObj.type;
              }
            }
            else if (typeof locObj.key === "string")
            {
              const bestKey = parsedJson ? findBestKey(locObj.key, jsonKeys) : undefined;
              locString = `key:${bestKey ?? locObj.key}`;
            }
            else if (typeof locObj.index === "number" && parsedJson)
            {
              const bestKey = findBestKey(field, jsonKeys);
              const fallback = jsonKeys[locObj.index];
              const chosen = bestKey ?? fallback;
              locString = chosen ? `key:${chosen}` : `index:${locObj.index}`;
            }
            else if (typeof locObj.regex === "string" && !parsedJson)
            {
              locString = `regex:${locObj.regex}`;
            }
            else if (typeof locObj.regex === "string")
            {
              locString = `regex:${locObj.regex}`;
            }

            if (typeof locObj.type === "string")
            {
              type = locObj.type;
            }
          }

          fieldMap[field] = { locator: locString, type };
        }

        return {
          ...base,
          field_map: fieldMap,
          structure,
          delimiter: template.delimiter as string | undefined,
          quote_char: template.quote_char as string | undefined,
          length_hint: (template.length_hint as number) ?? line.length,
        } as RecordTemplate;
      }

      const aiSignature: string = (template.signature as string) || "";
      const signature: string = this.tightenRubbishSignature(aiSignature, line);

      return {
        ...base,
        signature,
        confidence: (template.confidence as number) ?? 1,
      } as RubbishTemplate;
    }
    catch
    {
      return null;
    }
  }

  /**
   * Guards against an AI-proposed rubbish signature being too generic and
   * blast-radius-matching unrelated, genuinely valid lines that merely share
   * the same coarse shape (e.g. "url:user:pass"). Rubbish templates are
   * persisted globally and re-applied to every future line in every job, so
   * a single overly-broad regex here can silently drop huge amounts of good
   * data across unrelated jobs. If the AI's regex doesn't contain a long
   * enough literal (non-wildcard) run, or fails to actually match the line
   * that triggered it, fall back to an exact-literal signature scoped only
   * to that one line.
   *
   * @param aiSignature - The regex signature proposed by the model.
   * @param line - The original triggering line.
   * @returns A signature safe to persist and reuse globally.
   */

  private tightenRubbishSignature(aiSignature: string, line: string): string
  {
    if (
        aiSignature &&
        SafeRegexUtils.hasSpecificLiteralRun(aiSignature) &&
        SafeRegexUtils.safeRegexTest(aiSignature, line)
    )
    {
      return aiSignature;
    }

    return SafeRegexUtils.escapeRegexLiteral(line);
  }

  /**
   * Compute a quick fingerprint for line matching.
   *
   * @param line - The line to fingerprint.
   * @returns The MD5 hash of the line, as a hex string.
   */

  /**
   * Ask Vertex AI to suggest source-side aliases/labels for a user's target
   * field_spec, AND, independently, which target fields are "composite" —
   * built by concatenating 2+ separate source columns (e.g. "full name" from
   * "first name" + "last name", or "full address" from "street" + "city" +
   * "zip"). This lets the parser dynamically combine split source columns
   * for ANY target field the AI identifies as composite, not just names,
   * without any hardcoded per-field logic.
   *
   * @param fieldSpec - The ordered target field names from the job.
   * @param jobId - Job id for logging.
   * @returns Aliases (target field -> candidate source labels) and composites (target field -> ordered source component labels to concatenate), or null on failure.
   */

  public async resolveFieldAliases(fieldSpec: string[], jobId: string): Promise<{ aliases: Record<string, string[]>; composites: Record<string, string[]> } | null>
  {
    this.logger.info("ai_resolve_field_aliases_start", { job_id: jobId, field_spec: fieldSpec });

    const prompt = `You are a data field matching assistant.
Given the following target field names, return a JSON object with two parts:

1. "aliases": for each target field, a list of likely source field names, labels, or aliases (in any language) that mean the SAME single concept as that field. Include common variations, abbreviations, and synonyms. Do not include unrelated ID/admin columns.

2. "composites": for each target field that is normally built by concatenating 2+ SEPARATE source columns (not just a single-value synonym), list those component source labels in the natural order they should be joined. For example a "full name" field is often composed from separate "first name" and "last name" columns; a "full address" field might be composed from "street", "city", and "postal code" columns. If a target field is typically a single source value (like "email" or "phone"), omit it from "composites" or give it an empty list.

Return ONLY valid JSON in this exact shape (no markdown, no explanation):
{
  "aliases": {
    "field name 1": ["alias1", "alias2", "alias3"],
    "field name 2": ["alias1", "alias2"]
  },
  "composites": {
    "field name 1": ["component1", "component2"]
  }
}

Target fields:
${fieldSpec.join("\n")}

Output:`;

    try
    {
      const raw: string = await this.askVertexAI(prompt);
      const jsonStr: string = AiClassifierServiceImpl.extractJsonFromMarkdown(raw);
      const parsed = JSON.parse(jsonStr) as Record<string, unknown>;

      if (!parsed || typeof parsed !== "object" || !parsed.aliases || typeof parsed.aliases !== "object" || Array.isArray(parsed.aliases))
      {
        this.logger.warn("ai_resolve_field_aliases_invalid_response", { job_id: jobId, raw });
        return null;
      }

      const aliases = parsed.aliases as Record<string, unknown>;
      const outAliases: Record<string, string[]> = {};

      for (const [field, candidates] of Object.entries(aliases))
      {
        if (!fieldSpec.includes(field)) continue;
        if (Array.isArray(candidates))
        {
          outAliases[field] = candidates.filter((x): x is string => typeof x === "string").map((a) => String(a).trim()).filter((a) => a !== "");
        }
      }

      const outComposites: Record<string, string[]> = {};
      const composites = parsed.composites;

      if (composites && typeof composites === "object" && !Array.isArray(composites))
      {
        for (const [field, parts] of Object.entries(composites as Record<string, unknown>))
        {
          if (!fieldSpec.includes(field)) continue;
          if (Array.isArray(parts))
          {
            const cleaned: string[] = parts.filter((x): x is string => typeof x === "string").map((a) => String(a).trim()).filter((a) => a !== "");
            if (cleaned.length > 1)
            {
              outComposites[field] = cleaned;
            }
          }
        }
      }

      this.logger.info("ai_resolve_field_aliases_success", { job_id: jobId, aliases: outAliases, composites: outComposites });
      return { aliases: outAliases, composites: outComposites };
    }
    catch (err)
    {
      const errorMessage = err instanceof Error ? err.message : (typeof err === "string" ? err : JSON.stringify(err));
      this.logger.error("ai_resolve_field_aliases_failed", { job_id: jobId, error: errorMessage });
      return null;
    }
  }

  /**
   * Ask the model to detect the delimiter and infer a label for every column of a
   * headerless delimited file, plus a field_map locating any requested target fields
   * it can confidently identify by column position. Used as a last-resort structural
   * probe when local heuristics (fixed column_map, detected header row, KV/JSON) all
   * fail to identify the file's structure — so the pipeline never needs a hardcoded,
   * file-specific column layout.
   *
   * @param sampleLines - A handful of raw sample lines from the file (no header row present).
   * @param fieldSpec - The ordered target field names the job wants extracted.
   * @param jobId - Job id for logging.
   * @returns The detected header labels (one per column, in order) and a field_map of
   *          any target fields the model located, or null on failure/no usable response.
   */

  public async inferHeadersFromSample(sampleLines: string[], fieldSpec: string[], jobId: string): Promise<{ headers: string[]; fieldMap: ColumnMap } | null>
  {
    if (sampleLines.length === 0)
    {
      return null;
    }

    this.logger.info("ai_infer_headers_start", { job_id: jobId, sample_count: sampleLines.length, field_spec: fieldSpec });

    const prompt = `You are a data-structure detection assistant. You are given raw sample lines from a delimited text file that has NO header row — every line is raw data, never a header.

Target fields the pipeline wants to extract: ${fieldSpec.join(", ") || "(none specified — propose your own best-guess semantic labels for every column)"}.

Tasks:
1. Detect the delimiter used to separate columns (e.g. "|", ",", ";", ":", "\\t").
2. Assign a short label to EVERY column, in left-to-right order. If a column clearly corresponds to one of the target fields, use that EXACT target field name as its label. NEVER use a raw sample value as a label and NEVER invent an intermediate label like "username_or_email" — if a column is a username, email, login, phone, account name or any other account identifier and one of the target fields represents such an identifier, use that target field's exact name. Only fall back to a generic label like "col_2" if the content gives no semantic clue and no target field matches.
3. Build a "field_map": for each target field you confidently located, its 0-based column index. Include EVERY target field that a column represents. If no target fields were given, return an empty object for "field_map".

Return ONLY valid JSON in this exact shape (no markdown, no explanation):
{
  "delimiter": "|",
  "headers": ["label_for_col_0", "label_for_col_1", "label_for_col_2"],
  "field_map": { "target_field_name": 0 }
}

Sample lines:
${sampleLines.join("\n")}

Output:`;

    const MAX_ATTEMPTS: number = 3;
    let lastErrorMessage: string = "unknown";

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)
    {
      try
      {
        const raw: string = await this.askVertexAI(prompt, 8000);
        const jsonStr: string = AiClassifierServiceImpl.extractJsonFromMarkdown(raw);
        const parsed = JSON.parse(jsonStr) as Record<string, unknown>;

        if (!parsed || typeof parsed !== "object" || !Array.isArray(parsed.headers) || parsed.headers.length === 0)
        {
          lastErrorMessage = "invalid_response";
          this.logger.warn("ai_infer_headers_invalid_response", { job_id: jobId, attempt, raw });
          continue;
        }

        const headers: string[] = (parsed.headers as unknown[])
          .map((h) => String(h ?? "").trim())
          .map((h, i) => h || `col_${i}`);

        const fieldMap: ColumnMap = {};
        const rawFieldMap: unknown = parsed.field_map;

        if (rawFieldMap && typeof rawFieldMap === "object" && !Array.isArray(rawFieldMap))
        {
          for (const [field, idx] of Object.entries(rawFieldMap as Record<string, unknown>))
          {
            if (!fieldSpec.includes(field))
            {
              continue;
            }

            if (typeof idx === "number" && Number.isInteger(idx) && idx >= 0 && idx < headers.length)
            {
              fieldMap[field] = idx;
            }
            else if (Array.isArray(idx))
            {
              const idxs: number[] = idx.filter((n): n is number => typeof n === "number" && Number.isInteger(n) && n >= 0 && n < headers.length);

              if (idxs.length > 0)
              {
                fieldMap[field] = idxs;
              }
            }
          }
        }

        if (fieldSpec.length > 0 && Object.keys(fieldMap).length === 0)
        {
          lastErrorMessage = "no_field_map";
          this.logger.warn("ai_infer_headers_no_field_map", { job_id: jobId, attempt, headers });
          continue;
        }

        this.logger.info("ai_infer_headers_success", { job_id: jobId, attempt, headers, field_map: fieldMap });
        return { headers, fieldMap };
      }
      catch (err)
      {
        lastErrorMessage = err instanceof Error ? err.message : (typeof err === "string" ? err : JSON.stringify(err));
        this.logger.warn("ai_infer_headers_attempt_failed", { job_id: jobId, attempt, error: lastErrorMessage });
      }
    }

    this.logger.error("ai_infer_headers_failed", { job_id: jobId, attempts: MAX_ATTEMPTS, error: lastErrorMessage });
    return null;
  }

  private quickFingerprint(line: string): string
  {
    return crypto.createHash("md5").update(line).digest("hex");
  }

  /**
   * Header labels matching this pattern represent unrelated ID/admin/metadata
   * columns (national ID or insurance numbers, tax IDs, passport/document
   * numbers, dates of birth, employer names) and must never be accepted into
   * a target field mapping, regardless of what the model returned. Covers
   * common English terms plus their Russian equivalents since these labels
   * frequently appear in Cyrillic source files.
   * @private
   */

  private static readonly DENYLISTED_HEADER_RE: RegExp = /snils|снилс|passport|паспорт|\\btin\\b|\\binn\\b|инн|\\bssn\\b|national.?id|insurance.?number|insurer|employer|работодат|patronymic|date.?of.?birth|\\bdob\\b|birth.?date|дата.?рожд/i;

  /**
   * Does a source column header represent an unrelated ID/admin/metadata
   * column that must never be mapped to a target field?
   *
   * @param label - The raw source column header label.
   * @returns `true` if the label matches a denylisted ID/DOB/passport/tax/employer pattern.
   */

  private static isDenylistedHeader(label: string): boolean
  {
    return AiClassifierServiceImpl.DENYLISTED_HEADER_RE.test(label);
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
    let text: string = raw.trim();

    text = text.replace(/^```(?:json)?\s*\n?/, "").trim();
    text = text.replace(/\n?```\s*$/, "").trim();

    try
    {
      JSON.parse(text);
      return text;
    }
    catch
    {
      // continue to extraction
    }

    const extractBalanced = (open: string, close: string): string | null =>
    {
      let depth: number = 0;
      let start: number = -1;
      let inString: boolean = false;
      let escape: boolean = false;

      for (let i = 0; i < text.length; i++)
      {
        const c: string = text[i];
        if (inString)
        {
          if (escape)
          {
            escape = false;
          }
          else if (c === "\\")
          {
            escape = true;
          }
          else if (c === "\"")
          {
            inString = false;
          }
        }
        else
        {
          if (c === "\"")
          {
            inString = true;
          }
          else if (c === open)
          {
            if (depth === 0) start = i;
            depth++;
          }
          else if (c === close)
          {
            if (depth > 0) depth--;
            if (depth === 0 && start !== -1) return text.slice(start, i + 1);
          }
        }
      }
      return null;
    };

    const jsonObj: string = extractBalanced("{", "}");

    if (jsonObj)
    {
      return jsonObj;
    }

    const jsonArr: string = extractBalanced("[", "]");

    if (jsonArr)
    {
      return jsonArr;
    }

    return text.replace(/\\(u[0-9a-fA-F]{4}|["\\/bfnrt])|\\/g, (match) => (match.length > 1 ? match : "\\\\"));
  }
}

export const aiClassifierServiceImpl: AiClassifierServiceImpl = AiClassifierServiceImpl.getInstance();

export type { ClassifyRequest, ClassifyResponse, FieldLocator, CSVParseResult, AIVerdict } from "@service/ai-classifier/io/IAiClassifier.js";

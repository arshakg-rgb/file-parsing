/**
 * Global constants for the application.
 */
export class Constants
{
  public static readonly ENVIRONMENTS: Record<string, string> = {
    PRODUCTION: "production",
    STAGING: "staging",
    DEVELOPMENT: "development"
  };

  public static readonly MAX_STRING_LENGTH: number = 255;
  public static readonly SIGINT: string = "SIGINT";
  public static readonly SIGTERM: string =  "SIGTERM";

  public static readonly SYSTEM_PROMPT = `You are a data-parsing assistant embedded in a production file-parsing pipeline.
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
10. Regex patterns inside "regex" fields MUST use double backslashes for JSON validity (e.g. write "\\d+" or "\\s+", not "\d+" or "\s+"), since this is a JSON string, not a raw regex literal.

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

  public static readonly STRUCTURE_NAMES: Set<string> = new Set(["csv", "json", "kv", "fixed", "regex"]);
  public static readonly CSV_DELIMITERS = [",", ";", "\t", "|"] as const;
}

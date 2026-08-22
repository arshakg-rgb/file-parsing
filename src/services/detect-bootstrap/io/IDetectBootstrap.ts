import {RecordTemplate, RubbishTemplate} from "@shared/io/ITemplateRegistryService";

export interface ClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines?: string[];
  job_id?: string;
}

export interface ClassifyResponse {
  kind: "record-template" | "rubbish-signature" | "uncertain";
  template?: RecordTemplate | RubbishTemplate;
}

export interface IDetectBootstrap {
  detectBootstrap(req: ClassifyRequest): Promise<ClassifyResponse>;
}


/**
 * Classification request interface
 */
export interface ClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines?: string[];
  job_id?: string;
}

export type ClassifyKind = "record-template" | "rubbish-signature" | "uncertain";


/**
 * Result of probing a single window of the file
*/

export interface ProbeResult {
  fingerprint: string;
  templateId: string | null;
}

/**
 * Result of stripping a header line from a set of sample lines
*/

export interface HeaderStripResult {
  dataLines: string[];
  hadHeader: boolean;
  headerLine?: string;
}

/**
 * Header patterns for comma/semicolon/tab/pipe-delimited files.
 * A "header line" is a delimited line where every field looks like an identifier
 * (letters/digits/underscore, starting with a letter or underscore) rather than data.
 * Optional surrounding whitespace per field is tolerated so fixed-width, padded
 * exports (e.g. "  userid   |  fullname  | ...") are still recognized as headers.
 */

export const HEADER_PATTERNS: RegExp[] = [
  /^\s*[a-zA-Z_][a-zA-Z0-9_]*\s*(,\s*[a-zA-Z_][a-zA-Z0-9_]*\s*)+$/, // comma-delimited
  /^\s*[a-zA-Z_][a-zA-Z0-9_]*\s*(;\s*[a-zA-Z_][a-zA-Z0-9_]*\s*)+$/, // semicolon-delimited
  /^\s*[a-zA-Z_][a-zA-Z0-9_]*\s*(\t\s*[a-zA-Z_][a-zA-Z0-9_]*\s*)+$/, // tab-delimited
  /^\s*[a-zA-Z_][a-zA-Z0-9_]*\s*(\|\s*[a-zA-Z_][a-zA-Z0-9_]*\s*)+$/, // pipe-delimited
];

/**
 * Delimiters checked, in priority order, when fingerprinting a probe window
*/
export const CSV_DELIMITERS: string[] = [",", ";", "\t", "|"];


/**
 * Classification response interface
 */
export interface ClassifyResponse {
  kind: ClassifyKind;
  template?: RecordTemplate | RubbishTemplate;
}

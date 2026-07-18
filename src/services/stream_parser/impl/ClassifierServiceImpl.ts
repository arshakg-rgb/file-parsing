import { settings } from "../../../shared/config.js";
import { _FailureClass } from "../../../shared/models/job.js";
import { RecordTemplate, RubbishTemplate } from "../../../shared/templateRegistry.js";
import { safeRegex, safeRegexTest } from "../../../utils/validator/safeRegex.js";
import ServiceManager from "../../../config/ServiceManager.js";
import { Enforce } from "../../../config/ServiceManager.js";
import { InstantiationError } from "../../../errors/InstantiationError.js";
import { ClassifierService } from "../ClassifierService.js";
import { ClassifyRequest, ClassifyResult } from "../io/IClassifier.js";
import { FIELD_ALIASES, DELIMITERS, MAX_LINE_LENGTH, BINARY_THRESHOLD, MIN_HEADER_FIELDS, HEADER_MATCH_RATIO, PHONE_MIN_DIGITS, PHONE_MAX_DIGITS, TEMPLATE_IDS } from "../io/ClassifierConstants.js";

enum AIVerdict {
  RECORD_TEMPLATE = "record-template",
  RUBBISH_SIGNATURE = "rubbish-signature",
  UNCERTAIN = "uncertain"
}

class ClassifierServiceImpl extends ServiceManager implements ClassifierService 
{
  protected static instance: ClassifierServiceImpl;
  private jobId: string;
  private fieldSpec: string[];
  private recordTemplates: RecordTemplate[];
  private rubbishTemplates: RubbishTemplate[];
  private aiCache: Map<string, RecordTemplate | RubbishTemplate>;
  private headerMap: Record<string, number> | null = null;
  private firstLine = true;

  private constructor(enforce: () => void) 
{
    if (enforce !== Enforce) 
{
      throw new InstantiationError("Cannot instantiate ClassifierServiceImpl directly. Use getInstance()");
    }
    super(enforce);
    
    this.jobId = "";
    this.fieldSpec = [];
    this.recordTemplates = [];
    this.rubbishTemplates = [];
    this.aiCache = new Map();
    this.headerMap = null;
    this.firstLine = true;
  }

  public static getInstance(): ClassifierServiceImpl 
{
    if (!ClassifierServiceImpl.instance) 
{
      ClassifierServiceImpl.instance = new ClassifierServiceImpl(Enforce);
    }
    return ClassifierServiceImpl.instance;
  }

  public reset(jobId: string, fieldSpec: string[], recordTemplates: RecordTemplate[], rubbishTemplates: RubbishTemplate[]): void 
{
    this.jobId = jobId;
    this.fieldSpec = fieldSpec;
    this.recordTemplates = recordTemplates;
    this.rubbishTemplates = rubbishTemplates;
    this.aiCache = new Map();
    this.headerMap = null;
    this.firstLine = true;
  }

  public getHeaderMap(): Record<string, number> | null 
{
    return this.headerMap;
  }

  classify(line: string, _byteOffset: number, _byteLength: number): ClassifyResult 
{
    const trimmed = line.trim();
    if (trimmed === "") 
{
      return { verdict: "rubbish", template_id: TEMPLATE_IDS.LENGTH_GATE };
    }
    if (line.length > MAX_LINE_LENGTH) 
{
      return { verdict: "uncertain", failure_class: _FailureClass.TRANSFORM_ERROR };
    }
    const nonPrintable = (trimmed.match(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g) || []).length;
    if (nonPrintable / trimmed.length > BINARY_THRESHOLD) 
{
      return { verdict: "rubbish", template_id: TEMPLATE_IDS.BINARY_GATE };
    }

    if (this.firstLine) 
{
      this.firstLine = false;
      const hdr = this.detectHeader(line);
      if (hdr) 
{
        this.headerMap = hdr;
        return { verdict: "rubbish", template_id: TEMPLATE_IDS.HEADER };
      }
    }

    const fp = quickFingerprint(line);
    const cached = this.aiCache.get(fp);

    let bestRecord: { row: Record<string, any>; template: RecordTemplate; score: number } | null = null;
    for (const t of this.recordTemplates) 
{
      if (t.length_hint !== undefined && line.length < t.length_hint) continue;
      try 
{
        const row = this.extractLine(line, t);
        if (row) 
{
          const meaningful = Object.values(row).filter((v) => v !== undefined && v !== null && v !== "").length;
          const present = Object.values(row).filter((v) => v !== undefined).length;
          const score = meaningful + present * 0.1;
          if (bestRecord === null || score > bestRecord.score) 
{
            bestRecord = { row, template: t, score };
          }
        }
      }
 catch 
{
        continue;
      }
    }
    if (bestRecord) 
{
      return { verdict: "parsed", row: this.coerce(bestRecord.row), template_id: bestRecord.template.template_id, template_version: bestRecord.template.version };
    }

    if (cached && "field_map" in cached) 
{
      const row = this.extractLine(line, cached);
      if (row) return { verdict: "parsed", row: this.coerce(row), template_id: cached.template_id, template_version: cached.version };
    }

    const structural = this.parseJsonRecord(line) || this.parseKvRecord(line) || this.parseXmlRecord(line) || this.parseYamlRecord(line);
    if (structural) 
{
      return { verdict: "parsed", row: this.coerce(structural.row), template_id: structural.template_id };
    }

    for (const t of this.rubbishTemplates) 
{
      if ((t.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && safeRegexTest(t.signature, line)) 
{
        return { verdict: "rubbish", template_id: t.template_id };
      }
    }

    if (cached && "signature" in cached && (cached.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && safeRegexTest(cached.signature, line)) 
{
      return { verdict: "rubbish", template_id: cached.template_id };
    }

    const delimited = this.parseDelimitedRecord(line);
    if (delimited) 
{
      return { verdict: "parsed", row: this.coerce(delimited), template_id: this.headerMap ? TEMPLATE_IDS.CSV_MAPPED : TEMPLATE_IDS.CSV_AUTO };
    }

    return { verdict: "uncertain", failure_class: _FailureClass.UNCERTAIN };
  }

  async classifyWithAI(line: string, contextLines: string[]): Promise<ClassifyResult> 
{
    const fp = quickFingerprint(line);
    const cached = this.aiCache.get(fp);
    if (cached) return this.toResult(line, cached);

    const req: ClassifyRequest = {
      unknown_line: line,
      field_spec: this.fieldSpec,
      context_lines: contextLines,
      job_id: this.jobId,
    };

    const handler = await import("../../ai_classifier/handler.js");
    const resp = await handler.classifyAi(req);
    if (resp.kind === AIVerdict.UNCERTAIN || !resp.template) 
{
      return { verdict: "uncertain", failure_class: _FailureClass.UNCERTAIN };
    }
    this.aiCache.set(fp, resp.template);
    return this.toResult(line, resp.template);
  }

  async classifyWithTimeout(line: string, contextLines: string[], timeoutMs: number): Promise<ClassifyResult> 
{
    return Promise.race([
      this.classifyWithAI(line, contextLines),
      new Promise<ClassifyResult>((resolve) =>
        setTimeout(() => resolve({ verdict: "uncertain", failure_class: _FailureClass.UNCERTAIN }), timeoutMs)
      ),
    ]);
  }

  private toResult(line: string, tmpl: RecordTemplate | RubbishTemplate): ClassifyResult 
{
    if ("signature" in tmpl) 
{
      if ((tmpl.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && safeRegexTest(tmpl.signature, line)) 
{
        return { verdict: "rubbish", template_id: tmpl.template_id };
      }
      return { verdict: "uncertain", failure_class: _FailureClass.UNCERTAIN };
    }
    if ("field_map" in tmpl) 
{
      const row = this.extractLine(line, tmpl);
      if (row) return { verdict: "parsed", row: this.coerce(row), template_id: tmpl.template_id, template_version: tmpl.version };
    }
    return { verdict: "uncertain", failure_class: _FailureClass.UNCERTAIN };
  }

  private extractLine(line: string, rec: RecordTemplate): Record<string, any> | null 
{
    const parsed = this.parseStructure(line, rec);
    if (!parsed) return null;

    const row: Record<string, any> = {};
    let presentCount = 0;
    for (const field of this.fieldSpec) 
{
      const loc = rec.field_map[field];
      if (!loc) 
{
        row[field] = undefined;
        continue;
      }
      const value = this.applyLocator(line, parsed, loc.locator);
      if (value !== undefined) presentCount++;
      row[field] = value;
    }
    return presentCount > 0 ? row : null;
  }

  private parseStructure(line: string, rec: RecordTemplate): string | any[] | Record<string, any> | null 
{
    if (rec.structure === "json") 
{
      if (line[0] !== "{" && line[0] !== "[") return null;
      try 
{
        const obj = JSON.parse(line);
        if (obj && typeof obj === "object" && !Array.isArray(obj)) return obj;
      }
 catch 
{
        return null;
      }
    }
    if (rec.structure === "kv") 
{
      const obj: Record<string, string> = {};
      for (const part of line.split(/[;\s]/)) 
{
        const [k, v] = part.split("=", 2);
        if (k && v !== undefined) obj[k.trim()] = v.trim();
      }
      return Object.keys(obj).length > 0 ? obj : null;
    }
    if (rec.structure === "csv") 
{
      const delim = rec.field_map && Object.values(rec.field_map)[0]?.locator?.startsWith("index:")
        ? Object.values(rec.field_map)[0].locator.replace("index:", "")
        : ",";
      const quote = "\"";
      return parseCsvLine(line, delim, quote);
    }
    if (rec.structure === "xml") 
{
      return this.parseXml(line);
    }
    if (rec.structure === "yaml") 
{
      return this.parseYaml(line);
    }
    if (rec.structure === "regex" || rec.structure === "fixed") 
{
      return line;
    }
    return null;
  }

  private parseXml(line: string): Record<string, any> | null 
{
    if (!line.includes("<") || !line.includes(">")) return null;
    
    const obj: Record<string, any> = {};
    const tagRegex = /<(\w+)>([^<]*)<\/\1>/g;
    let match;
    while ((match = tagRegex.exec(line)) !== null) 
{
      const [, tag, value] = match;
      obj[tag] = value.trim();
    }
    
    return Object.keys(obj).length > 0 ? obj : null;
  }

  private parseYaml(line: string): Record<string, any> | null 
{
    if (!line.includes(":")) return null;
    
    const obj: Record<string, string> = {};
    for (const part of line.split("\n")) 
{
      const [k, v] = part.split(":", 2);
      if (k && v !== undefined) 
{
        obj[k.trim()] = v.trim();
      }
    }
    
    return Object.keys(obj).length > 0 ? obj : null;
  }

  private parseXmlRecord(line: string): { row: Record<string, any>; template_id: string } | null 
{
    const obj = this.parseXml(line);
    if (!obj) return null;
    return this.extractFromObject(obj, "xml", false);
  }

  private parseYamlRecord(line: string): { row: Record<string, any>; template_id: string } | null 
{
    const obj = this.parseYaml(line);
    if (!obj) return null;
    return this.extractFromObject(obj, "yaml", false);
  }

  /** Normalize a field/column/key label for tolerant matching. */
  private normalizeKey(s: string): string 
{
    return s.toLowerCase().replace(/[^a-z0-9]/g, "");
  }

  /** Does a source key/column label correspond to a requested field (exact or alias)? */
  private keyMatchesField(key: string, field: string): boolean 
{
    const nk = this.normalizeKey(key);
    const nf = this.normalizeKey(field);
    if (!nk) return false;
    if (nk === nf) return true;
    const aliases = FIELD_ALIASES[nf] || [nf];
    return aliases.some((a) => this.normalizeKey(a) === nk);
  }

  /** Content validation, used to identify columns in a headerless CSV and to reject junk. */
  private validateField(field: string, value: any): boolean 
{
    if (value === null || value === undefined) return false;
    const v = String(value).trim();
    if (v === "") return false;
    const nf = this.normalizeKey(field);
    
    if (nf === "email") return /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(v);
    if (nf === "phone") 
{
      if (v.includes("@")) return false;
      const digits = v.replace(/\D/g, "");
      return digits.length >= PHONE_MIN_DIGITS && digits.length <= PHONE_MAX_DIGITS;
    }
    if (nf === "zip" || nf === "zipcode" || nf === "postalcode") 
{
      return /^\d{5}(-\d{4})?$/.test(v) || /^[A-Za-z]\d[A-Za-z] \d[A-Za-z]\d$/.test(v);
    }
    if (nf === "date" || nf === "datetime" || nf === "timestamp") 
{
      return /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?)?$/.test(v);
    }
    if (nf === "url" || nf === "website" || nf === "link") 
{
      return /^https?:\/\/.+\..+/.test(v);
    }
    return true;
  }

  /**
   * Extract only the field_spec fields from an object, matching by key name/alias.
   * requireStrong=true (fragile "Label: value" lines): accept only when a strongly-typed
   * field (email/phone/date/zip/url) actually validates, or when ≥2 requested fields are present.
   * requireStrong=false (a genuine JSON/XML/YAML object, which is inherently structured): accept any single match.
   */
  private extractFromObject(
    obj: Record<string, any>,
    templateId: string,
    requireStrong: boolean
  ): { row: Record<string, any>; template_id: string } | null 
{
    const row: Record<string, any> = {};
    let matched = 0;
    let strong = 0;
    for (const field of this.fieldSpec) 
{
      let value: any = undefined;
      for (const [k, val] of Object.entries(obj)) 
{
        if (this.keyMatchesField(k, field)) 
{
 value = val; break; 
}
      }
      if (value !== undefined && value !== null && String(value).trim() !== "") 
{
        row[field] = value;
        matched++;
        const nf = this.normalizeKey(field);
        if ((nf === "email" || nf === "phone" || nf === "zip" || nf === "date" || nf === "url") && this.validateField(field, value)) 
{
          strong++;
        }
      }
 else 
{
        row[field] = null;
      }
    }
    const accept = requireStrong
      ? strong >= 1 || matched >= Math.min(2, this.fieldSpec.length)
      : matched >= 1;
    return accept ? { row, template_id: templateId } : null;
  }

  private parseJsonRecord(line: string): { row: Record<string, any>; template_id: string } | null 
{
    const t = line.trim();
    if (t[0] !== "{") return null;
    let obj: any;
    try 
{
 obj = JSON.parse(t); 
}
 catch 
{
 return null; 
}
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) return null;
    return this.extractFromObject(obj, TEMPLATE_IDS.JSON, false);
  }

  private parseKvRecord(line: string): { row: Record<string, any>; template_id: string } | null 
{
    if (!line.includes(":")) return null;
    const obj: Record<string, string> = {};
    for (const seg of line.split(/\s+-\s+/)) 
{
      const m = seg.match(/^\s*([A-Za-z][A-Za-z0-9 _]*?)\s*:\s*(.*)$/);
      if (m) obj[m[1].trim()] = m[2].trim();
    }
    if (Object.keys(obj).length === 0) return null;
    return this.extractFromObject(obj, TEMPLATE_IDS.KV, true);
  }

  private splitBestDelimited(line: string): string[] | null 
{
    let best: string[] | null = null;
    for (const delim of DELIMITERS) 
{
      const parts = parseCsvLine(line, delim, "\"");
      if (parts.length < 2) continue;
      if (!best || parts.length > best.length) best = parts;
    }
    return best;
  }

  /**
   * Treat the first line as a header only when it is unmistakably one: ≥2 cells, every cell
   * a bare label with NO data content (no '@', no ≥7-digit run), AND it locates a MAJORITY
   * (≥ half, and ≥2) of the requested fields.
   */
  private detectHeader(line: string): Record<string, number> | null 
{
    const parts = this.splitBestDelimited(line);
    if (!parts || parts.length < MIN_HEADER_FIELDS) return null;
    for (const c of parts) 
{
      const v = c.trim();
      if (v === "" || v.includes("@") || v.replace(/\D/g, "").length >= 7) return null;
      if (!/^[A-Za-z][A-Za-z0-9 _.\-]*$/.test(v)) return null;
    }
    const map: Record<string, number> = {};
    let matched = 0;
    for (const field of this.fieldSpec) 
{
      for (let i = 0; i < parts.length; i++) 
{
        if (this.keyMatchesField(parts[i].trim(), field)) 
{
 map[field] = i; matched++; break; 
}
      }
    }
    const need = Math.max(MIN_HEADER_FIELDS, Math.ceil(this.fieldSpec.length * HEADER_MATCH_RATIO));
    return matched >= need ? map : null;
  }

  private parseDelimitedRecord(line: string): Record<string, any> | null 
{
    if (this.fieldSpec.length === 0) return null;
    const parts = this.splitBestDelimited(line);
    if (!parts) return null;

    const row: Record<string, any> = {};
    let matched = 0;

    if (this.headerMap) 
{
      for (const field of this.fieldSpec) 
{
        const idx = this.headerMap[field];
        const value = idx !== undefined && idx < parts.length ? parts[idx] : "";
        row[field] = value === "" || value === undefined ? null : value;
        if (row[field] !== null) matched++;
      }
      return matched > 0 ? row : null;
    }

    const claimed = new Set<number>();
    for (const field of this.fieldSpec) 
{
      const nf = this.normalizeKey(field);
      let value: any = null;
      if (nf === "email" || nf === "phone" || nf === "zip" || nf === "date" || nf === "url") 
{
        for (let i = 0; i < parts.length; i++) 
{
          if (claimed.has(i)) continue;
          if (this.validateField(field, parts[i])) 
{
 value = parts[i]; claimed.add(i); break; 
}
        }
      }
      row[field] = value;
      if (value !== null) matched++;
    }
    return matched > 0 ? row : null;
  }

  private applyLocator(line: string, parsed: string | any[] | Record<string, any>, loc: string): any 
{
    if (loc.startsWith("index:")) 
{
      const index = parseInt(loc.replace("index:", ""));
      if (Array.isArray(parsed) && index < parsed.length) return parsed[index];
      return undefined;
    }
    if (loc.startsWith("key:")) 
{
      const key = loc.replace("key:", "");
      if (parsed && !Array.isArray(parsed) && typeof parsed === "object") return (parsed as any)[key];
      return undefined;
    }
    if (loc.startsWith("regex:")) 
{
      const regexStr = loc.replace("regex:", "");
      const re = safeRegex(regexStr);
      if (!re) return undefined;
      const target = typeof parsed === "string" ? parsed : line;
      const match = re.exec(target);
      if (match) return match[1] ?? match[0];
      return undefined;
    }
    return undefined;
  }

  private coerce(row: Record<string, any>): Record<string, any> 
{
    const out: Record<string, any> = {};
    for (const [k, v] of Object.entries(row)) 
{
      if (v === null || v === undefined || v === "") 
{
        out[k] = null;
      }
 else if (typeof v === "boolean" || typeof v === "number") 
{
        out[k] = v;
      }
 else 
{
        const s = String(v).trim();
        out[k] = s;
      }
    }
    return out;
  }
}

function parseCsvLine(line: string, delim: string, quoteChar: string = "\""): string[] 
{
  const quote = quoteChar || null;
  const parts: string[] = [];
  let current = "";
  let inQuote = false;

  for (let i = 0; i < line.length; i++) 
{
    const c = line[i];
    const next = line[i + 1];
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

function quickFingerprint(line: string): string 
{
  const trimmed = line.trim();
  if (trimmed.length === 0) return "empty";
  if (trimmed[0] === "{" || trimmed[0] === "[") 
{
    try 
{
      const parsed = JSON.parse(line);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) 
{
        return `json|${Object.keys(parsed).sort().join(",")}`;
      }
    }
 catch 
{ /* ignore */ }
  }
  for (const delim of DELIMITERS) 
{
    const parts = parseCsvLine(line, delim, "\"");
    if (parts.length >= 3) return `csv|${delim}|${parts.length}`;
  }
  return `text|${trimmed.length}`;
}

export default ClassifierServiceImpl;

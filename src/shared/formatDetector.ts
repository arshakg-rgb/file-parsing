import Config from "../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import { createLogger } from "./logger.js";

export enum LineFormat {
  CSV = "csv",
  JSON = "json",
  TEXT = "text",
  BINARY = "binary",
  UNKNOWN = "unknown"
}

export interface ParsedLine {
  format: LineFormat;
  data: Record<string, any> | null;
  error?: string;
}

class FormatDetectorService extends ServiceManager {
  protected static instance: FormatDetectorService;
  private logger: any;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate FormatDetectorService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("format_detector");
  }

  public static getInstance(): FormatDetectorService {
    if (!ServiceManager.instance) {
      ServiceManager.instance = new FormatDetectorService(Enforce);
    }
    return ServiceManager.instance as FormatDetectorService;
  }

  public detectLineFormat(line: string): LineFormat {
    const trimmed = line.trim();
    
    if (!trimmed) {
      return LineFormat.UNKNOWN;
    }
    
    const nonPrintableCount = (trimmed.match(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g) || []).length;
    const nonPrintableRatio = nonPrintableCount / trimmed.length;
    if (nonPrintableRatio > 0.3) {
      return LineFormat.BINARY;
    }
    
    if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
      try {
        JSON.parse(trimmed);
        return LineFormat.JSON;
      } catch {
      }
    }
    
    return LineFormat.TEXT;
  }

  public parseTwitterUserLine(line: string): Record<string, any> | null {
    try {
      const data: Record<string, any> = {};
      
      const emailMatch = line.match(/Email:\s*([^\s-]+)/);
      if (emailMatch) data.email = emailMatch[1];
      
      const nameMatch = line.match(/Name:\s*([^-]+)-/);
      if (nameMatch) data.name = nameMatch[1].trim();
      
      const screenNameMatch = line.match(/ScreenName:\s*([^-]+)-/);
      if (screenNameMatch) data.screen_name = screenNameMatch[1].trim();
      
      const followersMatch = line.match(/Followers:\s*(\d+)/);
      if (followersMatch) data.followers_count = parseInt(followersMatch[1], 10);
      
      const createdAtMatch = line.match(/Created At:\s*(.+)$/);
      if (createdAtMatch) data.created_at = createdAtMatch[1].trim();
      
      return data;
    } catch (error) {
      this.logger.warn("twitter_user_parse_failed", { line, error: String(error) });
      return null;
    }
  }

  public parseJsonLine(line: string): Record<string, any> | null {
    try {
      const parsed = JSON.parse(line);
      return parsed;
    } catch (error) {
      this.logger.warn("json_parse_failed", { line, error: String(error) });
      return null;
    }
  }

  public parseCsvLine(line: string, fieldSpec?: string[]): Record<string, any> | null {
    try {
      const parts = line.split(",");
      const data: Record<string, any> = {};
      
      if (fieldSpec && fieldSpec.length > 0) {
        for (let i = 0; i < fieldSpec.length; i++) {
          const fieldName = fieldSpec[i];
          if (i < parts.length) {
            data[fieldName] = parts[i].trim().replace(/^"|"$/g, '');
          } else {
            data[fieldName] = null;
          }
        }
      } else {
        for (let i = 0; i < parts.length; i++) {
          data[`field_${i}`] = parts[i].trim().replace(/^"|"$/g, '');
        }
      }
      
      return data;
    } catch (error) {
      this.logger.warn("csv_parse_failed", { line, error: String(error) });
      return null;
    }
  }

  public parseLine(line: string, fieldSpec?: string[]): ParsedLine {
    const format = this.detectLineFormat(line);
    
    switch (format) {
      case LineFormat.BINARY:
        return { format, data: null, error: "Binary data skipped" };
      
      case LineFormat.JSON:
        return { format, data: this.parseJsonLine(line) };
      
      case LineFormat.TEXT:
        return { format, data: null };
      
      default:
        return { format, data: null, error: "Unknown format" };
    }
  }
}


export default FormatDetectorService;

const formatDetectorService = FormatDetectorService.getInstance();

export function detectLineFormat(line: string): LineFormat {
  return formatDetectorService.detectLineFormat(line);
}

export function parseTwitterUserLine(line: string): Record<string, any> | null {
  return formatDetectorService.parseTwitterUserLine(line);
}

export function parseJsonLine(line: string): Record<string, any> | null {
  return formatDetectorService.parseJsonLine(line);
}

export function parseCsvLine(line: string, fieldSpec?: string[]): Record<string, any> | null {
  return formatDetectorService.parseCsvLine(line, fieldSpec);
}

export function parseLine(line: string, fieldSpec?: string[]): ParsedLine {
  return formatDetectorService.parseLine(line, fieldSpec);
}

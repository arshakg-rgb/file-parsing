import { createLogger } from "../logger/Log.js";

const logger = createLogger("format_detector");

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

/**
 * Detect the format of a line - only basic validation, AI handles structure detection
 */
export function detectLineFormat(line: string): LineFormat 
{
  const trimmed = line.trim();
  
  if (!trimmed) 
{
    return LineFormat.UNKNOWN;
  }
  
  const nonPrintableCount = (trimmed.match(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g) || []).length;
  const nonPrintableRatio = nonPrintableCount / trimmed.length;
  if (nonPrintableRatio > 0.3) 
{
    return LineFormat.BINARY;
  }
  
  if (trimmed.startsWith("{") || trimmed.startsWith("[")) 
{
    try 
{
      JSON.parse(trimmed);
      return LineFormat.JSON;
    }
 catch 
{
    }
  }
  
  return LineFormat.TEXT;
}

/**
 * Parse Twitter user data line
 * Format: "Email: xxx - Name: xxx - ScreenName: xxx - Followers: xxx - Created At: xxx"
 */
export function parseTwitterUserLine(line: string): Record<string, any> | null 
{
  try 
{
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
  }
 catch (error) 
{
    logger.warn("twitter_user_parse_failed", { line, error: String(error) });
    return null;
  }
}

/**
 * Parse JSON line
 */
export function parseJsonLine(line: string): Record<string, any> | null 
{
  try 
{
    const parsed = JSON.parse(line);
    return parsed;
  }
 catch (error) 
{
    logger.warn("json_parse_failed", { line, error: String(error) });
    return null;
  }
}

/**
 * Parse CSV line (simple implementation)
 */
export function parseCsvLine(line: string, fieldSpec?: string[]): Record<string, any> | null 
{
  try 
{
    const parts = line.split(",");
    const data: Record<string, any> = {};
    
    if (fieldSpec && fieldSpec.length > 0) 
{
      for (let i = 0; i < fieldSpec.length; i++) 
{
        const fieldName = fieldSpec[i];
        if (i < parts.length) 
{
          data[fieldName] = parts[i].trim().replace(/^"|"$/g, "");
        }
 else 
{
          data[fieldName] = null;
        }
      }
    }
 else 
{
      for (let i = 0; i < parts.length; i++) 
{
        data[`field_${i}`] = parts[i].trim().replace(/^"|"$/g, "");
      }
    }
    
    return data;
  }
 catch (error) 
{
    logger.warn("csv_parse_failed", { line, error: String(error) });
    return null;
  }
}

/**
 * Main parser function - detects format and parses accordingly
 * Only handles JSON and binary, everything else goes to AI classifier
 */
export function parseLine(line: string, fieldSpec?: string[]): ParsedLine 
{
  const format = detectLineFormat(line);
  
  switch (format) 
{
    case LineFormat.BINARY:
      return { format, data: null, error: "Binary data skipped" };
    
    case LineFormat.JSON:
      return { format, data: parseJsonLine(line) };
    
    case LineFormat.TEXT:
      return { format, data: null };
    
    default:
      return { format, data: null, error: "Unknown format" };
  }
}

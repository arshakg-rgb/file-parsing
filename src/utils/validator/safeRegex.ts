import Config from "../../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../../config/ServiceManager.js";
import { InstantiationError } from "../../errors/InstantiationError.js";

class SafeRegexService extends ServiceManager {
  protected static instance: SafeRegexService;
  private readonly MAX_REGEX_SOURCE_LENGTH = 1024;
  private readonly MAX_REGEX_LINE_LENGTH = 64 * 1024;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate SafeRegexService directly. Use getInstance()");
    }
    super(enforce);
  }

  public static getInstance(): SafeRegexService {
    if (!SafeRegexService.instance) {
      SafeRegexService.instance = new SafeRegexService(Enforce);
    }
    return SafeRegexService.instance;
  }

  private isSafeRegexSource(source: string): boolean {
    if (!source || source.length > this.MAX_REGEX_SOURCE_LENGTH) return false;

    let inCharClass = false;
    let escaped = false;

    for (let i = 0; i < source.length; i++) {
      const c = source[i];
      if (escaped) {
        escaped = false;
        continue;
      }
      if (c === "\\") {
        escaped = true;
        continue;
      }
      if (c === "[" && !inCharClass) {
        inCharClass = true;
        continue;
      }
      if (c === "]" && inCharClass) {
        inCharClass = false;
        continue;
      }
      if (c === ")" && !inCharClass) {
        const next = source[i + 1];
        if (next === "*" || next === "+" || next === "{") return false;
        if (next === "?") {
          const next2 = source[i + 2];
          if (next2 === "?") {
            const next3 = source[i + 3];
            if (next3 === "*" || next3 === "+" || next3 === "{") return false;
          } else if (next2 === "*" || next2 === "+" || next2 === "{") {
            return false;
          }
        }
      }
    }
    return true;
  }

  public safeRegex(source: string): RegExp | null {
    if (!this.isSafeRegexSource(source)) return null;
    try {
      return new RegExp(source);
    } catch {
      return null;
    }
  }

  public safeRegexTest(source: string, line: string): boolean {
    if (line.length > this.MAX_REGEX_LINE_LENGTH) return false;
    const re = this.safeRegex(source);
    if (!re) return false;
    return re.test(line);
  }
}


export default SafeRegexService;

const safeRegexService = SafeRegexService.getInstance();

export function safeRegex(source: string): RegExp | null {
  return safeRegexService.safeRegex(source);
}

export function safeRegexTest(source: string, line: string): boolean {
  return safeRegexService.safeRegexTest(source, line);
}

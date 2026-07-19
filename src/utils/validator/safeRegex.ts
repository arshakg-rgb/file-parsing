import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";

/**
 * SafeRegexService is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class SafeRegexService extends ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: SafeRegexService;
    /**
   * M A X_ R E G E X_ S O U R C E_ L E N G T H
   * @private
   */
  private readonly MAX_REGEX_SOURCE_LENGTH = 1024;
    /**
   * M A X_ R E G E X_ L I N E_ L E N G T H
   * @private
   */
  private readonly MAX_REGEX_LINE_LENGTH = 64 * 1024;

    /**
   * Constructs a new SafeRegexService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate SafeRegexService directly. Use getInstance()");
    }
    super(enforce);
  }

    /**
   * Gets the single instance of the SafeRegexService class.
   * @returns The single instance of the class
   */
  public static getInstance(): SafeRegexService {
    if (!SafeRegexService.instance) {
      SafeRegexService.instance = new SafeRegexService(Enforce);
    }
    return SafeRegexService.instance;
  }

    /**
   * Checks whether safe regex source
   * @param source - The source
   * @returns True if the condition is met, false otherwise
   */
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

    /**
   * Performs the safe regex operation.
   * @param source - The source
   * @returns The reg exp | null result
   */
  public safeRegex(source: string): RegExp | null {
    if (!this.isSafeRegexSource(source)) return null;
    try {
      return new RegExp(source);
    } catch {
      return null;
    }
  }

    /**
   * Performs the safe regex test operation.
   * @param source - The source
   * @param line - The line to process
   * @returns True if the operation succeeds, false otherwise
   */
  public safeRegexTest(source: string, line: string): boolean {
    if (line.length > this.MAX_REGEX_LINE_LENGTH) return false;
    const re = this.safeRegex(source);
    if (!re) return false;
    return re.test(line);
  }
}


export default SafeRegexService;

/**
 * The safe regex service
 */
const safeRegexService = SafeRegexService.getInstance();

/**
 * Performs the safe regex operation.
 * @param source - The source
 * @returns The reg exp | null result
 */
export function safeRegex(source: string): RegExp | null {
  return safeRegexService.safeRegex(source);
}

/**
 * Performs the safe regex test operation.
 * @param source - The source
 * @param line - The line to process
 * @returns True if the operation succeeds, false otherwise
 */
export function safeRegexTest(source: string, line: string): boolean {
  return safeRegexService.safeRegexTest(source, line);
}

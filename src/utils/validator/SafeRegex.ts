/**
 * SafeRegexService is a static utility class responsible for validating
 * and safely constructing/testing regular expressions. All members are
 * static; the class is never instantiated.
 */
class SafeRegexUtils {
  /**
   * M A X_ R E G E X_ S O U R C E_ L E N G T H
   * @private
   */
  private static readonly MAX_REGEX_SOURCE_LENGTH: number = 1024;

  /**
   * M A X_ R E G E X_ L I N E_ L E N G T H
   * @private
   */
  private static readonly MAX_REGEX_LINE_LENGTH = 64 * 1024;

  /**
   * Private constructor to prevent instantiation. This class is
   * intended to be used statically only.
   * @private
   */
  private constructor() {}

  /**
   * Checks whether safe regex source
   * @param source - The source
   * @returns True if the condition is met, false otherwise
   */
  private static isSafeRegexSource(source: string): boolean {
    if (!source || source.length > SafeRegexUtils.MAX_REGEX_SOURCE_LENGTH) return false;

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
  public static safeRegex(source: string): RegExp | null {
    if (!SafeRegexUtils.isSafeRegexSource(source)) return null;
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
  public static safeRegexTest(source: string, line: string): boolean {
    if (line.length > SafeRegexUtils.MAX_REGEX_LINE_LENGTH) return false;
    const re = SafeRegexUtils.safeRegex(source);
    if (!re) return false;
    return re.test(line);
  }
}

export default SafeRegexUtils;

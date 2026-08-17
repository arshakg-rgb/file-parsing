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
   * Compiled regex cache keyed by source pattern.
   * @private
   */
  private static readonly regexCache: Map<string, RegExp | null> = new Map();

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
    const cached = SafeRegexUtils.regexCache.get(source);
    if (cached !== undefined) return cached;

    if (!SafeRegexUtils.isSafeRegexSource(source)) {
      SafeRegexUtils.regexCache.set(source, null);
      return null;
    }

    try {
      const re = new RegExp(source);
      SafeRegexUtils.regexCache.set(source, re);
      return re;
    } catch {
      SafeRegexUtils.regexCache.set(source, null);
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

  /**
   * Minimum length of a plain-literal character run (i.e. not part of a
   * character class, generic escape class like \d/\w/\s, wildcard, or
   * quantified token) required for a regex source to be considered
   * "specific" rather than a generic structural shape.
   * @private
   */
  private static readonly MIN_SPECIFIC_LITERAL_RUN = 6;

  /**
   * Checks whether a regex source contains a sufficiently long run of plain
   * literal characters. A regex made entirely of generic tokens (e.g.
   * "^https?:\\/\\/.+:.+:.+$") describes a broad structural shape rather than
   * a specific piece of content, and must never be trusted as a global
   * rubbish/junk signature applied indiscriminately to every future line —
   * whether at the moment it's proposed, or every time it's loaded back out
   * of persistent storage and re-applied.
   * @param source - The regex source to inspect.
   * @returns True if a literal run of at least MIN_SPECIFIC_LITERAL_RUN characters was found.
   */
  public static hasSpecificLiteralRun(source: string): boolean {
    if (!source) return false;

    let run = 0;
    let i = 0;

    while (i < source.length) {
      const c = source[i];

      if (c === "\\") {
        const next = source[i + 1];
        const genericClasses = new Set(["d", "D", "w", "W", "s", "S", "b", "B"]);

        if (next && genericClasses.has(next)) {
          run = 0;
          i += 2;
          continue;
        }

        run++;
        i += 2;
        continue;
      }

      if (c === "[") {
        run = 0;
        let j = i + 1;

        while (j < source.length && source[j] !== "]") {
          j++;
        }

        i = j + 1;
        continue;
      }

      if (c === "." || c === "^" || c === "$" || c === "|") {
        run = 0;
        i++;
        continue;
      }

      if (c === "(" || c === ")") {
        i++;
        continue;
      }

      if (c === "*" || c === "+" || c === "?" || c === "{") {
        run = Math.max(0, run - 1);

        if (c === "{") {
          let j = i + 1;

          while (j < source.length && source[j] !== "}") {
            j++;
          }

          i = j + 1;
          continue;
        }

        i++;
        continue;
      }

      run++;

      if (run >= SafeRegexUtils.MIN_SPECIFIC_LITERAL_RUN) {
        return true;
      }

      i++;
    }

    return false;
  }

  /**
   * Builds a regex source that matches only the exact given line, by
   * escaping every regex-special character. Used as a safe fallback
   * signature when a proposed regex is too generic to trust as a global
   * signature.
   * @param line - The line to build an exact-match signature for.
   * @returns A regex source anchored to match only this exact line.
   */
  public static escapeRegexLiteral(line: string): string {
    const escaped: string = line.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    return `^${escaped}$`;
  }
}

export default SafeRegexUtils;

/**
 * Conservative regex safety guard.
 *
 * The parser treats the regular expressions in templates as untrusted user input
 * (the AI may produce them).  This module prevents the engine from compiling
 * patterns that are known to be pathological, especially the nested quantifiers
 * that cause catastrophic backtracking (e.g. (a+)*, (a+)+, (a+){n,}).
 *
 * The rule is intentionally conservative: it rejects any group that is followed
 * by * / + / {, and any ? quantifier that is itself followed by * / + / {.
 * Patterns such as ?, ?? (lazy optional), and char-class quantifiers are still
 * allowed.
 */

const MAX_REGEX_SOURCE_LENGTH = 1024;
const MAX_REGEX_LINE_LENGTH = 64 * 1024;

function isSafeRegexSource(source: string): boolean {
  if (!source || source.length > MAX_REGEX_SOURCE_LENGTH) return false;

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
        // optional group, possibly lazy (??). Anything after the optional group
        // must not be another quantifier (* / + / {), which would be invalid and
        // pathological anyway.
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

export function safeRegex(source: string): RegExp | null {
  if (!isSafeRegexSource(source)) return null;
  try {
    return new RegExp(source);
  } catch {
    return null;
  }
}

export function safeRegexTest(source: string, line: string): boolean {
  if (line.length > MAX_REGEX_LINE_LENGTH) return false;
  const re = safeRegex(source);
  if (!re) return false;
  return re.test(line);
}

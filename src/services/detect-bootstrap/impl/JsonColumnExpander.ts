/**
 * Pure, side-effect-free helpers for detecting CSV/delimited columns whose values are
 * JSON and expanding their header into dot-notation sub-headers. Extracted out of
 * DetectBootstrapServiceHandler.ts so this logic can be unit-tested directly (that
 * file starts the real detect-bootstrap service - DB/queue/GCS connections - as a
 * module-level side effect on import, which makes it unsafe to import in tests).
 */

/**
 * Returns the quote character used to protect embedded delimiters for a given delimiter
 * (tab-delimited files typically aren't quoted).
 *
 * @param delim - The delimiter character.
 * @returns The quote character to use, or an empty string to disable quote handling.
 */

export function csvQuoteFor(delim: string): string
{
  return delim === "\t" ? "" : "\"";
}

/**
 * Splits a single CSV/delimited line into cells, honoring a quote character so an
 * embedded JSON blob's own commas/quotes don't get mistaken for column boundaries.
 *
 * @param line - The raw line to split.
 * @param delim - The delimiter character to split on.
 * @param quoteChar - The quote character used to protect embedded delimiters (defaults to `"`); pass an empty string to disable quote handling.
 * @returns The line split into trimmed cell strings.
 */

export function parseCsvLine(line: string, delim: string, quoteChar: string = "\""): string[]
{
  const quote: string | null = quoteChar || null;
  const parts: string[] = [];
  let current: string = "";
  let isLastFieldQuoted = false;
  let i: number = 0;

  while (i < line.length)
  {
    const c: string = line[i];

    if (quote && c === quote && current.length === 0)
    {
      isLastFieldQuoted = true;
      i++;

      while (i < line.length)
      {
        const qc: string = line[i];

        if (qc === quote)
        {
          if (i + 1 < line.length && line[i + 1] === quote)
          {
            current += quote;
            i += 2;
          }
          else
          {
            i++;
            break;
          }
        }
        else
        {
          current += qc;
          i++;
        }
      }

      // If the quoted field ends at EOL, let the final push below handle it.
      // If a delimiter follows, push it here and clear state for the next cell.
      if (i < line.length && line[i] === delim)
      {
        parts.push(current);
        current = "";
        isLastFieldQuoted = false;
        i++;
      }
    }
    else if (c === delim)
    {
      parts.push(current.trim());
      current = "";
      isLastFieldQuoted = false;
      i++;
    }
    else
    {
      current += c;
      i++;
    }
  }

  parts.push(isLastFieldQuoted ? current : current.trim());

  return parts;
}

/**
 * Recursively collects dot-notation leaf paths from a sampled JSON object, so a column
 * whose value is a single JSON record can be represented as one header per leaf field
 * (e.g. "_source" -> "_source.AGE", "_source.BIRTHDAY", ...). Nested objects are
 * flattened further; arrays are kept intact as a single leaf at their own path (an
 * array of several records is a multi-record field, not a set of columns).
 *
 * @param obj - The sampled JSON object to walk.
 * @param prefix - The dot-notation path accumulated so far.
 * @param into - Set collecting every discovered leaf path.
 */

export function collectJsonLeafPaths(obj: Record<string, unknown>, prefix: string, into: Set<string>): void
{
  for (const [k, v] of Object.entries(obj))
  {
    const path: string = prefix ? `${prefix}.${k}` : k;

    if (v !== null && typeof v === "object" && !Array.isArray(v))
    {
      collectJsonLeafPaths(v as Record<string, unknown>, path, into);
    }
    else
    {
      into.add(path);
    }
  }
}

/**
 * Expands raw CSV headers whose column values are consistently a single JSON object
 * (across a sample of data rows) into one dot-notation header per leaf field of that
 * object (e.g. "_source" -> "_source.AGE", "_source.BPLACE", ...). A column whose
 * sampled values are instead a JSON array of several records is left as its original
 * header - those records must stay together as one field, not be split into columns.
 * Any column that isn't consistently JSON is returned unchanged.
 *
 * @param headers - The raw header names detected from the file's header row.
 * @param delimiter - The delimiter used to split the header row (reused for data rows).
 * @param dataLines - Raw (unsplit) sample data lines following the header row.
 * @returns The (possibly expanded) header list, same column order as `headers`.
 */

export function expandJsonColumns(headers: string[], delimiter: string, dataLines: string[]): string[]
{
  const quoteChar: string = csvQuoteFor(delimiter);
  const sampleRows: string[][] = dataLines
    .slice(0, 25)
    .map((line) => parseCsvLine(line, delimiter, quoteChar))
    .filter((parts) => parts.length === headers.length);

  if (sampleRows.length === 0)
  {
    return headers;
  }

  const expanded: string[] = [];

  for (let col = 0; col < headers.length; col++)
  {
    const header: string = headers[col];
    const values: string[] = sampleRows
      .map((parts) => (parts[col] ?? "").trim())
      .filter((v) => v.length > 0);

    if (values.length === 0)
    {
      expanded.push(header);
      continue;
    }

    let isConsistentSingleObject: boolean = true;
    const leafPaths = new Set<string>();

    for (const v of values)
    {
      if (v[0] !== "{" && v[0] !== "[")
      {
        isConsistentSingleObject = false;
        break;
      }

      let parsed: unknown;

      try
      {
        parsed = JSON.parse(v);
      }
      catch
      {
        // One malformed/truncated JSON sample should not stop us from expanding
        // when the other samples are valid single JSON objects.
        continue;
      }

      if (Array.isArray(parsed))
      {
        const allObjects: boolean = parsed.length > 0 && parsed.every((x) => x !== null && typeof x === "object" && !Array.isArray(x));

        if (!allObjects || parsed.length > 1)
        {
          // Multiple JSON records (or a non-object array) - keep the column intact.
          isConsistentSingleObject = false;
          break;
        }

        collectJsonLeafPaths(parsed[0] as Record<string, unknown>, header, leafPaths);
        continue;
      }

      if (!parsed || typeof parsed !== "object")
      {
        isConsistentSingleObject = false;
        break;
      }

      collectJsonLeafPaths(parsed as Record<string, unknown>, header, leafPaths);
    }

    expanded.push(...(isConsistentSingleObject && leafPaths.size > 0 ? Array.from(leafPaths).sort() : [header]));
  }

  return expanded;
}

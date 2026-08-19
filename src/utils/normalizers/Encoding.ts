import { TextDecoder } from "node:util";

/**
 * EncodingUtils is a static utility class responsible for encoding
 * normalization, detection, and decoding. All members are static;
 * the class is never instantiated.
 */
class EncodingUtils
{
  /**
   * N A T I V E
   * @private
   */
  private static readonly NATIVE: Record<string, BufferEncoding> = {
    "utf-8": "utf8",
    "utf8": "utf8",
    "ascii": "utf8",
    "us-ascii": "utf8",
    "utf-16le": "utf16le",
    "utf16le": "utf16le",
    "utf-16": "utf16le",
    "ucs-2": "utf16le",
    "ucs2": "utf16le",
    "latin1": "latin1",
    "latin-1": "latin1",
    "iso-8859-1": "latin1",
    "iso8859-1": "latin1",
    "iso_8859-1": "latin1",
    "cp819": "latin1",
    "l1": "latin1",
    "binary": "latin1",
  };

  /**
   * _decoders
   * @private
   */
  private static readonly _decoders: Map<string, TextDecoder | null> = new Map<string, TextDecoder | null>();

  /**
   * Checks whether likely utf8
   * @param raw - The raw
   * @returns True if the condition is met, false otherwise
   */

  public static isLikelyUtf8(raw: Buffer): boolean
  {
    try
    {
      new TextDecoder("utf-8", { fatal: true }).decode(raw, { stream: true });
      return true;
    }
    catch
    {
      return false;
    }
  }

  /**
   * Normalizes encoding
   * @param label - The label
   * @returns The string result
   */
  public static normalizeEncoding(label?: string | null): string
  {
    if (!label)
    {
      return "utf-8";
    }

    const trimmed: string = label.trim().toLowerCase();

    return trimmed || "utf-8";
  }

  /**
   * Performs the buffer encoding for operation.
   * @param label - The label
   * @returns The buffer encoding result
   */

  public static bufferEncodingFor(label?: string | null): BufferEncoding
  {
    return EncodingUtils.NATIVE[EncodingUtils.normalizeEncoding(label)] ?? "latin1";
  }

  /**
   * Performs the decoder for operation.
   * @param label - The label
   * @returns The text decoder | null result
   */

  private static decoderFor(label: string): TextDecoder | null
  {
    if (EncodingUtils._decoders.has(label))
    {
      return EncodingUtils._decoders.get(label)!;
    }

    let dec: TextDecoder | null;

    try
    {
      dec = new TextDecoder(label, { fatal: false });
    }
    catch
    {
      dec = null;
    }

    EncodingUtils._decoders.set(label, dec);

    return dec;
  }

  /**
   * Decodes the operation
   * @param raw - The raw
   * @param label - The label
   * @param start - The start
   * @param end - The end
   * @returns The string result
   */

  public static decode(raw: Buffer, label?: string | null, start = 0, end = raw.length): string
  {
    const enc: string = EncodingUtils.normalizeEncoding(label);
    const view: Buffer = start !== 0 || end !== raw.length ? raw.subarray(start, end) : raw;
    const native: BufferEncoding = EncodingUtils.NATIVE[enc];

    if (native)
    {
      return view.toString(native);
    }

    const dec: TextDecoder | null = EncodingUtils.decoderFor(enc);

    if (dec)
    {
      try
      {
        return dec.decode(view);
      }
      catch
      {

      }
    }
    return view.toString("latin1");
  }

  /**
   * Attempts to recover text that was double-encoded (mojibake):
   * UTF-8 bytes that were read as Latin-1 and stored as Latin-1 characters.
   * Returns the original string if it already contains characters outside the
   * Latin-1 range, if the bytes are not valid UTF-8, or if any step fails.
   */

  public static recoverMojibake(input: string): string
  {
    if (!input)
    {
      return input;
    }

    // Nothing to do for plain ASCII, and strings that already contain real
    // UTF-8 code points (> U+00FF) must not be recoded.
    if (!/[\u0080-\u00FF]/.test(input) || /[^\u0000-\u00FF]/.test(input))
    {
      return input;
    }

    try
    {
      const bytes: Buffer = Buffer.from(input, "latin1");
      const decoded: string = new TextDecoder("utf-8", { fatal: true }).decode(bytes);

      return decoded === input ? input : decoded;
    }
    catch
    {
      return input;
    }
  }
}

export default EncodingUtils;

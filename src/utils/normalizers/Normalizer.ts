
import { TextDecoder } from "node:util";

const NATIVE: Record<string, BufferEncoding> = {
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
 * True if the bytes are valid UTF-8. Used to override charset guessers (jschardet)
 * which frequently misclassify UTF-8 that contains a few multibyte chars as
 * ISO-8859-x / windows-125x at low confidence. Valid UTF-8 with high-bit bytes is a
 * near-zero false-positive signal, so "valid UTF-8 → treat as UTF-8" is safe.
 * Streaming decode tolerates a multibyte sequence truncated at the buffer's end
 * (e.g. a probe window boundary) rather than reporting it as invalid.
 */
export function isLikelyUtf8(raw: Buffer): boolean 
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

/** Canonical lowercased encoding label; defaults to utf-8 for empty/unknown input. */
export function normalizeEncoding(label?: string | null): string 
{
  if (!label) return "utf-8";
  const trimmed = label.trim().toLowerCase();
  return trimmed || "utf-8";
}

/** A valid Node BufferEncoding for the label (for Buffer.byteLength); latin1 when non-native. */
export function bufferEncodingFor(label?: string | null): BufferEncoding 
{
  return NATIVE[normalizeEncoding(label)] ?? "latin1";
}

const _decoders = new Map<string, TextDecoder | null>();
function decoderFor(label: string): TextDecoder | null 
{
  if (_decoders.has(label)) return _decoders.get(label)!;
  let dec: TextDecoder | null = null;
  try 
{
    dec = new TextDecoder(label, { fatal: false });
  }
 catch 
{
    dec = null;
  }
  _decoders.set(label, dec);
  return dec;
}

/**
 * Decode bytes to a string using a detected encoding label, never throwing.
 * Native Node encodings go through Buffer.toString; the rest through TextDecoder;
 * anything unsupported falls back to latin1 (1:1, lossless round-trip of bytes).
 */
export function decode(raw: Buffer, label?: string | null, start = 0, end = raw.length): string 
{
  const enc = normalizeEncoding(label);
  const view = start !== 0 || end !== raw.length ? raw.subarray(start, end) : raw;
  const native = NATIVE[enc];
  if (native) return view.toString(native);
  const dec = decoderFor(enc);
  if (dec) 
{
    try 
{
      return dec.decode(view);
    }
 catch 
{
      /* fall through to latin1 */
    }
  }
  return view.toString("latin1");
}

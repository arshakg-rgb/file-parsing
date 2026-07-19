import { TextDecoder } from "node:util";
import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";

/**
 * EncodingService is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class EncodingService extends ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: EncodingService;
    /**
   * N A T I V E
   * @private
   */
  private readonly NATIVE: Record<string, BufferEncoding>;
    /**
   * _decoders
   * @private
   */
  private readonly _decoders = new Map<string, TextDecoder | null>();

    /**
   * Constructs a new EncodingService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate EncodingService directly. Use getInstance()");
    }
    super(enforce);
    
    this.NATIVE = {
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
  }

    /**
   * Gets the single instance of the EncodingService class.
   * @returns The single instance of the class
   */
  public static getInstance(): EncodingService {
    if (!EncodingService.instance) {
      EncodingService.instance = new EncodingService(Enforce);
    }
    return EncodingService.instance;
  }

    /**
   * Checks whether likely utf8
   * @param raw - The raw
   * @returns True if the condition is met, false otherwise
   */
  public isLikelyUtf8(raw: Buffer): boolean {
    try {
      new TextDecoder("utf-8", { fatal: true }).decode(raw, { stream: true });
      return true;
    } catch {
      return false;
    }
  }

    /**
   * Normalizes encoding
   * @param label - The label
   * @returns The string result
   */
  public normalizeEncoding(label?: string | null): string {
    if (!label) return "utf-8";
    const trimmed = label.trim().toLowerCase();
    return trimmed || "utf-8";
  }

    /**
   * Performs the buffer encoding for operation.
   * @param label - The label
   * @returns The buffer encoding result
   */
  public bufferEncodingFor(label?: string | null): BufferEncoding {
    return this.NATIVE[this.normalizeEncoding(label)] ?? "latin1";
  }

    /**
   * Performs the decoder for operation.
   * @param label - The label
   * @returns The text decoder | null result
   */
  private decoderFor(label: string): TextDecoder | null {
    if (this._decoders.has(label)) return this._decoders.get(label)!;
    let dec: TextDecoder | null = null;
    try {
      dec = new TextDecoder(label, { fatal: false });
    } catch {
      dec = null;
    }
    this._decoders.set(label, dec);
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
  public decode(raw: Buffer, label?: string | null, start = 0, end = raw.length): string {
    const enc = this.normalizeEncoding(label);
    const view = start !== 0 || end !== raw.length ? raw.subarray(start, end) : raw;
    const native = this.NATIVE[enc];
    if (native) return view.toString(native);
    const dec = this.decoderFor(enc);
    if (dec) {
      try {
        return dec.decode(view);
      } catch {
      }
    }
    return view.toString("latin1");
  }
}

export default EncodingService;

/**
 * The encoding service
 */
const encodingService = EncodingService.getInstance();

/**
 * Checks whether likely utf8
 * @param raw - The raw
 * @returns True if the condition is met, false otherwise
 */
export function isLikelyUtf8(raw: Buffer): boolean {
  return encodingService.isLikelyUtf8(raw);
}

/**
 * Normalizes encoding
 * @param label - The label
 * @returns The string result
 */
export function normalizeEncoding(label?: string | null): string {
  return encodingService.normalizeEncoding(label);
}

/**
 * Performs the buffer encoding for operation.
 * @param label - The label
 * @returns The buffer encoding result
 */
export function bufferEncodingFor(label?: string | null): BufferEncoding {
  return encodingService.bufferEncodingFor(label);
}

/**
 * Decodes the operation
 * @param raw - The raw
 * @param label - The label
 * @param start - The start
 * @param end - The end
 * @returns The string result
 */
export function decode(raw: Buffer, label?: string | null, start = 0, end = raw.length): string {
  return encodingService.decode(raw, label, start, end);
}

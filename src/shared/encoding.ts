import { TextDecoder } from "node:util";
import Config from "../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";

class EncodingService extends ServiceManager {
  protected static instance: EncodingService;
  private readonly NATIVE: Record<string, BufferEncoding>;
  private readonly _decoders = new Map<string, TextDecoder | null>();

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

  public static getInstance(): EncodingService {
    if (!EncodingService.instance) {
      EncodingService.instance = new EncodingService(Enforce);
    }
    return EncodingService.instance;
  }

  public isLikelyUtf8(raw: Buffer): boolean {
    try {
      new TextDecoder("utf-8", { fatal: true }).decode(raw, { stream: true });
      return true;
    } catch {
      return false;
    }
  }

  public normalizeEncoding(label?: string | null): string {
    if (!label) return "utf-8";
    const trimmed = label.trim().toLowerCase();
    return trimmed || "utf-8";
  }

  public bufferEncodingFor(label?: string | null): BufferEncoding {
    return this.NATIVE[this.normalizeEncoding(label)] ?? "latin1";
  }

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

const encodingService = EncodingService.getInstance();

export function isLikelyUtf8(raw: Buffer): boolean {
  return encodingService.isLikelyUtf8(raw);
}

export function normalizeEncoding(label?: string | null): string {
  return encodingService.normalizeEncoding(label);
}

export function bufferEncodingFor(label?: string | null): BufferEncoding {
  return encodingService.bufferEncodingFor(label);
}

export function decode(raw: Buffer, label?: string | null, start = 0, end = raw.length): string {
  return encodingService.decode(raw, label, start, end);
}

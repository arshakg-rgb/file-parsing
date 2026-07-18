import { IDetectBootstrap, ClassifyRequest, ClassifyResponse } from "./io/IDetectBootstrap.js";
import DetectBootstrapServiceImpl from "./impl/DetectBootstrapServiceImpl.js";
import { ClassifyMessage } from "../../shared/models/job.js";

/**
 * Legacy DetectBootstrapService class - now a thin wrapper around DetectBootstrapServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class DetectBootstrapService implements IDetectBootstrap {
  private service: DetectBootstrapServiceImpl;

  constructor() {
    this.service = DetectBootstrapServiceImpl.getInstance();
  }

  async detectBootstrap(req: ClassifyRequest): Promise<ClassifyResponse> {
    return this.service.detectBootstrap(req);
  }

  async classifyLine(req: ClassifyRequest): Promise<ClassifyResponse> {
    return this.service.classifyLine(req);
  }

  computeWindowSize(avgRowBytes: number, maxRowBytes: number): number {
    return this.service.computeWindowSize(avgRowBytes, maxRowBytes);
  }

  computeProbeOffsets(fileSize: number, windowSize: number): number[] {
    return this.service.computeProbeOffsets(fileSize, windowSize);
  }

  detectEncoding(raw: Buffer): string {
    return this.service.detectEncoding(raw);
  }

  measureRowWidth(raw: Buffer, encoding: string): [number, number] {
    return this.service.measureRowWidth(raw, encoding);
  }

  fingerprintProbe(raw: Buffer, encoding: string): string {
    return this.service.fingerprintProbe(raw, encoding);
  }

  extractSampleLines(raw: Buffer, encoding: string, n: number): string[] {
    return this.service.extractSampleLines(raw, encoding, n);
  }

  async bootstrapJob(msg: ClassifyMessage): Promise<void> {
    return this.service.bootstrapJob(msg);
  }
}

// Re-export the new service for direct use
export { default as DetectBootstrapServiceImpl } from "./impl/DetectBootstrapServiceImpl.js";
export { IDetectBootstrap, ClassifyRequest, ClassifyResponse } from "./io/IDetectBootstrap.js";

// Backward compatibility wrappers
const detectService = new DetectBootstrapService();

export async function classifyLine(req: ClassifyRequest): Promise<ClassifyResponse> {
  return detectService.classifyLine(req);
}

export function computeWindowSize(avgRowBytes: number, maxRowBytes: number): number {
  return detectService.computeWindowSize(avgRowBytes, maxRowBytes);
}

export function computeProbeOffsets(fileSize: number, windowSize: number): number[] {
  return detectService.computeProbeOffsets(fileSize, windowSize);
}

export function detectEncoding(raw: Buffer): string {
  return detectService.detectEncoding(raw);
}

export function measureRowWidth(raw: Buffer, encoding: string): [number, number] {
  return detectService.measureRowWidth(raw, encoding);
}

export function fingerprintProbe(raw: Buffer, encoding: string): string {
  return detectService.fingerprintProbe(raw, encoding);
}

export function extractSampleLines(raw: Buffer, encoding: string, n: number): string[] {
  return detectService.extractSampleLines(raw, encoding, n);
}

export async function bootstrapJob(msg: ClassifyMessage): Promise<void> {
  return detectService.bootstrapJob(msg);
}

export default DetectBootstrapService;

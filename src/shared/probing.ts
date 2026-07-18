import Config from "../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import FirestoreCacheUtils from "../utils/cache/FirestoreCacheUtils.js";
import { createLogger, Logger } from "../utils/logger/logger.js";
import jschardet from "jschardet";

export interface ProbeResult {
  offset: number;
  size: number;
  encoding: string;
  avgRowWidth: number;
  maxRowWidth: number;
  lineCount: number;
  sampleLines: string[];
}

class ProbingService extends ServiceManager {
  protected static instance: ProbingService;
  private logger: Logger;
  private gcsUtils: FirestoreCacheUtils;
  private readonly PROBE_SIZE_PER_COUNT = 536870912; // 512MB
  private readonly PROBE_COUNT_MIN = 1;
  private readonly PROBE_COUNT_MAX = 10;
  private readonly PROBE_WINDOW_MIN_BYTES = 65536; // 64KB
  private readonly PROBE_WINDOW_MAX_BYTES = 1048576; // 1MB
  private readonly PROBE_TARGET_LINES = 150;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate ProbingService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("probing");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
  }

  public static getInstance(): ProbingService {
    if (!ProbingService.instance) {
      ProbingService.instance = new ProbingService(Enforce);
    }
    return ProbingService.instance;
  }

  public calculateProbeCount(fileSize: number): number {
    const sizePerProbe = this.PROBE_SIZE_PER_COUNT;
    const idealCount = Math.ceil(fileSize / sizePerProbe);
    
    return Math.max(
      this.PROBE_COUNT_MIN,
      Math.min(idealCount, this.PROBE_COUNT_MAX)
    );
  }

  public calculateProbeWindow(avgRowWidth: number, maxRowWidth: number): number {
    const minWidth = this.PROBE_WINDOW_MIN_BYTES;
    const maxWidth = this.PROBE_WINDOW_MAX_BYTES;
    const targetLines = this.PROBE_TARGET_LINES;
    
    const widthBased = Math.max(avgRowWidth * targetLines, maxRowWidth * 4);
    
    return Math.max(minWidth, Math.min(widthBased, maxWidth));
  }

  public generateProbeOffsets(fileSize: number, probeCount: number): number[] {
    const offsets: number[] = [];
    
    if (probeCount === 1) {
      return [0];
    }
    
    const step = Math.floor(fileSize / probeCount);
    
    for (let i = 0; i < probeCount; i++) {
      offsets.push(i * step);
    }
    
    if (!offsets.includes(0)) {
      offsets.push(0);
    }
    if (!offsets.includes(fileSize - 1)) {
      offsets.push(fileSize - 1);
    }
    
    return [...new Set(offsets)].sort((a, b) => a - b);
  }

  public async executeProbe(
    bucket: string,
    key: string,
    offset: number,
    fileSize: number
  ): Promise<ProbeResult> {
    const windowSize = this.PROBE_WINDOW_MIN_BYTES;
    const endOffset = Math.min(offset + windowSize - 1, fileSize - 1);
    
    const buffer = await this.gcsUtils.readRange(bucket, key, offset, endOffset);
    const content = buffer.toString("utf-8");
    
    const detected = jschardet.detect(buffer);
    const encoding = detected.encoding || "utf-8";
    
    const lines = content.split("\n").filter(line => line.trim());
    const lineCount = lines.length;
    
    if (lineCount === 0) {
      return {
        offset,
        size: buffer.length,
        encoding,
        avgRowWidth: 0,
        maxRowWidth: 0,
        lineCount: 0,
        sampleLines: [],
      };
    }
    
    const rowWidths = lines.map(line => line.length);
    const avgRowWidth = rowWidths.reduce((a, b) => a + b, 0) / rowWidths.length;
    const maxRowWidth = Math.max(...rowWidths);
    
    const sampleLines = lines.slice(0, 10);
    
    this.logger.info("probe_complete", { 
      offset, 
      window_size: windowSize, 
      line_count: lineCount,
      avg_row_width: avgRowWidth,
      encoding 
    });
    
    return {
      offset,
      size: buffer.length,
      encoding,
      avgRowWidth,
      maxRowWidth,
      lineCount,
      sampleLines,
    };
  }

  public async probeFile(bucket: string, key: string): Promise<{
    fileSize: number;
    probeCount: number;
    probeResults: ProbeResult[];
    finalWindow: number;
    encoding: string;
    avgRowWidth: number;
    maxRowWidth: number;
  }> {
    const fileSize = await this.gcsUtils.objectSize(bucket, key);
    const probeCount = this.calculateProbeCount(fileSize);
    const offsets = this.generateProbeOffsets(fileSize, probeCount);
    
    this.logger.info("probing_start", { 
      bucket, 
      key, 
      file_size: fileSize, 
      probe_count: probeCount 
    });
    
    const probeResults: ProbeResult[] = [];
    let totalAvgRowWidth = 0;
    let totalMaxRowWidth = 0;
    let finalEncoding = "utf-8";
    
    for (const offset of offsets) {
      const result = await this.executeProbe(bucket, key, offset, fileSize);
      probeResults.push(result);
      
      totalAvgRowWidth += result.avgRowWidth;
      totalMaxRowWidth = Math.max(totalMaxRowWidth, result.maxRowWidth);
      
      if (result.encoding !== "utf-8" && finalEncoding === "utf-8") {
        finalEncoding = result.encoding;
      }
    }
    
    const avgRowWidth = totalAvgRowWidth / probeResults.length;
    const maxRowWidth = totalMaxRowWidth;
    const finalWindow = this.calculateProbeWindow(avgRowWidth, maxRowWidth);
    
    this.logger.info("probing_complete", { 
      file_size: fileSize,
      final_window: finalWindow,
      encoding: finalEncoding,
      avg_row_width: avgRowWidth,
      max_row_width: maxRowWidth 
    });
    
    return {
      fileSize,
      probeCount,
      probeResults,
      finalWindow,
      encoding: finalEncoding,
      avgRowWidth,
      maxRowWidth,
    };
  }

  public analyzeProbes(probeResults: ProbeResult[]): {
    isHomogeneous: boolean;
    likelyHasEmbeddedNewlines: boolean;
    likelyHasQuotedFields: boolean;
    suggestedDelimiter: string;
  } {
    if (probeResults.length === 0) {
      return {
        isHomogeneous: true,
        likelyHasEmbeddedNewlines: false,
        likelyHasQuotedFields: false,
        suggestedDelimiter: ",",
      };
    }
    
    const firstSample = probeResults[0].sampleLines;
    let consistentStructure = true;
    let hasQuotes = false;
    let hasCommas = false;
    let hasTabs = false;
    let hasPipes = false;
    
    for (const result of probeResults) {
      for (const line of result.sampleLines) {
        if (line.includes("\"")) hasQuotes = true;
        
        if (line.includes(",")) hasCommas = true;
        if (line.includes("\t")) hasTabs = true;
        if (line.includes("|")) hasPipes = true;
        
        if ((line.match(/"/g) || []).length % 2 !== 0) {
          consistentStructure = false;
        }
      }
    }
    
    let suggestedDelimiter = ",";
    if (hasTabs && !hasCommas) {
      suggestedDelimiter = "\t";
    } else if (hasPipes && !hasCommas && !hasTabs) {
      suggestedDelimiter = "|";
    }
    
    return {
      isHomogeneous: consistentStructure,
      likelyHasEmbeddedNewlines: !consistentStructure,
      likelyHasQuotedFields: hasQuotes,
      suggestedDelimiter,
    };
  }
}


export default ProbingService;

const probingService = ProbingService.getInstance();

export class AdaptiveProbing {
  calculateProbeCount(fileSize: number): number {
    return probingService.calculateProbeCount(fileSize);
  }

  calculateProbeWindow(avgRowWidth: number, maxRowWidth: number): number {
    return probingService.calculateProbeWindow(avgRowWidth, maxRowWidth);
  }

  generateProbeOffsets(fileSize: number, probeCount: number): number[] {
    return probingService.generateProbeOffsets(fileSize, probeCount);
  }

  async executeProbe(
    bucket: string,
    key: string,
    offset: number,
    fileSize: number
  ): Promise<ProbeResult> {
    return probingService.executeProbe(bucket, key, offset, fileSize);
  }

  async probeFile(bucket: string, key: string): Promise<{
    fileSize: number;
    probeCount: number;
    probeResults: ProbeResult[];
    finalWindow: number;
    encoding: string;
    avgRowWidth: number;
    maxRowWidth: number;
  }> {
    return probingService.probeFile(bucket, key);
  }

  analyzeProbes(probeResults: ProbeResult[]): {
    isHomogeneous: boolean;
    likelyHasEmbeddedNewlines: boolean;
    likelyHasQuotedFields: boolean;
    suggestedDelimiter: string;
  } {
    return probingService.analyzeProbes(probeResults);
  }
}

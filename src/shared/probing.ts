import { readRange, objectSize } from "./gcsUtils.js";
import { settings } from "./config.js";
import { createLogger } from "./logger.js";
import jschardet from "jschardet";

const logger = createLogger("probing");

export interface ProbeResult {
  offset: number;
  size: number;
  encoding: string;
  avgRowWidth: number;
  maxRowWidth: number;
  lineCount: number;
  sampleLines: string[];
}

export class AdaptiveProbing {
  /**
   * Calculate optimal probe count based on file size
   */
  calculateProbeCount(fileSize: number): number {
    const sizePerProbe = settings.PROBE_SIZE_PER_COUNT; // 512MB
    const idealCount = Math.ceil(fileSize / sizePerProbe);
    
    // Clamp between min and max
    return Math.max(
      settings.PROBE_COUNT_MIN,
      Math.min(idealCount, settings.PROBE_COUNT_MAX)
    );
  }

  /**
   * Calculate optimal probe window size based on row width
   */
  calculateProbeWindow(avgRowWidth: number, maxRowWidth: number): number {
    const minWidth = settings.PROBE_WINDOW_MIN_BYTES; // 64KB
    const maxWidth = settings.PROBE_WINDOW_MAX_BYTES; // 1MB
    const targetLines = settings.PROBE_TARGET_LINES; // 150 lines
    
    // Calculate window based on row width
    const widthBased = Math.max(avgRowWidth * targetLines, maxRowWidth * 4);
    
    // Clamp between min and max
    return Math.max(minWidth, Math.min(widthBased, maxWidth));
  }

  /**
   * Generate probe offsets for a file
   */
  generateProbeOffsets(fileSize: number, probeCount: number): number[] {
    const offsets: number[] = [];
    
    if (probeCount === 1) {
      return [0]; // Single probe at start
    }
    
    // Even spacing across file
    const step = Math.floor(fileSize / probeCount);
    
    for (let i = 0; i < probeCount; i++) {
      offsets.push(i * step);
    }
    
    // Always include head and tail
    if (!offsets.includes(0)) {
      offsets.push(0);
    }
    if (!offsets.includes(fileSize - 1)) {
      offsets.push(fileSize - 1);
    }
    
    // Sort and deduplicate
    return [...new Set(offsets)].sort((a, b) => a - b);
  }

  /**
   * Execute a single probe at the given offset
   */
  async executeProbe(
    bucket: string,
    key: string,
    offset: number,
    fileSize: number
  ): Promise<ProbeResult> {
    // Calculate probe window size (start with 64KB minimum)
    let windowSize = settings.PROBE_WINDOW_MIN_BYTES;
    const endOffset = Math.min(offset + windowSize - 1, fileSize - 1);
    
    // Read probe data
    const buffer = await readRange(bucket, key, offset, endOffset);
    const content = buffer.toString('utf-8');
    
    // Detect encoding
    const detected = jschardet.detect(buffer);
    const encoding = detected.encoding || 'utf-8';
    
    // Analyze lines
    const lines = content.split('\n').filter(line => line.trim());
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
    
    // Calculate row widths
    const rowWidths = lines.map(line => line.length);
    const avgRowWidth = rowWidths.reduce((a, b) => a + b, 0) / rowWidths.length;
    const maxRowWidth = Math.max(...rowWidths);
    
    // Get sample lines (first 10)
    const sampleLines = lines.slice(0, 10);
    
    logger.info("probe_complete", { 
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

  /**
   * Run adaptive probing on a file
   */
  async probeFile(bucket: string, key: string): Promise<{
    fileSize: number;
    probeCount: number;
    probeResults: ProbeResult[];
    finalWindow: number;
    encoding: string;
    avgRowWidth: number;
    maxRowWidth: number;
  }> {
    const fileSize = await objectSize(bucket, key);
    const probeCount = this.calculateProbeCount(fileSize);
    const offsets = this.generateProbeOffsets(fileSize, probeCount);
    
    logger.info("probing_start", { 
      bucket, 
      key, 
      file_size: fileSize, 
      probe_count: probeCount 
    });
    
    const probeResults: ProbeResult[] = [];
    let totalAvgRowWidth = 0;
    let totalMaxRowWidth = 0;
    let finalEncoding = 'utf-8';
    
    for (const offset of offsets) {
      const result = await this.executeProbe(bucket, key, offset, fileSize);
      probeResults.push(result);
      
      totalAvgRowWidth += result.avgRowWidth;
      totalMaxRowWidth = Math.max(totalMaxRowWidth, result.maxRowWidth);
      
      // Use encoding from first successful probe
      if (result.encoding !== 'utf-8' && finalEncoding === 'utf-8') {
        finalEncoding = result.encoding;
      }
    }
    
    const avgRowWidth = totalAvgRowWidth / probeResults.length;
    const maxRowWidth = totalMaxRowWidth;
    const finalWindow = this.calculateProbeWindow(avgRowWidth, maxRowWidth);
    
    logger.info("probing_complete", { 
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

  /**
   * Analyze probe results to determine file characteristics
   */
  analyzeProbes(probeResults: ProbeResult[]): {
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
        suggestedDelimiter: ',',
      };
    }
    
    // Check for consistency across probes
    const firstSample = probeResults[0].sampleLines;
    let consistentStructure = true;
    let hasQuotes = false;
    let hasCommas = false;
    let hasTabs = false;
    let hasPipes = false;
    
    for (const result of probeResults) {
      for (const line of result.sampleLines) {
        // Check for quotes
        if (line.includes('"')) hasQuotes = true;
        
        // Check for delimiters
        if (line.includes(',')) hasCommas = true;
        if (line.includes('\t')) hasTabs = true;
        if (line.includes('|')) hasPipes = true;
        
        // Check for embedded newlines (quoted fields with newlines)
        if ((line.match(/"/g) || []).length % 2 !== 0) {
          // Odd number of quotes suggests embedded newline
          consistentStructure = false;
        }
      }
    }
    
    // Determine likely delimiter
    let suggestedDelimiter = ',';
    if (hasTabs && !hasCommas) {
      suggestedDelimiter = '\t';
    } else if (hasPipes && !hasCommas && !hasTabs) {
      suggestedDelimiter = '|';
    }
    
    return {
      isHomogeneous: consistentStructure,
      likelyHasEmbeddedNewlines: !consistentStructure,
      likelyHasQuotedFields: hasQuotes,
      suggestedDelimiter,
    };
  }
}

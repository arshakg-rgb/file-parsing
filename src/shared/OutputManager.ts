import { OutputBuffer, type OutputRow } from "./OutputBuffer.js";

/**
 * OutputManager manages the resource lifecycle.
 */
export class OutputManager {
    /**
   * Buffers
   * @private
   */
  private buffers = new Map<string, OutputBuffer>();

    /**
   * Gets or creates the single output buffer for a job.
   * @param jobId - The job identifier
   * @returns The output buffer result
   */
  getBuffer(jobId: string, _templateId?: string): OutputBuffer {
    if (!this.buffers.has(jobId)) {
      this.buffers.set(jobId, new OutputBuffer(jobId));
    }
    return this.buffers.get(jobId)!;
  }

    /**
   * Flushes all
   * @returns A promise that resolves to the list
   */
  async flushAll(): Promise<string[]> {
    const paths: string[] = [];

    for (const buffer of this.buffers.values()) {
      await buffer.waitForPendingFlush();
      await buffer.flush();
      const flushedPaths = buffer.getFlushedPaths();
      if (flushedPaths.length > 0) {
        paths.push(...flushedPaths);
      }
    }

    this.buffers.clear();
    return paths;
  }

    /**
   * Flushes template
   * @param jobId - The job identifier
   * @param templateId - The template id
   * @returns A promise that resolves to the result
   */
  async flushBuffer(jobId: string): Promise<string | null> {
    const buffer = this.buffers.get(jobId);
    if (buffer) {
      const path = await buffer.flush();
      this.buffers.delete(jobId);
      return path;
    }
    return null;
  }
}

export { OutputRow };

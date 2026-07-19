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
   * Gets buffer
   * @param jobId - The job identifier
   * @param templateId - The template id
   * @returns The output buffer result
   */
  getBuffer(jobId: string, templateId: string): OutputBuffer {
    const key = `${jobId}-${templateId}`;
    if (!this.buffers.has(key)) {
      this.buffers.set(key, new OutputBuffer(jobId, templateId));
    }
    return this.buffers.get(key)!;
  }

    /**
   * Flushes all
   * @returns A promise that resolves to the list
   */
  async flushAll(): Promise<string[]> {
    const paths: string[] = [];

    for (const buffer of this.buffers.values()) {
      await buffer.waitForPendingFlush();
      const path = await buffer.flush();
      if (path) {
        paths.push(path);
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
  async flushTemplate(jobId: string, templateId: string): Promise<string | null> {
    const key = `${jobId}-${templateId}`;
    const buffer = this.buffers.get(key);
    if (buffer) {
      const path = await buffer.flush();
      this.buffers.delete(key);
      return path;
    }
    return null;
  }
}

export { OutputRow };

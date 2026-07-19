import { OutputBuffer, type OutputRow } from "./OutputBuffer.js";

export class OutputManager {
  private buffers = new Map<string, OutputBuffer>();

  getBuffer(jobId: string, templateId: string): OutputBuffer {
    const key = `${jobId}-${templateId}`;
    if (!this.buffers.has(key)) {
      this.buffers.set(key, new OutputBuffer(jobId, templateId));
    }
    return this.buffers.get(key)!;
  }

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

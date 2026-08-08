import { randomUUID } from "node:crypto";
import { OutputBuffer } from "./OutputBuffer.js";
import { parquetOutputService } from "./ParquetOutputService.js";
import { settings } from "./Settings.js";
import { DatabaseService } from "./DatabaseManager.js";

/**
 * OutputManager manages the resource lifecycle of OutputBuffer instances
 * across jobs — creation on first use, and coordinated flush + cleanup
 * for all of them at once.
 */

export class OutputManager
{
  /**
   * Buffers
   * @private
   */

  private readonly buffers: Map<string, OutputBuffer> = new Map();

  /**
   * Gets or creates the single output buffer for a job.
   * @param jobId - The job identifier
   * @param _templateId - Reserved for future template-specific buffering; unused for now
   * @returns The output buffer result
   */

  public getBuffer(jobId: string, templateId = "mixed"): OutputBuffer
  {
    let buffer: OutputBuffer | undefined = this.buffers.get(jobId);

    if (!buffer)
    {
      buffer = OutputBuffer.getInstance(jobId, templateId);
      this.buffers.set(jobId, buffer);
    }

    return buffer;
  }

  /**
   * Flushes all buffers, collects every path written, and releases each
   * job's OutputBuffer singleton once it has been drained.
   * @returns A promise that resolves to the list of flushed GCS paths
   */

  public async flushAll(): Promise<string[]>
  {
    const paths: string[] = [];

    for (const [jobId, buffer] of this.buffers.entries())
    {
      await buffer.waitForPendingFlush();
      await buffer.flush();

      const flushedPaths: string[] = buffer.getFlushedPaths();
      const flushedParts = buffer.getFlushedParts();

      if (flushedPaths.length > 0)
      {
        paths.push(...flushedPaths);

        const manifest: Record<string, unknown> = {
          job_id: jobId,
          parts: flushedPaths,
          completed_at: new Date().toISOString(),
        };
        await parquetOutputService.getGcsUtils().putJson(
            settings.DATA_BUCKET,
            `output/${jobId}/_MANIFEST.json`,
            manifest,
        );
      }

      for (const part of flushedParts)
      {
        try
        {
          await DatabaseService.getInstance().repositories.outputParts.create({
            part_id: randomUUID(),
            job_id: jobId,
            template_id: part.template_id,
            s3_path: part.path,
            row_count: part.row_count,
            byte_size: 0,
          });
        }
        catch (err)
        {
          parquetOutputService.getLogger().error("output_part_record_failed", {
            job_id: jobId,
            path: part.path,
            error: String(err),
          });
        }
      }

      OutputBuffer.releaseInstance(jobId);
    }

    this.buffers.clear();
    return paths;
  }
}

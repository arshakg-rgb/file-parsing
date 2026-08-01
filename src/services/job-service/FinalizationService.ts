import pino from "pino";
import { randomUUID } from "crypto";
import { settings } from "@shared/Settings.js";
import { readFull, putObject, objectSize, deleteObject } from "@shared/GcsUtils.js";
import {DatabaseService, type DeadLetterRow} from "@shared/DatabaseManager.js";
import { createLogger } from "@utils/logger/Log.js";
import { LineNumberMapper } from "@service/job-service/finalize/LineNumberMapper.js";
import { ParquetEngine, type ParquetRow } from "@service/job-service/finalize/ParquetEngine.js";
import { StoragePath, type GcsProtocol } from "@service/job-service/finalize/StoragePath.js";
import type { FinalizeResult } from "@service/job-service/io/IFinalizationService.js";
export type { FinalizeResult } from "@service/job-service/io/IFinalizationService.js";

/**
 * High-level service that orchestrates output finalization.
 * Composes repository, storage, Parquet, and line-mapping concerns.
 */
class FinalizationService {
  private readonly logger: pino.Logger = createLogger(module);

    /**
   * Performs the finalize output operation.
   * @param jobId - The job identifier
   * @param partPaths - The part paths
   * @param bucket - The bucket
   * @returns A promise that resolves to the result
   */
  async finalizeOutput(jobId: string, partPaths: string[], bucket: string): Promise<FinalizeResult> {
    if (!partPaths.length) {
      return { failed: false, paths: [] };
    }

    const groups = this.groupByTemplate(partPaths);
    const mergedPaths: string[] = [];

    for (const group of groups.values()) {
      try {
        const groupPaths = await this.mergeGroup(jobId, group, bucket);
        if (groupPaths?.length) mergedPaths.push(...groupPaths);
        await DatabaseService.getInstance().repositories.jobLogs.log({
          job_id: jobId,
          event_type: "template_used",
          stage: "finalize",
          template_id: group.templateId,
          message: null,
          metadata: { part_count: group.paths.length, output_count: groupPaths?.length ?? 0 },
        });
      } catch (err) {
        this.logger.error({ jobId, templateId: group.templateId, error: String(err) }, "finalize_merge_failed");
        await DatabaseService.getInstance().repositories.jobLogs.log({
          job_id: jobId,
          event_type: "crashed",
          stage: "finalize",
          template_id: group.templateId,
          message: String(err),
          metadata: { part_count: group.paths.length },
        });
        return { failed: true, paths: partPaths, error: String(err) };
      }
    }

    // Cross-template final merge: if the per-template outputs are small enough, collapse
    // them into one job-level merged Parquet file so callers receive a single output_paths entry.
    try {
      this.logger.info({ jobId, mergedPaths_count: mergedPaths.length }, "finalize_cross_merge_check");
      if (mergedPaths.length > 1) {
        const mergedStoragePaths = mergedPaths.map((p) => StoragePath.parse(p));
        const totalMergedSize = await this.totalPartSize(mergedStoragePaths);
        this.logger.info({ jobId, totalMergedSize, max_size: settings.MAX_MERGED_PART_BYTES }, "finalize_cross_merge_size_check");
        if (totalMergedSize <= settings.MAX_MERGED_PART_BYTES) {
          const allRows = await this.mergeRows(mergedStoragePaths);
          this.logger.info({ jobId, rows_count: allRows.length }, "finalize_cross_merge_rows");
          if (allRows.length) {
            this.normalizeLineNumbers(allRows);
            const finalKey = `output/${jobId}.parquet`;
            const finalPath = new StoragePath(mergedStoragePaths[0].protocol, bucket, finalKey);
            await ParquetEngine.writeRows(finalPath, allRows);
            await this.backfillLineNumbers(jobId, [finalPath]);
            // Delete raw parts after successful merge
            this.logger.info({ jobId, parts_count: partPaths.length }, "finalize_delete_parts_start");
            for (const p of partPaths) {
              try {
                const storagePath = StoragePath.parse(p);
                this.logger.info({ jobId, path: p }, "finalize_delete_part");
                await deleteObject(storagePath.bucket, storagePath.key);
                this.logger.info({ jobId, path: p }, "finalize_delete_part_success");
              } catch (err) {
                this.logger.error({ path: p, error: String(err) }, "finalize_delete_part_failed");
              }
            }
            this.logger.info({ jobId }, "finalize_delete_parts_complete");
            // Delete per-template merged files after successful cross-merge
            this.logger.info({ jobId, merged_count: mergedPaths.length }, "finalize_delete_merged_start");
            for (const p of mergedPaths) {
              try {
                const storagePath = StoragePath.parse(p);
                this.logger.info({ jobId, path: p }, "finalize_delete_merged");
                await deleteObject(storagePath.bucket, storagePath.key);
                this.logger.info({ jobId, path: p }, "finalize_delete_merged_success");
              } catch (err) {
                this.logger.error({ path: p, error: String(err) }, "finalize_delete_merged_failed");
              }
            }
            this.logger.info({ jobId }, "finalize_delete_merged_complete");
            this.logger.info({ jobId, final_path: finalPath.toString() }, "finalize_cross_merge_success");
            return { failed: false, paths: [finalPath.toString()] };
          } else {
            this.logger.info({ jobId }, "finalize_cross_merge_skip_empty");
          }
        } else {
          this.logger.info({ jobId, totalMergedSize }, "finalize_cross_merge_skip_too_large");
        }
      }
    } catch (err) {
      this.logger.error({ jobId, error: String(err) }, "finalize_cross_merge_failed");
      // Continue with the per-template merged paths rather than failing the whole job.
    }

    await this.backfillLineNumbers(jobId, mergedPaths.map((p) => StoragePath.parse(p)));
    return { failed: false, paths: mergedPaths };
  }

    /**
   * Merges group
   * @param jobId - The job identifier
   * @param group - The group
   * @param bucket - The bucket
   * @returns A promise that resolves to the list
   */
  private async mergeGroup(
    jobId: string,
    group: { templateId: string; paths: StoragePath[]; protocol: GcsProtocol },
    bucket: string
  ): Promise<string[]> {
    const groupSize = await this.totalPartSize(group.paths);
    if (groupSize > settings.MAX_MERGED_PART_BYTES) {
      // Too large to merge safely; keep the original part paths.
      return group.paths.map((p) => p.toString());
    }

    const rows = await this.mergeRows(group.paths);
    if (!rows.length) {
      return group.paths.map((p) => p.toString());
    }

    this.normalizeLineNumbers(rows);

    const mergedId = randomUUID();
    const mergedKey = `outputs/${jobId}/merged/${group.templateId}/${mergedId}.parquet`;
    const mergedPath = new StoragePath(group.protocol, bucket, mergedKey);

    await ParquetEngine.writeRows(mergedPath, rows);
    return [mergedPath.toString()];
  }

    /**
   * Groups by template
   * @param partPaths - The part paths
   * @returns The map<string, { template id: string; paths:  storage path[]; protocol:  gcs protocol }> result
   */
  private groupByTemplate(
    partPaths: string[]
  ): Map<string, { templateId: string; paths: StoragePath[]; protocol: GcsProtocol }> {
    const groups = new Map<string, { templateId: string; paths: StoragePath[]; protocol: GcsProtocol }>();
    for (const url of partPaths) {
      const parsed = StoragePath.parse(url);
      const templateId = this.extractTemplateId(parsed.key);
      let group = groups.get(templateId);
      if (!group) {
        group = { templateId, paths: [], protocol: parsed.protocol };
        groups.set(templateId, group);
      }
      group.paths.push(parsed);
    }
    return groups;
  }

    /**
   * Extracts template id
   * @param key - The key
   * @returns The string result
   */
  private extractTemplateId(key: string): string {
    const segments = key.split("/");
    const partsIndex = segments.lastIndexOf("parts");
    if (partsIndex >= 0 && partsIndex + 1 < segments.length) {
      return segments[partsIndex + 1];
    }
    const fallback = segments[segments.length - 2];
    return fallback && fallback !== "" ? fallback : "unknown";
  }

    /**
   * Performs the total part size operation.
   * @param paths - The paths
   * @returns A promise that resolves to the result
   */
  private async totalPartSize(paths: StoragePath[]): Promise<number> {
    let total = 0;
    for (const p of paths) {
      total += await objectSize(p.bucket, p.key);
    }
    return total;
  }

    /**
   * Merges rows
   * @param paths - The paths
   * @returns A promise that resolves to the list
   */
  private async mergeRows(paths: StoragePath[]): Promise<ParquetRow[]> {
    const rows: ParquetRow[] = [];
    for (const p of paths) {
      const chunk = await ParquetEngine.readRows(p);
      for (const row of chunk) {
        rows.push(row);
      }
    }
    return rows;
  }

    /**
   * Normalizes line numbers
   * @param rows - The rows
   */
  private normalizeLineNumbers(rows: ParquetRow[]): void {
    rows.sort((a, b) => Number(a._line_no ?? 0) - Number(b._line_no ?? 0));
    let nextLineNo = 1;
    for (const r of rows) {
      if (r._line_no === undefined || r._line_no === null || r._line_no === 0) {
        r._line_no = nextLineNo;
      }
      nextLineNo++;
    }
  }

    /**
   * Performs the backfill line numbers operation.
   * @param jobId - The job identifier
   * @param mergedPaths - The merged paths
   */
  private async backfillLineNumbers(jobId: string, mergedPaths: StoragePath[]): Promise<void> {
    const job = await DatabaseService.getInstance().getJob(jobId);
    if (!job?.s3_url) {
      this.logger.info({ jobId }, "backfill_skip_no_source");
      return;
    }

    const sourcePath = StoragePath.parse(job.s3_url);
    const key = sourcePath.key.toLowerCase();
    if (key.endsWith(".json") && !key.endsWith(".ndjson")) {
      // Pretty-printed JSON files are not line-oriented; byte offsets stored during parsing
      // are record indexes, not source-file byte positions. Line-number backfill would map
      // many records to the same source line and break the (job_id, line_no) unique constraint.
      this.logger.info({ jobId, s3_url: job.s3_url }, "backfill_skip_json_source");
      return;
    }

    const timings = (job.timings as Record<string, unknown>) || {};
    const rubbishLogPath = timings._rubbish_log_path as string | undefined;

    let source: Buffer | undefined;
    try {
      const sourcePath = StoragePath.parse(job.s3_url);
      source = await readFull(sourcePath.bucket, sourcePath.key);
    } catch (e) {
      this.logger.warn({ jobId, error: String(e) }, "backfill_source_read_failed");
      return;
    }

    const targetOffsets = new Set<number>();
    for (const p of mergedPaths) {
      try {
        const rows = await ParquetEngine.readRows(p);
        for (const r of rows) {
          if (r._byte_offset !== undefined && r._byte_offset !== null) {
            targetOffsets.add(Number(ParquetEngine.sanitizeValue(r._byte_offset, false)));
          }
        }
      } catch (e) {
        this.logger.warn({ jobId, path: p.toString(), error: String(e) }, "backfill_parsed_read_failed");
      }
    }

    const deadLetters: DeadLetterRow[] = await DatabaseService.getInstance().repositories.deadLetters.findByJob(jobId);
    for (const dlq of deadLetters) {
      targetOffsets.add(Number(dlq.byte_offset));
    }

    let rubbishEntries: Array<Record<string, unknown>> = [];
    if (rubbishLogPath) {
      try {
        const raw = await readFull(StoragePath.parse(rubbishLogPath).bucket, StoragePath.parse(rubbishLogPath).key);
        const text = raw.toString("utf-8");
        rubbishEntries = text
          .split("\n")
          .filter((l) => l.trim())
          .map((l) => JSON.parse(l));
        for (const e of rubbishEntries) {
          if (typeof e.byte_offset === "number") {
            targetOffsets.add(e.byte_offset);
          }
        }
      } catch (e) {
        this.logger.warn({ jobId, error: String(e) }, "backfill_rubbish_read_failed");
      }
    }

    const sortedOffsets = Array.from(targetOffsets).sort((a, b) => a - b);
    const lineMap = LineNumberMapper.computeLineMap(source, sortedOffsets);

    for (const dlq of deadLetters) {
      const line = lineMap.get(Number(dlq.byte_offset));
      if (line !== undefined) {
        await DatabaseService.getInstance().repositories.deadLetters.updateLineNo(dlq.dlq_id, line);
      }
    }

    if (rubbishLogPath && rubbishEntries.length) {
      await this.updateRubbishLog(jobId, rubbishLogPath, rubbishEntries, lineMap);
    }

    for (const p of mergedPaths) {
      await this.backfillParquet(p, lineMap);
    }
  }

    /**
   * Updates rubbish log
   * @param jobId - The job identifier
   * @param rubbishLogPath - The rubbish log path
   * @param entries - The entries
   * @param lineMap - The line map
   */
  private async updateRubbishLog(
    jobId: string,
    rubbishLogPath: string,
    entries: Array<Record<string, unknown>>,
    lineMap: Map<number, number>
  ): Promise<void> {
    let changed = false;
    const updated = entries.map((e) => {
      const line = lineMap.get(e.byte_offset as number);
      if (line !== undefined && (e.line_no as number | undefined) !== line) {
        changed = true;
        return { ...e, line_no: line };
      }
      return e;
    });

    if (!changed) {
      return;
    }

    const logPath = StoragePath.parse(rubbishLogPath);
    const body = Buffer.from(updated.map((e) => JSON.stringify(e)).join("\n"));
    try {
      await putObject(logPath.bucket, logPath.key, body, "application/x-ndjson");
      this.logger.info({ jobId, entries: updated.length }, "rubbish_log_backfilled");
    } catch (e) {
      this.logger.warn({ jobId, error: String(e) }, "backfill_rubbish_write_failed");
    }
  }

    /**
   * Performs the backfill parquet operation.
   * @param storagePath - The storage path
   * @param lineMap - The line map
   */
  private async backfillParquet(storagePath: StoragePath, lineMap: Map<number, number>): Promise<void> {
    try {
      const rows = await ParquetEngine.readRows(storagePath);
      let fileChanged = false;
      for (const r of rows) {
        const line = lineMap.get(r._byte_offset as number);
        if (line !== undefined && (r._line_no as number | undefined) !== line) {
          r._line_no = line;
          fileChanged = true;
        }
      }

      if (fileChanged) {
        await ParquetEngine.writeRows(storagePath, rows);
      }
    } catch (e) {
      this.logger.warn({ path: storagePath.toString(), error: String(e) }, "backfill_output_failed");
    }
  }
}

/**
 * Performs the finalize output operation.
 * @param jobId - The job identifier
 * @param partPaths - The part paths
 * @param bucket - The bucket
 * @returns A promise that resolves to the result
 */
export async function finalizeOutput(jobId: string, partPaths: string[], bucket: string): Promise<FinalizeResult> {
  const service = new FinalizationService();
  return service.finalizeOutput(jobId, partPaths, bucket);
}

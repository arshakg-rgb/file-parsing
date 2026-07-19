import { randomUUID } from "crypto";
import { settings } from "../../shared/Settings.js";
import { FinalizeRepository } from "./finalize/FinalizeRepository.js";
import { GcsObjectStorage } from "./finalize/GcsObjectStorage.js";
import { IObjectStorage } from "./finalize/IObjectStorage.js";
import { LineNumberMapper } from "./finalize/LineNumberMapper.js";
import { ParquetEngine, type ParquetRow } from "./finalize/ParquetEngine.js";
import { StoragePath, type GcsProtocol } from "./finalize/StoragePath.js";
import type { FinalizeResult } from "./io/IFinalizationService.js";

export type { FinalizeResult } from "./io/IFinalizationService.js";

/**
 * High-level service that orchestrates output finalization.
 * Composes repository, storage, Parquet, and line-mapping concerns.
 */
class FinalizationService {
  private readonly repository: FinalizeRepository;
  private readonly storage: IObjectStorage;
  private readonly engine: typeof ParquetEngine;
  private readonly lineMapper: LineNumberMapper;

  constructor(
    repository: FinalizeRepository = new FinalizeRepository(),
    storage: IObjectStorage = new GcsObjectStorage(),
    engine: typeof ParquetEngine = ParquetEngine,
    lineMapper: LineNumberMapper = new LineNumberMapper()
  ) {
    this.repository = repository;
    this.storage = storage;
    this.engine = engine;
    this.lineMapper = lineMapper;
  }

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
      } catch (err) {
        console.error("finalize_merge_failed", { jobId, templateId: group.templateId, error: String(err) });
        return { failed: true, paths: partPaths, error: String(err) };
      }
    }

    // Cross-template final merge: if the per-template outputs are small enough, collapse
    // them into one job-level merged Parquet file so callers receive a single output_paths entry.
    try {
      if (mergedPaths.length > 1) {
        const mergedStoragePaths = mergedPaths.map((p) => StoragePath.parse(p));
        const totalMergedSize = await this.totalPartSize(mergedStoragePaths);
        if (totalMergedSize <= settings.MAX_MERGED_PART_BYTES) {
          const allRows = await this.mergeRows(mergedStoragePaths);
          if (allRows.length) {
            this.normalizeLineNumbers(allRows);
            const finalKey = `outputs/${jobId}/merged/output.parquet`;
            const finalPath = new StoragePath(mergedStoragePaths[0].protocol, bucket, finalKey);
            await this.engine.writeRows(this.storage, finalPath, allRows);
            await this.backfillLineNumbers(jobId, [finalPath]);
            return { failed: false, paths: [finalPath.toString()] };
          }
        }
      }
    } catch (err) {
      console.error("finalize_cross_merge_failed", { jobId, error: String(err) });
      // Continue with the per-template merged paths rather than failing the whole job.
    }

    await this.backfillLineNumbers(jobId, mergedPaths.map((p) => StoragePath.parse(p)));
    return { failed: false, paths: mergedPaths };
  }

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

    await this.engine.writeRows(this.storage, mergedPath, rows);
    return [mergedPath.toString()];
  }

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

  private extractTemplateId(key: string): string {
    const filename = key.split("/").pop() || "";
    const parts = filename.split("-");
    const jobIdEndIndex = 5;
    if (parts.length > jobIdEndIndex + 1) {
      const timestampIndex = parts.length - 1;
      return parts.slice(jobIdEndIndex, timestampIndex).join("-");
    }
    return "unknown";
  }

  private async totalPartSize(paths: StoragePath[]): Promise<number> {
    let total = 0;
    for (const p of paths) {
      total += await this.storage.size(p);
    }
    return total;
  }

  private async mergeRows(paths: StoragePath[]): Promise<ParquetRow[]> {
    const rows: ParquetRow[] = [];
    for (const p of paths) {
      rows.push(...(await this.engine.readRows(this.storage, p)));
    }
    return rows;
  }

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

  private async backfillLineNumbers(jobId: string, mergedPaths: StoragePath[]): Promise<void> {
    const job = await this.repository.getJob(jobId);
    if (!job?.s3_url) {
      console.log("backfill_skip_no_source", { jobId });
      return;
    }

    const timings = (job.timings as Record<string, unknown>) || {};
    const rubbishLogPath = timings._rubbish_log_path as string | undefined;

    let source: Buffer | undefined;
    try {
      source = await this.storage.read(StoragePath.parse(job.s3_url));
    } catch (e) {
      console.warn("backfill_source_read_failed", { jobId, error: String(e) });
      return;
    }

    const targetOffsets = new Set<number>();
    for (const p of mergedPaths) {
      try {
        const rows = await this.engine.readRows(this.storage, p);
        for (const r of rows) {
          if (r._byte_offset !== undefined && r._byte_offset !== null) {
            targetOffsets.add(Number(ParquetEngine.sanitizeValue(r._byte_offset, false)));
          }
        }
      } catch (e) {
        console.warn("backfill_parsed_read_failed", { jobId, path: p.toString(), error: String(e) });
      }
    }

    const deadLetters = await this.repository.getDeadLetters(jobId);
    for (const dlq of deadLetters) {
      targetOffsets.add(dlq.byteOffset);
    }

    let rubbishEntries: Array<Record<string, unknown>> = [];
    if (rubbishLogPath) {
      try {
        const raw = await this.storage.read(StoragePath.parse(rubbishLogPath));
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
        console.warn("backfill_rubbish_read_failed", { jobId, error: String(e) });
      }
    }

    const sortedOffsets = Array.from(targetOffsets).sort((a, b) => a - b);
    const lineMap = this.lineMapper.computeLineMap(source, sortedOffsets);

    for (const dlq of deadLetters) {
      const line = lineMap.get(dlq.byteOffset);
      if (line !== undefined) {
        await this.repository.updateDeadLetterLineNo(dlq.dlqId, line);
      }
    }

    if (rubbishLogPath && rubbishEntries.length) {
      await this.updateRubbishLog(jobId, rubbishLogPath, rubbishEntries, lineMap);
    }

    for (const p of mergedPaths) {
      await this.backfillParquet(p, lineMap);
    }
  }

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
      await this.storage.write(logPath, body, "application/x-ndjson");
      console.log("rubbish_log_backfilled", { jobId, entries: updated.length });
    } catch (e) {
      console.warn("backfill_rubbish_write_failed", { jobId, error: String(e) });
    }
  }

  private async backfillParquet(storagePath: StoragePath, lineMap: Map<number, number>): Promise<void> {
    try {
      const rows = await this.engine.readRows(this.storage, storagePath);
      let fileChanged = false;
      for (const r of rows) {
        const line = lineMap.get(r._byte_offset as number);
        if (line !== undefined && (r._line_no as number | undefined) !== line) {
          r._line_no = line;
          fileChanged = true;
        }
      }

      if (fileChanged) {
        await this.engine.writeRows(this.storage, storagePath, rows);
      }
    } catch (e) {
      console.warn("backfill_output_failed", { path: storagePath.toString(), error: String(e) });
    }
  }
}

export async function finalizeOutput(jobId: string, partPaths: string[], bucket: string): Promise<FinalizeResult> {
  const service = new FinalizationService();
  return service.finalizeOutput(jobId, partPaths, bucket);
}

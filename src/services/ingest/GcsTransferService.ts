import pino from "pino";
import { settings } from "@shared/Settings.js";
import { createLogger } from "@utils/logger/Log.js";
import { GcsEntryStore } from "./GcsEntryStore.js";
import {InstantiationError} from "@errors/InstantiationError";
import {SsrfGuard} from "@service/ingest/SsrfGuard";
import {GcsUtils} from "@shared/GcsUtils";

const logger: pino.Logger = createLogger(module);

export class GcsTransferService
{
    private static instance: GcsTransferService;

    private readonly entryStore: GcsEntryStore;

    private gcsUtils: GcsUtils;

    private constructor(enforce: () => void, gcsUtils: GcsUtils)
    {
        if (enforce !== Enforce)
        {
            throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use GcsTransferService.getInstance() instead of new.");
        }

        this.gcsUtils = gcsUtils;
        this.entryStore = GcsEntryStore.getInstance();
    }

    /**
     * Gets the singleton instance of GcsTransferService.
     *
     * @returns The singleton instance of GcsTransferService.
     */

    static getInstance(): GcsTransferService
    {
        if (!GcsTransferService.instance)
        {
            GcsTransferService.instance = new GcsTransferService(Enforce, GcsUtils.getInstance());
        }

        return GcsTransferService.instance;
    }

    public async fetchUrlToS3(jobId: string, url: string): Promise<[string, number]>
    {
        if (url.startsWith("gs://"))
        {
            const [bucket, key] = this.gcsUtils.parseGcsUrl(url);
            const size: number = await this.gcsUtils.objectSize(bucket, key);
            const s3Key: string = this.entryStore.sourceKeyFor(jobId);

            if (size > settings.SMALL_FILE_SINGLE_GET_THRESHOLD)
            {
                logger.info("gcs_streaming_copy", { jobId, size, threshold: settings.SMALL_FILE_SINGLE_GET_THRESHOLD });
                await this.streamGcsToGcs(bucket, key, settings.DATA_BUCKET, s3Key);
            }
            else
            {
                const data: Buffer = await this.gcsUtils.readFull(bucket, key);
                await this.gcsUtils.putObject(settings.DATA_BUCKET, s3Key, data);
            }

            const s3Url = `gs://${settings.DATA_BUCKET}/${s3Key}`;
            logger.info("gcs_copied_to_gcs", { jobId, s3Url, bytes: size });
            return [s3Url, size];
        }

        const s3Key: string = this.entryStore.sourceKeyFor(jobId);
        const chunks: Buffer[] = [];
        let total: number = 0;

        for await (const chunk of SsrfGuard.getInstance().fetchUrlStream(url))
        {
            total += chunk.length;
            chunks.push(chunk);
        }
        const body: Buffer<ArrayBuffer> = Buffer.concat(chunks);
        await this.gcsUtils.putObject(settings.DATA_BUCKET, s3Key, body);
        const s3Url = `gs://${settings.DATA_BUCKET}/${s3Key}`;
        logger.info("url_fetched_to_gcs", { jobId, s3Url, bytes: total });
        return [s3Url, total];
    }

    private async streamGcsToGcs(srcBucket: string, srcKey: string, dstBucket: string, dstKey: string): Promise<void>
    {
        await this.gcsUtils.copyObject(srcBucket, srcKey, dstBucket, dstKey);
        logger.info("gcs_copy_complete", { srcBucket, srcKey, dstBucket, dstKey });
    }

    async listS3Prefix(prefixUrl: string): Promise<[string, number][]>
    {
        const [bucket, prefix] = this.gcsUtils.parseGcsUrl(prefixUrl);
        return this.gcsUtils.listObjects(bucket, prefix);
    }
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}

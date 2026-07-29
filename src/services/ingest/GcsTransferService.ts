import pino from "pino";
import { settings } from "@shared/Settings.js";
import {
    parseGcsUrl as parseS3Url,
    objectSize,
    readFull,
    putObject,
    listObjects,
    copyObject,
} from "@shared/GcsUtils.js";
import { createLogger } from "@utils/logger/Log.js";
import { GcsEntryStore } from "./GcsEntryStore.js";
import {InstantiationError} from "@errors/InstantiationError";
import {SsrfGuard} from "@service/ingest/SsrfGuard";

const logger: pino.Logger = createLogger(module);

export class GcsTransferService
{
    private static instance: GcsTransferService;

    private readonly entryStore: GcsEntryStore;

    private constructor(enforce: () => void)
    {
        if (enforce !== Enforce)
        {
            throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use GcsTransferService.getInstance() instead of new.");
        }

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
            GcsTransferService.instance = new GcsTransferService(Enforce);
        }

        return GcsTransferService.instance;
    }

    public async fetchUrlToS3(jobId: string, url: string): Promise<[string, number]>
    {
        if (url.startsWith("gs://"))
        {
            const [bucket, key] = parseS3Url(url);
            const size: number = await objectSize(bucket, key);
            const s3Key: string = this.entryStore.sourceKeyFor(jobId);

            if (size > settings.SMALL_FILE_SINGLE_GET_THRESHOLD)
            {
                logger.info("gcs_streaming_copy", { jobId, size, threshold: settings.SMALL_FILE_SINGLE_GET_THRESHOLD });
                await this.streamGcsToGcs(bucket, key, settings.DATA_BUCKET, s3Key);
            }
            else
            {
                const data: Buffer = await readFull(bucket, key);
                await putObject(settings.DATA_BUCKET, s3Key, data);
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
        await putObject(settings.DATA_BUCKET, s3Key, body);
        const s3Url = `gs://${settings.DATA_BUCKET}/${s3Key}`;
        logger.info("url_fetched_to_gcs", { jobId, s3Url, bytes: total });
        return [s3Url, total];
    }

    private async streamGcsToGcs(srcBucket: string, srcKey: string, dstBucket: string, dstKey: string): Promise<void>
    {
        await copyObject(srcBucket, srcKey, dstBucket, dstKey);
        logger.info("gcs_copy_complete", { srcBucket, srcKey, dstBucket, dstKey });
    }

    async listS3Prefix(prefixUrl: string): Promise<[string, number][]>
    {
        const [bucket, prefix] = parseS3Url(prefixUrl);
        return listObjects(bucket, prefix);
    }
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}

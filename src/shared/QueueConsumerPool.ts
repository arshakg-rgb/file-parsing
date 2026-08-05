import pino from "pino";
import { QueueService } from "@shared/QueueService.js";
import type { QueueMessage } from "@shared/io/IQueueService.js";

/**
 * Options for a QueueConsumerPool.
 */
export interface QueueConsumerPoolOptions<TPayload extends object>
{
  queueUrl: string;
  parser: (body: string) => TPayload;
  concurrency?: number;
  waitSeconds?: number;
  pollBackoffMs?: number;
  isRunning?: () => boolean;
}

/**
 * Long-running queue consumer that keeps up to `concurrency` messages
 * in flight at once and re-polls as soon as a slot becomes free.
 */
export class QueueConsumerPool<TPayload extends object>
{
  private activeJobs: Map<string, Promise<void>> = new Map();

  constructor(
      private readonly queueService: QueueService,
      private readonly logger: pino.Logger,
      private readonly options: QueueConsumerPoolOptions<TPayload>
  ) {}

  async run(handler: (payload: TPayload, receiptHandle: string) => Promise<void>): Promise<void>
  {
    const concurrency: number = Math.max(1, this.options.concurrency ?? 3);
    const waitSeconds: number = Math.max(0, this.options.waitSeconds ?? 5);
    const pollBackoffMs: number = Math.max(0, this.options.pollBackoffMs ?? 1000);
    const isRunning: () => boolean = this.options.isRunning ?? (() => true);

    while (isRunning())
    {
      const freeSlots: number = concurrency - this.activeJobs.size;

      if (freeSlots > 0)
      {
        try
        {
          const messages: QueueMessage<TPayload>[] = await this.queueService.receiveMessages<TPayload>(
              this.options.queueUrl,
              this.options.parser,
              Math.min(freeSlots, 10),
              waitSeconds
          );

          if (messages.length > 0)
          {
            for (const { payload, receiptHandle } of messages)
            {
              const jobPromise: Promise<void> = this.processOne(handler, payload, receiptHandle)
                  .finally(() => { this.activeJobs.delete(receiptHandle); });
              this.activeJobs.set(receiptHandle, jobPromise);
            }
            continue;
          }
        }
        catch (err)
        {
          this.logger.error("queue_receive_error", { error: String(err) });
        }
      }

      if (this.activeJobs.size > 0)
      {
        if (freeSlots > 0)
        {
          await new Promise((resolve) => setTimeout(resolve, pollBackoffMs));
        }
        else
        {
          await Promise.race(Array.from(this.activeJobs.values()));
        }
      }
      else if (isRunning())
      {
        await new Promise((resolve) => setTimeout(resolve, pollBackoffMs));
      }
    }

    await Promise.all(Array.from(this.activeJobs.values()));
  }

  private async processOne(handler: (payload: TPayload, receiptHandle: string) => Promise<void>, payload: TPayload, receiptHandle: string): Promise<void>
  {
    try
    {
      await handler(payload, receiptHandle);
    }
    catch (err)
    {
      this.logger.error("queue_message_failed", { error: String(err) });
    }
  }
}

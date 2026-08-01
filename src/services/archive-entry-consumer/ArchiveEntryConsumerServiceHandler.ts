import pino from "pino";
import { IArchiveEntryConsumer, ArchiveEntryRequest, ArchiveEntryConsumerOptions } from "@service/archive-entry-consumer/io/IArchiveEntryConsumer.js";
import ArchiveEntryConsumerServiceImpl from "@service/archive-entry-consumer/impl/ArchiveEntryConsumerServiceImpl.js";
import { settings } from "@shared/Settings.js";
import { receiveMessages, deleteMessage, QueueMessage } from "@shared/QueueService.js";
import { createLogger } from "@utils/logger/Log.js";
import {DatabaseService} from "@shared/DatabaseManager";

/**
 * Consumes archive entry jobs from a queue and processes them one at a time.
 * Owns its own polling loop, error backoff/retry, and OS signal handling for
 * graceful shutdown.
 */

export class ArchiveEntryConsumer
{
    private readonly logger: pino.Logger;
    private readonly service: IArchiveEntryConsumer;
    private readonly queueUrl: string;
    private readonly maxMessages: number;
    private readonly errorBackoffMs: number;
    private readonly signals: NodeJS.Signals[] = ["SIGTERM", "SIGINT"];

    private running: boolean = false;
    private shuttingDown: boolean = false;
    private loopPromise: Promise<void> | null = null;

    /**
     * Creates a new consumer instance.
     *
     * @param service - Service implementation used to process each archive entry
     * @param options - Queue URL and optional tuning parameters (batch size, backoff)
     * @param logger - Logger instance; defaults to a new "archive-entry-consumer" logger
     */

    constructor(service: IArchiveEntryConsumer, options: ArchiveEntryConsumerOptions, logger: pino.Logger = createLogger(module))
    {
      this.service = service;
      this.queueUrl = options.queueUrl;
      this.maxMessages = options.maxMessages ?? 3;
      this.errorBackoffMs = options.errorBackoffMs ?? 5000;
      this.logger = logger;
    }

    /**
     * Builds a consumer wired to the real singleton service and app settings.
     * Use this for production; use the constructor directly when injecting
     * mocks for tests.
     *
     * @returns A fully configured ArchiveEntryConsumer ready to run
     */

    static create(): ArchiveEntryConsumer
    {
      return new ArchiveEntryConsumer(
          ArchiveEntryConsumerServiceImpl.getInstance(),
          { queueUrl: settings.ARCHIVE_ENTRY_QUEUE_URL }
      );
    }

    /**
     * Registers SIGTERM/SIGINT handlers for graceful shutdown and starts the
     * consume loop. Intended as the single entrypoint call for the process.
     *
     * @returns Nothing; the loop continues running until a shutdown signal is received
     */

    public run(): void
    {
      this.registerSignalHandlers();
      this.start();
    }

    /**
     * Starts the consume loop if it isn't already running.
     * Safe to call multiple times — subsequent calls while running are no-ops.
     *
     * @returns Nothing; starts the loop asynchronously in the background
     */

    public start(): void
    {
      if (this.running)
      {
        this.logger.warn("archive_entry_consumer_already_running");
        return;
      }

      this.running = true;
      this.loopPromise = this.loop();
    }

    /**
     * Signals the consume loop to stop and waits for the current iteration to
     * finish before resolving. Safe to call even if the loop is not running.
     *
     * @returns Resolves once the loop has fully stopped
     */

    public async stop(): Promise<void>
    {
      this.running = false;

      if (this.loopPromise)
      {
        await this.loopPromise;
      }

      this.logger.info("archive_entry_consumer_stopped");
    }

    /**
     * Attaches a shutdown handler to each configured OS signal.
     *
     * @returns Nothing; handlers are registered on the process object
     */

    private registerSignalHandlers(): void
    {
      for (const signal of this.signals)
      {
        process.on(signal, () => void this.shutdown(signal));
      }
    }

    /**
     * Handles an incoming OS shutdown signal. Ensures shutdown only runs once
     * even if multiple signals arrive in quick succession, then exits the process.
     *
     * @param signal - The OS signal that triggered the shutdown
     * @returns Resolves after the consumer has stopped and the process exits
     */

    private async shutdown(signal: NodeJS.Signals): Promise<void>
    {
      if (this.shuttingDown)
      {
        return;
      }
      this.shuttingDown =
          true;

      this.logger.info("archive_entry_consumer_shutdown_signal", { signal });
      await this.stop();
      process.exit(0);
    }

    /**
     * Top-level run loop. Waits for the database to be ready, then repeatedly
     * runs the queue consume loop, restarting with a backoff delay after any
     * unhandled error until `running` is set to false.
     *
     * @returns Resolves once `running` becomes false and the loop exits
     */

    private async loop(): Promise<void>
    {
      while (this.running)
      {
        try
        {
          await DatabaseService.getInstance().waitForDb();
          this.logger.info("archive_entry_consumer_started", { queue_url: this.queueUrl });
          await this.consumeLoop();
        }
        catch (err)
        {
          this.logger.error("archive_entry_consumer_error", { error: String(err) });

          if (this.running)
          {
            await this.sleep(this.errorBackoffMs);
          }
        }
      }
    }

    /**
     * Repeatedly polls the queue for new messages and processes each one in turn.
     * Runs until `running` is set to false.
     *
     * @returns Resolves once `running` becomes false and polling stops
     */

    private async consumeLoop(): Promise<void>
    {
      while (this.running)
      {
        const messages: QueueMessage<ArchiveEntryRequest>[] = await receiveMessages<ArchiveEntryRequest>(
            this.queueUrl,
            (body) => JSON.parse(body) as ArchiveEntryRequest,
            this.maxMessages
        );

        for (const { payload, receiptHandle } of messages)
        {
          await this.processMessage(payload, receiptHandle);
        }
      }
    }

    /**
     * Processes a single queue message. Deletes the message from the queue
     * only on success; on failure, leaves it in place so it retries once its
     * visibility timeout expires.
     *
     * @param payload - Parsed archive entry request body
     * @param receiptHandle - Queue receipt handle used to delete the message
     * @returns Resolves once processing (and, on success, deletion) completes
     */

    private async processMessage(payload: ArchiveEntryRequest, receiptHandle: string): Promise<void>
    {
      try
      {
        await this.service.processEntry(payload);
        await deleteMessage(this.queueUrl, receiptHandle);
      }
      catch (err)
      {
        this.logger.error("archive_entry_message_failed", {
          job_id: payload.job_id,
          entry_name: payload.entry_name,
          error: String(err),
        });
      }
    }

    /**
     * Pauses execution for a fixed duration. Used as a backoff delay between
     * failed consume-loop restarts.
     *
     * @param ms - Number of milliseconds to wait
     * @returns Resolves after the given delay has elapsed
     */

    private sleep(ms: number): Promise<void>
    {
      return new Promise((resolve) => setTimeout(resolve, ms));
    }
}

ArchiveEntryConsumer.create().run();

export default ArchiveEntryConsumerServiceImpl;

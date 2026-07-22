import { IArchiveEntryConsumer, ArchiveEntryRequest, ArchiveEntryResponse } from "@service/archive_entry_consumer/io/IArchiveEntryConsumer.js";
import ArchiveEntryConsumerServiceImpl from "@service/archive_entry_consumer/impl/ArchiveEntryConsumerServiceImpl.js";
import { settings } from "@shared/Settings.js";
import { receiveMessages, deleteMessage } from "@shared/QueueService.js";
import { waitForDb } from "@shared/DatabaseManager.js";
import { createLogger } from "@utils/logger/logger.js";

export { default as ArchiveEntryConsumerServiceImpl } from "@service/archive_entry_consumer/impl/ArchiveEntryConsumerServiceImpl.js";
export { IArchiveEntryConsumer, ArchiveEntryRequest, ArchiveEntryResponse } from "@service/archive_entry_consumer/io/IArchiveEntryConsumer.js";

const logger = createLogger("archive-entry-consumer");

async function start(): Promise<void> {
  const service = ArchiveEntryConsumerServiceImpl.getInstance();
  let running = true;

  process.on("SIGTERM", () => { running = false; });
  process.on("SIGINT", () => { running = false; });

  while (running) {
    try {
      await waitForDb();
      logger.info("archive_entry_consumer_started", { queue_url: settings.ARCHIVE_ENTRY_QUEUE_URL });

      while (running) {
        const messages = await receiveMessages<ArchiveEntryRequest>(
          settings.ARCHIVE_ENTRY_QUEUE_URL,
          (body) => JSON.parse(body) as ArchiveEntryRequest,
          3
        );
        for (const { payload, receiptHandle } of messages) {
          try {
            await service.processEntry(payload);
            await deleteMessage(settings.ARCHIVE_ENTRY_QUEUE_URL, receiptHandle);
          } catch (err) {
            logger.error("archive_entry_message_failed", { job_id: payload.job_id, entry_name: payload.entry_name, error: String(err) });
            // Do not delete — let the message retry (visibility timeout returns it to queue)
          }
        }
      }
    } catch (err) {
      logger.error("archive_entry_consumer_error", { error: String(err) });
      await new Promise((r) => setTimeout(r, 5000));
    }
  }

  logger.info("archive_entry_consumer_stopped");
}

start().catch((err) => {
  logger.error("archive_entry_consumer_bootstrap_failed", { error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});

export default ArchiveEntryConsumerServiceImpl;

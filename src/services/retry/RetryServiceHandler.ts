import { IRetry, RetryRequest, RetryResponse } from "@service/retry/io/IRetry.js";
import RetryServiceImpl from "@service/retry/impl/RetryServiceImpl.js";
import { DLQMessage } from "@shared/models/job.js";
import { createLogger } from "@utils/logger/logger.js";

const _moduleLogger = createLogger("retry");

/**
 * Legacy RetryService class - now a thin wrapper around RetryServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class RetryService implements IRetry {
    /**
   * Service
   * @private
   */
  private service: RetryServiceImpl;

    /**
   * Constructs a new RetryService instance.
   */
  constructor() {
    this.service = RetryServiceImpl.getInstance();
  }

    /**
   * Processes retry
   * @param req - The HTTP request object
   * @returns A promise that resolves to the result
   */
  async processRetry(req: RetryRequest): Promise<RetryResponse> {
    return this.service.processRetry(req);
  }

    /**
   * Handles dlq entry
   * @param msg - The msg
   */
  async handleDlqEntry(msg: DLQMessage): Promise<void> {
    return this.service.handleDlqEntry(msg);
  }

    /**
   * Performs the consumer loop operation.
   */
  async consumerLoop(): Promise<void> {
    return this.service.consumerLoop();
  }
}

// Re-export the new service for direct use
export { default as RetryServiceImpl } from "@service/retry/impl/RetryServiceImpl.js";
export { IRetry, RetryRequest, RetryResponse } from "@service/retry/io/IRetry.js";

// Backward compatibility wrappers
const retryService = new RetryService();

/**
 * Handles dlq entry
 * @param msg - The msg
 */
export async function handleDlqEntry(msg: DLQMessage): Promise<void> {
  return retryService.handleDlqEntry(msg);
}

/**
 * Performs the consumer loop operation.
 */
export async function consumerLoop(): Promise<void> {
  return retryService.consumerLoop();
}

// Auto-start the service when module is loaded
retryService.consumerLoop().catch(err => {
  _moduleLogger.error("retry_consumer_failed", { error: String(err) });
  process.exit(1);
});

export default RetryService;

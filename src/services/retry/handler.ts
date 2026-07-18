import { IRetry, RetryRequest, RetryResponse } from "./io/IRetry.js";
import RetryServiceImpl from "./impl/RetryServiceImpl.js";
import { DLQMessage } from "../../shared/models/job.js";

/**
 * Legacy RetryService class - now a thin wrapper around RetryServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class RetryService implements IRetry 
{
  private service: RetryServiceImpl;

  constructor() 
{
    this.service = RetryServiceImpl.getInstance();
  }

  async processRetry(req: RetryRequest): Promise<RetryResponse> 
{
    return this.service.processRetry(req);
  }

  async handleDlqEntry(msg: DLQMessage): Promise<void> 
{
    return this.service.handleDlqEntry(msg);
  }

  async consumerLoop(): Promise<void> 
{
    return this.service.consumerLoop();
  }
}

export { default as RetryServiceImpl } from "./impl/RetryServiceImpl.js";
export { IRetry, RetryRequest, RetryResponse } from "./io/IRetry.js";

const retryService = new RetryService();

export async function handleDlqEntry(msg: DLQMessage): Promise<void> 
{
  return retryService.handleDlqEntry(msg);
}

export async function consumerLoop(): Promise<void> 
{
  return retryService.consumerLoop();
}

retryService.consumerLoop().catch(err => 
{
  console.error("retry_consumer_failed", { error: String(err) });
  process.exit(1);
});

export default RetryService;

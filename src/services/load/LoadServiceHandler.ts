import { ILoad, LoadRequest, LoadResponse } from "@service/load/io/ILoad.js";
import LoadServiceImpl from "@service/load/impl/LoadServiceImpl.js";
import { LoadMessage } from "@shared/models/job.js";

/**
 * Legacy LoadService class - now a thin wrapper around LoadServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class LoadService implements ILoad {
    /**
   * Service
   * @private
   */
  private service: LoadServiceImpl;

    /**
   * Constructs a new LoadService instance.
   */
  constructor() {
    this.service = LoadServiceImpl.getInstance();
  }

    /**
   * Processes load
   * @param req - The HTTP request object
   * @returns A promise that resolves to the result
   */
  async processLoad(req: LoadRequest): Promise<LoadResponse> {
    return this.service.processLoad(req);
  }

    /**
   * Loads job
   * @param msg - The msg
   */
  async loadJob(msg: LoadMessage): Promise<void> {
    return this.service.loadJob(msg);
  }

    /**
   * Performs the consumer loop operation.
   */
  async consumerLoop(): Promise<void> {
    return this.service.consumerLoop();
  }
}

// Re-export the new service for direct use
export { default as LoadServiceImpl } from "@service/load/impl/LoadServiceImpl.js";
export { ILoad, LoadRequest, LoadResponse } from "@service/load/io/ILoad.js";

// Backward compatibility wrappers
const loadService = new LoadService();

/**
 * Loads job
 * @param msg - The msg
 */
export async function loadJob(msg: LoadMessage): Promise<void> {
  return loadService.loadJob(msg);
}

/**
 * Performs the consumer loop operation.
 */
export async function consumerLoop(): Promise<void> {
  return loadService.consumerLoop();
}

// Auto-start the service when module is loaded
loadService.consumerLoop().catch(err => {
  console.error("load_consumer_failed", { error: String(err) });
  process.exit(1);
});

export default LoadService;

import { ILoad, LoadRequest, LoadResponse } from "./io/ILoad.js";
import LoadServiceImpl from "./impl/LoadServiceImpl.js";
import { LoadMessage } from "../../shared/models/job.js";

/**
 * Legacy LoadService class - now a thin wrapper around LoadServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class LoadService implements ILoad {
  private service: LoadServiceImpl;

  constructor() {
    this.service = LoadServiceImpl.getInstance();
  }

  async processLoad(req: LoadRequest): Promise<LoadResponse> {
    return this.service.processLoad(req);
  }

  async loadJob(msg: LoadMessage): Promise<void> {
    return this.service.loadJob(msg);
  }

  async consumerLoop(): Promise<void> {
    return this.service.consumerLoop();
  }
}

// Re-export the new service for direct use
export { default as LoadServiceImpl } from "./impl/LoadServiceImpl.js";
export { ILoad, LoadRequest, LoadResponse } from "./io/ILoad.js";

// Backward compatibility wrappers
const loadService = new LoadService();

export async function loadJob(msg: LoadMessage): Promise<void> {
  return loadService.loadJob(msg);
}

export async function consumerLoop(): Promise<void> {
  return loadService.consumerLoop();
}

// Auto-start the service when module is loaded
loadService.consumerLoop().catch(err => {
  console.error("load_consumer_failed", { error: String(err) });
  process.exit(1);
});

export default LoadService;

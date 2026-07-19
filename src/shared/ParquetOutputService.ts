import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import FirestoreCacheUtils from "@utils/cache/FirestoreCacheUtils.js";
import { createLogger, Logger } from "@utils/logger/logger.js";

class ParquetOutputService extends ServiceManager {
  protected static instance: ParquetOutputService;
  private logger: Logger;
  private gcsUtils: FirestoreCacheUtils;
  private FLUSH_LINE_THRESHOLD: number;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate ParquetOutputService directly. Use getInstance()");
    }
    super(enforce);

    this.logger = createLogger("parquet-writer");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.FLUSH_LINE_THRESHOLD = 1000;
  }

  public static getInstance(): ParquetOutputService {
    if (!ParquetOutputService.instance) {
      ParquetOutputService.instance = new ParquetOutputService(Enforce);
    }
    return ParquetOutputService.instance;
  }

  public getLogger(): Logger {
    return this.logger;
  }

  public getGcsUtils(): FirestoreCacheUtils {
    return this.gcsUtils;
  }

  public getFlushLineThreshold(): number {
    return this.FLUSH_LINE_THRESHOLD;
  }
}

export default ParquetOutputService;

export const parquetOutputService = ParquetOutputService.getInstance();

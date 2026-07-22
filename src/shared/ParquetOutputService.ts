import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import FirestoreCacheUtils from "@utils/cache/FirestoreCacheUtils.js";
import { createLogger, Logger } from "@utils/logger/logger.js";

/**
 * ParquetOutputService is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class ParquetOutputService extends ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: ParquetOutputService;
    /**
   * Logger instance
   * @private
   */
  private logger: Logger;
    /**
   * Gcs Utils
   * @private
   */
  private gcsUtils: FirestoreCacheUtils;
    /**
   * F L U S H_ L I N E_ T H R E S H O L D
   * @private
   */
  private FLUSH_LINE_THRESHOLD: number;
  /**
   * F L U S H_ B Y T E_ T H R E S H O L D
   * @private
   */
  private FLUSH_BYTE_THRESHOLD: number;

    /**
   * Constructs a new ParquetOutputService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate ParquetOutputService directly. Use getInstance()");
    }
    super(enforce);

    this.logger = createLogger("parquet-writer");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.FLUSH_LINE_THRESHOLD = 2000;
    // Flush parquet buffer every 16 MB to keep Cloud Run 1 GiB memory safe.
    this.FLUSH_BYTE_THRESHOLD = 16 * 1024 * 1024;
  }

    /**
   * Gets the single instance of the ParquetOutputService class.
   * @returns The single instance of the class
   */
  public static getInstance(): ParquetOutputService {
    if (!ParquetOutputService.instance) {
      ParquetOutputService.instance = new ParquetOutputService(Enforce);
    }
    return ParquetOutputService.instance;
  }

    /**
   * Gets logger
   * @returns The logger result
   */
  public getLogger(): Logger {
    return this.logger;
  }

    /**
   * Gets gcs utils
   * @returns The firestore cache utils result
   */
  public getGcsUtils(): FirestoreCacheUtils {
    return this.gcsUtils;
  }

    /**
   * Gets flush line threshold
   * @returns The numeric result
   */
  public getFlushLineThreshold(): number {
    return this.FLUSH_LINE_THRESHOLD;
  }

  /**
   * Gets flush byte threshold
   * @returns The numeric result
   */
  public getFlushByteThreshold(): number {
    return this.FLUSH_BYTE_THRESHOLD;
  }
}

export default ParquetOutputService;

/**
 * The parquet output service
 */
export const parquetOutputService = ParquetOutputService.getInstance();

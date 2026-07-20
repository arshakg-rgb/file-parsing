import { InstantiationError } from "@errors/InstantiationError.js";
import Config from "@config/system-config/Config.js";
import { createLogger, Logger } from "@utils/logger/logger.js";

const logger: Logger = createLogger("ServiceManager");

/**
 * ServiceManager is the base lifecycle manager for long-lived connections and services.
 *
 * Subclasses may override connect() and gracefulStop(). The public initialize() and
 * shutdown() methods wrap these with logging and process-exit on fatal failures.
 */
export class ServiceManager {
  protected static instance: ServiceManager | undefined;
  protected readonly config: Config;

  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate ServiceManager directly. Use getInstance()");
    }
    this.config = Config.getInstance();
  }

  /**
   * Establishes the connection / starts the service.
   * Override in lifecycle managers.
   */
  public async connect(): Promise<void> {
    // no-op by default
  }

  /**
   * Closes the connection / stops the service gracefully.
   * Override in lifecycle managers.
   */
  public async gracefulStop(): Promise<void> {
    // no-op by default
  }

  /**
   * Initializes the manager by connecting and logging the result.
   * Exits the process on fatal connection failure.
   */
  public async initialize(): Promise<void> {
    try {
      await this.connect();
      logger.info(`${this.constructor.name} connected successfully`);
    } catch (error) {
      logger.error(`Failed to connect to ${this.constructor.name}: ${error instanceof Error ? error.message : String(error)}`);
      process.exit(1);
    }
  }

  /**
   * Shuts the manager down by gracefully stopping and logging the result.
   */
  public async shutdown(): Promise<void> {
    try {
      await this.gracefulStop();
      logger.info(`${this.constructor.name} closed successfully`);
    } catch (error) {
      logger.error(`Failed to close ${this.constructor.name}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Returns the shared configuration instance.
   * @returns The Config instance.
   */
  public getConfig(): Config {
    return this.config;
  }
}

function Enforce(): void {}

export { Enforce };

export default ServiceManager;

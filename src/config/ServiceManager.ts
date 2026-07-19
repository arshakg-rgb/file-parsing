/**
 * Service Manager - Centralized service initialization and management
 * Following the singleton pattern from knowledge.md
 */

import { InstantiationError } from "@errors/InstantiationError.js";
import Config from "@config/system-config/Config.js";

/**
 * ServiceManager manages the resource lifecycle.
 */
class ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: ServiceManager;
    /**
   * Services
   * @private
   */
  private services: Map<string, ServiceManager> = new Map();
    /**
   * Config
   * @private
   */
  protected config: Config;

    /**
   * Constructs a new ServiceManager instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate ServiceManager directly. Use getInstance()");
    }
    this.config = Config.getInstance();
  }

    /**
   * Gets the single instance of the ServiceManager class.
   * @returns The single instance of the class
   */
  public static getInstance(): ServiceManager {
    if (!ServiceManager.instance) {
      ServiceManager.instance = new ServiceManager(Enforce);
    }
    return ServiceManager.instance;
  }

  /**
   * Register a service
   */
  public registerService(name: string, service: ServiceManager): void {
    this.services.set(name, service);
  }

  /**
   * Get a registered service
   */
  public getService<T>(name: string): T | undefined {
    return this.services.get(name) as T | undefined;
  }

  /**
   * Get configuration
   */
  public getConfig(): Config {
    return this.config;
  }

  /**
   * Initialize all services
   */
  public async initialize(): Promise<void> {
    // Initialize services in order
    // This will be populated as services are created
  }

  /**
   * Shutdown all services
   */
  public async shutdown(): Promise<void> {
    // Cleanup services in reverse order
    for (const [name, service] of this.services) {
      if (service.shutdown && typeof service.shutdown === "function") {
        await service.shutdown();
      }
    }
  }
}

/**
 * Performs the enforce operation.
 */
function Enforce(): void {}

export { Enforce };

export default ServiceManager;

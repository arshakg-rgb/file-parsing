import { Firestore } from "@google-cloud/firestore";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { createLogger, Logger } from "@utils/logger/logger.js";

/**
 * FirestoreManager is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class FirestoreManager extends ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: FirestoreManager;
    /**
   * Firestore
   * @private
   */
  private firestore!: Firestore;
    /**
   * Logger
   * @private
   */
  private logger: Logger;

    /**
   * Constructs a new FirestoreManager instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate FirestoreManager directly. Use getInstance()");
    }
    super(enforce);
    this.logger = createLogger("FirestoreManager");
  }

    /**
   * Gets the single instance of the FirestoreManager class.
   * @returns The single instance of the class
   */
  public static getInstance(): FirestoreManager {
    if (!FirestoreManager.instance) {
      FirestoreManager.instance = new FirestoreManager(Enforce);
    }
    return FirestoreManager.instance;
  }

  /**
   * Connects to Firestore.
   */
  public async connect(): Promise<void> {
    this.logger.info("Connecting FirestoreManager...");
    // Firestore initialization will be done here when enabled
    // this.firestore = new Firestore();
    this.logger.info("FirestoreManager connected");
  }

  /**
   * Stops Firestore gracefully.
   */
  public async gracefulStop(): Promise<void> {
    this.logger.info("Stopping FirestoreManager...");
    if (this.firestore) {
      // await this.firestore.terminate();
    }
  }

    /**
   * Gets firestore
   * @returns The firestore result
   */
  public getFirestore(): Firestore {
    return this.firestore;
  }
}


export default FirestoreManager;

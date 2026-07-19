import { Firestore } from "@google-cloud/firestore";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";

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
   * Constructs a new FirestoreManager instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate FirestoreManager directly. Use getInstance()");
    }
    super(enforce);
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
   * Initializes the service
   */
  public async initialize(): Promise<void> {
    console.log("Initializing FirestoreManager...");
    // Firestore initialization will be done here
    // const { Firestore } = await import("@google-cloud/firestore");
    // this.firestore = new Firestore();
    console.log("FirestoreManager initialized");
  }

    /**
   * Stops the service gracefully
   */
  public async shutdown(): Promise<void> {
    console.log("Shutting down FirestoreManager...");
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

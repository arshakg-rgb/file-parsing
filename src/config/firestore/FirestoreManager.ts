import { Firestore } from "@google-cloud/firestore";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";

class FirestoreManager extends ServiceManager {
  protected static instance: FirestoreManager;
  private firestore!: Firestore;

  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate FirestoreManager directly. Use getInstance()");
    }
    super(enforce);
  }

  public static getInstance(): FirestoreManager {
    if (!FirestoreManager.instance) {
      FirestoreManager.instance = new FirestoreManager(Enforce);
    }
    return FirestoreManager.instance;
  }

  public async initialize(): Promise<void> {
    console.log("Initializing FirestoreManager...");
    // Firestore initialization will be done here
    // const { Firestore } = await import("@google-cloud/firestore");
    // this.firestore = new Firestore();
    console.log("FirestoreManager initialized");
  }

  public async shutdown(): Promise<void> {
    console.log("Shutting down FirestoreManager...");
    if (this.firestore) {
      // await this.firestore.terminate();
    }
  }

  public getFirestore(): Firestore {
    return this.firestore;
  }
}


export default FirestoreManager;

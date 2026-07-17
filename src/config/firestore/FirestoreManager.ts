import ServiceManager from "../ServiceManager.js";
import { InstantiationError } from "../../errors/InstantiationError.js";

class FirestoreManager extends ServiceManager {
  private firestore: any;

  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate FirestoreManager directly. Use getInstance()");
    }
    super(enforce);
  }

  public static getInstance(): FirestoreManager {
    if (!ServiceManager.instance) {
      ServiceManager.instance = new FirestoreManager(Enforce);
    }
    return ServiceManager.instance as FirestoreManager;
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

  public getFirestore(): any {
    return this.firestore;
  }
}

function Enforce(): void {}

export default FirestoreManager;

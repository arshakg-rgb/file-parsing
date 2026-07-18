import ServiceManager, { Enforce } from "../ServiceManager.js";
import { InstantiationError } from "../../errors/InstantiationError.js";

class FirestoreManager extends ServiceManager 
{
  protected static instance: FirestoreManager;
  private firestore: any;

  protected constructor(enforce: () => void) 
{
    if (enforce !== Enforce) 
{
      throw new InstantiationError("Cannot instantiate FirestoreManager directly. Use getInstance()");
    }
    super(enforce);
  }

  public static getInstance(): FirestoreManager 
{
    if (!FirestoreManager.instance) 
{
      FirestoreManager.instance = new FirestoreManager(Enforce);
    }
    return FirestoreManager.instance;
  }

  public async initialize(): Promise<void> 
{
    console.log("Initializing FirestoreManager...");
    console.log("FirestoreManager initialized");
  }

  public async shutdown(): Promise<void> 
{
    console.log("Shutting down FirestoreManager...");
    if (this.firestore) 
{
    }
  }

  public getFirestore(): any 
{
    return this.firestore;
  }
}


export default FirestoreManager;

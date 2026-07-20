import { Firestore } from "@google-cloud/firestore";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { existsSync } from "fs";
import { resolve } from "path";
import {IAppConfig} from "@config/system-config/io/IAppConfig.js";

/**
 * FirestoreManager is a singleton class responsible for managing the Firestore connection.
 * It provides methods to initialize and gracefully stop the service.
 */
class FirestoreManager extends ServiceManager
{
  /**
   * Singleton instance
   * @protected
   */

  protected static instance: FirestoreManager;

  /**
   * The Firestore client instance.
   * @private
   */

  private firestoreClient!: Firestore;

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

  protected constructor(enforce: () => void)
  {
      if (enforce !== Enforce)
      {
        throw new InstantiationError("Error: Instantiation failed: Use FirestoreManager.getInstance() instead of new.");
      }

      super(enforce);
      this.logger = createLogger("FirestoreManager");
  }

  /**
   * Gets the single instance of the FirestoreManager class.
   * @returns The single instance of the class
   */

  public static getInstance(): FirestoreManager
  {
      if (!FirestoreManager.instance)
      {
        FirestoreManager.instance = new FirestoreManager(Enforce);
      }

      return FirestoreManager.instance;
  }

  /**
   * Connects to Firestore using the Google Cloud Firestore client.
   * Initializes the Firestore client with proper configuration.
   * @throws Will throw an error if the connection fails.
   */

  public async connect(): Promise<void>
  {
      const settings = this.config.settings;
      const appConfig: IAppConfig = this.config.appConfig;

      try
      {
        const appEnv: "development" | "staging" | "production" = appConfig.environment;
        const projectId: string = settings.GCP_PROJECT_ID || "data-etl-499916";
        const databaseId: string = settings.FIRESTORE_DATABASE_ID || "file-parsing-db";
        const credentialsPath: string | undefined = settings.GOOGLE_APPLICATION_CREDENTIALS;

        this.logger.info(`Attempting to connect to Firestore [${appEnv}] with project: ${projectId}, database: ${databaseId}`);

        const firestoreConfig: { projectId: string; databaseId: string; keyFilename?: string } = { projectId, databaseId };

        if (appEnv === "development" && credentialsPath)
        {
          const absolutePath: string = resolve(credentialsPath);

          if (existsSync(absolutePath))
          {
            firestoreConfig.keyFilename = absolutePath;
            this.logger.info(`[LOCAL] Using credentials from: ${absolutePath}`);
          }
          else
          {
            this.logger.warn(`Credentials file not found at: ${absolutePath}, falling back to default credentials`);
          }
        }
        else
        {
          this.logger.info(`[${appEnv.toUpperCase()}] Using Application Default Credentials`);
        }

        this.firestoreClient = new Firestore(firestoreConfig);

        await this.firestoreClient.collection("_health_check").doc("test").set({ timestamp: new Date() });
        await this.firestoreClient.collection("_health_check").doc("test").delete();

        this.logger.info(`Firestore connected successfully to project: ${projectId}, database: ${databaseId}`);
    }
    catch (error)
    {
        const err: Error = error instanceof Error ? error : new Error(String(error));
        const errorCode = (error as { code?: number }).code;

        this.logger.error(`Unable to connect to Firestore: ${err.message}`);

        if (errorCode === 7)
        {
          this.logger.error("PERMISSION_DENIED: The service account does not have sufficient permissions.");
          this.logger.error("Required roles: Cloud Datastore User or Cloud Datastore Owner");
        }
        else if (errorCode === 5)
        {
          this.logger.error(`NOT_FOUND: Database "${settings.FIRESTORE_DATABASE_ID || "file-parsing-db"}" not found in project "${settings.GCP_PROJECT_ID || "data-etl-499916"}"`);
          this.logger.error("Verify the database exists in Google Cloud Console");
        }

        throw error;
    }
  }

  /**
   * Stops Firestore gracefully.
   */

  public async gracefulStop(): Promise<void>
  {
      if (this.firestoreClient)
      {
        await this.firestoreClient.terminate();
        this.logger.info("Firestore disconnected successfully");
      }
  }

  /**
   * Gets the Firestore client instance.
   * @returns The Firestore client instance.
   * @throws Will throw an error if the Firestore client is not initialized.
   */

  public get getFirestoreClient(): Firestore
  {
      if (!this.firestoreClient)
      {
        throw new Error("Firestore instance not initialized.");
      }

      return this.firestoreClient;
  }
}

export default FirestoreManager;

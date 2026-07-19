import { Firestore } from "@google-cloud/firestore";
import { settings } from "@shared/Settings.js";

/**
 * FirestoreClient is responsible for firestore client operations.
 */
export class FirestoreClient {
    /**
   * Singleton instance
   * @private
   */
  private static instance: FirestoreClient;
    /**
   * Firestore Client
   * @private
   */
  private readonly firestoreClient: Firestore;

    /**
   * Constructs a new FirestoreClient instance.
   */
  private constructor() {
    this.firestoreClient = new Firestore({
      projectId: settings.GCP_PROJECT_ID,
      databaseId: settings.FIRESTORE_DATABASE_ID,
      ...(settings.GOOGLE_APPLICATION_CREDENTIALS
        ? { keyFilename: settings.GOOGLE_APPLICATION_CREDENTIALS }
        : {}),
    });
  }

    /**
   * Gets the single instance of the FirestoreClient class.
   * @returns The single instance of the class
   */
  static getInstance(): FirestoreClient {
    if (!FirestoreClient.instance) {
      FirestoreClient.instance = new FirestoreClient();
    }
    return FirestoreClient.instance;
  }

    /**
   * Gets the firestore.
   * @returns The firestore result
   */
  get firestore(): Firestore {
    return this.firestoreClient;
  }
}

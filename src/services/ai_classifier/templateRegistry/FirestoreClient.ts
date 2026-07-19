import { Firestore } from "@google-cloud/firestore";
import { settings } from "@shared/Settings.js";

export class FirestoreClient {
  private static instance: FirestoreClient;
  private readonly firestoreClient: Firestore;

  private constructor() {
    this.firestoreClient = new Firestore({
      projectId: settings.GCP_PROJECT_ID,
      databaseId: settings.FIRESTORE_DATABASE_ID,
      ...(settings.GOOGLE_APPLICATION_CREDENTIALS
        ? { keyFilename: settings.GOOGLE_APPLICATION_CREDENTIALS }
        : {}),
    });
  }

  static getInstance(): FirestoreClient {
    if (!FirestoreClient.instance) {
      FirestoreClient.instance = new FirestoreClient();
    }
    return FirestoreClient.instance;
  }

  get firestore(): Firestore {
    return this.firestoreClient;
  }
}

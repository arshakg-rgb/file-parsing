import { Firestore } from "@google-cloud/firestore";
import FirestoreManager from "@config/firestore/FirestoreManager.js";

export class FirestoreClient
{
  /**
   * Singleton instance
   * @private
   */

  private static instance: FirestoreClient;

  /**
   * Firestore Manager
   * @private
   */

  private readonly manager: FirestoreManager;

  /**
   * Constructs a new FirestoreClient instance.
   */

  private constructor()
  {
      this.manager = FirestoreManager.getInstance();
  }

  /**
   * Gets the single instance of the FirestoreClient class.
   * @returns The single instance of the class
   */

  static getInstance(): FirestoreClient
  {
      if (!FirestoreClient.instance)
      {
        FirestoreClient.instance = new FirestoreClient();
      }

      return FirestoreClient.instance;
  }

  /**
   * Gets the firestore.
   * @returns The firestore result
   */
  get firestore(): Firestore
  {
    return this.manager.getFirestoreClient;
  }

}

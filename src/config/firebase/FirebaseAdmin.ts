import * as admin from "firebase-admin";
import { InstantiationError } from "@errors/InstantiationError.js";
import { settings } from "@shared/Settings.js";

/**
 * FirebaseAdmin is a singleton wrapper around the Firebase Admin SDK.
 *
 * Initializes the SDK using Application Default Credentials, reusing the
 * same GOOGLE_APPLICATION_CREDENTIALS / GCP_PROJECT_ID already relied on
 * by the other GCP clients in this project (BigQuery, Firestore, GCS).
 * The corresponding GCP project must have Firebase Authentication enabled.
 */
export class FirebaseAdmin
{
  /**
   * Singleton instance.
   * @private
   */

  private static instance: FirebaseAdmin;

  /**
   * The underlying Firebase Admin app instance.
   * @private
   */

  private readonly app: admin.app.App;

  /**
   * Constructs a new FirebaseAdmin instance.
   * @param enforce - A function to enforce the Singleton pattern.
   * @throws {InstantiationError} if instantiated directly instead of via {@link getInstance}
   */

  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate FirebaseAdmin directly. Use getInstance()");
    }

    this.app = admin.apps.length > 0 && admin.apps[0]
      ? (admin.apps[0] as admin.app.App)
      : admin.initializeApp({
          credential: admin.credential.applicationDefault(),
          projectId: settings.GCP_PROJECT_ID,
        });
  }

  /**
   * Gets the single instance of the FirebaseAdmin class.
   * @returns The single instance of the class.
   */

  public static getInstance(): FirebaseAdmin
  {
    if (!FirebaseAdmin.instance)
    {
      FirebaseAdmin.instance = new FirebaseAdmin(Enforce);
    }

    return FirebaseAdmin.instance;
  }

  /**
   * Gets the Firebase Auth service bound to this app.
   * @returns The Firebase Auth service.
   */

  public auth(): admin.auth.Auth
  {
    return this.app.auth();
  }
}

function Enforce(): void {}

export default FirebaseAdmin;

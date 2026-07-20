import "reflect-metadata";
import dotenv from "dotenv";
dotenv.config();

import { loadAllSecrets } from "@shared/SecretsService.js";

/**
 * Entry point of the application.
 * Loads secrets first, then initializes the App with required managers.
 */
async function bootstrap(): Promise<void> {
  await loadAllSecrets();

  const [{ App }, { default: MySqlManager }, { default: FirestoreManager }] = await Promise.all([
    import("./app.js"),
    import("@config/db/MySqlManager.js"),
    import("@config/firestore/FirestoreManager.js"),
  ]);

  await new App(
    MySqlManager.getInstance(),
    FirestoreManager.getInstance()
  ).listen();
}

bootstrap();

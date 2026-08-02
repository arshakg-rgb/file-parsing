import "reflect-metadata";
import dotenv from "dotenv";
import {SecretsService} from "@shared/SecretsService";
dotenv.config();


/**
 * Entry point of the application.
 * Loads secrets first, then initializes the App with required managers.
 */
async function bootstrap(): Promise<void> {
  await SecretsService.getInstance().loadAllSecrets();

  const [{ App }, { default: MySqlManager }, { default: FirestoreManager }] = await Promise.all([
    import("./app.js"),
    import("@config/db/PostgreSqlManager.js"),
    import("@config/firestore/FirestoreManager.js"),
  ]);

  await new App(
    MySqlManager.getInstance(),
    FirestoreManager.getInstance()
  ).listen();
}

bootstrap();

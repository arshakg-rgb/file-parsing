import dotenv from "dotenv";
dotenv.config();

import { App } from "./app.js";
import MySqlManager from "./config/db/MySqlManager.js";
import FirestoreManager from "./config/firestore/FirestoreManager.js";

/**
 * Entry point of the application.
 * Initializes the App with required managers and starts the server.
 */
new App(
  MySqlManager.getInstance(),
  FirestoreManager.getInstance()
).listen();

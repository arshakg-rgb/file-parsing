import { createTables, pool } from "../shared/DatabaseManager.js";

async function main() {
  console.log("Creating database tables...");
  await createTables();
  console.log("Tables created");
  await pool.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

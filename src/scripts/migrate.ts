import { createTables, pool, waitForDb } from "../shared/db.js";

async function main(): Promise<void> {
  console.log("Starting database migration...");
  try {
    await waitForDb();
    await createTables();
    console.log("✓ Database migration completed successfully");
    await pool.end();
    process.exit(0);
  } catch (error) {
    console.error("✗ Migration failed:", error);
    process.exit(1);
  }
}

main();

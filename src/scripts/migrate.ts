import { createTables } from "../shared/db.js";

async function main(): Promise<void> {
  console.log("Starting database migration...");
  try {
    await createTables();
    console.log("✓ Database migration completed successfully");
    process.exit(0);
  } catch (error) {
    console.error("✗ Migration failed:", error);
    process.exit(1);
  }
}

main();

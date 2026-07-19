import { readFile, readdir } from "fs/promises";
import { join } from "path";
import { fileURLToPath } from "url";
import MySqlManager from "@config/db/MySqlManager.js";

/**
 * The __filename
 */
const __filename = fileURLToPath(import.meta.url);
/**
 * The __dirname
 */
const __dirname = join(__filename, "..");

interface Migration {
  version: number;
  description: string;
  up: string;
}

/**
 * The m i g r a t i o n s_ d i r
 */
const MIGRATIONS_DIR = join(__dirname, "migrations");

/**
 * Loads migrations
 * @returns A promise that resolves to the list
 */
async function loadMigrations(): Promise<Migration[]> {
  const migrations: Migration[] = [];
  const files = await readdir(MIGRATIONS_DIR);
  const sqlFiles = files.filter((f) => f.endsWith(".sql")).sort();

  for (const file of sqlFiles) {
    const version = parseInt(file.match(/(\d+)_/)?.[1] || "0", 10);
    const sql = await readFile(join(MIGRATIONS_DIR, file), "utf-8");
    const description = sql.match(/-- Description: (.+)/)?.[1] || file;
    migrations.push({ version, description, up: sql });
  }

  return migrations.sort((a, b) => a.version - b.version);
}

/**
 * Splits statements
 * @param sql - The sql
 * @returns The list of results
 */
function splitStatements(sql: string): string[] {
  return sql
    .split("\n")
    .filter((line) => line.trim() !== "" && !line.trim().startsWith("--"))
    .join("\n")
    .split(";")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

/**
 * Gets applied versions
 * @param db - The db
 * @returns A promise that resolves to the list
 */
async function getAppliedVersions(db: MySqlManager): Promise<number[]> {
  return db.repositories.schemaMigrations.getAppliedVersions();
}

/**
 * Ensures migration table
 * @param db - The db
 */
async function ensureMigrationTable(db: MySqlManager): Promise<void> {
  await db.models.SchemaMigration.sync({ force: false });
}

/**
 * Runs migration
 * @param db - The db
 * @param migration - The migration
 */
async function runMigration(db: MySqlManager, migration: Migration): Promise<void> {
  const statements = splitStatements(migration.up);
  await db.sequelize.transaction(async (transaction) => {
    for (const stmt of statements) {
      await db.sequelize.query(`${stmt};`, { transaction });
    }
    await db.models.SchemaMigration.findOrCreate({
      where: { version: migration.version },
      defaults: { version: migration.version, description: migration.description },
      transaction,
    });
  });
  console.log(`Migration ${migration.version} applied: ${migration.description}`);
}

/**
 * Performs the migrate operation.
 */
export async function migrate(): Promise<void> {
  const db = MySqlManager.getInstance();
  await db.initialize();
  await ensureMigrationTable(db);

  const migrations = await loadMigrations();
  const applied = await getAppliedVersions(db);
  const pending = migrations.filter((m) => !applied.includes(m.version));

  if (pending.length === 0) {
    console.log("No pending migrations");
    return;
  }

  console.log(`Running ${pending.length} pending migrations...`);

  for (const migration of pending) {
    await runMigration(db, migration);
  }

  console.log("Migrations completed successfully");
}

if (import.meta.url === `file://${process.argv[1]}`) {
  migrate()
    .then(() => process.exit(0))
    .catch((err) => {
      console.error("Migration failed:", err);
      process.exit(1);
    });
}

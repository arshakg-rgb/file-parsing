import { pool } from "../shared/db.js";
import { readFile, readdir } from "fs/promises";
import { join } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = join(__filename, "..");

interface Migration {
  version: number;
  description: string;
  up: string;
}

const MIGRATIONS_DIR = join(__dirname, "migrations");

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

async function getAppliedVersions(): Promise<number[]> {
  const result = await pool.query("SELECT version FROM schema_migrations ORDER BY version");
  return result.rows.map((r: any) => r.version);
}

async function ensureMigrationTable(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version INTEGER PRIMARY KEY,
      applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      description TEXT
    )
  `);
}

async function runMigration(migration: Migration): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(migration.up);
    await client.query(
      "INSERT INTO schema_migrations (version, description) VALUES ($1, $2)",
      [migration.version, migration.description]
    );
    await client.query("COMMIT");
    console.log(`Migration ${migration.version} applied: ${migration.description}`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function migrate(): Promise<void> {
  await ensureMigrationTable();
  
  const migrations = await loadMigrations();
  const applied = await getAppliedVersions();
  const pending = migrations.filter((m) => !applied.includes(m.version));
  
  if (pending.length === 0) {
    console.log("No pending migrations");
    return;
  }
  
  console.log(`Running ${pending.length} pending migrations...`);
  
  for (const migration of pending) {
    await runMigration(migration);
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

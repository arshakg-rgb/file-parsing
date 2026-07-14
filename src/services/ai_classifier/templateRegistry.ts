import { randomUUID } from "crypto";
import { Firestore } from "@google-cloud/firestore";
import { settings } from "../../shared/config.js";
import { Template, TemplateKind } from "../../shared/models/template.js";

let _db: Firestore | undefined;

function db(): Firestore {
  if (_db) return _db;
  _db = new Firestore({
    projectId: settings.GCP_PROJECT_ID,
    databaseId: settings.FIRESTORE_DATABASE_ID,
    ...(settings.GOOGLE_APPLICATION_CREDENTIALS
      ? { keyFilename: settings.GOOGLE_APPLICATION_CREDENTIALS }
      : {}),
  });
  return _db;
}

const _cache: Template[] = [];
let _warmed = false;

function ensureWarmed() {
  if (!_warmed) {
    // Fire-and-forget synchronous guard; callers should await warmCache() before use.
    warmCache().catch(() => {});
  }
}

export function getByFingerprint(fingerprint: string): Template[] {
  ensureWarmed();
  return _cache.filter((t) => t.fingerprint === fingerprint);
}

export function getLatest(fingerprint: string, kind?: TemplateKind): Template | null {
  ensureWarmed();
  const templates = getByFingerprint(fingerprint).filter((t) => (kind ? t.kind === kind : true));
  if (!templates.length) return null;
  return templates.reduce((max, t) => (t.version > max.version ? t : max));
}

export async function save(tmpl: Template): Promise<Template> {
  await warmCache();
  const existing = getLatest(tmpl.fingerprint, tmpl.kind);
  if (existing) {
    tmpl = { ...tmpl, version: existing.version + 1, template_id: randomUUID() };
  }
  tmpl.updated_at = new Date().toISOString();
  _cache.push(tmpl);
  try {
    await db().collection(settings.TEMPLATE_COLLECTION).doc(tmpl.template_id).set(tmpl);
  } catch (e) {
    console.error("firestore_save_error", { template_id: tmpl.template_id, error: String(e) });
  }
  return tmpl;
}

export function incrementMatchCount(templateId: string, fingerprint: string): void {
  const t = _cache.find((x) => x.template_id === templateId && x.fingerprint === fingerprint);
  if (t) {
    t.match_count += 1;
    t.updated_at = new Date().toISOString();
    db()
      .collection(settings.TEMPLATE_COLLECTION)
      .doc(templateId)
      .update({ match_count: t.match_count, updated_at: t.updated_at })
      .catch(() => {});
  }
}

export function listAll(kind?: TemplateKind): Template[] {
  ensureWarmed();
  if (!kind) return [..._cache];
  return _cache.filter((t) => t.kind === kind);
}

let _warming: Promise<void> | null = null;

/** Warm in-memory cache from Firestore at startup. */
export async function warmCache(): Promise<void> {
  if (_warmed) return;
  if (_warming) return _warming;
  _warming = loadCache();
  try {
    await _warming;
  } finally {
    _warmed = true;
    _warming = null;
  }
}

async function loadCache(): Promise<void> {
  try {
    const snap = await db().collection(settings.TEMPLATE_COLLECTION).get();
    for (const doc of snap.docs) {
      const tmpl = doc.data() as Template;
      if (!_cache.find((c) => c.template_id === tmpl.template_id)) _cache.push(tmpl);
    }
    console.log("template_cache_warmed", { count: _cache.length });
  } catch (e) {
    console.warn("template_cache_warm_failed", { error: String(e) });
  }
}

export function ensureTableExists(): void {
  // Firestore is schemaless — no setup needed
}

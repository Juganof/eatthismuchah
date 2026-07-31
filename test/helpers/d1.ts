import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

/**
 * A D1Database backed by node:sqlite, so tests exercise the real SQL in
 * `src/db/queries.ts` instead of a hand-written stub that agrees with whatever
 * the query happens to be. `node:sqlite` is a CJS builtin, hence createRequire.
 */
const require = createRequire(import.meta.url);
const { DatabaseSync } = require("node:sqlite") as {
  DatabaseSync: new (path: string) => SqliteDb;
};

interface SqliteStatement {
  get(...params: unknown[]): Record<string, unknown> | undefined;
  all(...params: unknown[]): Record<string, unknown>[];
  run(...params: unknown[]): { changes: number | bigint; lastInsertRowid: number | bigint };
}

interface SqliteDb {
  prepare(sql: string): SqliteStatement;
  exec(sql: string): void;
  close(): void;
}

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(here, "..", "..");

class MockPreparedStatement {
  constructor(
    private readonly db: SqliteDb,
    private readonly sql: string,
    private readonly params: unknown[] = [],
  ) {}

  /** D1 returns a new statement rather than mutating, and callers rely on that. */
  bind(...params: unknown[]): MockPreparedStatement {
    return new MockPreparedStatement(this.db, this.sql, params.map(normalise));
  }

  async first<T = Record<string, unknown>>(column?: string): Promise<T | null> {
    const row = this.db.prepare(this.sql).get(...this.params);
    if (row === undefined) return null;
    const clean = plainify(row);
    return (column ? ((clean as Record<string, unknown>)[column] ?? null) : clean) as T;
  }

  async all<T = Record<string, unknown>>(): Promise<{
    results: T[];
    success: boolean;
    meta: Record<string, unknown>;
  }> {
    const rows = this.db.prepare(this.sql).all(...this.params);
    return { results: rows.map(plainify) as T[], success: true, meta: {} };
  }

  async run(): Promise<{ success: boolean; meta: Record<string, unknown> }> {
    const info = this.db.prepare(this.sql).run(...this.params);
    return {
      success: true,
      meta: { changes: Number(info.changes), last_row_id: Number(info.lastInsertRowid) },
    };
  }
}

/** sqlite rejects undefined and booleans; D1 accepts both. */
function normalise(value: unknown): unknown {
  if (value === undefined) return null;
  if (typeof value === "boolean") return value ? 1 : 0;
  return value;
}

/** node:sqlite hands back null-prototype objects and BigInts; make them ordinary. */
function plainify(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(row)) {
    out[key] = typeof value === "bigint" ? Number(value) : value;
  }
  return out;
}

export interface TestDb extends D1Database {
  close(): void;
}

/**
 * Fresh in-memory database with the production schema applied. Uses schema.sql
 * itself, so a table that is missing there will fail the tests rather than only
 * failing in production.
 */
export function createTestDb(): TestDb {
  const db = new DatabaseSync(":memory:");
  db.exec(readFileSync(join(repoRoot, "schema.sql"), "utf8"));

  const api = {
    prepare: (sql: string) => new MockPreparedStatement(db, sql),
    async batch(statements: MockPreparedStatement[]) {
      const out = [];
      for (const statement of statements) out.push(await statement.run());
      return out;
    },
    async exec(sql: string) {
      db.exec(sql);
      return { count: 0, duration: 0 };
    },
    close: () => db.close(),
  };
  return api as unknown as TestDb;
}

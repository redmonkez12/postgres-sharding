import { readFile } from "node:fs/promises";
import { createConnection } from "node:net";
import { resolve } from "node:path";

import { SQL } from "bun";

let testSql: InstanceType<typeof SQL> | undefined;
let dbAvailable: boolean | undefined;

function getConnectionUrl(): string {
  if (process.env.TEST_DATABASE_URL) return process.env.TEST_DATABASE_URL;
  if (process.env.DATABASE_URL) return process.env.DATABASE_URL;

  const host = process.env.PGHOST ?? "localhost";
  const port = process.env.PGPORT ?? "5432";
  const user = process.env.PGUSER ?? "postgres";
  const password = process.env.PGPASSWORD ?? "postgres";
  const database = process.env.PGDATABASE ?? "postgres";

  return `postgres://${user}:${password}@${host}:${port}/${database}`;
}

/**
 * Probes the database with a raw TCP connection so the check works
 * even when `mock.module("pg")` is active from other test files.
 */
export async function isDatabaseAvailable(): Promise<boolean> {
  if (dbAvailable !== undefined) return dbAvailable;

  const host = process.env.PGHOST ?? "localhost";
  const port = Number(process.env.PGPORT ?? 5432);

  dbAvailable = await new Promise<boolean>((resolve) => {
    const socket = createConnection({ host, port });
    socket.setTimeout(1000);
    socket.once("connect", () => {
      socket.destroy();
      resolve(true);
    });
    socket.once("error", () => {
      socket.destroy();
      resolve(false);
    });
    socket.once("timeout", () => {
      socket.destroy();
      resolve(false);
    });
  });

  return dbAvailable;
}

/**
 * Returns a Bun.SQL instance for test use. Uses Bun's native postgres
 * driver instead of the `pg` package to avoid interference from
 * `mock.module("pg")` in unit tests.
 */
export function getTestSql(): InstanceType<typeof SQL> {
  if (!testSql) {
    testSql = new SQL(getConnectionUrl());
  }
  return testSql;
}

export async function closeTestPool(): Promise<void> {
  if (!testSql) return;
  await testSql.close();
  testSql = undefined;
}

/**
 * Ensures the schema (tables + indexes) exists in the test database.
 * Safe to call multiple times — uses IF NOT EXISTS.
 */
export async function ensureTestSchema(): Promise<void> {
  const sqlDir = resolve(import.meta.dir, "../../sql");
  const schema = await readFile(resolve(sqlDir, "001_schema.sql"), "utf8");
  const indexes = await readFile(resolve(sqlDir, "002_indexes.sql"), "utf8");

  const sql = getTestSql();
  await sql.unsafe(schema);
  await sql.unsafe(indexes);
}

/**
 * Truncates all application tables. Use in beforeAll/afterAll to guarantee
 * a clean slate between test suites.
 */
export async function truncateAll(): Promise<void> {
  await getTestSql().unsafe(
    "TRUNCATE TABLE order_items, orders, products, categories, customers RESTART IDENTITY CASCADE",
  );
}

/**
 * A lightweight client wrapper around a Bun.SQL reserved connection
 * that exposes a pg-like `query(sql, params)` interface for use
 * inside `withTestTransaction`.
 */
export type TestClient = {
  query: <T = Record<string, unknown>>(text: string, params?: unknown[]) => Promise<{ rows: T[]; rowCount: number }>;
};

/**
 * Wraps a test callback in a transaction that is always rolled back,
 * ensuring zero side-effects on the database. The callback receives
 * a TestClient scoped to the transaction.
 *
 * Usage:
 *   test("my test", () => withTestTransaction(async (client) => {
 *     await client.query("INSERT INTO ...", [value]);
 *     const { rows } = await client.query("SELECT ...");
 *     expect(rows).toHaveLength(1);
 *   }));
 */
export async function withTestTransaction(
  fn: (client: TestClient) => Promise<void>,
): Promise<void> {
  const sql = getTestSql();
  const reserved = await sql.reserve();

  const client: TestClient = {
    async query<T = Record<string, unknown>>(text: string, params: unknown[] = []) {
      const result = await reserved.unsafe<T[]>(text, params);
      return { rows: Array.from(result), rowCount: result.count ?? result.length };
    },
  };

  try {
    await reserved.unsafe("BEGIN");
    await fn(client);
  } finally {
    await reserved.unsafe("ROLLBACK");
    reserved.release();
  }
}

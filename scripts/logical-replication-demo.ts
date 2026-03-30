import "dotenv/config";

import { Pool, type QueryResultRow } from "pg";
import { randomUUID } from "node:crypto";
import { logger } from "../src/utils/logger.js";

// ── Pools ────────────────────────────────────────────────────────────

function createPool(port: number): Pool {
  return new Pool({
    host: process.env.PGHOST ?? "localhost",
    port,
    user: process.env.PGUSER ?? "postgres",
    password: process.env.PGPASSWORD ?? "postgres",
    database: process.env.PGDATABASE ?? "postgres",
    max: 4,
  });
}

const primary = createPool(Number(process.env.PRIMARY_PORT ?? 5432));
const logical = createPool(Number(process.env.LOGICAL_PORT ?? 5434));

// ── Helpers ──────────────────────────────────────────────────────────

function hr(title: string): void {
  console.log(`\n${"═".repeat(60)}`);
  console.log(`  ${title}`);
  console.log(`${"═".repeat(60)}\n`);
}

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

interface CountRow extends QueryResultRow {
  table_name: string;
  row_count: string;
}

async function tableCounts(pool: Pool, label: string): Promise<void> {
  const tables = ["customers", "categories", "products", "orders", "order_items"];
  console.log(`  ${label}:`);
  for (const t of tables) {
    try {
      const { rows } = await pool.query<CountRow>(
        `SELECT '${t}' AS table_name, COUNT(*)::text AS row_count FROM ${t}`,
      );
      console.log(`    ${t.padEnd(20)} ${rows[0].row_count.padStart(8)} rows`);
    } catch {
      console.log(`    ${t.padEnd(20)}    (table does not exist)`);
    }
  }
}

// ── Experiment 1: Selective replication ──────────────────────────────

async function testSelectiveReplication(): Promise<void> {
  hr("Experiment 1: Selective replication");
  console.log("  Publication covers only orders + order_items.");
  console.log("  customers, categories, products should NOT replicate.\n");

  await tableCounts(primary, "Primary");
  console.log();
  await tableCounts(logical, "Logical replica");
}

// ── Experiment 2: Writable replica ──────────────────────────────────

async function testWritableReplica(): Promise<void> {
  hr("Experiment 2: Write to logical replica");
  console.log("  Logical replicas allow direct writes (streaming does not).\n");

  const orderId = randomUUID();
  const customerId = randomUUID();

  try {
    await logical.query(
      `INSERT INTO orders (id, customer_id, status, total_cents, region)
       VALUES ($1, $2, 'pending', 999, 'local-write')`,
      [orderId, customerId],
    );
    console.log(`  INSERT on logical replica succeeded (order_id=${orderId})`);

    const { rows } = await logical.query(
      `SELECT id, status, region FROM orders WHERE id = $1`,
      [orderId],
    );
    console.log(`  Read back: ${JSON.stringify(rows[0])}`);

    // Verify it did NOT propagate to primary
    const { rows: pRows } = await primary.query(
      `SELECT id FROM orders WHERE id = $1`,
      [orderId],
    );
    console.log(
      `  Exists on primary? ${pRows.length > 0 ? "YES (unexpected)" : "NO (correct — logical sub is one-way)"}`,
    );

    // Clean up
    await logical.query(`DELETE FROM orders WHERE id = $1`, [orderId]);
    console.log("  Cleaned up local write.");
  } catch (err) {
    console.log(
      `  Write failed: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

// ── Experiment 3: Replication lag comparison ─────────────────────────

async function testLagComparison(): Promise<void> {
  hr("Experiment 3: Replication lag during write load");
  console.log("  Inserting 500 orders on primary, measuring lag...\n");

  // Ensure we have at least one customer on primary for FK
  const customerId = randomUUID();
  await primary.query(
    `INSERT INTO customers (id, email, name, region) VALUES ($1, $2, 'Lag Test', 'test')
     ON CONFLICT (email) DO NOTHING`,
    [customerId, `lagtest-${Date.now()}@example.com`],
  );

  const start = performance.now();
  const orderIds: string[] = [];

  for (let i = 0; i < 500; i++) {
    const oid = randomUUID();
    orderIds.push(oid);
    await primary.query(
      `INSERT INTO orders (id, customer_id, status, total_cents, region)
       VALUES ($1, $2, 'pending', ${(i + 1) * 100}, 'lag-test')`,
      [oid, customerId],
    );
  }
  const writeMs = (performance.now() - start).toFixed(1);
  console.log(`  Wrote 500 orders in ${writeMs} ms`);

  // Poll logical replica until it catches up
  const pollStart = performance.now();
  let logicalCount = 0;
  for (let attempt = 0; attempt < 40; attempt++) {
    const { rows } = await logical.query<CountRow>(
      `SELECT COUNT(*)::text AS row_count FROM orders WHERE region = 'lag-test'`,
    );
    logicalCount = Number(rows[0].row_count);
    if (logicalCount >= 500) break;
    await sleep(250);
  }
  const catchUpMs = (performance.now() - pollStart).toFixed(1);

  console.log(
    `  Logical replica has ${logicalCount}/500 lag-test orders (catch-up: ${catchUpMs} ms)`,
  );

  // Clean up
  await primary.query(`DELETE FROM orders WHERE region = 'lag-test'`);
  await primary.query(`DELETE FROM customers WHERE id = $1`, [customerId]);
  // Wait a moment for deletes to replicate, then clean logical side too
  await sleep(2000);
  await logical.query(`DELETE FROM orders WHERE region = 'lag-test'`).catch(() => {});
  console.log("  Cleaned up lag-test data.");
}

// ── Experiment 4: Break it — schema mismatch ────────────────────────

async function testSchemaBreak(): Promise<void> {
  hr("Experiment 4: Break replication with schema change");
  console.log("  Adding a NOT NULL column on subscriber that the publisher doesn't have.");
  console.log("  This should cause replication to fail on the next INSERT.\n");

  try {
    // Add column on logical replica only
    await logical.query(
      `ALTER TABLE orders ADD COLUMN IF NOT EXISTS break_col TEXT NOT NULL DEFAULT 'x'`,
    );
    console.log("  ALTER TABLE orders ADD COLUMN break_col on logical replica: OK");

    // Now insert on primary — replication should error because primary
    // doesn't send break_col and logical replica requires it (NOT NULL)
    const customerId = randomUUID();
    await primary.query(
      `INSERT INTO customers (id, email, name, region) VALUES ($1, $2, 'Break Test', 'test')
       ON CONFLICT (email) DO NOTHING`,
      [customerId, `breaktest-${Date.now()}@example.com`],
    );

    const orderId = randomUUID();
    await primary.query(
      `INSERT INTO orders (id, customer_id, status, total_cents, region)
       VALUES ($1, $2, 'pending', 1234, 'break-test')`,
      [orderId, customerId],
    );
    console.log("  Inserted order on primary.");

    // Give replication time to attempt apply
    await sleep(3000);

    // Check subscription status on logical replica
    const { rows } = await logical.query(`
      SELECT subname, subenabled,
             (SELECT srsubstate FROM pg_subscription_rel
              WHERE srsubid = s.oid LIMIT 1) AS rel_state
      FROM pg_subscription s
    `);
    console.log("  Subscription state after schema-mismatch insert:");
    for (const row of rows) {
      console.log(`    ${row.subname}: enabled=${row.subenabled}, rel_state=${row.rel_state ?? "?"}`);
    }

    // Check if the order arrived
    const { rows: checkRows } = await logical.query(
      `SELECT id FROM orders WHERE id = $1`,
      [orderId],
    );
    console.log(
      `  Order arrived on logical replica? ${checkRows.length > 0 ? "YES" : "NO (replication likely broken)"}`,
    );

    // Fix: revert the schema change
    await logical.query(`ALTER TABLE orders DROP COLUMN IF EXISTS break_col`);
    console.log("  Reverted: DROP COLUMN break_col");

    // Re-enable subscription if it was disabled
    await logical.query(`ALTER SUBSCRIPTION orders_sub DISABLE`);
    await logical.query(`ALTER SUBSCRIPTION orders_sub ENABLE`);
    console.log("  Subscription re-enabled (toggle disable/enable to retry).");

    // Clean up
    await sleep(2000);
    await primary.query(`DELETE FROM orders WHERE region = 'break-test'`);
    await primary.query(`DELETE FROM customers WHERE id = $1`, [customerId]);
    await logical.query(`DELETE FROM orders WHERE region = 'break-test'`).catch(() => {});
  } catch (err) {
    console.log(
      `  Error: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

// ── Main ─────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  logger.info("Logical replication experiments starting");

  await testSelectiveReplication();
  await testWritableReplica();
  await testLagComparison();
  await testSchemaBreak();

  hr("Done");
  console.log("  All experiments complete. Run monitor-lag.ts to observe ongoing lag.\n");

  await Promise.all([primary.end(), logical.end()]);
}

main().catch((err) => {
  logger.error("Experiment failed", {
    error: err instanceof Error ? err.message : String(err),
  });
  process.exitCode = 1;
});

import { Pool } from "pg";
import { execSync } from "node:child_process";
import { logger } from "../src/utils/logger.js";

// ── Config ─────────────────────────────────────────────────────────

const PRIMARY_PORT = 5432;
const REPLICA_PORT = 5433;

const POOL_CONFIG = {
  host: "localhost",
  user: "postgres",
  password: "postgres",
  database: "postgres",
  max: 5,
};

const COMPOSE_FILES = "-f docker-compose.yml -f docker-compose.stage2.yml";

const FAILOVER_TABLE = "failover_test";

// ── Helpers ────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function docker(cmd: string): string {
  const full = `docker compose ${COMPOSE_FILES} ${cmd}`;
  logger.info(`Exec: ${full}`);
  return execSync(full, { encoding: "utf-8", timeout: 30_000 }).trim();
}

function dockerExec(service: string, cmd: string): string {
  const full = `docker compose ${COMPOSE_FILES} exec -T ${service} ${cmd}`;
  logger.info(`Exec: ${full}`);
  return execSync(full, { encoding: "utf-8", timeout: 30_000 }).trim();
}

function createPool(port: number): Pool {
  return new Pool({ ...POOL_CONFIG, port });
}

function banner(text: string): void {
  console.log(`\n${"═".repeat(60)}`);
  console.log(`  ${text}`);
  console.log(`${"═".repeat(60)}\n`);
}

// ── Step 1: Setup & start write workload ───────────────────────────

async function setupTable(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${FAILOVER_TABLE} (
      seq SERIAL PRIMARY KEY,
      payload TEXT NOT NULL,
      written_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
  await pool.query(`TRUNCATE ${FAILOVER_TABLE}`);
  logger.info(`Table "${FAILOVER_TABLE}" ready`);
}

type WriteResult = {
  inserted: number;
  lastSeq: number;
  errors: string[];
};

async function writeWorkload(pool: Pool, count: number, delayMs: number): Promise<WriteResult> {
  const result: WriteResult = { inserted: 0, lastSeq: 0, errors: [] };

  for (let i = 1; i <= count; i++) {
    try {
      const res = await pool.query(
        `INSERT INTO ${FAILOVER_TABLE} (payload) VALUES ($1) RETURNING seq`,
        [`write-${i}-${Date.now()}`],
      );
      result.lastSeq = res.rows[0].seq;
      result.inserted++;
      if (i % 10 === 0) {
        console.log(`  ✓ Wrote ${i}/${count}  (last seq=${result.lastSeq})`);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      result.errors.push(msg);
      console.log(`  ✗ Write ${i} failed: ${msg}`);
    }
    await sleep(delayMs);
  }

  return result;
}

// ── Step 2: Crash primary ──────────────────────────────────────────

function crashPrimary(): void {
  banner("STEP 2 — Simulate primary crash (docker stop)");
  docker("stop postgres");
  logger.info("Primary container stopped");
}

// ── Step 3: Observe connection errors ──────────────────────────────

async function observeErrors(pool: Pool, attempts: number): Promise<void> {
  banner("STEP 3 — Observe connection errors against dead primary");

  for (let i = 1; i <= attempts; i++) {
    try {
      await pool.query("SELECT 1");
      console.log(`  Attempt ${i}: unexpectedly succeeded`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`  Attempt ${i}: ${msg}`);
    }
    await sleep(500);
  }
}

// ── Step 4: Promote replica ────────────────────────────────────────

function promoteReplica(): number {
  banner("STEP 4 — Promote streaming replica");

  const start = performance.now();
  dockerExec(
    "pg-replica-streaming",
    "su postgres -c 'pg_ctl promote -D /var/lib/postgresql/data'",
  );
  const promotionMs = performance.now() - start;

  logger.info(`Promotion command returned in ${promotionMs.toFixed(0)}ms`);

  return promotionMs;
}

async function waitForPromotion(pool: Pool, timeoutMs: number): Promise<number> {
  const start = performance.now();
  const deadline = start + timeoutMs;

  while (Date.now() < deadline) {
    try {
      const res = await pool.query("SELECT pg_is_in_recovery()");
      const inRecovery: boolean = res.rows[0].pg_is_in_recovery;

      if (!inRecovery) {
        const elapsed = performance.now() - start;
        logger.info(`Replica is now accepting writes (took ${elapsed.toFixed(0)}ms)`);
        return elapsed;
      }
    } catch {
      // connection not ready yet
    }
    await sleep(200);
  }

  throw new Error(`Replica did not complete promotion within ${timeoutMs}ms`);
}

// ── Step 5: Reconfigure & write to promoted replica ────────────────

async function writeToPromoted(pool: Pool, count: number): Promise<WriteResult> {
  banner("STEP 5 — Write to promoted replica");
  return writeWorkload(pool, count, 50);
}

// ── Step 6: Verify data consistency ────────────────────────────────

async function verifyConsistency(
  pool: Pool,
  lastSeqBeforeCrash: number,
): Promise<void> {
  banner("STEP 6 — Verify data consistency");

  const countRes = await pool.query(`SELECT count(*)::int AS cnt FROM ${FAILOVER_TABLE}`);
  const totalRows: number = countRes.rows[0].cnt;

  const maxRes = await pool.query(`SELECT max(seq) AS max_seq FROM ${FAILOVER_TABLE}`);
  const maxSeq: number = maxRes.rows[0].max_seq;

  const minAfterRes = await pool.query(
    `SELECT min(seq) AS min_seq FROM ${FAILOVER_TABLE} WHERE seq > $1`,
    [lastSeqBeforeCrash],
  );
  const firstSeqAfterPromote: number | null = minAfterRes.rows[0].min_seq;

  console.log(`  Total rows in "${FAILOVER_TABLE}": ${totalRows}`);
  console.log(`  Last seq before crash:             ${lastSeqBeforeCrash}`);
  console.log(`  Max seq now:                       ${maxSeq}`);
  console.log(`  First seq after promotion:         ${firstSeqAfterPromote ?? "N/A"}`);

  // Check for gaps — any sequence values that should exist but don't
  const gapRes = await pool.query(`
    SELECT s.seq AS missing
    FROM generate_series(1, $1) AS s(seq)
    LEFT JOIN ${FAILOVER_TABLE} ft ON ft.seq = s.seq
    WHERE ft.seq IS NULL
  `, [lastSeqBeforeCrash]);

  if (gapRes.rows.length === 0) {
    console.log(`  ✓ No gaps in pre-crash sequence — all writes replicated`);
  } else {
    const missing = gapRes.rows.map((r) => r.missing as number);
    console.log(`  ✗ ${missing.length} missing sequence(s) — potential data loss: ${missing.join(", ")}`);
  }
}

// ── Step 7: Cleanup ────────────────────────────────────────────────

async function cleanup(pool: Pool): Promise<void> {
  banner("CLEANUP");
  await pool.query(`DROP TABLE IF EXISTS ${FAILOVER_TABLE}`);
  logger.info("Dropped failover_test table");
  await pool.end();
}

function restartPrimary(): void {
  logger.info("Restarting primary container");
  docker("start postgres");
}

// ── Main ───────────────────────────────────────────────────────────

async function main(): Promise<void> {
  banner("FAILOVER SIMULATION TEST");
  console.log("  Primary:  localhost:5432  (docker: postgres)");
  console.log("  Replica:  localhost:5433  (docker: pg-replica-streaming)");
  console.log();

  const primaryPool = createPool(PRIMARY_PORT);
  const replicaPool = createPool(REPLICA_PORT);

  // ── Step 1: write workload against primary
  banner("STEP 1 — Write workload against primary");
  await setupTable(primaryPool);

  const preWriteResult = await writeWorkload(primaryPool, 50, 50);
  logger.info("Pre-crash writes complete", {
    inserted: preWriteResult.inserted,
    lastSeq: preWriteResult.lastSeq,
    errors: preWriteResult.errors.length,
  });

  // Give replication a moment to catch up
  console.log("\n  Waiting 2s for replication to catch up…");
  await sleep(2000);

  // ── Step 2: crash primary
  crashPrimary();

  // ── Step 3: observe errors
  await observeErrors(primaryPool, 5);
  await primaryPool.end();

  // ── Step 4: promote replica
  const cmdMs = promoteReplica();
  const readyMs = await waitForPromotion(replicaPool, 15_000);

  console.log(`\n  Promotion summary:`);
  console.log(`    pg_ctl promote returned in:  ${cmdMs.toFixed(0)}ms`);
  console.log(`    Replica writable after:      ${readyMs.toFixed(0)}ms`);

  // ── Step 5: write to promoted replica
  const postWriteResult = await writeToPromoted(replicaPool, 20);
  logger.info("Post-promotion writes complete", {
    inserted: postWriteResult.inserted,
    lastSeq: postWriteResult.lastSeq,
    errors: postWriteResult.errors.length,
  });

  // ── Step 6: verify consistency
  await verifyConsistency(replicaPool, preWriteResult.lastSeq);

  // ── Cleanup
  await cleanup(replicaPool);

  // Restart primary so the environment is usable again
  restartPrimary();

  banner("DONE");
  console.log("  Review the output above to understand failover behavior.");
  console.log("  Key things to note:");
  console.log("    • Time to promote replica");
  console.log("    • Whether any writes were lost");
  console.log("    • How the pg driver behaved during the outage\n");
}

main().catch((err) => {
  logger.error("Failover test failed", {
    error: err instanceof Error ? err.message : String(err),
  });
  // Try to restart primary even on failure
  try {
    restartPrimary();
  } catch {
    logger.warn("Could not restart primary — run manually: docker compose start postgres");
  }
  process.exitCode = 1;
});

import { closePool, query } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

// ── pg_stat_statements — top 10 by total_time, calls, mean_time ──────

async function reportStatStatements(): Promise<void> {
  const available = await query(
    `SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'`,
  );

  if (available.rowCount === 0) {
    logger.warn("pg_stat_statements extension not installed — skipping");
    return;
  }

  console.log("\n═══ pg_stat_statements — Top 10 by total_time ═══\n");

  const byTime = await query<{
    query: string;
    calls: string;
    total_time: string;
    mean_time: string;
    rows: string;
  }>(
    `SELECT
       LEFT(query, 80) AS query,
       calls::text,
       ROUND(total_exec_time::numeric, 2)::text AS total_time,
       ROUND(mean_exec_time::numeric, 2)::text AS mean_time,
       rows::text
     FROM pg_stat_statements
     ORDER BY total_exec_time DESC
     LIMIT 10`,
  );

  console.log(
    "Query".padEnd(82),
    "Calls".padStart(10),
    "Total ms".padStart(12),
    "Mean ms".padStart(12),
    "Rows".padStart(10),
  );
  console.log("─".repeat(126));

  for (const row of byTime.rows) {
    console.log(
      row.query.padEnd(82),
      row.calls.padStart(10),
      row.total_time.padStart(12),
      row.mean_time.padStart(12),
      row.rows.padStart(10),
    );
  }

  console.log("\n═══ pg_stat_statements — Top 10 by calls ═══\n");

  const byCalls = await query<{
    query: string;
    calls: string;
    total_time: string;
    mean_time: string;
    rows: string;
  }>(
    `SELECT
       LEFT(query, 80) AS query,
       calls::text,
       ROUND(total_exec_time::numeric, 2)::text AS total_time,
       ROUND(mean_exec_time::numeric, 2)::text AS mean_time,
       rows::text
     FROM pg_stat_statements
     ORDER BY calls DESC
     LIMIT 10`,
  );

  console.log(
    "Query".padEnd(82),
    "Calls".padStart(10),
    "Total ms".padStart(12),
    "Mean ms".padStart(12),
    "Rows".padStart(10),
  );
  console.log("─".repeat(126));

  for (const row of byCalls.rows) {
    console.log(
      row.query.padEnd(82),
      row.calls.padStart(10),
      row.total_time.padStart(12),
      row.mean_time.padStart(12),
      row.rows.padStart(10),
    );
  }

  console.log("\n═══ pg_stat_statements — Top 10 by mean_time ═══\n");

  const byMean = await query<{
    query: string;
    calls: string;
    total_time: string;
    mean_time: string;
    rows: string;
  }>(
    `SELECT
       LEFT(query, 80) AS query,
       calls::text,
       ROUND(total_exec_time::numeric, 2)::text AS total_time,
       ROUND(mean_exec_time::numeric, 2)::text AS mean_time,
       rows::text
     FROM pg_stat_statements
     ORDER BY mean_exec_time DESC
     LIMIT 10`,
  );

  console.log(
    "Query".padEnd(82),
    "Calls".padStart(10),
    "Total ms".padStart(12),
    "Mean ms".padStart(12),
    "Rows".padStart(10),
  );
  console.log("─".repeat(126));

  for (const row of byMean.rows) {
    console.log(
      row.query.padEnd(82),
      row.calls.padStart(10),
      row.total_time.padStart(12),
      row.mean_time.padStart(12),
      row.rows.padStart(10),
    );
  }
}

// ── pg_stat_user_tables — seq_scan vs idx_scan, dead tuples ──────────

async function reportTableStats(): Promise<void> {
  console.log("\n═══ pg_stat_user_tables — scan counts & dead tuples ═══\n");

  const result = await query<{
    table_name: string;
    seq_scan: string;
    idx_scan: string;
    n_live_tup: string;
    n_dead_tup: string;
    table_size: string;
  }>(
    `SELECT
       relname AS table_name,
       seq_scan::text,
       COALESCE(idx_scan, 0)::text AS idx_scan,
       n_live_tup::text,
       n_dead_tup::text,
       pg_size_pretty(pg_total_relation_size(relid)) AS table_size
     FROM pg_stat_user_tables
     ORDER BY seq_scan DESC`,
  );

  console.log(
    "Table".padEnd(20),
    "Seq Scan".padStart(10),
    "Idx Scan".padStart(10),
    "Live Rows".padStart(12),
    "Dead Rows".padStart(12),
    "Size".padStart(12),
  );
  console.log("─".repeat(76));

  for (const row of result.rows) {
    console.log(
      row.table_name.padEnd(20),
      row.seq_scan.padStart(10),
      row.idx_scan.padStart(10),
      row.n_live_tup.padStart(12),
      row.n_dead_tup.padStart(12),
      row.table_size.padStart(12),
    );
  }
}

// ── Buffer cache hit ratio from pg_stat_bgwriter ─────────────────────

async function reportBufferCacheHitRatio(): Promise<void> {
  console.log("\n═══ Buffer Cache Hit Ratio ═══\n");

  const result = await query<{
    buffers_alloc: string;
    buffers_hit: string;
    hit_ratio: string;
  }>(
    `SELECT
       (SELECT SUM(blks_hit)  FROM pg_stat_database)::text AS buffers_hit,
       (SELECT SUM(blks_read) FROM pg_stat_database)::text AS buffers_alloc,
       CASE
         WHEN (SELECT SUM(blks_hit) + SUM(blks_read) FROM pg_stat_database) = 0 THEN '0.00'
         ELSE ROUND(
           (SELECT SUM(blks_hit) FROM pg_stat_database)::numeric /
           (SELECT SUM(blks_hit) + SUM(blks_read) FROM pg_stat_database)::numeric * 100,
           2
         )::text
       END AS hit_ratio`,
  );

  const row = result.rows[0];
  console.log(`  Buffers hit:   ${row.buffers_hit}`);
  console.log(`  Buffers read:  ${row.buffers_alloc}`);
  console.log(`  Hit ratio:     ${row.hit_ratio}%`);
}

// ── pg_stat_activity — active queries, connections, waiting locks ─────

async function reportActivity(): Promise<void> {
  console.log("\n═══ pg_stat_activity — Connection summary ═══\n");

  const summary = await query<{
    state: string;
    count: string;
  }>(
    `SELECT COALESCE(state, 'null') AS state, COUNT(*)::text AS count
     FROM pg_stat_activity
     WHERE backend_type = 'client backend'
     GROUP BY state
     ORDER BY count DESC`,
  );

  console.log("State".padEnd(22), "Count".padStart(8));
  console.log("─".repeat(30));

  for (const row of summary.rows) {
    console.log(row.state.padEnd(22), row.count.padStart(8));
  }

  console.log("\n═══ pg_stat_activity — Active queries ═══\n");

  const active = await query<{
    pid: string;
    duration: string;
    state: string;
    wait_event_type: string;
    wait_event: string;
    query_text: string;
  }>(
    `SELECT
       pid::text,
       (NOW() - query_start)::text AS duration,
       state,
       COALESCE(wait_event_type, '-') AS wait_event_type,
       COALESCE(wait_event, '-') AS wait_event,
       LEFT(query, 80) AS query_text
     FROM pg_stat_activity
     WHERE backend_type = 'client backend'
       AND state = 'active'
       AND pid != pg_backend_pid()
     ORDER BY query_start ASC
     LIMIT 20`,
  );

  if (active.rowCount === 0) {
    console.log("  No active queries (besides this one)");
  } else {
    console.log(
      "PID".padEnd(8),
      "Duration".padEnd(22),
      "Wait Type".padEnd(14),
      "Wait Event".padEnd(14),
      "Query",
    );
    console.log("─".repeat(120));

    for (const row of active.rows) {
      console.log(
        row.pid.padEnd(8),
        row.duration.padEnd(22),
        row.wait_event_type.padEnd(14),
        row.wait_event.padEnd(14),
        row.query_text,
      );
    }
  }

  console.log("\n═══ pg_stat_activity — Blocked / waiting on locks ═══\n");

  const waiting = await query<{
    pid: string;
    duration: string;
    wait_event_type: string;
    wait_event: string;
    query_text: string;
  }>(
    `SELECT
       pid::text,
       (NOW() - query_start)::text AS duration,
       COALESCE(wait_event_type, '-') AS wait_event_type,
       COALESCE(wait_event, '-') AS wait_event,
       LEFT(query, 80) AS query_text
     FROM pg_stat_activity
     WHERE backend_type = 'client backend'
       AND wait_event_type = 'Lock'
     ORDER BY query_start ASC
     LIMIT 20`,
  );

  if (waiting.rowCount === 0) {
    console.log("  No blocked queries");
  } else {
    console.log(
      "PID".padEnd(8),
      "Duration".padEnd(22),
      "Wait Type".padEnd(14),
      "Wait Event".padEnd(14),
      "Query",
    );
    console.log("─".repeat(120));

    for (const row of waiting.rows) {
      console.log(
        row.pid.padEnd(8),
        row.duration.padEnd(22),
        row.wait_event_type.padEnd(14),
        row.wait_event.padEnd(14),
        row.query_text,
      );
    }
  }
}

// ── Main ──────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  await reportStatStatements();
  await reportTableStats();
  await reportBufferCacheHitRatio();
  await reportActivity();

  console.log();
  logger.info("Stats collection complete");
}

main()
  .catch((error) => {
    logger.error("Stats query failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });

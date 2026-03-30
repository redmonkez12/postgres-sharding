import { Pool, type QueryResultRow } from "pg";
import { logger } from "../src/utils/logger.js";

// ── CLI flags ────────────────────────────────────────────────────────

function parseArgs(): { interval: number } {
  const args = process.argv.slice(2);
  let interval = 500;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--interval" && args[i + 1]) {
      const parsed = Number(args[i + 1]);
      if (!Number.isNaN(parsed) && parsed > 0) {
        interval = parsed;
      }
    }
  }

  return { interval };
}

// ── Pool helpers ─────────────────────────────────────────────────────

function createPool(port: number): Pool {
  return new Pool({
    host: process.env.PGHOST ?? "localhost",
    port,
    user: process.env.PGUSER ?? "postgres",
    password: process.env.PGPASSWORD ?? "postgres",
    database: process.env.PGDATABASE ?? "postgres",
    max: 2,
  });
}

async function isPoolReachable(pool: Pool): Promise<boolean> {
  try {
    await pool.query("SELECT 1");
    return true;
  } catch {
    return false;
  }
}

// ── Queries ──────────────────────────────────────────────────────────

interface PrimaryReplicationRow extends QueryResultRow {
  application_name: string;
  client_addr: string;
  state: string;
  sent_lsn: string;
  write_lsn: string;
  flush_lsn: string;
  replay_lsn: string;
  replay_lag: string | null;
  byte_lag: string;
}

interface ReplicaLsnRow extends QueryResultRow {
  receive_lsn: string | null;
  replay_lsn: string | null;
  receive_replay_diff: string | null;
  is_in_recovery: boolean;
}

interface LogicalSubRow extends QueryResultRow {
  subname: string;
  received_lsn: string | null;
  latest_end_lsn: string | null;
  last_msg_send_time: string | null;
  last_msg_receipt_time: string | null;
}

interface LogicalTableRow extends QueryResultRow {
  relname: string;
  row_count: string;
}

async function queryPrimary(pool: Pool): Promise<PrimaryReplicationRow[]> {
  const result = await pool.query<PrimaryReplicationRow>(`
    SELECT
      application_name,
      COALESCE(client_addr::text, '-') AS client_addr,
      state,
      sent_lsn::text,
      write_lsn::text,
      flush_lsn::text,
      replay_lsn::text,
      replay_lag::text,
      pg_wal_lsn_diff(sent_lsn, replay_lsn)::text AS byte_lag
    FROM pg_stat_replication
    ORDER BY application_name
  `);
  return result.rows;
}

async function queryReplica(pool: Pool): Promise<ReplicaLsnRow> {
  const result = await pool.query<ReplicaLsnRow>(`
    SELECT
      pg_last_wal_receive_lsn()::text AS receive_lsn,
      pg_last_wal_replay_lsn()::text  AS replay_lsn,
      CASE
        WHEN pg_last_wal_receive_lsn() IS NOT NULL
         AND pg_last_wal_replay_lsn()  IS NOT NULL
        THEN pg_wal_lsn_diff(
               pg_last_wal_receive_lsn(),
               pg_last_wal_replay_lsn()
             )::text
        ELSE NULL
      END AS receive_replay_diff,
      pg_is_in_recovery() AS is_in_recovery
  `);
  return result.rows[0];
}

async function queryLogicalSub(pool: Pool): Promise<LogicalSubRow[]> {
  const result = await pool.query<LogicalSubRow>(`
    SELECT
      s.subname,
      st.received_lsn::text,
      st.latest_end_lsn::text,
      st.last_msg_send_time::text,
      st.last_msg_receipt_time::text
    FROM pg_subscription s
    JOIN pg_stat_subscription st ON st.subid = s.oid
    WHERE st.relid IS NULL
    ORDER BY s.subname
  `);
  return result.rows;
}

async function queryLogicalTables(pool: Pool): Promise<LogicalTableRow[]> {
  const result = await pool.query<LogicalTableRow>(`
    SELECT relname, n_live_tup::text AS row_count
    FROM pg_stat_user_tables
    WHERE relname IN ('orders', 'order_items')
    ORDER BY relname
  `);
  return result.rows;
}

// ── Formatting helpers ───────────────────────────────────────────────

function formatBytes(bytes: string | null): string {
  if (bytes === null) return "-";
  const n = Number(bytes);
  if (n === 0) return "0 B";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

function col(text: string, width: number): string {
  return text.slice(0, width).padEnd(width);
}

function rcol(text: string, width: number): string {
  return text.slice(0, width).padStart(width);
}

// ── Render ───────────────────────────────────────────────────────────

function render(
  primaryRows: PrimaryReplicationRow[],
  replicaRow: ReplicaLsnRow | null,
  logicalSubs: LogicalSubRow[] | null,
  logicalTables: LogicalTableRow[] | null,
): void {
  // Clear screen and move cursor to top
  process.stdout.write("\x1B[2J\x1B[H");

  const now = new Date().toISOString();
  console.log(`Replica Lag Monitor — ${now}\n`);

  // ── Primary: pg_stat_replication ──
  console.log("═══ Primary: pg_stat_replication ═══\n");

  if (primaryRows.length === 0) {
    console.log("  No connected replicas\n");
  } else {
    console.log(
      col("App", 18),
      col("Client", 16),
      col("State", 12),
      rcol("Sent LSN", 14),
      rcol("Write LSN", 14),
      rcol("Flush LSN", 14),
      rcol("Replay LSN", 14),
      rcol("Byte Lag", 12),
      rcol("Replay Lag", 16),
    );
    console.log("─".repeat(130));

    for (const row of primaryRows) {
      console.log(
        col(row.application_name, 18),
        col(row.client_addr, 16),
        col(row.state, 12),
        rcol(row.sent_lsn, 14),
        rcol(row.write_lsn, 14),
        rcol(row.flush_lsn, 14),
        rcol(row.replay_lsn, 14),
        rcol(formatBytes(row.byte_lag), 12),
        rcol(row.replay_lag ?? "-", 16),
      );
    }
    console.log();
  }

  // ── Streaming replica: WAL positions ──
  if (replicaRow) {
    console.log("═══ Streaming Replica (port 5433): WAL positions ═══\n");
    console.log(`  In recovery:         ${replicaRow.is_in_recovery}`);
    console.log(`  Receive LSN:         ${replicaRow.receive_lsn ?? "-"}`);
    console.log(`  Replay  LSN:         ${replicaRow.replay_lsn ?? "-"}`);
    console.log(`  Receive→Replay diff: ${formatBytes(replicaRow.receive_replay_diff)}`);
    console.log();
  }

  // ── Logical replica: subscription status ──
  if (logicalSubs) {
    console.log("═══ Logical Replica (port 5434): Subscription status ═══\n");

    if (logicalSubs.length === 0) {
      console.log("  No active subscriptions\n");
    } else {
      for (const sub of logicalSubs) {
        console.log(`  Subscription:        ${sub.subname}`);
        console.log(`  Received LSN:        ${sub.received_lsn ?? "-"}`);
        console.log(`  Latest end LSN:      ${sub.latest_end_lsn ?? "-"}`);
        console.log(`  Last msg sent:       ${sub.last_msg_send_time ?? "-"}`);
        console.log(`  Last msg received:   ${sub.last_msg_receipt_time ?? "-"}`);
      }
      console.log();
    }

    if (logicalTables && logicalTables.length > 0) {
      console.log("  Replicated table row counts:");
      for (const t of logicalTables) {
        console.log(`    ${col(t.relname, 20)} ${rcol(t.row_count, 10)} rows`);
      }
      console.log();
    }
  } else {
    console.log("═══ Logical Replica (port 5434): not connected ═══\n");
  }

  console.log("(Ctrl+C to stop)");
}

// ── Main loop ────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const { interval } = parseArgs();

  const primaryPool = createPool(
    Number(process.env.PRIMARY_PORT ?? 5432),
  );
  const streamingPool = createPool(
    Number(process.env.REPLICA_PORT ?? 5433),
  );
  const logicalPool = createPool(
    Number(process.env.LOGICAL_PORT ?? 5434),
  );

  const streamingUp = await isPoolReachable(streamingPool);
  const logicalUp = await isPoolReachable(logicalPool);

  logger.info("Starting replica lag monitor", {
    intervalMs: interval,
    streaming: streamingUp,
    logical: logicalUp,
  });

  const tick = async (): Promise<void> => {
    const promises: [
      Promise<PrimaryReplicationRow[]>,
      Promise<ReplicaLsnRow | null>,
      Promise<LogicalSubRow[] | null>,
      Promise<LogicalTableRow[] | null>,
    ] = [
      queryPrimary(primaryPool),
      streamingUp
        ? queryReplica(streamingPool)
        : Promise.resolve(null),
      logicalUp
        ? queryLogicalSub(logicalPool)
        : Promise.resolve(null),
      logicalUp
        ? queryLogicalTables(logicalPool)
        : Promise.resolve(null),
    ];

    const [primaryRows, replicaRow, logicalSubs, logicalTables] =
      await Promise.all(promises);
    render(primaryRows, replicaRow, logicalSubs, logicalTables);
  };

  // Initial render
  await tick();

  const timer = setInterval(() => {
    tick().catch((err) => {
      logger.error("Poll failed", {
        error: err instanceof Error ? err.message : String(err),
      });
    });
  }, interval);

  // Graceful shutdown
  const shutdown = async (): Promise<void> => {
    clearInterval(timer);
    await Promise.all([
      primaryPool.end(),
      streamingPool.end(),
      logicalPool.end(),
    ]);
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((error) => {
  logger.error("Monitor failed to start", {
    error: error instanceof Error ? error.message : String(error),
  });
  process.exitCode = 1;
});

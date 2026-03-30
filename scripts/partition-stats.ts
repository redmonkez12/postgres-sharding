import { closePool, query } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

interface PartitionRow {
  partition_name: string;
  row_count: string;
  table_size: string;
  seq_scan: string;
  idx_scan: string;
  n_live_tup: string;
  n_dead_tup: string;
  n_tup_ins: string;
  n_tup_upd: string;
  n_tup_del: string;
}

async function reportPartitionStats(): Promise<void> {
  console.log("\n═══ Partition Stats — orders (range by created_at) ═══\n");

  const result = await query<PartitionRow>(
    `SELECT
       c.relname AS partition_name,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS table_size,
       s.n_live_tup::text AS row_count,
       s.seq_scan::text AS seq_scan,
       COALESCE(s.idx_scan, 0)::text AS idx_scan,
       s.n_live_tup::text,
       s.n_dead_tup::text,
       s.n_tup_ins::text,
       s.n_tup_upd::text,
       s.n_tup_del::text
     FROM pg_inherits i
     JOIN pg_class c ON c.oid = i.inhrelid
     JOIN pg_stat_user_tables s ON s.relid = c.oid
     WHERE i.inhparent = 'orders'::regclass
     ORDER BY c.relname`,
  );

  if (result.rowCount === 0) {
    console.log("  No partitions found for 'orders'. Is 101_partition_range.sql applied?");
    return;
  }

  // Print header
  const cols = [
    { label: "Partition", width: 24 },
    { label: "Rows", width: 10 },
    { label: "Size", width: 12 },
    { label: "Seq Scan", width: 10 },
    { label: "Idx Scan", width: 10 },
    { label: "Dead Tup", width: 10 },
    { label: "Inserts", width: 10 },
    { label: "Updates", width: 10 },
    { label: "Deletes", width: 10 },
  ];

  console.log(cols.map((c) => c.label.padStart(c.width)).join(""));
  console.log("─".repeat(cols.reduce((sum, c) => sum + c.width, 0)));

  // Find hot partition (most writes + dead tuples)
  let hotPartition = "";
  let hotScore = 0;

  for (const row of result.rows) {
    const score =
      parseInt(row.n_tup_ins) +
      parseInt(row.n_tup_upd) +
      parseInt(row.n_tup_del) +
      parseInt(row.n_dead_tup);

    if (score > hotScore) {
      hotScore = score;
      hotPartition = row.partition_name;
    }
  }

  // Print rows
  for (const row of result.rows) {
    const isHot = row.partition_name === hotPartition && hotScore > 0;
    const marker = isHot ? " 🔥" : "";

    const line = [
      (row.partition_name + marker).padEnd(cols[0].width),
      row.row_count.padStart(cols[1].width),
      row.table_size.padStart(cols[2].width),
      row.seq_scan.padStart(cols[3].width),
      row.idx_scan.padStart(cols[4].width),
      row.n_dead_tup.padStart(cols[5].width),
      row.n_tup_ins.padStart(cols[6].width),
      row.n_tup_upd.padStart(cols[7].width),
      row.n_tup_del.padStart(cols[8].width),
    ].join("");

    console.log(line);
  }

  if (hotPartition && hotScore > 0) {
    console.log(
      `\n  Hot partition: ${hotPartition} (writes + dead tuples = ${hotScore})`,
    );
  }

  // Check default partition
  console.log("\n═══ Default Partition Check ═══\n");

  const defaultCheck = await query<{ row_count: string; table_size: string }>(
    `SELECT
       count(*)::text AS row_count,
       pg_size_pretty(pg_total_relation_size('orders_default'::regclass)) AS table_size
     FROM orders_default`,
  );

  const def = defaultCheck.rows[0];
  const defaultRows = parseInt(def.row_count);

  if (defaultRows > 0) {
    console.log(
      `  ⚠ orders_default has ${def.row_count} rows (${def.table_size}) — missing partition coverage!`,
    );
  } else {
    console.log(
      `  orders_default is empty (${def.table_size}) — all rows routed correctly`,
    );
  }
}

async function main(): Promise<void> {
  await reportPartitionStats();
  console.log();
  logger.info("Partition stats complete");
}

main()
  .catch((error) => {
    logger.error("Partition stats failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });

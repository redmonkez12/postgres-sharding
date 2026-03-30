import { logger } from "../src/utils/logger.js";
import { ShardRouter } from "../src/db/shard-router.js";

type RowCountRow = { table_name: string; row_count: string };
type TableSizeRow = { table_name: string; table_size: string; size_bytes: string };

const TABLES = ["customers", "products", "orders", "order_items", "categories"] as const;

// ── Row counts per shard per table ────────────────────────────────

async function rowCounts(router: ShardRouter): Promise<Map<string, Map<string, number>>> {
  console.log("\n═══ Row Counts per Shard per Table ═══\n");

  const countsByRegion = new Map<string, Map<string, number>>();

  for (const region of router.regions) {
    const regionCounts = new Map<string, number>();

    // Use pg_stat_user_tables for fast approximate counts, fall back to exact count
    const result = await router.query<RowCountRow>(
      region,
      `SELECT
         s.relname AS table_name,
         CASE
           WHEN s.n_live_tup = 0 THEN (
             SELECT COUNT(*)::text FROM information_schema.tables WHERE table_name = s.relname
           )
           ELSE s.n_live_tup::text
         END AS row_count
       FROM pg_stat_user_tables s
       WHERE s.relname = ANY($1)
       ORDER BY s.relname`,
      [TABLES as unknown as string[]],
    );

    for (const row of result.rows) {
      regionCounts.set(row.table_name, Number(row.row_count));
    }

    countsByRegion.set(region, regionCounts);
  }

  // print table
  const regionNames = router.regions;
  const cols = [
    { label: "Table", width: 16 },
    ...regionNames.map((r) => ({ label: r.toUpperCase(), width: 12 })),
    { label: "Total", width: 12 },
  ];

  console.log(cols.map((c) => c.label.padStart(c.width)).join(""));
  console.log("─".repeat(cols.reduce((sum, c) => sum + c.width, 0)));

  for (const table of TABLES) {
    let total = 0;
    const values: string[] = [table.padEnd(cols[0].width)];

    for (const region of regionNames) {
      const count = countsByRegion.get(region)?.get(table) ?? 0;
      total += count;
      values.push(count.toLocaleString().padStart(12));
    }

    values.push(total.toLocaleString().padStart(12));
    console.log(values.join(""));
  }

  console.log();
  return countsByRegion;
}

// ── Table sizes per shard ─────────────────────────────────────────

async function tableSizes(router: ShardRouter): Promise<void> {
  console.log("═══ Table Sizes per Shard ═══\n");

  const regionNames = router.regions;
  const cols = [
    { label: "Table", width: 16 },
    ...regionNames.map((r) => ({ label: r.toUpperCase(), width: 14 })),
  ];

  console.log(cols.map((c) => c.label.padStart(c.width)).join(""));
  console.log("─".repeat(cols.reduce((sum, c) => sum + c.width, 0)));

  const sizesByRegion = new Map<string, Map<string, string>>();

  for (const region of regionNames) {
    const result = await router.query<TableSizeRow>(
      region,
      `SELECT
         c.relname AS table_name,
         pg_size_pretty(pg_total_relation_size(c.oid)) AS table_size,
         pg_total_relation_size(c.oid)::text AS size_bytes
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relname = ANY($1)
         AND n.nspname = 'public'
         AND c.relkind = 'r'
       ORDER BY c.relname`,
      [TABLES as unknown as string[]],
    );

    const sizes = new Map<string, string>();
    for (const row of result.rows) {
      sizes.set(row.table_name, row.table_size);
    }
    sizesByRegion.set(region, sizes);
  }

  for (const table of TABLES) {
    const values: string[] = [table.padEnd(cols[0].width)];

    for (const region of regionNames) {
      const size = sizesByRegion.get(region)?.get(table) ?? "—";
      values.push(size.padStart(14));
    }

    console.log(values.join(""));
  }

  console.log();
}

// ── Imbalance detection ───────────────────────────────────────────

function highlightImbalance(countsByRegion: Map<string, Map<string, number>>, regions: string[]): void {
  console.log("═══ Imbalance Detection ═══\n");

  const IMBALANCE_THRESHOLD = 0.3; // 30% deviation from average

  let anyImbalance = false;

  for (const table of TABLES) {
    const counts: { region: string; count: number }[] = [];

    for (const region of regions) {
      counts.push({
        region,
        count: countsByRegion.get(region)?.get(table) ?? 0,
      });
    }

    const total = counts.reduce((sum, c) => sum + c.count, 0);
    if (total === 0) continue;

    const avg = total / counts.length;
    const imbalanced = counts.filter(
      (c) => Math.abs(c.count - avg) / avg > IMBALANCE_THRESHOLD,
    );

    if (imbalanced.length > 0) {
      anyImbalance = true;
      console.log(`  [!] ${table}:`);

      for (const { region, count } of counts) {
        const pct = ((count / total) * 100).toFixed(1);
        const deviation = (((count - avg) / avg) * 100).toFixed(1);
        const marker = Math.abs(count - avg) / avg > IMBALANCE_THRESHOLD ? " <-- skewed" : "";
        console.log(`       ${region.toUpperCase()}: ${count.toLocaleString()} (${pct}%, ${Number(deviation) >= 0 ? "+" : ""}${deviation}% from avg)${marker}`);
      }

      console.log();
    }
  }

  if (!anyImbalance) {
    console.log("  All tables are balanced across shards (within 30% threshold).\n");
  }
}

// ── Main ──────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const router = new ShardRouter();

  const health = await router.healthCheck();
  const downShards = [...health.entries()].filter(([, ok]) => !ok);
  if (downShards.length > 0) {
    logger.warn("Some shards are down", { down: downShards.map(([r]) => r) });
  }

  const countsByRegion = await rowCounts(router);
  await tableSizes(router);
  highlightImbalance(countsByRegion, router.regions);

  logger.info("Shard stats complete");
  await router.close();
}

main().catch((error) => {
  logger.error("Shard stats failed", {
    error: error instanceof Error ? error.message : String(error),
  });
  process.exitCode = 1;
});

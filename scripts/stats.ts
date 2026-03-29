import { closePool, query } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

type TableStatsRow = {
  relname: string;
  live_rows: string;
  dead_rows: string;
  table_size: string;
};

async function main(): Promise<void> {
  const result = await query<TableStatsRow>(
    `
      SELECT
        relname,
        n_live_tup::text AS live_rows,
        n_dead_tup::text AS dead_rows,
        pg_size_pretty(pg_total_relation_size(relid)) AS table_size
      FROM pg_stat_user_tables
      WHERE relname = 'benchmark_users'
    `,
  );

  logger.info("pg_stat_user_tables snapshot", {
    rows: result.rows,
  });
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

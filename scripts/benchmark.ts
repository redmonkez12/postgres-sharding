import { closePool, query } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

type BenchmarkRow = {
  tenant_id: number;
  user_count: string;
};

async function main(): Promise<void> {
  const startedAt = performance.now();
  const result = await query<BenchmarkRow>(
    `
      SELECT tenant_id, COUNT(*)::text AS user_count
      FROM benchmark_users
      GROUP BY tenant_id
      ORDER BY COUNT(*) DESC
      LIMIT 10
    `,
  );
  const durationMs = Number((performance.now() - startedAt).toFixed(2));

  logger.info("Benchmark query completed", {
    durationMs,
    rows: result.rows,
  });
}

main()
  .catch((error) => {
    logger.error("Benchmark failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });

import { closePool, query } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

type ExplainRow = {
  "QUERY PLAN": string;
};

async function main(): Promise<void> {
  const result = await query<ExplainRow>(
    `
      EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
      SELECT *
      FROM benchmark_users
      WHERE tenant_id = $1
      ORDER BY created_at DESC
      LIMIT 25
    `,
    [1],
  );

  logger.info("Execution plan for tenant query");
  for (const row of result.rows) {
    console.log(row["QUERY PLAN"]);
  }
}

main()
  .catch((error) => {
    logger.error("Explain failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });

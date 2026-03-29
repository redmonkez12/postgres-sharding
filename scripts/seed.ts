import { faker } from "@faker-js/faker";

import { closePool, withClient } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

const seedUserCount = Number(process.env.SEED_USER_COUNT ?? "1000");

async function main(): Promise<void> {
  await withClient(async (client) => {
    await client.query("BEGIN");

    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS benchmark_users (
          id BIGSERIAL PRIMARY KEY,
          tenant_id INTEGER NOT NULL,
          email TEXT NOT NULL UNIQUE,
          full_name TEXT NOT NULL,
          region TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS benchmark_users_tenant_created_at_idx
        ON benchmark_users (tenant_id, created_at DESC)
      `);

      await client.query("TRUNCATE TABLE benchmark_users RESTART IDENTITY");

      const users = Array.from({ length: seedUserCount }, () => ({
        tenantId: faker.number.int({ min: 1, max: 32 }),
        email: faker.internet.email().toLowerCase(),
        fullName: faker.person.fullName(),
        region: faker.location.countryCode("alpha-2"),
      }));

      const columns = ["tenant_id", "email", "full_name", "region"];
      const values = users.flatMap((user) => [user.tenantId, user.email, user.fullName, user.region]);
      const placeholders = users
        .map((_, index) => {
          const base = index * columns.length;
          return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4})`;
        })
        .join(", ");

      await client.query(
        `INSERT INTO benchmark_users (${columns.join(", ")}) VALUES ${placeholders}`,
        values,
      );

      await client.query("COMMIT");

      logger.info("Seeded benchmark_users table", { insertedRows: users.length });
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    }
  });
}

main()
  .catch((error) => {
    logger.error("Seed failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });

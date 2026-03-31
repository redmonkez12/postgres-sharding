import { describe, expect, test, beforeAll, afterAll } from "bun:test";

import {
  ensureTestSchema,
  closeTestPool,
  truncateAll,
  withTestTransaction,
  isDatabaseAvailable,
} from "../helpers/db.js";
import { seedMinimal, insertCustomer, insertProduct, insertCategory } from "../helpers/seed.js";
import { buildCustomer, buildProduct, buildCategory, resetFixtureSeq } from "../helpers/fixtures.js";

// ── Skip entire file when no database is reachable ───────────────

const dbUp = await isDatabaseAvailable();

const describeDb = dbUp ? describe : describe.skip;

if (dbUp) {
  beforeAll(async () => {
    await ensureTestSchema();
    await truncateAll();
  });

  afterAll(async () => {
    await truncateAll();
    await closeTestPool();
  });
}

// ── Transaction rollback isolation ───────────────────────────────

describeDb("Transaction rollback isolation", () => {
  test("inserted rows are visible within the transaction", () =>
    withTestTransaction(async (client) => {
      const cat = await insertCategory(client, buildCategory({ name: "Gadgets" }));
      const product = await insertProduct(
        client,
        buildProduct({ category_id: cat.id, sku: "GAD-00001" }),
      );

      const { rows } = await client.query("SELECT * FROM products WHERE id = $1", [product.id]);
      expect(rows).toHaveLength(1);
      expect(rows[0].sku).toBe("GAD-00001");
    }));

  test("rows from previous test are not visible (rollback worked)", () =>
    withTestTransaction(async (client) => {
      const { rows } = await client.query("SELECT * FROM products WHERE sku = $1", ["GAD-00001"]);
      expect(rows).toHaveLength(0);
    }));

  test("customer inserts are rolled back between tests", () =>
    withTestTransaction(async (client) => {
      await insertCustomer(client, buildCustomer({ email: "rollback-test@example.test" }));

      const { rows } = await client.query("SELECT * FROM customers WHERE email = $1", [
        "rollback-test@example.test",
      ]);
      expect(rows).toHaveLength(1);
    }));

  test("customer from previous test does not exist", () =>
    withTestTransaction(async (client) => {
      const { rows } = await client.query("SELECT * FROM customers WHERE email = $1", [
        "rollback-test@example.test",
      ]);
      expect(rows).toHaveLength(0);
    }));
});

// ── Minimal seed ─────────────────────────────────────────────────

describeDb("Minimal seed helper", () => {
  test("seeds a complete dataset and it is queryable", () =>
    withTestTransaction(async (client) => {
      const data = await seedMinimal(client);

      expect(data.categories).toHaveLength(2);
      expect(data.products).toHaveLength(3);
      expect(data.customers).toHaveLength(2);
      expect(data.orders).toHaveLength(2);
      expect(data.orderItems).toHaveLength(3);

      // Verify foreign keys are valid
      const { rows: orderRows } = await client.query(
        "SELECT o.id, c.name AS customer_name FROM orders o JOIN customers c ON c.id = o.customer_id",
      );
      expect(orderRows).toHaveLength(2);

      const { rows: itemRows } = await client.query(
        "SELECT oi.id, p.name AS product_name FROM order_items oi JOIN products p ON p.id = oi.product_id",
      );
      expect(itemRows).toHaveLength(3);
    }));

  test("seed data does not leak into the next test", () =>
    withTestTransaction(async (client) => {
      const { rows: customers } = await client.query("SELECT count(*)::int AS cnt FROM customers");
      const { rows: orders } = await client.query("SELECT count(*)::int AS cnt FROM orders");

      expect(customers[0].cnt).toBe(0);
      expect(orders[0].cnt).toBe(0);
    }));
});

// ── Idempotency ──────────────────────────────────────────────────

describeDb("Idempotency", () => {
  test("can seed twice in separate transactions without conflicts", async () => {
    // First seed — rolled back
    await withTestTransaction(async (client) => {
      const data = await seedMinimal(client);
      expect(data.products).toHaveLength(3);
    });

    // Second seed — same SKUs, same emails — should succeed because first was rolled back
    await withTestTransaction(async (client) => {
      resetFixtureSeq();
      const data = await seedMinimal(client);
      expect(data.products).toHaveLength(3);
    });
  });

  test("database is still empty after rolled-back seeds", () =>
    withTestTransaction(async (client) => {
      const { rows } = await client.query(
        "SELECT (SELECT count(*) FROM customers) + (SELECT count(*) FROM orders) + (SELECT count(*) FROM products) AS total",
      );
      expect(Number(rows[0].total)).toBe(0);
    }));
});

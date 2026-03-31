import type { FastifyInstance } from "fastify";
import { randomUUID } from "node:crypto";

import { query, withPrimaryClient } from "../../db/read-write-split.js";

// ── Types ──────────────────────────────────────────────────────────

type OrderItem = {
  product_id: string;
  quantity: number;
  unit_price_cents: number;
};

// ── Orders CRUD ────────────────────────────────────────────────────

export async function orderRoutes(app: FastifyInstance) {
  // List orders
  app.get("/", async (_req, reply) => {
    const result = await query(
      "SELECT * FROM orders ORDER BY created_at DESC",
      [],
      { readonly: true },
    );
    return reply.send(result.rows);
  });

  // Get order by ID (with items)
  app.get<{ Params: { id: string } }>("/:id", async (req, reply) => {
    const [orderResult, itemsResult] = await Promise.all([
      query(
        "SELECT * FROM orders WHERE id = $1",
        [req.params.id],
        { readonly: true },
      ),
      query(
        "SELECT * FROM order_items WHERE order_id = $1",
        [req.params.id],
        { readonly: true },
      ),
    ]);

    if (orderResult.rows.length === 0) {
      return reply.code(404).send({ error: "Order not found" });
    }

    return reply.send({ ...orderResult.rows[0], items: itemsResult.rows });
  });

  // Create order (with items, in a transaction)
  app.post<{
    Body: {
      customer_id: string;
      region: string;
      items: OrderItem[];
    };
  }>("/", async (req, reply) => {
    const { customer_id, region, items } = req.body;
    const orderId = randomUUID();
    const totalCents = items.reduce((sum, i) => sum + i.quantity * i.unit_price_cents, 0);

    const order = await withPrimaryClient(async (client) => {
      await client.query("BEGIN");

      try {
        const orderResult = await client.query(
          `INSERT INTO orders (id, customer_id, status, total_cents, region)
           VALUES ($1, $2, 'pending', $3, $4)
           RETURNING *`,
          [orderId, customer_id, totalCents, region],
        );

        for (const item of items) {
          await client.query(
            `INSERT INTO order_items (id, order_id, product_id, quantity, unit_price_cents)
             VALUES ($1, $2, $3, $4, $5)`,
            [randomUUID(), orderId, item.product_id, item.quantity, item.unit_price_cents],
          );
        }

        await client.query("COMMIT");
        return orderResult.rows[0];
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });

    return reply.code(201).send(order);
  });

  // Update order status
  app.patch<{
    Params: { id: string };
    Body: { status: string };
  }>("/:id", async (req, reply) => {
    const result = await query(
      `UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING *`,
      [req.body.status, req.params.id],
    );

    if (result.rows.length === 0) {
      return reply.code(404).send({ error: "Order not found" });
    }

    return reply.send(result.rows[0]);
  });
}

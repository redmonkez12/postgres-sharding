import type { FastifyInstance } from "fastify";
import { randomUUID } from "node:crypto";

import { query } from "../../db/read-write-split.js";

// ── Products CRUD ──────────────────────────────────────────────────

export async function productRoutes(app: FastifyInstance) {
  // List products
  app.get("/", async (_req, reply) => {
    const result = await query(
      "SELECT * FROM products ORDER BY created_at DESC",
      [],
      { readonly: true },
    );
    return reply.send(result.rows);
  });

  // Get product by ID
  app.get<{ Params: { id: string } }>("/:id", async (req, reply) => {
    const result = await query(
      "SELECT * FROM products WHERE id = $1",
      [req.params.id],
      { readonly: true },
    );

    if (result.rows.length === 0) {
      return reply.code(404).send({ error: "Product not found" });
    }

    return reply.send(result.rows[0]);
  });

  // Create product
  app.post<{
    Body: { name: string; sku: string; price_cents: number; category_id: number; stock_qty: number };
  }>("/", async (req, reply) => {
    const { name, sku, price_cents, category_id, stock_qty } = req.body;
    const id = randomUUID();

    const result = await query(
      `INSERT INTO products (id, name, sku, price_cents, category_id, stock_qty)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING *`,
      [id, name, sku, price_cents, category_id, stock_qty],
    );

    return reply.code(201).send(result.rows[0]);
  });

  // Update product
  app.patch<{
    Params: { id: string };
    Body: Partial<{ name: string; price_cents: number; stock_qty: number }>;
  }>("/:id", async (req, reply) => {
    const entries = Object.entries(req.body);
    if (entries.length === 0) {
      return reply.code(400).send({ error: "No fields to update" });
    }

    const setClauses: string[] = [];
    const params: unknown[] = [];

    for (const [col, value] of entries) {
      params.push(value);
      setClauses.push(`${col} = $${params.length}`);
    }

    params.push(req.params.id);
    const result = await query(
      `UPDATE products SET ${setClauses.join(", ")} WHERE id = $${params.length} RETURNING *`,
      params,
    );

    if (result.rows.length === 0) {
      return reply.code(404).send({ error: "Product not found" });
    }

    return reply.send(result.rows[0]);
  });

  // Delete product
  app.delete<{ Params: { id: string } }>("/:id", async (req, reply) => {
    const result = await query(
      "DELETE FROM products WHERE id = $1 RETURNING id",
      [req.params.id],
    );

    if (result.rows.length === 0) {
      return reply.code(404).send({ error: "Product not found" });
    }

    return reply.code(204).send();
  });
}

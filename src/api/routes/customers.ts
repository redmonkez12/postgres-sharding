import type { FastifyInstance } from "fastify";
import { randomUUID } from "node:crypto";

import { query } from "../../db/read-write-split.js";

// ── Customers CRUD ─────────────────────────────────────────────────

export async function customerRoutes(app: FastifyInstance) {
  // List customers
  app.get("/", async (_req, reply) => {
    const result = await query(
      "SELECT * FROM customers ORDER BY created_at DESC",
      [],
      { readonly: true },
    );
    return reply.send(result.rows);
  });

  // Get customer by ID
  app.get<{ Params: { id: string } }>("/:id", async (req, reply) => {
    const result = await query(
      "SELECT * FROM customers WHERE id = $1",
      [req.params.id],
      { readonly: true },
    );

    if (result.rows.length === 0) {
      return reply.code(404).send({ error: "Customer not found" });
    }

    return reply.send(result.rows[0]);
  });

  // Create customer
  app.post<{
    Body: { email: string; name: string; region: string };
  }>("/", async (req, reply) => {
    const { email, name, region } = req.body;
    const id = randomUUID();

    const result = await query(
      `INSERT INTO customers (id, email, name, region)
       VALUES ($1, $2, $3, $4)
       RETURNING *`,
      [id, email, name, region],
    );

    return reply.code(201).send(result.rows[0]);
  });

  // Update customer
  app.patch<{
    Params: { id: string };
    Body: Partial<{ email: string; name: string; region: string }>;
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
      `UPDATE customers SET ${setClauses.join(", ")} WHERE id = $${params.length} RETURNING *`,
      params,
    );

    if (result.rows.length === 0) {
      return reply.code(404).send({ error: "Customer not found" });
    }

    return reply.send(result.rows[0]);
  });
}

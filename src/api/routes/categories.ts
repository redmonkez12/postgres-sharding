import type { FastifyInstance } from "fastify";

import { query } from "../../db/read-write-split.js";

// ── Categories CRUD ───────────────────────────────────────────────

export async function categoryRoutes(app: FastifyInstance) {
  // List categories
  app.get("/", async (_req, reply) => {
    const result = await query(
      "SELECT * FROM categories ORDER BY id",
      [],
      { readonly: true },
    );
    return reply.send(result.rows);
  });

  // Create category
  app.post<{
    Body: { name: string };
  }>("/", async (req, reply) => {
    const { name } = req.body;

    const result = await query(
      `INSERT INTO categories (name)
       VALUES ($1)
       RETURNING *`,
      [name],
    );

    return reply.code(201).send(result.rows[0]);
  });
}

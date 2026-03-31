import "dotenv/config";

import Fastify from "fastify";

import { logger } from "../utils/logger.js";
import { closePools } from "../db/read-write-split.js";
import { productRoutes } from "./routes/products.js";
import { customerRoutes } from "./routes/customers.js";
import { orderRoutes } from "./routes/orders.js";

// ── Server ─────────────────────────────────────────────────────────

const PORT = Number(process.env.API_PORT ?? 3000);
const HOST = process.env.API_HOST ?? "0.0.0.0";

const app = Fastify({ logger: false });

// ── Request logging with query timing ──────────────────────────────

app.addHook("onRequest", async (req) => {
  (req as any).startTime = process.hrtime.bigint();
});

app.addHook("onResponse", async (req, reply) => {
  const start = (req as any).startTime as bigint;
  const durationMs = Number(process.hrtime.bigint() - start) / 1e6;

  logger.info("HTTP request", {
    method: req.method,
    url: req.url,
    status: reply.statusCode,
    durationMs: Math.round(durationMs * 100) / 100,
  });
});

// ── Health check ───────────────────────────────────────────────────

app.get("/health", async () => ({ status: "ok" }));

// ── Routes ─────────────────────────────────────────────────────────

app.register(productRoutes, { prefix: "/products" });
app.register(customerRoutes, { prefix: "/customers" });
app.register(orderRoutes, { prefix: "/orders" });

// ── Graceful shutdown ──────────────────────────────────────────────

async function shutdown() {
  logger.info("Shutting down...");
  await app.close();
  await closePools();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// ── Start ──────────────────────────────────────────────────────────

app.listen({ port: PORT, host: HOST }, (err, address) => {
  if (err) {
    logger.error("Failed to start server", { error: err.message });
    process.exit(1);
  }
  logger.info("API server listening", { address });
});

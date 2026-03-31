import { describe, expect, test, beforeEach, beforeAll, afterAll, mock } from "bun:test";

// ── In-memory Redis stores (for rate limiter) ─────────────────────
const sortedSets = new Map<string, Map<string, number>>();
const counters = new Map<string, number>();

function executePipelineOps(ops: Array<{ op: string; args: unknown[] }>) {
  const results: [null, unknown][] = [];
  for (const { op, args } of ops) {
    switch (op) {
      case "zremrangebyscore": {
        const [key, min, max] = args as [string, number, number];
        const set = sortedSets.get(key);
        let removed = 0;
        if (set) {
          for (const [member, score] of set) {
            if (score >= min && score <= max) { set.delete(member); removed++; }
          }
        }
        results.push([null, removed]);
        break;
      }
      case "zadd": {
        const [key, score, member] = args as [string, string, string];
        if (!sortedSets.has(key)) sortedSets.set(key, new Map());
        sortedSets.get(key)!.set(member, Number(score));
        results.push([null, 1]);
        break;
      }
      case "zcard": {
        const [key] = args as [string];
        const set = sortedSets.get(key);
        results.push([null, set ? set.size : 0]);
        break;
      }
      case "pexpire":
        results.push([null, 1]);
        break;
      case "incr": {
        const [key] = args as [string];
        const val = (counters.get(key) ?? 0) + 1;
        counters.set(key, val);
        results.push([null, val]);
        break;
      }
      case "expire":
        results.push([null, 1]);
        break;
      default:
        results.push([null, null]);
    }
  }
  return results;
}

mock.module("ioredis", () => ({
  Redis: class MockRedis {
    del = mock(async () => 0);
    disconnect = mock(() => {});
    pipeline = mock(() => {
      const ops: Array<{ op: string; args: unknown[] }> = [];
      const p = {
        zremrangebyscore(...args: unknown[]) { ops.push({ op: "zremrangebyscore", args }); return p; },
        zadd(...args: unknown[]) { ops.push({ op: "zadd", args }); return p; },
        zcard(...args: unknown[]) { ops.push({ op: "zcard", args }); return p; },
        pexpire(...args: unknown[]) { ops.push({ op: "pexpire", args }); return p; },
        incr(...args: unknown[]) { ops.push({ op: "incr", args }); return p; },
        expire(...args: unknown[]) { ops.push({ op: "expire", args }); return p; },
        async exec() { return executePipelineOps(ops); },
      };
      return p;
    });
  },
}));

// ── Mock database layer ───────────────────────────────────────────

type QueryCall = { sql: string; params: unknown[]; opts?: { readonly?: boolean } };

let queryCalls: QueryCall[] = [];
let transactionCalls: QueryCall[] = [];
let queryResponder: (sql: string, params: unknown[]) => { rows: unknown[]; rowCount: number };

const mockQuery = mock(async (sql: string, params: unknown[] = [], opts: any = {}) => {
  queryCalls.push({ sql, params, opts });
  return queryResponder(sql, params);
});

const mockWithPrimaryClient = mock(async (callback: any) => {
  const client = {
    query: mock(async (sql: string, params?: unknown[]) => {
      transactionCalls.push({ sql, params: params ?? [] });
      return queryResponder(sql, params ?? []);
    }),
  };
  return callback(client);
});

mock.module("../../src/db/read-write-split.js", () => ({
  query: mockQuery,
  withPrimaryClient: mockWithPrimaryClient,
  closePools: mock(async () => {}),
}));

mock.module("../../src/utils/logger.js", () => ({
  logger: { info: () => {}, warn: () => {}, error: () => {} },
}));

mock.module("dotenv/config", () => ({}));

// ── Imports (after mocks) ─────────────────────────────────────────

import Fastify, { type FastifyInstance } from "fastify";

const { productRoutes } = await import("../../src/api/routes/products.js");
const { customerRoutes } = await import("../../src/api/routes/customers.js");
const { orderRoutes } = await import("../../src/api/routes/orders.js");
const { SlidingWindowRateLimiter, buildHeaders } = await import("../../src/rate-limiter.js");

// ── App builder ───────────────────────────────────────────────────

function buildApp(): FastifyInstance {
  const app = Fastify({ logger: false });

  app.addHook("onRequest", async (req) => {
    (req as any).startTime = process.hrtime.bigint();
  });

  app.addHook("onResponse", async (req, reply) => {
    const start = (req as any).startTime as bigint;
    const durationMs = Number(process.hrtime.bigint() - start) / 1e6;
    // Logging suppressed in tests
  });

  app.get("/health", async () => ({ status: "ok" }));
  app.register(productRoutes, { prefix: "/products" });
  app.register(customerRoutes, { prefix: "/customers" });
  app.register(orderRoutes, { prefix: "/orders" });

  return app;
}

// ── Shared setup ──────────────────────────────────────────────────

let app: FastifyInstance;

beforeAll(async () => {
  app = buildApp();
  await app.ready();
});

afterAll(async () => {
  await app.close();
});

beforeEach(() => {
  queryCalls = [];
  transactionCalls = [];
  mockQuery.mockClear();
  mockWithPrimaryClient.mockClear();
  queryResponder = () => ({ rows: [], rowCount: 0 });
});

// ── Helpers ───────────────────────────────────────────────────────

const PRODUCT = {
  id: "prod-001",
  name: "Widget",
  sku: "WDG-001",
  price_cents: 1999,
  category_id: 1,
  stock_qty: 50,
  created_at: "2026-01-01T00:00:00Z",
};

const CUSTOMER = {
  id: "cust-001",
  email: "alice@example.com",
  name: "Alice",
  region: "eu",
  created_at: "2026-01-01T00:00:00Z",
};

const ORDER = {
  id: "ord-001",
  customer_id: "cust-001",
  status: "pending",
  total_cents: 3998,
  region: "eu",
  created_at: "2026-01-01T00:00:00Z",
};

const ORDER_ITEMS = [
  { id: "item-001", order_id: "ord-001", product_id: "prod-001", quantity: 2, unit_price_cents: 1999 },
];

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

// ── Health Check ──────────────────────────────────────────────────

describe("GET /health", () => {
  test("returns { status: 'ok' }", async () => {
    const res = await app.inject({ method: "GET", url: "/health" });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ status: "ok" });
  });
});

// ── Products CRUD ─────────────────────────────────────────────────

describe("Products CRUD", () => {
  test("POST /products — creates a product (201)", async () => {
    queryResponder = (sql) => {
      if (sql.includes("INSERT INTO products")) return { rows: [PRODUCT], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({
      method: "POST",
      url: "/products",
      headers: { "content-type": "application/json" },
      payload: { name: "Widget", sku: "WDG-001", price_cents: 1999, category_id: 1, stock_qty: 50 },
    });

    expect(res.statusCode).toBe(201);
    expect(res.json()).toMatchObject({ id: "prod-001", name: "Widget", sku: "WDG-001" });

    const insertCall = queryCalls.find((c) => c.sql.includes("INSERT INTO products"));
    expect(insertCall).toBeDefined();
    expect(insertCall!.opts?.readonly).toBeUndefined();
  });

  test("GET /products — lists products (readonly)", async () => {
    queryResponder = (sql) => {
      if (sql.includes("SELECT * FROM products ORDER BY")) return { rows: [PRODUCT], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({ method: "GET", url: "/products" });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual([PRODUCT]);

    const selectCall = queryCalls.find((c) => c.sql.includes("SELECT * FROM products ORDER BY"));
    expect(selectCall!.opts).toEqual({ readonly: true });
  });

  test("GET /products/:id — returns single product (readonly)", async () => {
    queryResponder = (sql) => {
      if (sql.includes("WHERE id = $1")) return { rows: [PRODUCT], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({ method: "GET", url: "/products/prod-001" });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ id: "prod-001" });

    const selectCall = queryCalls.find((c) => c.sql.includes("WHERE id = $1"));
    expect(selectCall!.opts).toEqual({ readonly: true });
  });

  test("GET /products/:id — 404 when not found", async () => {
    queryResponder = () => ({ rows: [], rowCount: 0 });

    const res = await app.inject({ method: "GET", url: "/products/nonexistent" });

    expect(res.statusCode).toBe(404);
    expect(res.json()).toEqual({ error: "Product not found" });
  });

  test("PATCH /products/:id — updates product", async () => {
    const updated = { ...PRODUCT, name: "Super Widget", price_cents: 2499 };
    queryResponder = (sql) => {
      if (sql.includes("UPDATE products SET")) return { rows: [updated], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({
      method: "PATCH",
      url: "/products/prod-001",
      headers: { "content-type": "application/json" },
      payload: { name: "Super Widget", price_cents: 2499 },
    });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ name: "Super Widget", price_cents: 2499 });

    const updateCall = queryCalls.find((c) => c.sql.includes("UPDATE products SET"));
    expect(updateCall).toBeDefined();
    expect(updateCall!.opts?.readonly).toBeUndefined();
  });

  test("PATCH /products/:id — 404 when not found", async () => {
    queryResponder = () => ({ rows: [], rowCount: 0 });

    const res = await app.inject({
      method: "PATCH",
      url: "/products/nonexistent",
      headers: { "content-type": "application/json" },
      payload: { name: "Nope" },
    });

    expect(res.statusCode).toBe(404);
    expect(res.json()).toEqual({ error: "Product not found" });
  });

  test("PATCH /products/:id — 400 with empty body", async () => {
    const res = await app.inject({
      method: "PATCH",
      url: "/products/prod-001",
      headers: { "content-type": "application/json" },
      payload: {},
    });

    expect(res.statusCode).toBe(400);
    expect(res.json()).toEqual({ error: "No fields to update" });
  });

  test("DELETE /products/:id — deletes product (204)", async () => {
    queryResponder = (sql) => {
      if (sql.includes("DELETE FROM products")) return { rows: [{ id: "prod-001" }], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({ method: "DELETE", url: "/products/prod-001" });

    expect(res.statusCode).toBe(204);

    const deleteCall = queryCalls.find((c) => c.sql.includes("DELETE FROM products"));
    expect(deleteCall).toBeDefined();
    expect(deleteCall!.opts?.readonly).toBeUndefined();
  });

  test("DELETE /products/:id — 404 when not found", async () => {
    queryResponder = () => ({ rows: [], rowCount: 0 });

    const res = await app.inject({ method: "DELETE", url: "/products/nonexistent" });

    expect(res.statusCode).toBe(404);
    expect(res.json()).toEqual({ error: "Product not found" });
  });
});

// ── Customers CRUD ────────────────────────────────────────────────

describe("Customers CRUD", () => {
  test("POST /customers — creates a customer (201)", async () => {
    queryResponder = (sql) => {
      if (sql.includes("INSERT INTO customers")) return { rows: [CUSTOMER], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({
      method: "POST",
      url: "/customers",
      headers: { "content-type": "application/json" },
      payload: { email: "alice@example.com", name: "Alice", region: "eu" },
    });

    expect(res.statusCode).toBe(201);
    expect(res.json()).toMatchObject({ id: "cust-001", email: "alice@example.com" });
  });

  test("GET /customers — lists customers (readonly)", async () => {
    queryResponder = (sql) => {
      if (sql.includes("SELECT * FROM customers ORDER BY")) return { rows: [CUSTOMER], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({ method: "GET", url: "/customers" });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual([CUSTOMER]);

    const call = queryCalls.find((c) => c.sql.includes("SELECT * FROM customers ORDER BY"));
    expect(call!.opts).toEqual({ readonly: true });
  });

  test("GET /customers/:id — returns single customer (readonly)", async () => {
    queryResponder = (sql) => {
      if (sql.includes("FROM customers WHERE id")) return { rows: [CUSTOMER], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({ method: "GET", url: "/customers/cust-001" });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ id: "cust-001" });

    const call = queryCalls.find((c) => c.sql.includes("FROM customers WHERE id"));
    expect(call!.opts).toEqual({ readonly: true });
  });

  test("GET /customers/:id — 404 when not found", async () => {
    queryResponder = () => ({ rows: [], rowCount: 0 });

    const res = await app.inject({ method: "GET", url: "/customers/nonexistent" });

    expect(res.statusCode).toBe(404);
    expect(res.json()).toEqual({ error: "Customer not found" });
  });

  test("PATCH /customers/:id — updates customer", async () => {
    const updated = { ...CUSTOMER, name: "Alice Smith" };
    queryResponder = (sql) => {
      if (sql.includes("UPDATE customers SET")) return { rows: [updated], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({
      method: "PATCH",
      url: "/customers/cust-001",
      headers: { "content-type": "application/json" },
      payload: { name: "Alice Smith" },
    });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ name: "Alice Smith" });
  });

  test("PATCH /customers/:id — 404 when not found", async () => {
    queryResponder = () => ({ rows: [], rowCount: 0 });

    const res = await app.inject({
      method: "PATCH",
      url: "/customers/nonexistent",
      headers: { "content-type": "application/json" },
      payload: { name: "Nobody" },
    });

    expect(res.statusCode).toBe(404);
    expect(res.json()).toEqual({ error: "Customer not found" });
  });

  test("PATCH /customers/:id — 400 with empty body", async () => {
    const res = await app.inject({
      method: "PATCH",
      url: "/customers/cust-001",
      headers: { "content-type": "application/json" },
      payload: {},
    });

    expect(res.statusCode).toBe(400);
    expect(res.json()).toEqual({ error: "No fields to update" });
  });
});

// ── Orders CRUD ───────────────────────────────────────────────────

describe("Orders CRUD", () => {
  test("POST /orders — creates order with items in a transaction (201)", async () => {
    queryResponder = (sql) => {
      if (sql.includes("INSERT INTO orders")) return { rows: [ORDER], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({
      method: "POST",
      url: "/orders",
      headers: { "content-type": "application/json" },
      payload: {
        customer_id: "cust-001",
        region: "eu",
        items: [{ product_id: "prod-001", quantity: 2, unit_price_cents: 1999 }],
      },
    });

    expect(res.statusCode).toBe(201);
    expect(res.json()).toMatchObject({ id: "ord-001", status: "pending" });

    // Verify transaction was used
    expect(mockWithPrimaryClient).toHaveBeenCalledTimes(1);

    // Verify BEGIN / INSERT orders / INSERT order_items / COMMIT sequence
    const txSqls = transactionCalls.map((c) => c.sql);
    expect(txSqls[0]).toBe("BEGIN");
    expect(txSqls[1]).toContain("INSERT INTO orders");
    expect(txSqls[2]).toContain("INSERT INTO order_items");
    expect(txSqls[3]).toBe("COMMIT");
  });

  test("GET /orders — lists orders (readonly)", async () => {
    queryResponder = (sql) => {
      if (sql.includes("SELECT * FROM orders ORDER BY")) return { rows: [ORDER], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({ method: "GET", url: "/orders" });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual([ORDER]);

    const call = queryCalls.find((c) => c.sql.includes("SELECT * FROM orders ORDER BY"));
    expect(call!.opts).toEqual({ readonly: true });
  });

  test("GET /orders/:id — returns order with items (readonly)", async () => {
    queryResponder = (sql) => {
      if (sql.includes("FROM orders WHERE id")) return { rows: [ORDER], rowCount: 1 };
      if (sql.includes("FROM order_items WHERE order_id")) return { rows: ORDER_ITEMS, rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({ method: "GET", url: "/orders/ord-001" });

    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body).toMatchObject({ id: "ord-001", status: "pending" });
    expect(body.items).toEqual(ORDER_ITEMS);

    // Both queries should be readonly
    const orderQuery = queryCalls.find((c) => c.sql.includes("FROM orders WHERE id"));
    const itemsQuery = queryCalls.find((c) => c.sql.includes("FROM order_items WHERE order_id"));
    expect(orderQuery!.opts).toEqual({ readonly: true });
    expect(itemsQuery!.opts).toEqual({ readonly: true });
  });

  test("GET /orders/:id — 404 when not found", async () => {
    queryResponder = () => ({ rows: [], rowCount: 0 });

    const res = await app.inject({ method: "GET", url: "/orders/nonexistent" });

    expect(res.statusCode).toBe(404);
    expect(res.json()).toEqual({ error: "Order not found" });
  });

  test("PATCH /orders/:id — updates order status", async () => {
    const updated = { ...ORDER, status: "shipped", updated_at: "2026-01-02T00:00:00Z" };
    queryResponder = (sql) => {
      if (sql.includes("UPDATE orders SET status")) return { rows: [updated], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    const res = await app.inject({
      method: "PATCH",
      url: "/orders/ord-001",
      headers: { "content-type": "application/json" },
      payload: { status: "shipped" },
    });

    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ status: "shipped" });

    const updateCall = queryCalls.find((c) => c.sql.includes("UPDATE orders SET status"));
    expect(updateCall).toBeDefined();
    expect(updateCall!.opts?.readonly).toBeUndefined();
  });

  test("PATCH /orders/:id — 404 when not found", async () => {
    queryResponder = () => ({ rows: [], rowCount: 0 });

    const res = await app.inject({
      method: "PATCH",
      url: "/orders/nonexistent",
      headers: { "content-type": "application/json" },
      payload: { status: "shipped" },
    });

    expect(res.statusCode).toBe(404);
    expect(res.json()).toEqual({ error: "Order not found" });
  });
});

// ── Read-write routing ────────────────────────────────────────────

describe("Read-write routing", () => {
  test("GET endpoints pass readonly: true", async () => {
    queryResponder = () => ({ rows: [PRODUCT], rowCount: 1 });

    await app.inject({ method: "GET", url: "/products" });
    await app.inject({ method: "GET", url: "/products/prod-001" });
    await app.inject({ method: "GET", url: "/customers" });
    await app.inject({ method: "GET", url: "/customers/cust-001" });
    await app.inject({ method: "GET", url: "/orders" });

    const readCalls = queryCalls.filter((c) => c.sql.startsWith("SELECT"));
    expect(readCalls.length).toBeGreaterThanOrEqual(5);
    for (const call of readCalls) {
      expect(call.opts).toEqual({ readonly: true });
    }
  });

  test("write endpoints do not set readonly", async () => {
    queryResponder = (sql) => {
      if (sql.includes("INSERT") || sql.includes("UPDATE") || sql.includes("DELETE")) {
        return { rows: [PRODUCT], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    };

    await app.inject({
      method: "POST",
      url: "/products",
      headers: { "content-type": "application/json" },
      payload: { name: "X", sku: "X-1", price_cents: 100, category_id: 1, stock_qty: 1 },
    });

    await app.inject({
      method: "PATCH",
      url: "/products/prod-001",
      headers: { "content-type": "application/json" },
      payload: { name: "Y" },
    });

    await app.inject({ method: "DELETE", url: "/products/prod-001" });

    const writeCalls = queryCalls.filter(
      (c) => c.sql.includes("INSERT") || c.sql.includes("UPDATE") || c.sql.includes("DELETE"),
    );
    expect(writeCalls.length).toBe(3);
    for (const call of writeCalls) {
      expect(call.opts?.readonly).toBeUndefined();
    }
  });

  test("POST /orders uses withPrimaryClient (not query) for transactional writes", async () => {
    queryResponder = (sql) => {
      if (sql.includes("INSERT INTO orders")) return { rows: [ORDER], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    };

    await app.inject({
      method: "POST",
      url: "/orders",
      headers: { "content-type": "application/json" },
      payload: {
        customer_id: "cust-001",
        region: "eu",
        items: [{ product_id: "prod-001", quantity: 1, unit_price_cents: 1999 }],
      },
    });

    // Order creation should go through withPrimaryClient, not mockQuery
    expect(mockWithPrimaryClient).toHaveBeenCalledTimes(1);
    const directInserts = queryCalls.filter((c) => c.sql.includes("INSERT INTO orders"));
    expect(directInserts).toHaveLength(0);
  });
});

// ── Rate limiting headers ─────────────────────────────────────────

describe("Rate limiting headers", () => {
  let rateLimitedApp: FastifyInstance;

  beforeAll(async () => {
    sortedSets.clear();
    counters.clear();

    rateLimitedApp = Fastify({ logger: false });

    const limiter = new SlidingWindowRateLimiter({ maxRequests: 3, windowSizeMs: 60_000 });
    rateLimitedApp.addHook("onRequest", async (req, reply) => {
      const identifier = (req.headers as Record<string, string>)["x-api-key"] ?? req.ip;
      const result = await limiter.check(identifier);
      reply
        .header("X-RateLimit-Limit", String(result.limit))
        .header("X-RateLimit-Remaining", String(result.remaining))
        .header("X-RateLimit-Reset", String(result.resetAt));
      if (!result.allowed) {
        reply.code(429);
        await reply.send({
          error: "Too Many Requests",
          retryAfter: result.resetAt - Math.floor(Date.now() / 1000),
        });
        return reply;
      }
    });

    rateLimitedApp.get("/health", async () => ({ status: "ok" }));
    rateLimitedApp.register(productRoutes, { prefix: "/products" });

    await rateLimitedApp.ready();
  });

  afterAll(async () => {
    await rateLimitedApp.close();
  });

  beforeEach(() => {
    sortedSets.clear();
    counters.clear();
    queryResponder = () => ({ rows: [PRODUCT], rowCount: 1 });
  });

  test("responses include X-RateLimit-* headers", async () => {
    const res = await rateLimitedApp.inject({
      method: "GET",
      url: "/health",
      headers: { "x-api-key": "test-key" },
    });

    expect(res.statusCode).toBe(200);
    expect(res.headers["x-ratelimit-limit"]).toBe("3");
    expect(res.headers["x-ratelimit-remaining"]).toBeDefined();
    expect(res.headers["x-ratelimit-reset"]).toBeDefined();
  });

  test("remaining decreases with each request", async () => {
    const headers = { "x-api-key": "decr-key" };

    const r1 = await rateLimitedApp.inject({ method: "GET", url: "/health", headers });
    const r2 = await rateLimitedApp.inject({ method: "GET", url: "/health", headers });

    expect(Number(r1.headers["x-ratelimit-remaining"])).toBe(2);
    expect(Number(r2.headers["x-ratelimit-remaining"])).toBe(1);
  });

  test("returns 429 when rate limit exceeded", async () => {
    const headers = { "x-api-key": "flood-key" };

    // Exhaust the limit (3 requests)
    for (let i = 0; i < 3; i++) {
      await rateLimitedApp.inject({ method: "GET", url: "/health", headers });
    }

    // 4th request should be rejected
    const res = await rateLimitedApp.inject({ method: "GET", url: "/health", headers });

    expect(res.statusCode).toBe(429);
    const body = res.json();
    expect(body.error).toBe("Too Many Requests");
    expect(body.retryAfter).toBeGreaterThan(0);
  });

  test("rate limit applies to API routes too", async () => {
    const headers: Record<string, string> = { "x-api-key": "api-key", "content-type": "application/json" };

    // Exhaust with product reads
    for (let i = 0; i < 3; i++) {
      await rateLimitedApp.inject({ method: "GET", url: "/products", headers });
    }

    // Next request to any endpoint should be blocked
    const res = await rateLimitedApp.inject({ method: "GET", url: "/products", headers });

    expect(res.statusCode).toBe(429);
  });

  test("different API keys have independent limits", async () => {
    // Exhaust key-a
    for (let i = 0; i < 3; i++) {
      await rateLimitedApp.inject({
        method: "GET",
        url: "/health",
        headers: { "x-api-key": "key-a" },
      });
    }

    // key-b should still work
    const res = await rateLimitedApp.inject({
      method: "GET",
      url: "/health",
      headers: { "x-api-key": "key-b" },
    });

    expect(res.statusCode).toBe(200);
    expect(Number(res.headers["x-ratelimit-remaining"])).toBe(2);
  });
});

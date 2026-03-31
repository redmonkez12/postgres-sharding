import { describe, expect, test, beforeEach, mock } from "bun:test";

// ── In-memory Redis store ───────────────────────────────────────

const store = new Map<string, string>();

const mockGet = mock(async (key: string) => store.get(key) ?? null);
const mockSet = mock(
  async (key: string, value: string, _ex?: string, _ttl?: number) => {
    store.set(key, value);
    return "OK";
  },
);
const mockDel = mock(async (...keys: string[]) => {
  let n = 0;
  for (const k of keys) if (store.delete(k)) n++;
  return n;
});
const mockDisconnect = mock(() => {});
const mockInfo = mock(async () => "used_memory_human:1.50M\r\n");
const mockDbsize = mock(async () => store.size);

let pipelineCalls: unknown[][] = [];
const mockPipelineExec = mock(async () => {
  for (const args of pipelineCalls) store.set(args[0] as string, args[1] as string);
  return pipelineCalls.map(() => [null, "OK"]);
});

const mockPipeline = mock(() => {
  pipelineCalls = [];
  const p = {
    set(...args: unknown[]) {
      pipelineCalls.push(args);
      return p;
    },
    exec: mockPipelineExec,
  };
  return p;
});

mock.module("ioredis", () => ({
  Redis: class MockRedis {
    get = mockGet;
    set = mockSet;
    del = mockDel;
    disconnect = mockDisconnect;
    pipeline = mockPipeline;
    info = mockInfo;
    dbsize = mockDbsize;
  },
}));

// ── Mock DB query ───────────────────────────────────────────────

const mockQuery = mock();

mock.module("../../src/db/connection.js", () => ({ query: mockQuery }));

// ── Suppress logger output ──────────────────────────────────────

mock.module("../../src/utils/logger.js", () => ({
  logger: { info: () => {}, warn: () => {}, error: () => {} },
}));

// ── Prevent dotenv side-effects ─────────────────────────────────

mock.module("dotenv/config", () => ({}));

// ── Import under test ───────────────────────────────────────────

const { ProductCache } = await import("../../src/cache/product-cache.js");

// ── Fixtures ────────────────────────────────────────────────────

const PRODUCT_A = {
  id: "p1",
  name: "Widget",
  sku: "W-001",
  price_cents: 999,
  category_id: 1,
  stock_qty: 50,
  created_at: "2025-01-01T00:00:00Z",
};

const PRODUCT_B = {
  id: "p2",
  name: "Gadget",
  sku: "G-002",
  price_cents: 1999,
  category_id: 2,
  stock_qty: 25,
  created_at: "2025-01-02T00:00:00Z",
};

// ── Tests ───────────────────────────────────────────────────────

describe("ProductCache", () => {
  let cache: InstanceType<typeof ProductCache>;

  beforeEach(() => {
    store.clear();
    pipelineCalls = [];
    mockGet.mockClear();
    mockSet.mockClear();
    mockDel.mockClear();
    mockDisconnect.mockClear();
    mockPipeline.mockClear();
    mockPipelineExec.mockClear();
    mockInfo.mockClear();
    mockDbsize.mockClear();
    mockQuery.mockClear();
    cache = new ProductCache();
  });

  // ── getProduct ──────────────────────────────────────────────

  describe("getProduct", () => {
    test("cache miss → DB fetch → cache set", async () => {
      mockQuery.mockResolvedValueOnce({ rows: [PRODUCT_A] });

      const result = await cache.getProduct("p1");

      expect(result).toEqual(PRODUCT_A);
      expect(mockGet).toHaveBeenCalledWith("product:p1");
      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockSet).toHaveBeenCalledWith(
        "product:p1",
        JSON.stringify(PRODUCT_A),
        "EX",
        300,
      );
    });

    test("cache hit returns cached data without DB call", async () => {
      store.set("product:p1", JSON.stringify(PRODUCT_A));

      const result = await cache.getProduct("p1");

      expect(result).toEqual(PRODUCT_A);
      expect(mockQuery).not.toHaveBeenCalled();
    });

    test("returns null when product not found in DB", async () => {
      mockQuery.mockResolvedValueOnce({ rows: [] });

      const result = await cache.getProduct("missing");

      expect(result).toBeNull();
      expect(mockSet).not.toHaveBeenCalled();
    });

    test("sets product TTL to 300 seconds", async () => {
      mockQuery.mockResolvedValueOnce({ rows: [PRODUCT_A] });

      await cache.getProduct("p1");

      const [, , ex, ttl] = mockSet.mock.calls[0];
      expect(ex).toBe("EX");
      expect(ttl).toBe(300);
    });
  });

  // ── listProducts ────────────────────────────────────────────

  describe("listProducts", () => {
    test("cache miss fetches from DB and caches the result", async () => {
      mockQuery.mockResolvedValueOnce({ rows: [PRODUCT_A, PRODUCT_B] });

      const result = await cache.listProducts();

      expect(result).toEqual([PRODUCT_A, PRODUCT_B]);
      expect(mockGet).toHaveBeenCalledWith("products:list");
      expect(mockQuery).toHaveBeenCalledTimes(1);
      expect(mockSet).toHaveBeenCalledWith(
        "products:list",
        JSON.stringify([PRODUCT_A, PRODUCT_B]),
        "EX",
        60,
      );
    });

    test("cache hit returns cached data without DB call", async () => {
      store.set("products:list", JSON.stringify([PRODUCT_A]));

      const result = await cache.listProducts();

      expect(result).toEqual([PRODUCT_A]);
      expect(mockQuery).not.toHaveBeenCalled();
    });

    test("sets list TTL to 60 seconds", async () => {
      mockQuery.mockResolvedValueOnce({ rows: [PRODUCT_A] });

      await cache.listProducts();

      const [, , ex, ttl] = mockSet.mock.calls[0];
      expect(ex).toBe("EX");
      expect(ttl).toBe(60);
    });
  });

  // ── invalidateProduct ───────────────────────────────────────

  describe("invalidateProduct", () => {
    test("deletes both the product key and the list key", async () => {
      store.set("product:p1", JSON.stringify(PRODUCT_A));
      store.set("products:list", JSON.stringify([PRODUCT_A]));

      await cache.invalidateProduct("p1");

      expect(mockDel).toHaveBeenCalledWith("product:p1", "products:list");
      expect(store.has("product:p1")).toBe(false);
      expect(store.has("products:list")).toBe(false);
    });
  });

  // ── updateProduct ───────────────────────────────────────────

  describe("updateProduct", () => {
    test("updates DB and invalidates cache", async () => {
      const updated = { ...PRODUCT_A, name: "Super Widget" };
      mockQuery.mockResolvedValueOnce({ rows: [updated] });

      const result = await cache.updateProduct("p1", { name: "Super Widget" });

      expect(result).toEqual(updated);
      expect(mockQuery).toHaveBeenCalledTimes(1);

      const [sql, params] = mockQuery.mock.calls[0];
      expect(sql).toContain("UPDATE products SET");
      expect(sql).toContain("RETURNING *");
      expect(params).toEqual(["Super Widget", "p1"]);

      // Invalidation must follow the update
      expect(mockDel).toHaveBeenCalledWith("product:p1", "products:list");
    });

    test("returns null when product not found", async () => {
      mockQuery.mockResolvedValueOnce({ rows: [] });

      const result = await cache.updateProduct("missing", { name: "X" });

      expect(result).toBeNull();
      expect(mockDel).not.toHaveBeenCalled();
    });

    test("delegates to getProduct when no fields provided", async () => {
      store.set("product:p1", JSON.stringify(PRODUCT_A));

      const result = await cache.updateProduct("p1", {});

      expect(result).toEqual(PRODUCT_A);
      // No UPDATE query — getProduct served from cache
      expect(mockQuery).not.toHaveBeenCalled();
    });
  });

  // ── warmUp ──────────────────────────────────────────────────

  describe("warmUp", () => {
    test("populates cache with all products from DB", async () => {
      mockQuery.mockResolvedValueOnce({ rows: [PRODUCT_A, PRODUCT_B] });

      const count = await cache.warmUp();

      expect(count).toBe(2);
      expect(mockPipeline).toHaveBeenCalledTimes(1);
      expect(mockPipelineExec).toHaveBeenCalledTimes(1);

      // One pipeline.set per product + one for the list
      expect(pipelineCalls).toHaveLength(3);
      expect(pipelineCalls[0]).toEqual(["product:p1", JSON.stringify(PRODUCT_A), "EX", 300]);
      expect(pipelineCalls[1]).toEqual(["product:p2", JSON.stringify(PRODUCT_B), "EX", 300]);
      expect(pipelineCalls[2]).toEqual([
        "products:list",
        JSON.stringify([PRODUCT_A, PRODUCT_B]),
        "EX",
        60,
      ]);
    });
  });

  // ── Redis failure ───────────────────────────────────────────

  describe("Redis failure", () => {
    test("propagates error when Redis is unavailable", async () => {
      mockGet.mockImplementationOnce(async () => {
        throw new Error("ECONNREFUSED");
      });

      await expect(cache.getProduct("p1")).rejects.toThrow("ECONNREFUSED");
    });

    test("propagates error on cache set failure", async () => {
      mockQuery.mockResolvedValueOnce({ rows: [PRODUCT_A] });
      mockSet.mockImplementationOnce(async () => {
        throw new Error("ECONNREFUSED");
      });

      await expect(cache.getProduct("p1")).rejects.toThrow("ECONNREFUSED");
    });
  });

  // ── close ───────────────────────────────────────────────────

  describe("close", () => {
    test("disconnects Redis client", async () => {
      await cache.close();

      expect(mockDisconnect).toHaveBeenCalledTimes(1);
    });
  });
});

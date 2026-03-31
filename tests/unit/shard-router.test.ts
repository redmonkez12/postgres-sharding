import { describe, expect, test, beforeEach, mock } from "bun:test";

// ── Mock pg pools ────────────────────────────────────────────────

type MockPool = {
  query: ReturnType<typeof mock>;
  end: ReturnType<typeof mock>;
};

const createdPools: Array<{ connectionString: string; pool: MockPool }> = [];

mock.module("pg", () => ({
  Pool: class FakePool {
    query: ReturnType<typeof mock>;
    end: ReturnType<typeof mock>;

    constructor(opts: { connectionString: string }) {
      this.query = mock(async () => ({ rows: [], rowCount: 0 }));
      this.end = mock(async () => {});
      createdPools.push({ connectionString: opts.connectionString, pool: this });
    }
  },
}));

mock.module("../../src/utils/logger.js", () => ({
  logger: { info: () => {}, warn: () => {}, error: () => {} },
}));

mock.module("dotenv/config", () => ({}));

// ── Import under test ────────────────────────────────────────────

const { ShardRouter } = await import("../../src/db/shard-router.js");

// ── Helpers ──────────────────────────────────────────────────────

const CONFIGS = [
  { region: "eu" as const, connectionString: "postgresql://eu" },
  { region: "us" as const, connectionString: "postgresql://us" },
  { region: "ap" as const, connectionString: "postgresql://ap" },
];

function getPool(router: InstanceType<typeof ShardRouter>, region: string): MockPool {
  return router.getPool(region) as unknown as MockPool;
}

// ── Tests ────────────────────────────────────────────────────────

describe("ShardRouter", () => {
  let router: InstanceType<typeof ShardRouter>;

  beforeEach(() => {
    createdPools.length = 0;
    router = new ShardRouter(CONFIGS);
  });

  // ── Construction ─────────────────────────────────────────────

  describe("constructor", () => {
    test("creates a pool per region", () => {
      expect(router.regions).toEqual(["eu", "us", "ap"]);
      expect(createdPools).toHaveLength(3);
    });

    test("uses supplied connection strings", () => {
      const urls = createdPools.map((p) => p.connectionString);
      expect(urls).toEqual(["postgresql://eu", "postgresql://us", "postgresql://ap"]);
    });
  });

  // ── getPool / invalid region ─────────────────────────────────

  describe("getPool", () => {
    test("returns the pool for a valid region", () => {
      const pool = getPool(router, "eu");
      expect(pool.query).toBeDefined();
    });

    test("throws for an unknown region", () => {
      expect(() => router.getPool("xx")).toThrow("Unknown shard region: xx");
    });
  });

  // ── query – region routing ───────────────────────────────────

  describe("query", () => {
    test("routes to the EU shard pool", async () => {
      const euPool = getPool(router, "eu");
      euPool.query.mockResolvedValueOnce({ rows: [{ id: 1 }], rowCount: 1 });

      const result = await router.query("eu", "SELECT 1");

      expect(euPool.query).toHaveBeenCalledWith("SELECT 1", undefined);
      expect(result.rows).toEqual([{ id: 1 }]);
    });

    test("routes to the US shard pool", async () => {
      const usPool = getPool(router, "us");
      usPool.query.mockResolvedValueOnce({ rows: [{ id: 2 }], rowCount: 1 });

      const result = await router.query("us", "SELECT 2");

      expect(usPool.query).toHaveBeenCalledWith("SELECT 2", undefined);
      expect(result.rows).toEqual([{ id: 2 }]);
    });

    test("routes to the AP shard pool", async () => {
      const apPool = getPool(router, "ap");
      apPool.query.mockResolvedValueOnce({ rows: [{ id: 3 }], rowCount: 1 });

      const result = await router.query("ap", "SELECT 3");

      expect(apPool.query).toHaveBeenCalledWith("SELECT 3", undefined);
      expect(result.rows).toEqual([{ id: 3 }]);
    });

    test("passes parameters to the pool", async () => {
      const euPool = getPool(router, "eu");
      euPool.query.mockResolvedValueOnce({ rows: [], rowCount: 0 });

      await router.query("eu", "SELECT * FROM t WHERE id = $1", [42]);

      expect(euPool.query).toHaveBeenCalledWith("SELECT * FROM t WHERE id = $1", [42]);
    });

    test("throws for an invalid region", async () => {
      await expect(router.query("xx", "SELECT 1")).rejects.toThrow(
        "Unknown shard region: xx",
      );
    });

    test("does not touch other shard pools", async () => {
      const euPool = getPool(router, "eu");
      const usPool = getPool(router, "us");
      const apPool = getPool(router, "ap");
      euPool.query.mockResolvedValueOnce({ rows: [], rowCount: 0 });

      await router.query("eu", "SELECT 1");

      expect(usPool.query).not.toHaveBeenCalled();
      expect(apPool.query).not.toHaveBeenCalled();
    });
  });

  // ── queryAll – scatter-gather ────────────────────────────────

  describe("queryAll", () => {
    test("executes across all 3 shards and merges results", async () => {
      const euPool = getPool(router, "eu");
      const usPool = getPool(router, "us");
      const apPool = getPool(router, "ap");

      euPool.query.mockResolvedValueOnce({ rows: [{ r: "eu" }], rowCount: 1 });
      usPool.query.mockResolvedValueOnce({ rows: [{ r: "us" }], rowCount: 1 });
      apPool.query.mockResolvedValueOnce({ rows: [{ r: "ap" }], rowCount: 1 });

      const results = await router.queryAll("SELECT region()");

      expect(results).toHaveLength(3);
      expect(results[0].rows).toEqual([{ r: "eu" }]);
      expect(results[1].rows).toEqual([{ r: "us" }]);
      expect(results[2].rows).toEqual([{ r: "ap" }]);
    });

    test("passes parameters to every shard", async () => {
      const euPool = getPool(router, "eu");
      const usPool = getPool(router, "us");
      const apPool = getPool(router, "ap");

      await router.queryAll("SELECT * FROM t WHERE id = $1", [7]);

      expect(euPool.query).toHaveBeenCalledWith("SELECT * FROM t WHERE id = $1", [7]);
      expect(usPool.query).toHaveBeenCalledWith("SELECT * FROM t WHERE id = $1", [7]);
      expect(apPool.query).toHaveBeenCalledWith("SELECT * FROM t WHERE id = $1", [7]);
    });

    test("returns partial results when one shard fails", async () => {
      const euPool = getPool(router, "eu");
      const usPool = getPool(router, "us");
      const apPool = getPool(router, "ap");

      euPool.query.mockResolvedValueOnce({ rows: [{ r: "eu" }], rowCount: 1 });
      usPool.query.mockRejectedValueOnce(new Error("connection refused"));
      apPool.query.mockResolvedValueOnce({ rows: [{ r: "ap" }], rowCount: 1 });

      const results = await router.queryAll("SELECT 1");

      expect(results).toHaveLength(2);
      expect(results[0].rows).toEqual([{ r: "eu" }]);
      expect(results[1].rows).toEqual([{ r: "ap" }]);
    });

    test("returns empty array when all shards fail", async () => {
      const euPool = getPool(router, "eu");
      const usPool = getPool(router, "us");
      const apPool = getPool(router, "ap");

      euPool.query.mockRejectedValueOnce(new Error("down"));
      usPool.query.mockRejectedValueOnce(new Error("down"));
      apPool.query.mockRejectedValueOnce(new Error("down"));

      const results = await router.queryAll("SELECT 1");

      expect(results).toHaveLength(0);
    });
  });

  // ── healthCheck ──────────────────────────────────────────────

  describe("healthCheck", () => {
    test("returns true for all healthy shards", async () => {
      const euPool = getPool(router, "eu");
      const usPool = getPool(router, "us");
      const apPool = getPool(router, "ap");

      euPool.query.mockResolvedValueOnce({ rows: [{ "?column?": 1 }], rowCount: 1 });
      usPool.query.mockResolvedValueOnce({ rows: [{ "?column?": 1 }], rowCount: 1 });
      apPool.query.mockResolvedValueOnce({ rows: [{ "?column?": 1 }], rowCount: 1 });

      const health = await router.healthCheck();

      expect(health.get("eu")).toBe(true);
      expect(health.get("us")).toBe(true);
      expect(health.get("ap")).toBe(true);
    });

    test("returns false for a down shard", async () => {
      const euPool = getPool(router, "eu");
      const usPool = getPool(router, "us");
      const apPool = getPool(router, "ap");

      euPool.query.mockResolvedValueOnce({ rows: [], rowCount: 0 });
      usPool.query.mockRejectedValueOnce(new Error("connection refused"));
      apPool.query.mockResolvedValueOnce({ rows: [], rowCount: 0 });

      const health = await router.healthCheck();

      expect(health.get("eu")).toBe(true);
      expect(health.get("us")).toBe(false);
      expect(health.get("ap")).toBe(true);
    });

    test("returns false for all shards when all are down", async () => {
      const euPool = getPool(router, "eu");
      const usPool = getPool(router, "us");
      const apPool = getPool(router, "ap");

      euPool.query.mockRejectedValueOnce(new Error("down"));
      usPool.query.mockRejectedValueOnce(new Error("down"));
      apPool.query.mockRejectedValueOnce(new Error("down"));

      const health = await router.healthCheck();

      expect(health.get("eu")).toBe(false);
      expect(health.get("us")).toBe(false);
      expect(health.get("ap")).toBe(false);
    });

    test("issues SELECT 1 to each shard", async () => {
      const euPool = getPool(router, "eu");
      const usPool = getPool(router, "us");
      const apPool = getPool(router, "ap");

      await router.healthCheck();

      expect(euPool.query).toHaveBeenCalledWith("SELECT 1");
      expect(usPool.query).toHaveBeenCalledWith("SELECT 1");
      expect(apPool.query).toHaveBeenCalledWith("SELECT 1");
    });
  });

  // ── close ────────────────────────────────────────────────────

  describe("close", () => {
    test("ends all pools", async () => {
      const euPool = getPool(router, "eu");
      const usPool = getPool(router, "us");
      const apPool = getPool(router, "ap");

      await router.close();

      expect(euPool.end).toHaveBeenCalledTimes(1);
      expect(usPool.end).toHaveBeenCalledTimes(1);
      expect(apPool.end).toHaveBeenCalledTimes(1);
    });
  });
});

import { describe, expect, test, beforeEach, mock } from "bun:test";
import { mockPgPools, type MockPool } from "../helpers/mock-pg.js";

mock.module("../../src/utils/logger.js", () => ({
  logger: { info: () => {}, warn: () => {}, error: () => {} },
}));

mock.module("dotenv/config", () => ({}));

// ── Import under test ────────────────────────────────────────────

const {
  query,
  closePools,
  resetReplicaFlag,
  getPrimaryPool,
  getReplicaPool,
  withPrimaryClient,
} = await import("../../src/db/read-write-split.js");

// ── Helpers ──────────────────────────────────────────────────────

function findPoolByPort(port: number): MockPool | undefined {
  return mockPgPools.find((p) => p.config.port === port)?.pool;
}

function findConfigByPort(port: number) {
  return mockPgPools.find((p) => p.config.port === port)?.config;
}

// ── Tests ────────────────────────────────────────────────────────

describe("read-write-split", () => {
  beforeEach(async () => {
    await closePools();
    mockPgPools.length = 0;
  });

  // ── Query routing ─────────────────────────────────────────────

  describe("query routing", () => {
    test("readonly query goes to replica pool", async () => {
      await query("SELECT 1", [], { readonly: true });

      const replica = findPoolByPort(5433);
      expect(replica).toBeDefined();
      expect(replica!.query).toHaveBeenCalledWith("SELECT 1", []);
    });

    test("non-readonly query goes to primary pool", async () => {
      await query("SELECT 1");

      const primary = findPoolByPort(5432);
      expect(primary).toBeDefined();
      expect(primary!.query).toHaveBeenCalledWith("SELECT 1", []);
    });

    test("readonly query does not create primary pool", async () => {
      await query("SELECT 1", [], { readonly: true });

      expect(findPoolByPort(5432)).toBeUndefined();
    });

    test("non-readonly query does not create replica pool", async () => {
      await query("SELECT 1");

      expect(findPoolByPort(5433)).toBeUndefined();
    });

    test("passes parameters to replica pool", async () => {
      await query("SELECT * FROM t WHERE id = $1", [42], { readonly: true });

      const replica = findPoolByPort(5433)!;
      expect(replica.query).toHaveBeenCalledWith("SELECT * FROM t WHERE id = $1", [42]);
    });

    test("passes parameters to primary pool", async () => {
      await query("INSERT INTO t VALUES ($1)", [99]);

      const primary = findPoolByPort(5432)!;
      expect(primary.query).toHaveBeenCalledWith("INSERT INTO t VALUES ($1)", [99]);
    });

    test("returns query result from replica", async () => {
      getReplicaPool();
      const replica = findPoolByPort(5433)!;
      replica.query.mockResolvedValueOnce({ rows: [{ id: 1 }], rowCount: 1 });

      const result = await query("SELECT 1", [], { readonly: true });

      expect(result.rows).toEqual([{ id: 1 }]);
    });

    test("returns query result from primary", async () => {
      getPrimaryPool();
      const primary = findPoolByPort(5432)!;
      primary.query.mockResolvedValueOnce({ rows: [{ id: 2 }], rowCount: 1 });

      const result = await query("SELECT 2");

      expect(result.rows).toEqual([{ id: 2 }]);
    });
  });

  // ── Replica fallback ──────────────────────────────────────────

  describe("replica fallback", () => {
    test("falls back to primary when replica query fails", async () => {
      getReplicaPool();
      const replica = findPoolByPort(5433)!;
      replica.query.mockRejectedValueOnce(new Error("connection refused"));

      await query("SELECT 1", [], { readonly: true });

      const primary = findPoolByPort(5432);
      expect(primary).toBeDefined();
      expect(primary!.query).toHaveBeenCalledWith("SELECT 1", []);
    });

    test("subsequent readonly queries go to primary after replica failure", async () => {
      getReplicaPool();
      const replica = findPoolByPort(5433)!;
      replica.query.mockRejectedValueOnce(new Error("connection refused"));

      // First query — fails on replica, falls back to primary
      await query("SELECT 1", [], { readonly: true });

      const primary = findPoolByPort(5432)!;
      primary.query.mockResolvedValueOnce({ rows: [{ n: 2 }], rowCount: 1 });

      // Second query — should go straight to primary (replicaDown = true)
      const result = await query("SELECT 2", [], { readonly: true });

      expect(result.rows).toEqual([{ n: 2 }]);
      expect(replica.query).toHaveBeenCalledTimes(1);
      expect(primary.query).toHaveBeenCalledTimes(2);
    });

    test("primary query throws normally (no fallback mechanism)", async () => {
      getPrimaryPool();
      const primary = findPoolByPort(5432)!;
      primary.query.mockRejectedValueOnce(new Error("primary down"));

      await expect(query("SELECT 1")).rejects.toThrow("primary down");
    });
  });

  // ── resetReplicaFlag ──────────────────────────────────────────

  describe("resetReplicaFlag", () => {
    test("re-enables replica routing after failure", async () => {
      getReplicaPool();
      const replica = findPoolByPort(5433)!;
      replica.query.mockRejectedValueOnce(new Error("down"));

      // Trigger fallback — sets replicaDown = true
      await query("SELECT 1", [], { readonly: true });
      expect(findPoolByPort(5432)).toBeDefined();

      // Reset the flag
      await resetReplicaFlag();

      // Next readonly query should go to replica again
      replica.query.mockResolvedValueOnce({ rows: [{ ok: true }], rowCount: 1 });
      const result = await query("SELECT 2", [], { readonly: true });

      expect(replica.query).toHaveBeenCalledTimes(2);
      expect(result.rows).toEqual([{ ok: true }]);
    });
  });

  // ── Pool configuration ────────────────────────────────────────

  describe("pool configuration", () => {
    test("primary pool uses port 5432 by default", () => {
      getPrimaryPool();
      expect(findConfigByPort(5432)).toBeDefined();
    });

    test("replica pool uses port 5433 by default", () => {
      getReplicaPool();
      expect(findConfigByPort(5433)).toBeDefined();
    });

    test("pools use default max connections of 10", () => {
      getPrimaryPool();
      const config = findConfigByPort(5432);
      expect(config.max).toBe(10);
    });

    test("pools use localhost by default", () => {
      getPrimaryPool();
      const config = findConfigByPort(5432);
      expect(config.host).toBe("localhost");
    });

    test("pools use default credentials", () => {
      getPrimaryPool();
      const config = findConfigByPort(5432);
      expect(config.user).toBe("postgres");
      expect(config.password).toBe("postgres");
      expect(config.database).toBe("postgres");
    });
  });

  // ── closePools ────────────────────────────────────────────────

  describe("closePools", () => {
    test("ends both pools", async () => {
      getPrimaryPool();
      getReplicaPool();
      const primary = findPoolByPort(5432)!;
      const replica = findPoolByPort(5433)!;

      await closePools();

      expect(primary.end).toHaveBeenCalledTimes(1);
      expect(replica.end).toHaveBeenCalledTimes(1);
    });

    test("creates fresh pools after close", async () => {
      getPrimaryPool();
      getReplicaPool();
      expect(mockPgPools).toHaveLength(2);

      await closePools();
      mockPgPools.length = 0;

      await query("SELECT 1");
      expect(mockPgPools).toHaveLength(1);
    });

    test("resets replica down flag", async () => {
      getReplicaPool();
      const replica = findPoolByPort(5433)!;
      replica.query.mockRejectedValueOnce(new Error("down"));

      await query("SELECT 1", [], { readonly: true });

      await closePools();
      mockPgPools.length = 0;

      // After close, replica should be tried again
      await query("SELECT 2", [], { readonly: true });
      const newReplica = findPoolByPort(5433);
      expect(newReplica).toBeDefined();
      expect(newReplica!.query).toHaveBeenCalledWith("SELECT 2", []);
    });
  });

  // ── withPrimaryClient ─────────────────────────────────────────

  describe("withPrimaryClient", () => {
    test("returns callback result", async () => {
      const result = await withPrimaryClient(async () => "done");

      expect(result).toBe("done");
    });

    test("releases the client after callback", async () => {
      getPrimaryPool();
      const primary = findPoolByPort(5432)!;
      const mockRelease = mock(() => {});
      primary.connect.mockResolvedValueOnce({
        query: mock(async () => ({ rows: [], rowCount: 0 })),
        release: mockRelease,
      });

      await withPrimaryClient(async () => "ok");

      expect(mockRelease).toHaveBeenCalledTimes(1);
    });

    test("releases the client even if callback throws", async () => {
      getPrimaryPool();
      const primary = findPoolByPort(5432)!;
      const mockRelease = mock(() => {});
      primary.connect.mockResolvedValueOnce({ release: mockRelease });

      await expect(
        withPrimaryClient(async () => {
          throw new Error("boom");
        }),
      ).rejects.toThrow("boom");

      expect(mockRelease).toHaveBeenCalledTimes(1);
    });
  });
});

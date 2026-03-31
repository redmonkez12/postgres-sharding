/**
 * Shared pg mock — imported by every unit test that needs a fake Pool.
 *
 * Centralising the mock.module("pg") call here avoids cross-file
 * conflicts: Bun caches modules globally across test files, so two
 * files each calling mock.module("pg") with different FakePool classes
 * leads to the first mock "winning" and the second file's tracking
 * array staying empty.
 *
 * Usage:
 *   import { mockPgPools, type MockPool } from "../helpers/mock-pg.js";
 */

import { mock } from "bun:test";

export type MockPool = {
  query: ReturnType<typeof mock>;
  end: ReturnType<typeof mock>;
  connect: ReturnType<typeof mock>;
};

export const mockPgPools: Array<{ config: any; pool: MockPool }> = [];

mock.module("pg", () => ({
  Pool: class FakePool {
    query = mock(async () => ({ rows: [], rowCount: 0 }));
    end = mock(async () => {});
    connect = mock(async () => ({
      query: mock(async () => ({ rows: [], rowCount: 0 })),
      release: mock(() => {}),
    }));

    constructor(config: any) {
      mockPgPools.push({ config, pool: this as unknown as MockPool });
    }
  },
}));

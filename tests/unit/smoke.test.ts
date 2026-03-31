import { describe, expect, test } from "bun:test";

describe("smoke", () => {
  test("bun test runner works", () => {
    expect(1 + 1).toBe(2);
  });

  test("async support works", async () => {
    const value = await Promise.resolve(42);
    expect(value).toBe(42);
  });
});

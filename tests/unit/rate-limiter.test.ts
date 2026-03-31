import { describe, expect, test, beforeEach, afterEach, mock } from "bun:test";

// ── In-memory Redis stores ─────────────────────────────────────

// Sorted sets: key → Map<member, score>
const sortedSets = new Map<string, Map<string, number>>();
// Counters: key → number
const counters = new Map<string, number>();

const mockDel = mock(async (...keys: string[]) => {
  let n = 0;
  for (const k of keys) {
    if (sortedSets.delete(k)) n++;
    if (counters.delete(k)) n++;
  }
  return n;
});

const mockDisconnect = mock(() => {});

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
            if (score >= min && score <= max) {
              set.delete(member);
              removed++;
            }
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
      case "pexpire": {
        results.push([null, 1]);
        break;
      }
      case "incr": {
        const [key] = args as [string];
        const val = (counters.get(key) ?? 0) + 1;
        counters.set(key, val);
        results.push([null, val]);
        break;
      }
      case "expire": {
        results.push([null, 1]);
        break;
      }
      default:
        results.push([null, null]);
    }
  }
  return results;
}

const mockPipeline = mock(() => {
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

mock.module("ioredis", () => ({
  Redis: class MockRedis {
    del = mockDel;
    disconnect = mockDisconnect;
    pipeline = mockPipeline;
  },
}));

mock.module("../../src/utils/logger.js", () => ({
  logger: { info: () => {}, warn: () => {}, error: () => {} },
}));

mock.module("dotenv/config", () => ({}));

// ── Import under test ───────────────────────────────────────────

const {
  SlidingWindowRateLimiter,
  FixedWindowRateLimiter,
  buildHeaders,
  rateLimitHook,
} = await import("../../src/rate-limiter.js");

// ── Helpers ─────────────────────────────────────────────────────

const originalDateNow = Date.now;

function setNow(ts: number) {
  Date.now = () => ts;
}

// ── Tests ───────────────────────────────────────────────────────

describe("buildHeaders", () => {
  test("formats RateLimitResult into header strings", () => {
    const headers = buildHeaders({
      allowed: true,
      limit: 100,
      remaining: 42,
      resetAt: 1700000000,
    });

    expect(headers).toEqual({
      "X-RateLimit-Limit": "100",
      "X-RateLimit-Remaining": "42",
      "X-RateLimit-Reset": "1700000000",
    });
  });
});

// ── Sliding Window ──────────────────────────────────────────────

describe("SlidingWindowRateLimiter", () => {
  let limiter: InstanceType<typeof SlidingWindowRateLimiter>;
  const CONFIG = { maxRequests: 3, windowSizeMs: 10_000 };

  beforeEach(() => {
    sortedSets.clear();
    counters.clear();
    mockDel.mockClear();
    mockDisconnect.mockClear();
    mockPipeline.mockClear();
    Date.now = originalDateNow;
    limiter = new SlidingWindowRateLimiter(CONFIG);
  });

  afterEach(() => {
    Date.now = originalDateNow;
  });

  test("allows requests under the limit", async () => {
    const result = await limiter.check("user-1");

    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(2);
    expect(result.limit).toBe(3);
  });

  test("remaining decreases with each request", async () => {
    const r1 = await limiter.check("user-1");
    const r2 = await limiter.check("user-1");
    const r3 = await limiter.check("user-1");

    expect(r1.remaining).toBe(2);
    expect(r2.remaining).toBe(1);
    expect(r3.remaining).toBe(0);
    expect(r3.allowed).toBe(true);
  });

  test("rejects requests over the limit with remaining 0", async () => {
    for (let i = 0; i < 3; i++) await limiter.check("user-1");

    const result = await limiter.check("user-1");

    expect(result.allowed).toBe(false);
    expect(result.remaining).toBe(0);
  });

  test("resetAt is correct", async () => {
    setNow(1_700_000_000_000);

    const result = await limiter.check("user-1");

    // resetAt = ceil((now + windowSizeMs) / 1000)
    expect(result.resetAt).toBe(Math.ceil((1_700_000_000_000 + 10_000) / 1000));
  });

  test("window expiry resets the counter", async () => {
    const baseTime = 1_700_000_000_000;
    setNow(baseTime);

    // Exhaust the limit
    for (let i = 0; i < 3; i++) await limiter.check("user-1");
    const rejected = await limiter.check("user-1");
    expect(rejected.allowed).toBe(false);

    // Advance past the window
    setNow(baseTime + CONFIG.windowSizeMs + 1);

    const afterExpiry = await limiter.check("user-1");
    expect(afterExpiry.allowed).toBe(true);
    expect(afterExpiry.remaining).toBe(2);
  });

  test("burst at window boundary — old entries expire, new ones count", async () => {
    const baseTime = 1_700_000_000_000;
    setNow(baseTime);

    // Use 2 of 3 slots
    await limiter.check("user-1");
    await limiter.check("user-1");

    // Move to just past the window so old entries are pruned
    setNow(baseTime + CONFIG.windowSizeMs + 1);

    // Burst of 4 requests — first 3 should be allowed
    const results = [];
    for (let i = 0; i < 4; i++) {
      results.push(await limiter.check("user-1"));
    }

    expect(results.filter((r) => r.allowed)).toHaveLength(3);
    expect(results[3].allowed).toBe(false);
  });

  test("different identifiers have independent limits", async () => {
    for (let i = 0; i < 3; i++) await limiter.check("user-a");
    const rejectedA = await limiter.check("user-a");

    const allowedB = await limiter.check("user-b");

    expect(rejectedA.allowed).toBe(false);
    expect(allowedB.allowed).toBe(true);
    expect(allowedB.remaining).toBe(2);
  });

  test("reset clears entries for identifier", async () => {
    for (let i = 0; i < 3; i++) await limiter.check("user-1");

    await limiter.reset("user-1");
    const result = await limiter.check("user-1");

    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(2);
    expect(mockDel).toHaveBeenCalledWith("ratelimit:sliding:user-1");
  });

  test("close disconnects Redis", async () => {
    await limiter.close();
    expect(mockDisconnect).toHaveBeenCalledTimes(1);
  });
});

// ── Fixed Window ────────────────────────────────────────────────

describe("FixedWindowRateLimiter", () => {
  let limiter: InstanceType<typeof FixedWindowRateLimiter>;
  const CONFIG = { maxRequests: 3, windowSizeMs: 10_000 };

  beforeEach(() => {
    sortedSets.clear();
    counters.clear();
    mockDel.mockClear();
    mockDisconnect.mockClear();
    mockPipeline.mockClear();
    Date.now = originalDateNow;
    limiter = new FixedWindowRateLimiter(CONFIG);
  });

  afterEach(() => {
    Date.now = originalDateNow;
  });

  test("allows requests under the limit", async () => {
    const result = await limiter.check("user-1");

    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(2);
    expect(result.limit).toBe(3);
  });

  test("remaining decreases with each request", async () => {
    const r1 = await limiter.check("user-1");
    const r2 = await limiter.check("user-1");
    const r3 = await limiter.check("user-1");

    expect(r1.remaining).toBe(2);
    expect(r2.remaining).toBe(1);
    expect(r3.remaining).toBe(0);
    expect(r3.allowed).toBe(true);
  });

  test("rejects requests over the limit with remaining 0", async () => {
    for (let i = 0; i < 3; i++) await limiter.check("user-1");

    const result = await limiter.check("user-1");

    expect(result.allowed).toBe(false);
    expect(result.remaining).toBe(0);
  });

  test("resetAt equals the end of the current fixed window", async () => {
    const windowSizeSec = Math.ceil(CONFIG.windowSizeMs / 1000); // 10
    const now = 1_700_000_000_000;
    setNow(now);

    const result = await limiter.check("user-1");

    const currentWindow = Math.floor(now / 1000 / windowSizeSec);
    const expectedResetAt = (currentWindow + 1) * windowSizeSec;
    expect(result.resetAt).toBe(expectedResetAt);
  });

  test("window expiry resets the counter", async () => {
    const windowSizeSec = Math.ceil(CONFIG.windowSizeMs / 1000); // 10

    // Pick a time at the start of a window
    const windowIndex = 170_000_000;
    const baseTime = windowIndex * windowSizeSec * 1000;
    setNow(baseTime);

    // Exhaust the limit
    for (let i = 0; i < 3; i++) await limiter.check("user-1");
    const rejected = await limiter.check("user-1");
    expect(rejected.allowed).toBe(false);

    // Advance to the next window
    setNow(baseTime + CONFIG.windowSizeMs);

    const afterExpiry = await limiter.check("user-1");
    expect(afterExpiry.allowed).toBe(true);
    expect(afterExpiry.remaining).toBe(2);
  });

  test("burst at window boundary — new window resets count", async () => {
    const windowSizeSec = Math.ceil(CONFIG.windowSizeMs / 1000);
    const windowIndex = 170_000_000;
    const baseTime = windowIndex * windowSizeSec * 1000;
    setNow(baseTime);

    // Use 2 of 3 slots in current window
    await limiter.check("user-1");
    await limiter.check("user-1");

    // Jump to next window
    setNow(baseTime + CONFIG.windowSizeMs);

    // All 3 slots available in new window, 4th should be rejected
    const results = [];
    for (let i = 0; i < 4; i++) {
      results.push(await limiter.check("user-1"));
    }

    expect(results.filter((r) => r.allowed)).toHaveLength(3);
    expect(results[3].allowed).toBe(false);
  });

  test("different identifiers have independent limits", async () => {
    for (let i = 0; i < 3; i++) await limiter.check("user-a");
    const rejectedA = await limiter.check("user-a");

    const allowedB = await limiter.check("user-b");

    expect(rejectedA.allowed).toBe(false);
    expect(allowedB.allowed).toBe(true);
    expect(allowedB.remaining).toBe(2);
  });

  test("reset clears counter for identifier", async () => {
    for (let i = 0; i < 3; i++) await limiter.check("user-1");

    await limiter.reset("user-1");
    const result = await limiter.check("user-1");

    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(2);
    expect(mockDel).toHaveBeenCalledTimes(1);
  });

  test("close disconnects Redis", async () => {
    await limiter.close();
    expect(mockDisconnect).toHaveBeenCalledTimes(1);
  });
});

// ── rateLimitHook ───────────────────────────────────────────────

describe("rateLimitHook", () => {
  const CONFIG = { maxRequests: 2, windowSizeMs: 10_000 };

  beforeEach(() => {
    sortedSets.clear();
    counters.clear();
    mockDel.mockClear();
    mockDisconnect.mockClear();
    mockPipeline.mockClear();
    Date.now = originalDateNow;
  });

  afterEach(() => {
    Date.now = originalDateNow;
  });

  function createMockReply() {
    const state = { statusCode: 200, headers: {} as Record<string, string>, body: null as unknown };
    const reply = {
      code(n: number) { state.statusCode = n; return reply; },
      headers(h: Record<string, string>) { state.headers = h; return reply; },
      send(payload: unknown) { state.body = payload; return reply; },
    };
    return { reply, state };
  }

  test("sets rate-limit headers on every response", async () => {
    const limiter = new FixedWindowRateLimiter(CONFIG);
    const hook = rateLimitHook(limiter);
    const req = { ip: "10.0.0.1", headers: {} };
    const { reply, state } = createMockReply();

    await hook(req, reply);

    expect(state.headers["X-RateLimit-Limit"]).toBe("2");
    expect(Number(state.headers["X-RateLimit-Remaining"])).toBeLessThanOrEqual(2);
    expect(state.headers["X-RateLimit-Reset"]).toBeDefined();
  });

  test("returns 429 when limit is exceeded", async () => {
    const limiter = new FixedWindowRateLimiter(CONFIG);
    const hook = rateLimitHook(limiter);
    const req = { ip: "10.0.0.1", headers: {} };

    // Exhaust limit
    for (let i = 0; i < 2; i++) {
      const { reply } = createMockReply();
      await hook(req, reply);
    }

    const { reply, state } = createMockReply();
    await hook(req, reply);

    expect(state.statusCode).toBe(429);
    expect((state.body as { error: string }).error).toBe("Too Many Requests");
    expect((state.body as { retryAfter: number }).retryAfter).toBeGreaterThan(0);
  });

  test("does not send 429 body when under limit", async () => {
    const limiter = new FixedWindowRateLimiter(CONFIG);
    const hook = rateLimitHook(limiter);
    const req = { ip: "10.0.0.1", headers: {} };
    const { reply, state } = createMockReply();

    await hook(req, reply);

    expect(state.statusCode).toBe(200);
    expect(state.body).toBeNull();
  });

  test("uses x-api-key header as identifier by default", async () => {
    const limiter = new FixedWindowRateLimiter({ maxRequests: 1, windowSizeMs: 10_000 });
    const hook = rateLimitHook(limiter);

    const reqA = { ip: "10.0.0.1", headers: { "x-api-key": "key-a" } };
    const reqB = { ip: "10.0.0.1", headers: { "x-api-key": "key-b" } };

    const { reply: replyA, state: stateA } = createMockReply();
    await hook(reqA, replyA);
    expect(stateA.statusCode).toBe(200);

    const { reply: replyB, state: stateB } = createMockReply();
    await hook(reqB, replyB);
    expect(stateB.statusCode).toBe(200);
  });

  test("falls back to IP when x-api-key is missing", async () => {
    const limiter = new FixedWindowRateLimiter({ maxRequests: 1, windowSizeMs: 10_000 });
    const hook = rateLimitHook(limiter);

    const req1 = { ip: "10.0.0.1", headers: {} };
    const { reply: r1 } = createMockReply();
    await hook(req1, r1);

    // Same IP → rejected
    const { reply: r2, state: s2 } = createMockReply();
    await hook(req1, r2);
    expect(s2.statusCode).toBe(429);

    // Different IP → allowed
    const req3 = { ip: "10.0.0.2", headers: {} };
    const { reply: r3, state: s3 } = createMockReply();
    await hook(req3, r3);
    expect(s3.statusCode).toBe(200);
  });

  test("accepts custom identifier function", async () => {
    const limiter = new FixedWindowRateLimiter({ maxRequests: 1, windowSizeMs: 10_000 });
    const hook = rateLimitHook(limiter, (req) => req.headers["x-tenant-id"] ?? "default");

    const reqA = { ip: "10.0.0.1", headers: { "x-tenant-id": "tenant-1" } };
    const reqB = { ip: "10.0.0.1", headers: { "x-tenant-id": "tenant-2" } };

    const { reply: rA, state: sA } = createMockReply();
    await hook(reqA, rA);
    expect(sA.statusCode).toBe(200);

    const { reply: rB, state: sB } = createMockReply();
    await hook(reqB, rB);
    expect(sB.statusCode).toBe(200);
  });

  test("works with SlidingWindowRateLimiter", async () => {
    const limiter = new SlidingWindowRateLimiter({ maxRequests: 1, windowSizeMs: 10_000 });
    const hook = rateLimitHook(limiter);
    const req = { ip: "10.0.0.1", headers: {} };

    const { reply: r1, state: s1 } = createMockReply();
    await hook(req, r1);
    expect(s1.statusCode).toBe(200);

    const { reply: r2, state: s2 } = createMockReply();
    await hook(req, r2);
    expect(s2.statusCode).toBe(429);
  });
});

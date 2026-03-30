import "dotenv/config";

import { SlidingWindowRateLimiter, FixedWindowRateLimiter, buildHeaders } from "../src/rate-limiter.js";
import { logger } from "../src/utils/logger.js";

// ── Config ─────────────────────────────────────────────────────────

const LIMIT = 10;
const WINDOW_MS = 1_000;
const TOTAL_REQUESTS = 100;
const TEST_IDENTIFIER = "benchmark-client";

// ── Benchmark runner ──────────────────────────────────────────────

type LimiterUnion = SlidingWindowRateLimiter | FixedWindowRateLimiter;

async function benchmark(name: string, limiter: LimiterUnion): Promise<{ allowed: number; rejected: number; durationMs: number }> {
  // Reset any leftover state
  await limiter.reset(TEST_IDENTIFIER);

  let allowed = 0;
  let rejected = 0;

  const start = performance.now();

  // Fire all requests as fast as possible (burst)
  const results = await Promise.all(
    Array.from({ length: TOTAL_REQUESTS }, () => limiter.check(TEST_IDENTIFIER)),
  );

  const durationMs = Math.round(performance.now() - start);

  for (const result of results) {
    if (result.allowed) {
      allowed++;
    } else {
      rejected++;
    }
  }

  // Show headers from the last request
  const lastHeaders = buildHeaders(results[results.length - 1]);

  logger.info(`[${name}] Results`, {
    total: TOTAL_REQUESTS,
    allowed,
    rejected,
    expectedRejections: TOTAL_REQUESTS - LIMIT,
    accurate: rejected === TOTAL_REQUESTS - LIMIT,
    durationMs,
    lastHeaders,
  });

  return { allowed, rejected, durationMs };
}

// ── Main ──────────────────────────────────────────────────────────

async function main() {
  const config = { maxRequests: LIMIT, windowSizeMs: WINDOW_MS };

  logger.info("Rate limiter benchmark", { limit: LIMIT, windowMs: WINDOW_MS, totalRequests: TOTAL_REQUESTS });
  console.log("─".repeat(72));

  // ── Sliding window ──────────────────────────────────────────────

  const sliding = new SlidingWindowRateLimiter(config);
  const slidingResult = await benchmark("Sliding Window", sliding);
  await sliding.close();

  console.log("─".repeat(72));

  // ── Fixed window ────────────────────────────────────────────────

  const fixed = new FixedWindowRateLimiter(config);
  const fixedResult = await benchmark("Fixed Window", fixed);
  await fixed.close();

  console.log("─".repeat(72));

  // ── Comparison ──────────────────────────────────────────────────

  console.log("\n  Comparison:");
  console.log(`    Sliding window: ${slidingResult.rejected}/${TOTAL_REQUESTS - LIMIT} correct rejections (${slidingResult.durationMs}ms)`);
  console.log(`    Fixed window:   ${fixedResult.rejected}/${TOTAL_REQUESTS - LIMIT} correct rejections (${fixedResult.durationMs}ms)`);

  const slidingAccurate = slidingResult.rejected === TOTAL_REQUESTS - LIMIT;
  const fixedAccurate = fixedResult.rejected === TOTAL_REQUESTS - LIMIT;

  if (slidingAccurate && !fixedAccurate) {
    console.log("\n  → Sliding window is more accurate under burst.");
  } else if (!slidingAccurate && fixedAccurate) {
    console.log("\n  → Fixed window is more accurate under burst.");
  } else if (slidingAccurate && fixedAccurate) {
    console.log("\n  → Both algorithms are equally accurate.");
    if (slidingResult.durationMs < fixedResult.durationMs) {
      console.log("  → Sliding window is faster.");
    } else if (fixedResult.durationMs < slidingResult.durationMs) {
      console.log("  → Fixed window is faster.");
    }
  } else {
    console.log("\n  → Neither algorithm achieved exact rejection count.");
  }

  console.log();
}

main().catch(console.error);

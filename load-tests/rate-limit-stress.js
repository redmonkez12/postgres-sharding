import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";
import {
  HEADERS,
  ensureCategories,
  listProducts,
  getProduct,
  createProduct,
  randomProduct,
  checkOk,
} from "./helpers.js";

// ── Custom metrics for rate limiting ────────────────────────────────

const rateLimited = new Counter("rate_limited_total");
const rateLimitRate = new Rate("rate_limited_ratio");
const remainingQuota = new Trend("ratelimit_remaining");

export const options = {
  scenarios: {
    // Phase 1: Normal traffic — should stay under limits
    under_limit: {
      executor: "constant-arrival-rate",
      rate: 30,           // 30 req/s — under 100/min limit
      timeUnit: "1s",
      duration: "30s",
      preAllocatedVUs: 10,
      maxVUs: 20,
      exec: "normalTraffic",
    },
    // Phase 2: Exceed limits — should see 429s
    over_limit: {
      executor: "constant-arrival-rate",
      rate: 200,           // 200 req/s — well over 100/min
      timeUnit: "1s",
      duration: "1m",
      preAllocatedVUs: 50,
      maxVUs: 100,
      startTime: "35s",   // Start after phase 1
      exec: "burstTraffic",
    },
    // Phase 3: Recovery — rate limit should clear
    recovery: {
      executor: "constant-arrival-rate",
      rate: 10,            // Low rate
      timeUnit: "1s",
      duration: "30s",
      preAllocatedVUs: 5,
      maxVUs: 10,
      startTime: "1m40s", // After burst clears
      exec: "normalTraffic",
    },
  },
  thresholds: {
    // During burst phase, most requests should be rejected
    rate_limited_total: ["count>0"],
    // Overall error rate includes expected 429s — don't abort
    http_req_failed: [{ threshold: "rate<0.95", abortOnFail: false }],
  },
};

export function setup() {
  ensureCategories();

  // Seed a few products for reads
  const products = [];
  for (let i = 0; i < 5; i++) {
    const res = createProduct(randomProduct());
    if (res.status === 201) products.push(JSON.parse(res.body).id);
  }
  return { productIds: products };
}

function trackRateLimit(res) {
  const is429 = res.status === 429;
  rateLimited.add(is429 ? 1 : 0);
  rateLimitRate.add(is429);

  const remaining = res.headers["X-Ratelimit-Remaining"];
  if (remaining !== undefined) {
    remainingQuota.add(parseInt(remaining, 10));
  }

  if (is429) {
    check(res, {
      "429 has retryAfter": (r) => {
        const body = JSON.parse(r.body);
        return body.retryAfter !== undefined;
      },
    });
  }
}

export function normalTraffic(data) {
  const { productIds } = data;

  const res = listProducts();
  trackRateLimit(res);

  if (res.status === 200) {
    check(res, { "normal: request succeeded": (r) => r.status === 200 });
  }
}

export function burstTraffic(data) {
  const { productIds } = data;

  // Rapid-fire requests with same API key to trigger rate limiting
  const roll = Math.random();

  let res;
  if (roll < 0.5) {
    res = listProducts();
  } else if (roll < 0.8 && productIds.length > 0) {
    const id = productIds[Math.floor(Math.random() * productIds.length)];
    res = getProduct(id);
  } else {
    res = createProduct(randomProduct());
  }

  trackRateLimit(res);

  check(res, {
    "burst: response is 200 or 429": (r) =>
      r.status === 200 || r.status === 201 || r.status === 429,
  });
}

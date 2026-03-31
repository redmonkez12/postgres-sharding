import { check } from "k6";
import http from "k6/http";

export const BASE_URL = __ENV.BASE_URL || "http://localhost:3000";

export const HEADERS = {
  "Content-Type": "application/json",
  "x-api-key": __ENV.API_KEY || "load-test-key",
};

// ── Random data generators ──────────────────────────────────────────

let counter = 0;

function uid() {
  const vu = typeof __VU !== "undefined" ? __VU : 0;
  const iter = typeof __ITER !== "undefined" ? __ITER : 0;
  return `${vu}-${iter}-${++counter}-${Date.now()}`;
}

export function randomCustomer() {
  const id = uid();
  return {
    email: `loadtest+${id}@example.com`,
    name: `Load User ${id}`,
    region: randomRegion(),
  };
}

export function randomProduct() {
  const id = uid();
  return {
    name: `Product ${id}`,
    sku: `SKU-${id}`,
    price_cents: Math.floor(Math.random() * 50000) + 100,
    category_id: Math.floor(Math.random() * 5) + 1,
    stock_qty: Math.floor(Math.random() * 1000) + 1,
  };
}

export function randomRegion() {
  const regions = ["EU", "US", "AP"];
  return regions[Math.floor(Math.random() * regions.length)];
}

export function randomOrderFor(customerId, productIds) {
  const numItems = Math.floor(Math.random() * 3) + 1;
  const items = [];
  for (let i = 0; i < numItems; i++) {
    const pid = productIds[Math.floor(Math.random() * productIds.length)];
    items.push({
      product_id: pid,
      quantity: Math.floor(Math.random() * 5) + 1,
      unit_price_cents: Math.floor(Math.random() * 10000) + 100,
    });
  }
  return {
    customer_id: customerId,
    region: randomRegion(),
    items,
  };
}

// ── Category seeding ───────────────────────────────────────────────

export function ensureCategories() {
  const names = ["Electronics", "Books", "Clothing", "Home", "Sports"];
  for (const name of names) {
    http.post(`${BASE_URL}/categories`, JSON.stringify({ name }), {
      headers: HEADERS,
    });
  }
}

// ── API helpers ─────────────────────────────────────────────────────

export function getHealth() {
  return http.get(`${BASE_URL}/health`);
}

export function listProducts() {
  return http.get(`${BASE_URL}/products`, { headers: HEADERS });
}

export function getProduct(id) {
  return http.get(`${BASE_URL}/products/${id}`, { headers: HEADERS });
}

export function createProduct(data) {
  return http.post(`${BASE_URL}/products`, JSON.stringify(data), {
    headers: HEADERS,
  });
}

export function updateProduct(id, data) {
  return http.patch(`${BASE_URL}/products/${id}`, JSON.stringify(data), {
    headers: HEADERS,
  });
}

export function deleteProduct(id) {
  return http.del(`${BASE_URL}/products/${id}`, null, { headers: HEADERS });
}

export function listCustomers() {
  return http.get(`${BASE_URL}/customers`, { headers: HEADERS });
}

export function getCustomer(id) {
  return http.get(`${BASE_URL}/customers/${id}`, { headers: HEADERS });
}

export function createCustomer(data) {
  return http.post(`${BASE_URL}/customers`, JSON.stringify(data), {
    headers: HEADERS,
  });
}

export function listOrders() {
  return http.get(`${BASE_URL}/orders`, { headers: HEADERS });
}

export function getOrder(id) {
  return http.get(`${BASE_URL}/orders/${id}`, { headers: HEADERS });
}

export function createOrder(data) {
  return http.post(`${BASE_URL}/orders`, JSON.stringify(data), {
    headers: HEADERS,
  });
}

// ── Checks ──────────────────────────────────────────────────────────

export function checkStatus(res, expected, label) {
  check(res, {
    [`${label} status is ${expected}`]: (r) => r.status === expected,
  });
}

export function checkOk(res, label) {
  checkStatus(res, 200, label);
}

// ── Shared thresholds ───────────────────────────────────────────────

export const defaultThresholds = {
  http_req_failed: [{ threshold: "rate<0.05", abortOnFail: false }],
  http_req_duration: ["p(50)<300", "p(95)<800", "p(99)<1500"],
};

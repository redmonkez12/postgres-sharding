import { sleep } from "k6";
import {
  defaultThresholds,
  ensureCategories,
  randomCustomer,
  randomProduct,
  listProducts,
  getProduct,
  createProduct,
  listCustomers,
  getCustomer,
  createCustomer,
  listOrders,
  getOrder,
  checkStatus,
  checkOk,
} from "./helpers.js";

export const options = {
  scenarios: {
    read_heavy: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "30s", target: 50 },
        { duration: "2m", target: 50 },
        { duration: "30s", target: 0 },
      ],
      gracefulRampDown: "10s",
    },
  },
  thresholds: {
    ...defaultThresholds,
    // Reads should be fast — tighter p95 target
    http_req_duration: ["p(50)<200", "p(95)<500", "p(99)<1000"],
  },
};

export function setup() {
  ensureCategories();

  // Pre-seed data so reads always have something to hit
  const customers = [];
  const products = [];

  for (let i = 0; i < 20; i++) {
    const cRes = createCustomer(randomCustomer());
    if (cRes.status === 201) customers.push(JSON.parse(cRes.body).id);

    const pRes = createProduct(randomProduct());
    if (pRes.status === 201) products.push(JSON.parse(pRes.body).id);
  }

  return { customerIds: customers, productIds: products };
}

export default function (data) {
  const { customerIds, productIds } = data;

  // 90% reads, 10% writes
  const roll = Math.random();

  if (roll < 0.30) {
    // List products (cache-friendly, should show improving hit rates)
    const res = listProducts();
    checkOk(res, "list products");
  } else if (roll < 0.50) {
    // Get single product (tests cache per-item lookups)
    if (productIds.length > 0) {
      const id = productIds[Math.floor(Math.random() * productIds.length)];
      const res = getProduct(id);
      checkOk(res, "get product");
    }
  } else if (roll < 0.65) {
    // List customers (replica routing)
    const res = listCustomers();
    checkOk(res, "list customers");
  } else if (roll < 0.80) {
    // Get single customer
    if (customerIds.length > 0) {
      const id = customerIds[Math.floor(Math.random() * customerIds.length)];
      const res = getCustomer(id);
      checkOk(res, "get customer");
    }
  } else if (roll < 0.90) {
    // List orders
    const res = listOrders();
    checkOk(res, "list orders");
  } else if (roll < 0.95) {
    // Write: create product (triggers cache invalidation)
    const res = createProduct(randomProduct());
    checkStatus(res, 201, "create product");
    if (res.status === 201) productIds.push(JSON.parse(res.body).id);
  } else {
    // Write: create customer
    const res = createCustomer(randomCustomer());
    checkStatus(res, 201, "create customer");
    if (res.status === 201) customerIds.push(JSON.parse(res.body).id);
  }

  sleep(0.1 + Math.random() * 0.3); // Faster think time for read-heavy
}

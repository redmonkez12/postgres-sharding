import { sleep } from "k6";
import {
  defaultThresholds,
  ensureCategories,
  randomCustomer,
  randomProduct,
  randomOrderFor,
  createProduct,
  updateProduct,
  deleteProduct,
  createCustomer,
  createOrder,
  listProducts,
  checkStatus,
  checkOk,
} from "./helpers.js";

export const options = {
  scenarios: {
    write_burst: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "20s", target: 30 },
        { duration: "40s", target: 60 },  // Burst phase
        { duration: "1m", target: 60 },
        { duration: "30s", target: 0 },
      ],
      gracefulRampDown: "10s",
    },
  },
  thresholds: {
    ...defaultThresholds,
    // Writes are slower — relax thresholds
    http_req_duration: ["p(50)<500", "p(95)<1500", "p(99)<3000"],
  },
};

export function setup() {
  ensureCategories();

  const customers = [];
  const products = [];

  for (let i = 0; i < 15; i++) {
    const cRes = createCustomer(randomCustomer());
    if (cRes.status === 201) customers.push(JSON.parse(cRes.body).id);

    const pRes = createProduct(randomProduct());
    if (pRes.status === 201) products.push(JSON.parse(pRes.body).id);
  }

  return { customerIds: customers, productIds: products };
}

export default function (data) {
  const { customerIds, productIds } = data;

  // ~85% writes, ~15% reads (to observe replication lag effects)
  const roll = Math.random();

  if (roll < 0.30) {
    // Create product
    const res = createProduct(randomProduct());
    checkStatus(res, 201, "create product");
    if (res.status === 201) productIds.push(JSON.parse(res.body).id);
  } else if (roll < 0.45) {
    // Update product
    if (productIds.length > 0) {
      const id = productIds[Math.floor(Math.random() * productIds.length)];
      const res = updateProduct(id, {
        price_cents: Math.floor(Math.random() * 50000) + 100,
        stock_qty: Math.floor(Math.random() * 500),
      });
      checkOk(res, "update product");
    }
  } else if (roll < 0.55) {
    // Create customer
    const res = createCustomer(randomCustomer());
    checkStatus(res, 201, "create customer");
    if (res.status === 201) customerIds.push(JSON.parse(res.body).id);
  } else if (roll < 0.75) {
    // Create order (transactional write — heaviest operation)
    if (customerIds.length > 0 && productIds.length > 0) {
      const cid = customerIds[Math.floor(Math.random() * customerIds.length)];
      const res = createOrder(randomOrderFor(cid, productIds));
      checkStatus(res, 201, "create order");
    }
  } else if (roll < 0.85) {
    // Delete product (then remove from pool)
    if (productIds.length > 5) {
      const idx = Math.floor(Math.random() * productIds.length);
      const id = productIds[idx];
      const res = deleteProduct(id);
      checkStatus(res, 204, "delete product");
      if (res.status === 204) productIds.splice(idx, 1);
    }
  } else {
    // Read: list products (observe replication lag — stale reads after writes)
    const res = listProducts();
    checkOk(res, "list products (lag check)");
  }

  sleep(0.1 + Math.random() * 0.2); // Minimal think time for write pressure
}

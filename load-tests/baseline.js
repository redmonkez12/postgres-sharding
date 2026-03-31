import { sleep } from "k6";
import {
  BASE_URL,
  HEADERS,
  defaultThresholds,
  ensureCategories,
  randomCustomer,
  randomProduct,
  randomOrderFor,
  listProducts,
  getProduct,
  createProduct,
  listCustomers,
  getCustomer,
  createCustomer,
  listOrders,
  getOrder,
  createOrder,
  checkStatus,
  checkOk,
} from "./helpers.js";

export const options = {
  scenarios: {
    baseline: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "1m", target: 50 },
        { duration: "2m", target: 50 },
        { duration: "30s", target: 0 },
      ],
      gracefulRampDown: "10s",
    },
  },
  thresholds: defaultThresholds,
};

// Collect IDs created during setup for use in the test
const state = {
  customerIds: [],
  productIds: [],
  orderIds: [],
};

export function setup() {
  // Seed categories so product inserts satisfy the foreign key
  ensureCategories();

  // Seed a few records so reads have data to hit
  const customers = [];
  const products = [];

  for (let i = 0; i < 10; i++) {
    const cRes = createCustomer(randomCustomer());
    if (cRes.status === 201) {
      customers.push(JSON.parse(cRes.body).id);
    }

    const pRes = createProduct(randomProduct());
    if (pRes.status === 201) {
      products.push(JSON.parse(pRes.body).id);
    }
  }

  return { customerIds: customers, productIds: products };
}

export default function (data) {
  const { customerIds, productIds } = data;

  // Weighted random: ~60% reads, ~40% writes
  const roll = Math.random();

  if (roll < 0.2) {
    // List products
    const res = listProducts();
    checkOk(res, "list products");
  } else if (roll < 0.35) {
    // Get single product
    if (productIds.length > 0) {
      const id = productIds[Math.floor(Math.random() * productIds.length)];
      const res = getProduct(id);
      checkOk(res, "get product");
    }
  } else if (roll < 0.45) {
    // List customers
    const res = listCustomers();
    checkOk(res, "list customers");
  } else if (roll < 0.55) {
    // Get single customer
    if (customerIds.length > 0) {
      const id = customerIds[Math.floor(Math.random() * customerIds.length)];
      const res = getCustomer(id);
      checkOk(res, "get customer");
    }
  } else if (roll < 0.6) {
    // List orders
    const res = listOrders();
    checkOk(res, "list orders");
  } else if (roll < 0.75) {
    // Create product
    const res = createProduct(randomProduct());
    checkStatus(res, 201, "create product");
    if (res.status === 201) {
      productIds.push(JSON.parse(res.body).id);
    }
  } else if (roll < 0.85) {
    // Create customer
    const res = createCustomer(randomCustomer());
    checkStatus(res, 201, "create customer");
    if (res.status === 201) {
      customerIds.push(JSON.parse(res.body).id);
    }
  } else {
    // Create order
    if (customerIds.length > 0 && productIds.length > 0) {
      const cid = customerIds[Math.floor(Math.random() * customerIds.length)];
      const res = createOrder(randomOrderFor(cid, productIds));
      checkStatus(res, 201, "create order");
    }
  }

  sleep(0.3 + Math.random() * 0.7); // 300-1000ms think time
}

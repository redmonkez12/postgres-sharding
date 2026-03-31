import type { TestClient } from "./db.js";
import {
  buildCategory,
  buildCustomer,
  buildOrder,
  buildOrderItem,
  buildProduct,
  type CategoryFixture,
  type CustomerFixture,
  type OrderFixture,
  type OrderItemFixture,
  type ProductFixture,
} from "./fixtures.js";

// ── Inserters ────────────────────────────────────────────────────

export async function insertCategory(client: TestClient, data: CategoryFixture): Promise<{ id: number }> {
  const { rows } = await client.query<{ id: number }>(
    "INSERT INTO categories (name) VALUES ($1) RETURNING id",
    [data.name],
  );
  return rows[0];
}

export async function insertCustomer(client: TestClient, data: CustomerFixture): Promise<CustomerFixture> {
  const { rows } = await client.query(
    `INSERT INTO customers (id, email, name, region, created_at)
     VALUES ($1, $2, $3, $4, $5) RETURNING *`,
    [data.id, data.email, data.name, data.region, data.created_at],
  );
  return rows[0] as CustomerFixture;
}

export async function insertProduct(client: TestClient, data: ProductFixture): Promise<ProductFixture> {
  const { rows } = await client.query(
    `INSERT INTO products (id, name, sku, price_cents, category_id, stock_qty, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
    [data.id, data.name, data.sku, data.price_cents, data.category_id, data.stock_qty, data.created_at],
  );
  return rows[0] as ProductFixture;
}

export async function insertOrder(client: TestClient, data: OrderFixture): Promise<OrderFixture> {
  const { rows } = await client.query(
    `INSERT INTO orders (id, customer_id, status, total_cents, region, created_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
    [data.id, data.customer_id, data.status, data.total_cents, data.region, data.created_at, data.updated_at],
  );
  return rows[0] as OrderFixture;
}

export async function insertOrderItem(client: TestClient, data: OrderItemFixture): Promise<OrderItemFixture> {
  const { rows } = await client.query(
    `INSERT INTO order_items (id, order_id, product_id, quantity, unit_price_cents)
     VALUES ($1, $2, $3, $4, $5) RETURNING *`,
    [data.id, data.order_id, data.product_id, data.quantity, data.unit_price_cents],
  );
  return rows[0] as OrderItemFixture;
}

// ── Lightweight seed ─────────────────────────────────────────────

export type TestSeedResult = {
  categories: { id: number; name: string }[];
  customers: CustomerFixture[];
  products: ProductFixture[];
  orders: OrderFixture[];
  orderItems: OrderItemFixture[];
};

/**
 * Seeds a minimal dataset into the database via the provided client.
 * Designed to run inside a transaction so it can be rolled back.
 *
 * Creates: 2 categories, 3 products, 2 customers, 2 orders, 3 order items.
 */
export async function seedMinimal(client: TestClient): Promise<TestSeedResult> {
  // Categories
  const cat1 = await insertCategory(client, buildCategory({ name: "Electronics" }));
  const cat2 = await insertCategory(client, buildCategory({ name: "Books" }));

  // Products
  const p1 = await insertProduct(client, buildProduct({ category_id: cat1.id, sku: "ELC-00001", price_cents: 9999 }));
  const p2 = await insertProduct(client, buildProduct({ category_id: cat1.id, sku: "ELC-00002", price_cents: 4999 }));
  const p3 = await insertProduct(client, buildProduct({ category_id: cat2.id, sku: "BOK-00001", price_cents: 1599 }));

  // Customers
  const c1 = await insertCustomer(client, buildCustomer({ region: "eu" }));
  const c2 = await insertCustomer(client, buildCustomer({ region: "us" }));

  // Orders
  const o1 = await insertOrder(client, buildOrder({
    customer_id: c1.id,
    region: c1.region,
    total_cents: p1.price_cents * 2,
    status: "pending",
  }));
  const o2 = await insertOrder(client, buildOrder({
    customer_id: c2.id,
    region: c2.region,
    total_cents: p2.price_cents + p3.price_cents,
    status: "delivered",
  }));

  // Order items
  const oi1 = await insertOrderItem(client, buildOrderItem({
    order_id: o1.id,
    product_id: p1.id,
    quantity: 2,
    unit_price_cents: p1.price_cents,
  }));
  const oi2 = await insertOrderItem(client, buildOrderItem({
    order_id: o2.id,
    product_id: p2.id,
    quantity: 1,
    unit_price_cents: p2.price_cents,
  }));
  const oi3 = await insertOrderItem(client, buildOrderItem({
    order_id: o2.id,
    product_id: p3.id,
    quantity: 1,
    unit_price_cents: p3.price_cents,
  }));

  return {
    categories: [
      { id: cat1.id, name: "Electronics" },
      { id: cat2.id, name: "Books" },
    ],
    customers: [c1, c2],
    products: [p1, p2, p3],
    orders: [o1, o2],
    orderItems: [oi1, oi2, oi3],
  };
}

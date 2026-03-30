import { readFile } from "node:fs/promises";

import { faker } from "@faker-js/faker";
import type { PoolClient } from "pg";

import { closePool, withClient } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

const BASE_ORDER_COUNT = 100_000;
const BASE_PRODUCT_COUNT = 500;
const BASE_CUSTOMER_COUNT = 1_000;
const CATEGORY_NAMES = [
  "Electronics",
  "Home",
  "Garden",
  "Fitness",
  "Books",
  "Beauty",
  "Toys",
  "Pets",
  "Office",
  "Kitchen",
] as const;
const REGIONS = [
  { value: "eu", weight: 35 },
  { value: "us", weight: 45 },
  { value: "ap", weight: 20 },
] as const;
const ORDER_STATUSES = [
  { value: "pending", weight: 8 },
  { value: "processing", weight: 12 },
  { value: "shipped", weight: 17 },
  { value: "delivered", weight: 55 },
  { value: "canceled", weight: 5 },
  { value: "returned", weight: 3 },
] as const;
const ITEM_COUNT_DISTRIBUTION = [
  { value: 1, weight: 10 },
  { value: 2, weight: 20 },
  { value: 3, weight: 40 },
  { value: 4, weight: 20 },
  { value: 5, weight: 10 },
] as const;
const PRODUCT_BATCH_SIZE = 500;
const CUSTOMER_BATCH_SIZE = 1_000;
const ORDER_BATCH_SIZE = 2_000;
const ORDER_ITEM_BATCH_SIZE = 5_000;

type WeightedValue<T> = {
  value: T;
  weight: number;
};

type CategoryRow = {
  name: string;
};

type ProductRow = {
  id: string;
  name: string;
  sku: string;
  priceCents: number;
  categoryId: number;
  stockQty: number;
  createdAt: Date;
};

type CustomerRow = {
  id: string;
  email: string;
  name: string;
  region: string;
  createdAt: Date;
};

type OrderRow = {
  id: string;
  customerId: string;
  status: string;
  totalCents: number;
  region: string;
  createdAt: Date;
  updatedAt: Date;
};

type OrderItemRow = {
  id: string;
  orderId: string;
  productId: string;
  quantity: number;
  unitPriceCents: number;
};

type SeedConfig = {
  categoryCount: number;
  productCount: number;
  customerCount: number;
  orderCount: number;
};

function parseCountArg(argv: string[]): number {
  const countFlag = argv.find((arg) => arg.startsWith("--count="));
  const countValue = countFlag ? countFlag.split("=")[1] : undefined;
  const indexFlag = argv.findIndex((arg) => arg === "--count");
  const positionalValue = indexFlag >= 0 ? argv[indexFlag + 1] : undefined;
  const rawValue = countValue ?? positionalValue ?? String(BASE_ORDER_COUNT);
  const parsed = Number(rawValue);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid --count value: ${rawValue}`);
  }

  return parsed;
}

function getSeedConfig(orderCount: number): SeedConfig {
  const scale = orderCount / BASE_ORDER_COUNT;

  return {
    categoryCount: CATEGORY_NAMES.length,
    productCount: Math.max(50, Math.round(BASE_PRODUCT_COUNT * scale)),
    customerCount: Math.max(100, Math.round(BASE_CUSTOMER_COUNT * scale)),
    orderCount,
  };
}

function weightedPick<T>(values: readonly WeightedValue<T>[]): T {
  const totalWeight = values.reduce((sum, entry) => sum + entry.weight, 0);
  let cursor = Math.random() * totalWeight;

  for (const entry of values) {
    cursor -= entry.weight;
    if (cursor <= 0) {
      return entry.value;
    }
  }

  return values[values.length - 1].value;
}

function randomDateBetween(start: Date, end: Date): Date {
  const startMs = start.getTime();
  const endMs = end.getTime();
  return new Date(faker.number.int({ min: startMs, max: endMs }));
}

function addHours(date: Date, minHours: number, maxHours: number): Date {
  const addedMs = faker.number.int({ min: minHours, max: maxHours }) * 60 * 60 * 1000;
  const nextDate = new Date(date.getTime() + addedMs);
  const now = Date.now();
  return nextDate.getTime() > now ? new Date(now) : nextDate;
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ".")
    .replace(/^\.+|\.+$/g, "");
}

function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];

  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }

  return chunks;
}

async function insertRows(
  client: PoolClient,
  tableName: string,
  columns: string[],
  rows: unknown[][],
): Promise<void> {
  if (rows.length === 0) {
    return;
  }

  const values = rows.flat();
  const placeholders = rows
    .map((row, rowIndex) => {
      const base = rowIndex * row.length;
      const columnsPlaceholders = row.map((_, columnIndex) => `$${base + columnIndex + 1}`).join(", ");
      return `(${columnsPlaceholders})`;
    })
    .join(", ");

  await client.query(`INSERT INTO ${tableName} (${columns.join(", ")}) VALUES ${placeholders}`, values);
}

function buildCategories(): CategoryRow[] {
  return CATEGORY_NAMES.map((name) => ({ name }));
}

function buildProducts(config: SeedConfig): ProductRow[] {
  const products: ProductRow[] = [];
  const categoryCodes = CATEGORY_NAMES.map((name) => name.slice(0, 3).toUpperCase());
  const now = new Date();
  const oldestDate = new Date(now);
  oldestDate.setFullYear(oldestDate.getFullYear() - 2);

  for (let index = 0; index < config.productCount; index += 1) {
    const categoryId = (index % config.categoryCount) + 1;
    const categoryCode = categoryCodes[categoryId - 1];
    const productName = faker.commerce.productName();

    products.push({
      id: faker.string.uuid(),
      name: productName,
      sku: `${categoryCode}-${String(index + 1).padStart(5, "0")}`,
      priceCents: faker.number.int({ min: 799, max: 49_999 }),
      categoryId,
      stockQty: faker.number.int({ min: 10, max: 500 }),
      createdAt: randomDateBetween(oldestDate, now),
    });
  }

  return products;
}

function buildCustomers(config: SeedConfig): CustomerRow[] {
  const customers: CustomerRow[] = [];
  const now = new Date();
  const oldestDate = new Date(now);
  oldestDate.setFullYear(oldestDate.getFullYear() - 3);

  for (let index = 0; index < config.customerCount; index += 1) {
    const firstName = faker.person.firstName();
    const lastName = faker.person.lastName();
    const emailLocalPart = `${slugify(firstName)}.${slugify(lastName)}.${index + 1}`;

    customers.push({
      id: faker.string.uuid(),
      email: `${emailLocalPart}@example.test`,
      name: `${firstName} ${lastName}`,
      region: weightedPick(REGIONS),
      createdAt: randomDateBetween(oldestDate, now),
    });
  }

  return customers;
}

function getOrderTimeline(status: string): { createdAt: Date; updatedAt: Date } {
  const now = new Date();
  const oneYearAgo = new Date(now);
  oneYearAgo.setMonth(oneYearAgo.getMonth() - 12);

  switch (status) {
    case "pending": {
      const recentStart = new Date(now.getTime() - 14 * 24 * 60 * 60 * 1000);
      const createdAt = randomDateBetween(recentStart, now);
      return { createdAt, updatedAt: addHours(createdAt, 1, 48) };
    }
    case "processing": {
      const recentStart = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
      const createdAt = randomDateBetween(recentStart, now);
      return { createdAt, updatedAt: addHours(createdAt, 4, 96) };
    }
    case "shipped": {
      const shippedStart = new Date(now.getTime() - 60 * 24 * 60 * 60 * 1000);
      const createdAt = randomDateBetween(shippedStart, now);
      return { createdAt, updatedAt: addHours(createdAt, 24, 168) };
    }
    case "canceled": {
      const createdAt = randomDateBetween(oneYearAgo, now);
      return { createdAt, updatedAt: addHours(createdAt, 1, 72) };
    }
    case "returned": {
      const createdAt = randomDateBetween(oneYearAgo, now);
      return { createdAt, updatedAt: addHours(createdAt, 120, 720) };
    }
    case "delivered":
    default: {
      const createdAt = randomDateBetween(oneYearAgo, now);
      return { createdAt, updatedAt: addHours(createdAt, 48, 336) };
    }
  }
}

function buildOrderBatch(
  customers: CustomerRow[],
  products: ProductRow[],
  count: number,
): { orders: OrderRow[]; orderItems: OrderItemRow[] } {
  const orders: OrderRow[] = [];
  const orderItems: OrderItemRow[] = [];

  for (let index = 0; index < count; index += 1) {
    const customer = customers[faker.number.int({ min: 0, max: customers.length - 1 })];
    const status = weightedPick(ORDER_STATUSES);
    const { createdAt, updatedAt } = getOrderTimeline(status);
    const orderId = faker.string.uuid();
    const itemCount = weightedPick(ITEM_COUNT_DISTRIBUTION);
    const usedProductIds = new Set<string>();
    let totalCents = 0;

    for (let itemIndex = 0; itemIndex < itemCount; itemIndex += 1) {
      let product = products[faker.number.int({ min: 0, max: products.length - 1 })];

      while (usedProductIds.has(product.id)) {
        product = products[faker.number.int({ min: 0, max: products.length - 1 })];
      }

      usedProductIds.add(product.id);

      const quantity = faker.number.int({ min: 1, max: 4 });
      totalCents += product.priceCents * quantity;
      orderItems.push({
        id: faker.string.uuid(),
        orderId,
        productId: product.id,
        quantity,
        unitPriceCents: product.priceCents,
      });
    }

    orders.push({
      id: orderId,
      customerId: customer.id,
      status,
      totalCents,
      region: customer.region,
      createdAt,
      updatedAt,
    });
  }

  return { orders, orderItems };
}

async function seedCategories(client: PoolClient, categories: CategoryRow[]): Promise<void> {
  const categoryRows = categories.map((category) => [category.name]);
  await insertRows(client, "categories", ["name"], categoryRows);
}

async function seedProducts(client: PoolClient, products: ProductRow[]): Promise<void> {
  const columns = ["id", "name", "sku", "price_cents", "category_id", "stock_qty", "created_at"];

  for (const batch of chunk(products, PRODUCT_BATCH_SIZE)) {
    await insertRows(
      client,
      "products",
      columns,
      batch.map((product) => [
        product.id,
        product.name,
        product.sku,
        product.priceCents,
        product.categoryId,
        product.stockQty,
        product.createdAt,
      ]),
    );
  }
}

async function seedCustomers(client: PoolClient, customers: CustomerRow[]): Promise<void> {
  const columns = ["id", "email", "name", "region", "created_at"];

  for (const batch of chunk(customers, CUSTOMER_BATCH_SIZE)) {
    await insertRows(
      client,
      "customers",
      columns,
      batch.map((customer) => [
        customer.id,
        customer.email,
        customer.name,
        customer.region,
        customer.createdAt,
      ]),
    );
  }
}

async function seedOrders(
  client: PoolClient,
  customers: CustomerRow[],
  products: ProductRow[],
  orderCount: number,
): Promise<{ orderCount: number; orderItemCount: number }> {
  const orderColumns = ["id", "customer_id", "status", "total_cents", "region", "created_at", "updated_at"];
  const orderItemColumns = ["id", "order_id", "product_id", "quantity", "unit_price_cents"];
  let insertedOrders = 0;
  let insertedOrderItems = 0;

  while (insertedOrders < orderCount) {
    const nextBatchSize = Math.min(ORDER_BATCH_SIZE, orderCount - insertedOrders);
    const { orders, orderItems } = buildOrderBatch(customers, products, nextBatchSize);

    await insertRows(
      client,
      "orders",
      orderColumns,
      orders.map((order) => [
        order.id,
        order.customerId,
        order.status,
        order.totalCents,
        order.region,
        order.createdAt,
        order.updatedAt,
      ]),
    );

    for (const batch of chunk(orderItems, ORDER_ITEM_BATCH_SIZE)) {
      await insertRows(
        client,
        "order_items",
        orderItemColumns,
        batch.map((item) => [item.id, item.orderId, item.productId, item.quantity, item.unitPriceCents]),
      );
    }

    insertedOrders += orders.length;
    insertedOrderItems += orderItems.length;
  }

  return { orderCount: insertedOrders, orderItemCount: insertedOrderItems };
}

async function ensureSchema(client: PoolClient): Promise<void> {
  const migrationFiles = ["../sql/001_schema.sql", "../sql/002_indexes.sql"];

  for (const migrationFile of migrationFiles) {
    const sql = await readFile(new URL(migrationFile, import.meta.url), "utf8");
    await client.query(sql);
  }
}

async function main(): Promise<void> {
  const startedAt = performance.now();
  const orderCount = parseCountArg(process.argv.slice(2));
  const config = getSeedConfig(orderCount);
  const categories = buildCategories();
  const products = buildProducts(config);
  const customers = buildCustomers(config);

  await withClient(async (client) => {
    await client.query("BEGIN");

    try {
      await ensureSchema(client);
      await client.query("TRUNCATE TABLE order_items, orders, products, categories, customers RESTART IDENTITY CASCADE");

      await seedCategories(client, categories);
      await seedProducts(client, products);
      await seedCustomers(client, customers);
      const orderSummary = await seedOrders(client, customers, products, config.orderCount);

      await client.query("COMMIT");

      const durationMs = Number((performance.now() - startedAt).toFixed(2));
      logger.info("Seed completed", {
        durationMs,
        categories: categories.length,
        products: products.length,
        customers: customers.length,
        orders: orderSummary.orderCount,
        orderItems: orderSummary.orderItemCount,
      });
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    }
  });
}

main()
  .catch((error) => {
    logger.error("Seed failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });

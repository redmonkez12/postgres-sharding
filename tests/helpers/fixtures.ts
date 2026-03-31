import { randomUUID } from "node:crypto";

// ── Types ────────────────────────────────────────────────────────

export type CategoryFixture = {
  name: string;
};

export type CustomerFixture = {
  id: string;
  email: string;
  name: string;
  region: string;
  created_at: Date;
};

export type ProductFixture = {
  id: string;
  name: string;
  sku: string;
  price_cents: number;
  category_id: number;
  stock_qty: number;
  created_at: Date;
};

export type OrderFixture = {
  id: string;
  customer_id: string;
  status: string;
  total_cents: number;
  region: string;
  created_at: Date;
  updated_at: Date;
};

export type OrderItemFixture = {
  id: string;
  order_id: string;
  product_id: string;
  quantity: number;
  unit_price_cents: number;
};

// ── Factory functions ────────────────────────────────────────────

let seqCounter = 0;

/** Reset the sequence counter — call in beforeEach if you need deterministic IDs. */
export function resetFixtureSeq(): void {
  seqCounter = 0;
}

function nextSeq(): number {
  return ++seqCounter;
}

export function buildCategory(overrides: Partial<CategoryFixture> = {}): CategoryFixture {
  return {
    name: overrides.name ?? `Category-${nextSeq()}`,
  };
}

export function buildCustomer(overrides: Partial<CustomerFixture> = {}): CustomerFixture {
  const seq = nextSeq();
  return {
    id: overrides.id ?? randomUUID(),
    email: overrides.email ?? `test-customer-${seq}@example.test`,
    name: overrides.name ?? `Test Customer ${seq}`,
    region: overrides.region ?? "eu",
    created_at: overrides.created_at ?? new Date(),
  };
}

export function buildProduct(overrides: Partial<ProductFixture> = {}): ProductFixture {
  const seq = nextSeq();
  return {
    id: overrides.id ?? randomUUID(),
    name: overrides.name ?? `Test Product ${seq}`,
    sku: overrides.sku ?? `TST-${String(seq).padStart(5, "0")}`,
    price_cents: overrides.price_cents ?? 1999,
    category_id: overrides.category_id ?? 1,
    stock_qty: overrides.stock_qty ?? 100,
    created_at: overrides.created_at ?? new Date(),
  };
}

export function buildOrder(overrides: Partial<OrderFixture> = {}): OrderFixture {
  const now = new Date();
  return {
    id: overrides.id ?? randomUUID(),
    customer_id: overrides.customer_id ?? randomUUID(),
    status: overrides.status ?? "pending",
    total_cents: overrides.total_cents ?? 3998,
    region: overrides.region ?? "eu",
    created_at: overrides.created_at ?? now,
    updated_at: overrides.updated_at ?? now,
  };
}

export function buildOrderItem(overrides: Partial<OrderItemFixture> = {}): OrderItemFixture {
  return {
    id: overrides.id ?? randomUUID(),
    order_id: overrides.order_id ?? randomUUID(),
    product_id: overrides.product_id ?? randomUUID(),
    quantity: overrides.quantity ?? 2,
    unit_price_cents: overrides.unit_price_cents ?? 1999,
  };
}

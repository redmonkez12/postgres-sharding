CREATE INDEX IF NOT EXISTS orders_customer_id_idx
  ON orders (customer_id);

CREATE INDEX IF NOT EXISTS orders_created_at_idx
  ON orders (created_at DESC);

CREATE INDEX IF NOT EXISTS orders_status_idx
  ON orders (status);

CREATE INDEX IF NOT EXISTS orders_region_idx
  ON orders (region);

CREATE INDEX IF NOT EXISTS order_items_order_id_idx
  ON order_items (order_id);

CREATE INDEX IF NOT EXISTS order_items_product_id_idx
  ON order_items (product_id);

CREATE INDEX IF NOT EXISTS products_category_id_idx
  ON products (category_id);

CREATE INDEX IF NOT EXISTS products_sku_idx
  ON products (sku);

CREATE TABLE IF NOT EXISTS benchmark_users (
  id BIGSERIAL PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  email TEXT NOT NULL UNIQUE,
  full_name TEXT NOT NULL,
  region TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS benchmark_users_tenant_created_at_idx
  ON benchmark_users (tenant_id, created_at DESC);

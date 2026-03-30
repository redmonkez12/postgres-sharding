#!/bin/bash
set -e

# ── Wait for primary ────────────────────────────────────────────────
until pg_isready -h postgres -p 5432 -U postgres; do
    echo "Waiting for primary to be ready..."
    sleep 2
done

# ── Start Postgres in background so we can run SQL ──────────────────
docker-entrypoint.sh postgres \
    -c config_file=/etc/postgresql/postgresql.conf &
PG_PID=$!

# Wait for local Postgres to accept connections
until pg_isready -h localhost -p 5432 -U postgres; do
    echo "Waiting for local logical replica to start..."
    sleep 1
done

# ── Create schema (logical replication requires matching tables) ────
psql -v ON_ERROR_STOP=1 -U postgres -d postgres <<-'EOSQL'
    CREATE TABLE IF NOT EXISTS orders (
        id UUID PRIMARY KEY,
        customer_id UUID NOT NULL,
        status TEXT NOT NULL,
        total_cents INTEGER NOT NULL,
        region TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS order_items (
        id UUID PRIMARY KEY,
        order_id UUID NOT NULL,
        product_id UUID NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price_cents INTEGER NOT NULL
    );
EOSQL

echo "Schema created on logical replica."

# ── Create subscription (connects back to primary) ──────────────────
# Check if subscription already exists to make the script idempotent
SUB_EXISTS=$(psql -U postgres -d postgres -tAc \
    "SELECT 1 FROM pg_subscription WHERE subname = 'orders_sub'" 2>/dev/null || true)

if [ "$SUB_EXISTS" != "1" ]; then
    psql -v ON_ERROR_STOP=1 -U postgres -d postgres <<-'EOSQL'
        CREATE SUBSCRIPTION orders_sub
            CONNECTION 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
            PUBLICATION orders_pub;
EOSQL
    echo "Subscription orders_sub created."
else
    echo "Subscription orders_sub already exists — skipping."
fi

# ── Bring Postgres back to foreground ───────────────────────────────
wait $PG_PID

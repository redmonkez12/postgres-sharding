# sharding-postgres

TypeScript playground for raw-SQL PostgreSQL experiments — replication, sharding, partitioning, caching, rate limiting, job queues, and a Fastify REST API.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (Compose V2)
- [Bun](https://bun.sh/) >= 1.3
- [Node.js](https://nodejs.org/) >= 20 (used by `tsx`)
- [k6](https://k6.io/) (optional, for load tests)

## Quick start

```bash
# Install dependencies
bun install

# Copy env and adjust if needed
cp .env.example .env

# Start the primary Postgres
docker compose up -d

# Run the schema & seed
bun run seed

# Start the API
bun run dev:api        # http://localhost:3000
```

## Compose files

Each stage adds infrastructure on top of the base primary:

| File | Services | Purpose |
|---|---|---|
| `docker-compose.yml` | `postgres` (port 5432) | Single primary — benchmarks, partitioning, seeding |
| `docker-compose.stage2.yml` | Extends primary + `pg-replica-streaming` (5433), `pg-replica-logical` (5434, profile `logical`) | Streaming & logical replication, read-write split |
| `docker-compose.stage4.yml` | `pg-shard-eu` (5435), `pg-shard-us` (5436), `pg-shard-ap` (5437) | Region-based sharding |
| `docker-compose.stage5.yml` | `pg-primary` (5432) + `redis` (6379) | Caching, rate limiting, job queues |

Start a stage by merging compose files:

```bash
# Primary + replicas
docker compose -f docker-compose.yml -f docker-compose.stage2.yml up -d

# Include logical replica
docker compose -f docker-compose.yml -f docker-compose.stage2.yml --profile logical up -d

# Shards
docker compose -f docker-compose.stage4.yml up -d

# Primary + Redis
docker compose -f docker-compose.stage5.yml up -d
```

## npm scripts

### Seeding & data

| Script | Description |
|---|---|
| `bun run seed` | Seed the primary database |
| `bun run shard-seed` | Seed all three shard databases |

### Demos & benchmarks

| Script | Description |
|---|---|
| `bun run benchmark` | Query benchmarks on the primary |
| `bun run explain` | EXPLAIN ANALYZE for key queries |
| `bun run stats` | Database statistics |
| `bun run monitor-lag` | Monitor replication lag (stage 2) |
| `bun run rw-split-demo` | Read-write split demo (stage 2) |
| `bun run logical-demo` | Logical replication demo (stage 2) |
| `bun run failover-test` | Replica failover test (stage 2) |
| `bun run partition-stats` | Partition statistics |
| `bun run partition-benchmark` | Partition benchmark |
| `bun run shard-router-demo` | Shard routing demo (stage 4) |
| `bun run cross-shard-query` | Cross-shard query demo (stage 4) |
| `bun run shard-stats` | Shard statistics (stage 4) |
| `bun run cache-demo` | Redis cache demo (stage 5) |
| `bun run cache-benchmark` | Cache vs DB benchmark (stage 5) |
| `bun run cache-invalidation-test` | Cache invalidation test (stage 5) |
| `bun run rate-limit-bench` | Rate limiter benchmark (stage 5) |
| `bun run job-queue-demo` | BullMQ job queue demo (stage 5) |
| `bun run job-worker` | Start the order-confirmation worker (stage 5) |

### API & load tests

| Script | Description |
|---|---|
| `bun run dev:api` | Start Fastify API server |
| `bun run load-test` | k6 baseline load test |
| `bun run load-test:read-heavy` | k6 read-heavy scenario |
| `bun run load-test:write-heavy` | k6 write-heavy scenario |
| `bun run load-test:rate-limit` | k6 rate-limit stress test |

### Quality

| Script | Description |
|---|---|
| `bun test` | Run unit & integration tests |
| `bun run typecheck` | TypeScript type check |
| `bun run build` | Compile TypeScript |

## Running tests

```bash
bun test
```

Tests live in `tests/` with `unit/` and `integration/` subdirectories. Test helpers and fixtures are in `tests/helpers/`.

## Architecture overview

```
┌─────────────┐
│  Fastify API │  (src/api/)
│  :3000       │  Routes: /customers, /orders, /products
└──────┬───────┘
       │
  ┌────┴─────────────────────────────────┐
  │          Application layer           │
  │  read-write-split · shard-router     │
  │  product-cache · rate-limiter        │
  │  job queue (BullMQ)                  │
  └────┬──────────┬──────────┬───────────┘
       │          │          │
  ┌────▼────┐ ┌───▼───┐ ┌───▼────┐
  │ Primary │ │Replica│ │ Redis  │
  │  :5432  │ │ :5433 │ │ :6379  │
  └─────────┘ └───────┘ └────────┘
                          ┌──────────────────┐
  Shards (stage 4):       │ EU:5435 US:5436  │
                          │ AP:5437          │
                          └──────────────────┘
```

- **Primary** — single Postgres instance for writes and default reads
- **Streaming replica** — hot standby for read scaling and failover
- **Logical replica** — selective table replication (opt-in via `logical` profile)
- **Shards** — region-based horizontal partitioning (EU / US / AP)
- **Redis** — caching layer, rate limiting (sliding window), and BullMQ job broker
- **Fastify API** — REST endpoints with read-write split middleware

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/postgres` | Primary connection string |
| `PGHOST` | `localhost` | Postgres host |
| `PGPORT` | `5432` | Postgres port |
| `PGUSER` | `postgres` | Postgres user |
| `PGPASSWORD` | `postgres` | Postgres password |
| `PGDATABASE` | `postgres` | Postgres database name |
| `PGSSLMODE` | `disable` | SSL mode |
| `PGPOOLMAX` | `10` | Connection pool maximum |
| `SEED_USER_COUNT` | `1000` | Number of users to seed |

## Project structure

```
src/
  api/           Fastify server & route handlers
  cache/         Redis product cache
  db/            Connection pool, read-write split, shard router
  jobs/          BullMQ worker (order confirmations)
  rate-limiter.ts  Sliding-window rate limiter
  utils/         Logger
scripts/         Runnable demos and benchmarks
sql/             Schema, indexes, partitioning DDL
tests/           Unit & integration tests
docker/          Postgres config for primary & replicas
load-tests/      k6 load test scenarios
```

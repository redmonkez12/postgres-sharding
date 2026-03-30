import { closePool, getPool, query } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

// ── Types ────────────────────────────────────────────────────────────

type ExplainNode = {
  "Node Type": string;
  "Relation Name"?: string;
  "Index Name"?: string;
  "Actual Rows": number;
  "Actual Loops": number;
  "Actual Total Time": number;
  "Shared Hit Blocks"?: number;
  "Shared Read Blocks"?: number;
  "Subplans Removed"?: number;
  Plans?: ExplainNode[];
  [key: string]: unknown;
};

type ExplainResult = [
  {
    Plan: ExplainNode;
    "Planning Time": number;
    "Execution Time": number;
    [key: string]: unknown;
  },
];

type PlanMetrics = {
  planningTimeMs: number;
  executionTimeMs: number;
  totalTimeMs: number;
  bufferHits: number;
  bufferReads: number;
  partitionsScanned: number;
  partitionsPruned: number;
  seqScans: string[];
};

type BenchmarkRow = {
  label: string;
  partitioned: PlanMetrics;
  baseline: PlanMetrics;
};

// ── Plan analysis helpers ────────────────────────────────────────────

function collectMetrics(node: ExplainNode): {
  hits: number;
  reads: number;
  partitionsScanned: number;
  partitionsPruned: number;
  seqScans: string[];
} {
  let hits = node["Shared Hit Blocks"] ?? 0;
  let reads = node["Shared Read Blocks"] ?? 0;
  const seqScans: string[] = [];
  let partitionsScanned = 0;
  let partitionsPruned = node["Subplans Removed"] ?? 0;

  if (node["Node Type"] === "Seq Scan" && node["Relation Name"]) {
    seqScans.push(node["Relation Name"]);
  }

  // Append node children = partitions actually scanned
  if (node["Node Type"] === "Append") {
    partitionsScanned = node.Plans?.length ?? 0;
    partitionsPruned = node["Subplans Removed"] ?? 0;
  }

  if (node.Plans) {
    for (const child of node.Plans) {
      const childMetrics = collectMetrics(child);
      hits += childMetrics.hits;
      reads += childMetrics.reads;
      seqScans.push(...childMetrics.seqScans);
      // Only take partition info from Append nodes (already captured above)
      if (node["Node Type"] !== "Append") {
        partitionsScanned += childMetrics.partitionsScanned;
        partitionsPruned += childMetrics.partitionsPruned;
      }
    }
  }

  return { hits, reads, partitionsScanned, partitionsPruned, seqScans };
}

function analyzeExplain(result: ExplainResult): PlanMetrics {
  const entry = result[0];
  const metrics = collectMetrics(entry.Plan);

  return {
    planningTimeMs: entry["Planning Time"],
    executionTimeMs: entry["Execution Time"],
    totalTimeMs: entry["Planning Time"] + entry["Execution Time"],
    bufferHits: metrics.hits,
    bufferReads: metrics.reads,
    partitionsScanned: metrics.partitionsScanned,
    partitionsPruned: metrics.partitionsPruned,
    seqScans: metrics.seqScans,
  };
}

function walkPlan(node: ExplainNode, depth: number, lines: string[]): void {
  const indent = "  ".repeat(depth);
  const isSeqScan = node["Node Type"] === "Seq Scan";
  const marker = isSeqScan ? "SEQ SCAN" : node["Node Type"];

  const relation = node["Relation Name"] ? ` on ${node["Relation Name"]}` : "";
  const index = node["Index Name"] ? ` using ${node["Index Name"]}` : "";
  const rows = `rows=${node["Actual Rows"]}`;
  const loops = node["Actual Loops"] > 1 ? ` loops=${node["Actual Loops"]}` : "";
  const time = `time=${node["Actual Total Time"].toFixed(3)}ms`;
  const pruned =
    node["Subplans Removed"] != null ? ` pruned=${node["Subplans Removed"]}` : "";

  const hits = node["Shared Hit Blocks"] ?? 0;
  const reads = node["Shared Read Blocks"] ?? 0;
  const buffers = hits + reads > 0 ? ` buf hit=${hits} read=${reads}` : "";

  lines.push(
    `${indent}-> ${marker}${relation}${index}  (${rows}${loops}, ${time}${pruned}${buffers})`,
  );

  if (node.Plans) {
    for (const child of node.Plans) {
      walkPlan(child, depth + 1, lines);
    }
  }
}

function formatPlan(result: ExplainResult): string {
  const lines: string[] = [];
  walkPlan(result[0].Plan, 0, lines);
  lines.push("");
  lines.push(`  Planning Time: ${result[0]["Planning Time"].toFixed(3)} ms`);
  lines.push(`  Execution Time: ${result[0]["Execution Time"].toFixed(3)} ms`);
  return lines.join("\n");
}

// ── Benchmark runner ─────────────────────────────────────────────────

async function runExplain(sql: string, params: unknown[] = []): Promise<ExplainResult> {
  const wrapped = `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`;
  const result = await query<{ "QUERY PLAN": ExplainResult }>(wrapped, params);
  return result.rows[0]["QUERY PLAN"];
}

// Run N iterations, return the median-run explain result
async function benchQuery(
  sql: string,
  params: unknown[] = [],
  iterations: number = 3,
): Promise<{ metrics: PlanMetrics; plan: ExplainResult }> {
  const runs: { metrics: PlanMetrics; plan: ExplainResult }[] = [];

  for (let i = 0; i < iterations; i++) {
    const plan = await runExplain(sql, params);
    runs.push({ metrics: analyzeExplain(plan), plan });
  }

  // Pick the median by execution time
  runs.sort((a, b) => a.metrics.executionTimeMs - b.metrics.executionTimeMs);
  return runs[Math.floor(runs.length / 2)];
}

function printComparison(row: BenchmarkRow): void {
  const p = row.partitioned;
  const b = row.baseline;

  const speedup = b.executionTimeMs > 0 ? b.executionTimeMs / p.executionTimeMs : 0;
  const sign = speedup >= 1 ? "faster" : "slower";
  const ratio = speedup >= 1 ? speedup : 1 / speedup;

  console.log(
    [
      `  Planning Time    : ${p.planningTimeMs.toFixed(3)} ms  vs  ${b.planningTimeMs.toFixed(3)} ms`,
      `  Execution Time   : ${p.executionTimeMs.toFixed(3)} ms  vs  ${b.executionTimeMs.toFixed(3)} ms  (${ratio.toFixed(2)}x ${sign})`,
      `  Total Time       : ${p.totalTimeMs.toFixed(3)} ms  vs  ${b.totalTimeMs.toFixed(3)} ms`,
      `  Buffer Hits      : ${p.bufferHits}  vs  ${b.bufferHits}`,
      `  Buffer Reads     : ${p.bufferReads}  vs  ${b.bufferReads}`,
      `  Partitions Scan  : ${p.partitionsScanned || "n/a"}`,
      `  Partitions Pruned: ${p.partitionsPruned || "n/a"}`,
    ].join("\n"),
  );
}

// ── Setup ────────────────────────────────────────────────────────────

async function ensureBaseline(): Promise<void> {
  const exists = await query<{ exists: boolean }>(
    `SELECT EXISTS (
       SELECT 1 FROM information_schema.tables
       WHERE table_name = 'orders_baseline'
     ) AS exists`,
  );

  if (exists.rows[0].exists) {
    const counts = await query<{ partitioned: string; baseline: string }>(
      `SELECT
         (SELECT count(*) FROM orders)::text AS partitioned,
         (SELECT count(*) FROM orders_baseline)::text AS baseline`,
    );

    console.log(
      `  orders_baseline exists (${counts.rows[0].baseline} rows vs ${counts.rows[0].partitioned} partitioned)`,
    );
    return;
  }

  console.log("  Creating orders_baseline from partitioned orders...");

  await query(`
    CREATE TABLE orders_baseline (
      id UUID PRIMARY KEY,
      customer_id UUID NOT NULL,
      status TEXT NOT NULL,
      total_cents INTEGER NOT NULL,
      region TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await query(`
    INSERT INTO orders_baseline
    SELECT id, customer_id, status, total_cents, region, created_at, updated_at
    FROM orders
  `);

  await query(
    `CREATE INDEX orders_baseline_customer_id_idx ON orders_baseline (customer_id)`,
  );
  await query(
    `CREATE INDEX orders_baseline_created_at_idx ON orders_baseline (created_at DESC)`,
  );
  await query(`CREATE INDEX orders_baseline_status_idx ON orders_baseline (status)`);
  await query(`CREATE INDEX orders_baseline_region_idx ON orders_baseline (region)`);

  await query(`ANALYZE orders_baseline`);
  await query(`ANALYZE orders`);

  const count = await query<{ cnt: string }>(
    `SELECT count(*)::text AS cnt FROM orders_baseline`,
  );
  console.log(`  Created orders_baseline with ${count.rows[0].cnt} rows`);
}

// ── Benchmark queries ────────────────────────────────────────────────

async function benchPointLookup(): Promise<BenchmarkRow> {
  const sql = (table: string) =>
    `SELECT * FROM ${table} WHERE created_at BETWEEN '2025-03-01' AND '2025-03-31'`;

  const partitioned = await benchQuery(sql("orders"));
  const baseline = await benchQuery(sql("orders_baseline"));

  return { label: "Point lookup (date range)", partitioned: partitioned.metrics, baseline: baseline.metrics };
}

async function benchCustomerLookup(): Promise<BenchmarkRow> {
  // Pick a random customer that has orders
  const cust = await query<{ id: string }>(
    `SELECT customer_id AS id FROM orders LIMIT 1`,
  );
  const customerId = cust.rows[0].id;

  const sql = (table: string) =>
    `SELECT * FROM ${table} WHERE customer_id = '${customerId}'`;

  const partitioned = await benchQuery(sql("orders"));
  const baseline = await benchQuery(sql("orders_baseline"));

  return { label: "Customer lookup (no pruning)", partitioned: partitioned.metrics, baseline: baseline.metrics };
}

async function benchCrossPartitionAggregate(): Promise<BenchmarkRow> {
  const sql = (table: string) =>
    `SELECT count(*), sum(total_cents) FROM ${table} WHERE status = 'paid'`;

  const partitioned = await benchQuery(sql("orders"));
  const baseline = await benchQuery(sql("orders_baseline"));

  return { label: "Cross-partition aggregate", partitioned: partitioned.metrics, baseline: baseline.metrics };
}

async function benchHotPartitionWrite(): Promise<BenchmarkRow> {
  const BATCH_SIZE = 10_000;

  // Get a valid customer for FK
  const cust = await query<{ id: string; region: string }>(
    `SELECT id, region FROM customers LIMIT 1`,
  );
  const { id: customerId, region } = cust.rows[0];

  // Build batch values
  const buildValues = () => {
    const rows: string[] = [];
    for (let i = 0; i < BATCH_SIZE; i++) {
      const id = crypto.randomUUID();
      const totalCents = Math.floor(Math.random() * 50000) + 500;
      rows.push(
        `('${id}', '${customerId}', 'pending', ${totalCents}, '${region}', NOW(), NOW())`,
      );
    }
    return rows.join(",\n");
  };

  // -- Partitioned table write --
  const values1 = buildValues();
  const insertPartitioned = `INSERT INTO orders (id, customer_id, status, total_cents, region, created_at, updated_at) VALUES ${values1}`;

  const pStart = performance.now();
  await query(insertPartitioned);
  const pElapsed = performance.now() - pStart;

  // -- Baseline table write --
  const values2 = buildValues();
  const insertBaseline = `INSERT INTO orders_baseline (id, customer_id, status, total_cents, region, created_at, updated_at) VALUES ${values2}`;

  const bStart = performance.now();
  await query(insertBaseline);
  const bElapsed = performance.now() - bStart;

  // Return as PlanMetrics (executionTimeMs = wall clock for writes)
  return {
    label: `Hot partition write (${BATCH_SIZE} rows)`,
    partitioned: {
      planningTimeMs: 0,
      executionTimeMs: pElapsed,
      totalTimeMs: pElapsed,
      bufferHits: 0,
      bufferReads: 0,
      partitionsScanned: 0,
      partitionsPruned: 0,
      seqScans: [],
    },
    baseline: {
      planningTimeMs: 0,
      executionTimeMs: bElapsed,
      totalTimeMs: bElapsed,
      bufferHits: 0,
      bufferReads: 0,
      partitionsScanned: 0,
      partitionsPruned: 0,
      seqScans: [],
    },
  };
}

async function benchColdPartitionScan(): Promise<BenchmarkRow> {
  // Analytics query on data ~3 months ago (2025-12 to 2026-02)
  const sql = (table: string) => `
    SELECT date_trunc('week', created_at) AS week,
           count(*) AS orders,
           sum(total_cents) AS revenue_cents
    FROM ${table}
    WHERE created_at BETWEEN '2025-12-01' AND '2026-02-28'
    GROUP BY week
    ORDER BY week`;

  const partitioned = await benchQuery(sql("orders"));
  const baseline = await benchQuery(sql("orders_baseline"));

  return { label: "Cold partition scan (3-month analytics)", partitioned: partitioned.metrics, baseline: baseline.metrics };
}

// ── Summary table ────────────────────────────────────────────────────

function printSummaryTable(rows: BenchmarkRow[]): void {
  console.log("\n" + "=".repeat(110));
  console.log("  SUMMARY — Partitioned vs Baseline (non-partitioned)");
  console.log("=".repeat(110) + "\n");

  const header = [
    "Query".padEnd(38),
    "Part. ms".padStart(10),
    "Base. ms".padStart(10),
    "Speedup".padStart(10),
    "Part Scan".padStart(11),
    "Part Prune".padStart(12),
    "Buf Hits P".padStart(12),
    "Buf Hits B".padStart(12),
  ].join("");

  console.log(header);
  console.log("-".repeat(115));

  for (const row of rows) {
    const p = row.partitioned;
    const b = row.baseline;
    const speedup =
      b.executionTimeMs > 0 && p.executionTimeMs > 0
        ? b.executionTimeMs / p.executionTimeMs
        : 0;
    const speedupStr =
      speedup > 0 ? `${speedup.toFixed(2)}x` : "n/a";

    console.log(
      [
        row.label.padEnd(38),
        p.executionTimeMs.toFixed(2).padStart(10),
        b.executionTimeMs.toFixed(2).padStart(10),
        speedupStr.padStart(10),
        (p.partitionsScanned || "-").toString().padStart(11),
        (p.partitionsPruned || "-").toString().padStart(12),
        p.bufferHits.toString().padStart(12),
        b.bufferHits.toString().padStart(12),
      ].join(""),
    );
  }

  console.log("-".repeat(115));
}

// ── Detailed output per query ────────────────────────────────────────

async function runDetailedBenchmark(
  label: string,
  sqlFn: (table: string) => string,
): Promise<BenchmarkRow> {
  console.log(`\n${"=".repeat(80)}`);
  console.log(`  ${label}`);
  console.log("=".repeat(80));

  console.log("\n  --- Partitioned (orders) ---\n");
  const partitioned = await benchQuery(sqlFn("orders"));
  console.log(formatPlan(partitioned.plan));

  console.log("\n  --- Baseline (orders_baseline) ---\n");
  const baseline = await benchQuery(sqlFn("orders_baseline"));
  console.log(formatPlan(baseline.plan));

  const row: BenchmarkRow = {
    label,
    partitioned: partitioned.metrics,
    baseline: baseline.metrics,
  };

  console.log("\n  --- Comparison ---\n");
  printComparison(row);

  return row;
}

// ── Main ─────────────────────────────────────────────────────────────

function parseFlag(argv: string[], name: string, fallback: boolean): boolean {
  return argv.includes(`--${name}`) || fallback;
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const verbose = parseFlag(argv, "verbose", false);

  console.log("\n  Partition Pruning Benchmarks — TOM-1200");
  console.log("  Partitioned (range by created_at) vs Non-partitioned baseline\n");

  // Step 1: ensure baseline table exists
  console.log("--- Setup ---\n");
  await ensureBaseline();

  // Step 2: run benchmarks
  const results: BenchmarkRow[] = [];

  if (verbose) {
    // Detailed mode: show full EXPLAIN plans
    results.push(
      await runDetailedBenchmark("1. Point lookup (date range — partition pruning)", (t) =>
        `SELECT * FROM ${t} WHERE created_at BETWEEN '2025-03-01' AND '2025-03-31'`,
      ),
    );

    const cust = await query<{ id: string }>(
      `SELECT customer_id AS id FROM orders LIMIT 1`,
    );
    const customerId = cust.rows[0].id;
    results.push(
      await runDetailedBenchmark("2. Customer lookup (no pruning on range-by-date)", (t) =>
        `SELECT * FROM ${t} WHERE customer_id = '${customerId}'`,
      ),
    );

    results.push(
      await runDetailedBenchmark("3. Cross-partition aggregate (status = paid)", (t) =>
        `SELECT count(*), sum(total_cents) FROM ${t} WHERE status = 'paid'`,
      ),
    );

    // Write test is special — not EXPLAIN-based
    console.log(`\n${"=".repeat(80)}`);
    console.log("  4. Hot partition write (10K rows into current month)");
    console.log("=".repeat(80));
    const writeResult = await benchHotPartitionWrite();
    console.log(
      `\n  Partitioned: ${writeResult.partitioned.executionTimeMs.toFixed(2)} ms`,
    );
    console.log(
      `  Baseline:    ${writeResult.baseline.executionTimeMs.toFixed(2)} ms`,
    );
    results.push(writeResult);

    results.push(
      await runDetailedBenchmark("5. Cold partition scan (3-month analytics)", (t) => `
        SELECT date_trunc('week', created_at) AS week,
               count(*) AS orders,
               sum(total_cents) AS revenue_cents
        FROM ${t}
        WHERE created_at BETWEEN '2025-12-01' AND '2026-02-28'
        GROUP BY week
        ORDER BY week`,
      ),
    );
  } else {
    // Compact mode: just metrics
    console.log("\n--- Running benchmarks (3 iterations each, median reported) ---\n");

    results.push(await benchPointLookup());
    console.log("  [1/5] Point lookup (date range)            done");

    results.push(await benchCustomerLookup());
    console.log("  [2/5] Customer lookup (no pruning)         done");

    results.push(await benchCrossPartitionAggregate());
    console.log("  [3/5] Cross-partition aggregate             done");

    results.push(await benchHotPartitionWrite());
    console.log("  [4/5] Hot partition write (10K rows)        done");

    results.push(await benchColdPartitionScan());
    console.log("  [5/5] Cold partition scan (3-month)         done");
  }

  // Step 3: summary table
  printSummaryTable(results);

  // Step 4: cleanup inserted test rows
  await query(`DELETE FROM orders WHERE status = 'pending' AND customer_id = (
    SELECT customer_id FROM orders WHERE status = 'pending' ORDER BY created_at DESC LIMIT 1
  ) AND created_at > NOW() - INTERVAL '5 minutes'`);
  await query(`DELETE FROM orders_baseline WHERE status = 'pending' AND customer_id = (
    SELECT customer_id FROM orders_baseline WHERE status = 'pending' ORDER BY created_at DESC LIMIT 1
  ) AND created_at > NOW() - INTERVAL '5 minutes'`);

  console.log("\n  Cleaned up benchmark write-test rows");
}

main()
  .catch((error) => {
    logger.error("Partition benchmark failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });

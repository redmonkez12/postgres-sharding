import { closePool, query } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

type ExplainNode = {
  "Node Type": string;
  "Relation Name"?: string;
  "Index Name"?: string;
  "Startup Cost": number;
  "Total Cost": number;
  "Actual Startup Time": number;
  "Actual Total Time": number;
  "Actual Rows": number;
  "Actual Loops": number;
  "Shared Hit Blocks"?: number;
  "Shared Read Blocks"?: number;
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

type QueryDefinition = {
  label: string;
  sql: string;
  params: unknown[];
};

const QUERIES: QueryDefinition[] = [
  {
    label: "Get order by ID (random)",
    sql: `SELECT o.*, c.name AS customer_name
          FROM orders o
          JOIN customers c ON c.id = o.customer_id
          WHERE o.id = (SELECT id FROM orders ORDER BY RANDOM() LIMIT 1)`,
    params: [],
  },
  {
    label: "List customer orders",
    sql: `SELECT id, status, total_cents, created_at
          FROM orders
          WHERE customer_id = (SELECT id FROM customers ORDER BY RANDOM() LIMIT 1)
          ORDER BY created_at DESC
          LIMIT 20`,
    params: [],
  },
  {
    label: "Orders by month",
    sql: `SELECT DATE_TRUNC('month', created_at) AS month, COUNT(*) AS order_count
          FROM orders
          GROUP BY month
          ORDER BY month DESC`,
    params: [],
  },
  {
    label: "Top 10 products by revenue",
    sql: `SELECT p.name, SUM(oi.quantity * oi.unit_price_cents) AS revenue_cents
          FROM order_items oi
          JOIN products p ON p.id = oi.product_id
          GROUP BY p.id, p.name
          ORDER BY revenue_cents DESC
          LIMIT 10`,
    params: [],
  },
  {
    label: "Revenue by region",
    sql: `SELECT region, SUM(total_cents) AS revenue_cents, COUNT(*) AS order_count
          FROM orders
          GROUP BY region
          ORDER BY revenue_cents DESC`,
    params: [],
  },
];

// ── Plan pretty-printer ───────────────────────────────────────────────

const SEQ_SCAN = "Seq Scan";

function walkPlan(node: ExplainNode, depth: number, lines: string[]): void {
  const indent = "  ".repeat(depth);
  const isSeqScan = node["Node Type"] === SEQ_SCAN;
  const marker = isSeqScan ? "⚠  SEQ SCAN" : node["Node Type"];

  const relation = node["Relation Name"] ? ` on ${node["Relation Name"]}` : "";
  const index = node["Index Name"] ? ` using ${node["Index Name"]}` : "";
  const rows = `rows=${node["Actual Rows"]}`;
  const loops = node["Actual Loops"] > 1 ? ` loops=${node["Actual Loops"]}` : "";
  const time = `time=${node["Actual Total Time"].toFixed(3)}ms`;

  const hits = node["Shared Hit Blocks"] ?? 0;
  const reads = node["Shared Read Blocks"] ?? 0;
  const buffers = hits + reads > 0 ? ` buffers hit=${hits} read=${reads}` : "";

  lines.push(`${indent}→ ${marker}${relation}${index}  (${rows}${loops}, ${time}${buffers})`);

  if (node.Plans) {
    for (const child of node.Plans) {
      walkPlan(child, depth + 1, lines);
    }
  }
}

function formatPlan(result: ExplainResult): string {
  const entry = result[0];
  const lines: string[] = [];

  walkPlan(entry.Plan, 0, lines);

  lines.push("");
  lines.push(`  Planning Time: ${entry["Planning Time"].toFixed(3)} ms`);
  lines.push(`  Execution Time: ${entry["Execution Time"].toFixed(3)} ms`);

  return lines.join("\n");
}

function collectSeqScans(node: ExplainNode, found: string[]): void {
  if (node["Node Type"] === SEQ_SCAN && node["Relation Name"]) {
    found.push(node["Relation Name"]);
  }

  if (node.Plans) {
    for (const child of node.Plans) {
      collectSeqScans(child, found);
    }
  }
}

// ── Main ──────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  for (const q of QUERIES) {
    const wrappedSql = `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${q.sql}`;
    const result = await query<{ "QUERY PLAN": ExplainResult }>(wrappedSql, q.params);
    const plan = result.rows[0]["QUERY PLAN"];

    console.log(`\n═══ ${q.label} ═══\n`);
    console.log(formatPlan(plan));

    const seqScans: string[] = [];
    collectSeqScans(plan[0].Plan, seqScans);

    if (seqScans.length > 0) {
      console.log(`\n  ⚠  Sequential scans on: ${seqScans.join(", ")}`);
    }

    console.log();
  }

  logger.info("EXPLAIN analysis complete");
}

main()
  .catch((error) => {
    logger.error("Explain failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });

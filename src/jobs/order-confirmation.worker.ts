import "dotenv/config";

import { Worker, Job } from "bullmq";

import { logger } from "../utils/logger.js";
import { QUEUE_NAME, type OrderConfirmationJob } from "./queue.js";

// ── Config ─────────────────────────────────────────────────────────

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
const url = new URL(REDIS_URL);

const connection = {
  host: url.hostname,
  port: Number(url.port) || 6379,
};

// Probability of simulated failure (0.0 – 1.0)
const FAILURE_RATE = Number(process.env.JOB_FAILURE_RATE ?? 0.2);

// Simulated processing delay in ms
const PROCESSING_DELAY_MS = Number(process.env.JOB_PROCESSING_DELAY_MS ?? 500);

// ── Processor ──────────────────────────────────────────────────────

async function processOrderConfirmation(job: Job<OrderConfirmationJob>): Promise<void> {
  const { orderId, customerEmail } = job.data;
  const attempt = job.attemptsMade + 1;

  logger.info("Processing order confirmation", {
    jobId: job.id,
    orderId,
    customerEmail,
    attempt,
  });

  // Simulate work
  await new Promise((resolve) => setTimeout(resolve, PROCESSING_DELAY_MS));

  // Simulate random failure for retry testing
  if (Math.random() < FAILURE_RATE) {
    const msg = `Simulated transient failure (attempt ${attempt}/3)`;
    logger.warn("Job failed (simulated)", { jobId: job.id, orderId, attempt });
    throw new Error(msg);
  }

  logger.info("Confirmation sent", {
    jobId: job.id,
    orderId,
    customerEmail,
    attempt,
    processingMs: PROCESSING_DELAY_MS,
  });
}

// ── Worker ─────────────────────────────────────────────────────────

export const worker = new Worker<OrderConfirmationJob>(QUEUE_NAME, processOrderConfirmation, {
  connection,
  concurrency: 5,
});

// ── Lifecycle events ──────────────────────────────────────────────

worker.on("ready", () => {
  logger.info("Worker ready", { queue: QUEUE_NAME, concurrency: 5 });
});

worker.on("completed", (job) => {
  logger.info("Worker: job completed", {
    jobId: job.id,
    orderId: job.data.orderId,
    attempts: job.attemptsMade,
  });
});

worker.on("failed", (job, err) => {
  if (!job) return;

  const maxAttempts = job.opts.attempts ?? 3;
  const isDeadLetter = job.attemptsMade >= maxAttempts;

  if (isDeadLetter) {
    logger.error("Job moved to DLQ (all retries exhausted)", {
      jobId: job.id,
      orderId: job.data.orderId,
      attempts: job.attemptsMade,
      error: err.message,
    });
  } else {
    logger.warn("Job failed, will retry", {
      jobId: job.id,
      orderId: job.data.orderId,
      attempt: job.attemptsMade,
      maxAttempts,
      error: err.message,
    });
  }
});

worker.on("error", (err) => {
  logger.error("Worker error", { error: err.message });
});

// ── Graceful shutdown ─────────────────────────────────────────────

async function shutdown() {
  logger.info("Shutting down worker…");
  await worker.close();
  logger.info("Worker stopped");
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

logger.info("Order-confirmation worker starting…", { queue: QUEUE_NAME });

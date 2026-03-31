import "dotenv/config";

import { Queue, QueueEvents } from "bullmq";

import { logger } from "../utils/logger.js";

// ── Config ─────────────────────────────────────────────────────────

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
const url = new URL(REDIS_URL);

const connection = {
  host: url.hostname,
  port: Number(url.port) || 6379,
};

// ── Types ──────────────────────────────────────────────────────────

export type OrderConfirmationJob = {
  orderId: string;
  customerEmail: string;
};

// ── Queue ──────────────────────────────────────────────────────────

export const QUEUE_NAME = "order-confirmation";

export const orderConfirmationQueue = new Queue<OrderConfirmationJob>(QUEUE_NAME, {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 1000,
    },
    removeOnComplete: { count: 200 },
    removeOnFail: false,
  },
});

// ── Queue events (optional listener) ──────────────────────────────

export const queueEvents = new QueueEvents(QUEUE_NAME, { connection });

queueEvents.on("completed", ({ jobId }) => {
  logger.info("Job completed", { jobId, queue: QUEUE_NAME });
});

queueEvents.on("failed", ({ jobId, failedReason }) => {
  logger.error("Job failed", { jobId, queue: QUEUE_NAME, reason: failedReason });
});

// ── Helpers ────────────────────────────────────────────────────────

export async function enqueueOrderConfirmation(orderId: string, customerEmail: string): Promise<string> {
  const job = await orderConfirmationQueue.add("send-confirmation", {
    orderId,
    customerEmail,
  });

  logger.info("Job enqueued", { jobId: job.id, orderId, customerEmail });
  return job.id!;
}

export async function getQueueStats() {
  const [waiting, active, completed, failed, delayed] = await Promise.all([
    orderConfirmationQueue.getWaitingCount(),
    orderConfirmationQueue.getActiveCount(),
    orderConfirmationQueue.getCompletedCount(),
    orderConfirmationQueue.getFailedCount(),
    orderConfirmationQueue.getDelayedCount(),
  ]);

  return { waiting, active, completed, failed, delayed };
}

export async function closeQueue(): Promise<void> {
  await queueEvents.close();
  await orderConfirmationQueue.close();
  logger.info("Queue closed", { queue: QUEUE_NAME });
}

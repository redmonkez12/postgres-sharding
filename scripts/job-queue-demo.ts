import "dotenv/config";

import { faker } from "@faker-js/faker";

import { logger } from "../src/utils/logger.js";
import {
  enqueueOrderConfirmation,
  getQueueStats,
  closeQueue,
  queueEvents,
} from "../src/jobs/queue.js";

// ── Config ─────────────────────────────────────────────────────────

const JOB_COUNT = Number(process.argv[2] ?? 10);

// ── Main ──────────────────────────────────────────────────────────

async function main() {
  logger.info("=== BullMQ Job-Queue Demo ===");
  logger.info(`Enqueuing ${JOB_COUNT} order-confirmation jobs…`);

  // 1. Enqueue jobs
  const jobIds: string[] = [];

  for (let i = 0; i < JOB_COUNT; i++) {
    const orderId = faker.string.uuid();
    const customerEmail = faker.internet.email();
    const jobId = await enqueueOrderConfirmation(orderId, customerEmail);
    jobIds.push(jobId);
  }

  logger.info("All jobs enqueued", { count: jobIds.length });

  // 2. Show queue stats
  const stats = await getQueueStats();
  logger.info("Queue stats after enqueue", stats);

  // 3. Wait for processing (worker must be running separately)
  logger.info("Waiting for jobs to settle (start the worker in another terminal if not running)…");

  let settled = 0;
  const target = JOB_COUNT;
  const startTime = Date.now();
  const TIMEOUT_MS = 60_000;

  await new Promise<void>((resolve) => {
    const check = async () => {
      const { completed, failed } = await getQueueStats();
      const total = completed + failed;

      if (total >= target || Date.now() - startTime > TIMEOUT_MS) {
        resolve();
        return;
      }

      if (total > settled) {
        settled = total;
        logger.info("Progress", { completed, failed, remaining: target - total });
      }

      setTimeout(check, 500);
    };

    check();
  });

  // 4. Final stats
  const finalStats = await getQueueStats();
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  logger.info("=== Final Results ===");
  logger.info("Queue stats", { ...finalStats, elapsedSec: elapsed });

  if (finalStats.failed > 0) {
    logger.warn("Some jobs are in the dead-letter state (failed after all retries)", {
      failed: finalStats.failed,
    });
  }

  await closeQueue();
  process.exit(0);
}

main().catch((err) => {
  logger.error("Demo failed", { error: err.message });
  process.exit(1);
});

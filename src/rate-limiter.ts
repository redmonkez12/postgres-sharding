import "dotenv/config";

import { Redis } from "ioredis";

import { logger } from "./utils/logger.js";

// ── Types ──────────────────────────────────────────────────────────

export type RateLimitResult = {
  allowed: boolean;
  limit: number;
  remaining: number;
  resetAt: number; // Unix timestamp in seconds
};

export type RateLimitConfig = {
  maxRequests: number;
  windowSizeMs: number;
};

export type RateLimitHeaders = {
  "X-RateLimit-Limit": string;
  "X-RateLimit-Remaining": string;
  "X-RateLimit-Reset": string;
};

// ── Helpers ────────────────────────────────────────────────────────

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

function keyFor(identifier: string, prefix: string): string {
  return `ratelimit:${prefix}:${identifier}`;
}

export function buildHeaders(result: RateLimitResult): RateLimitHeaders {
  return {
    "X-RateLimit-Limit": String(result.limit),
    "X-RateLimit-Remaining": String(result.remaining),
    "X-RateLimit-Reset": String(result.resetAt),
  };
}

// ── Sliding Window Rate Limiter (sorted sets) ─────────────────────

export class SlidingWindowRateLimiter {
  private readonly redis: Redis;
  private readonly config: RateLimitConfig;

  constructor(config: RateLimitConfig, redisUrl?: string) {
    this.config = config;
    this.redis = new Redis(redisUrl ?? REDIS_URL);
    logger.info("SlidingWindowRateLimiter created", {
      maxRequests: config.maxRequests,
      windowSizeMs: config.windowSizeMs,
    });
  }

  async check(identifier: string): Promise<RateLimitResult> {
    const key = keyFor(identifier, "sliding");
    const now = Date.now();
    const windowStart = now - this.config.windowSizeMs;

    const pipeline = this.redis.pipeline();
    // Remove entries outside the window
    pipeline.zremrangebyscore(key, 0, windowStart);
    // Add current request with timestamp as score and unique member
    pipeline.zadd(key, String(now), `${now}:${Math.random().toString(36).slice(2, 10)}`);
    // Count entries in window
    pipeline.zcard(key);
    // Set TTL so keys auto-expire
    pipeline.pexpire(key, this.config.windowSizeMs);

    const results = await pipeline.exec();

    const count = (results![2][1] as number) ?? 0;
    const allowed = count <= this.config.maxRequests;
    const remaining = Math.max(0, this.config.maxRequests - count);
    const resetAt = Math.ceil((now + this.config.windowSizeMs) / 1000);

    if (!allowed) {
      logger.info("Rate limit exceeded (sliding)", { identifier, count, limit: this.config.maxRequests });
    }

    return { allowed, limit: this.config.maxRequests, remaining, resetAt };
  }

  async reset(identifier: string): Promise<void> {
    await this.redis.del(keyFor(identifier, "sliding"));
  }

  async close(): Promise<void> {
    this.redis.disconnect();
  }
}

// ── Fixed Window Rate Limiter (INCR + EXPIRE) ────────────────────

export class FixedWindowRateLimiter {
  private readonly redis: Redis;
  private readonly config: RateLimitConfig;

  constructor(config: RateLimitConfig, redisUrl?: string) {
    this.config = config;
    this.redis = new Redis(redisUrl ?? REDIS_URL);
    logger.info("FixedWindowRateLimiter created", {
      maxRequests: config.maxRequests,
      windowSizeMs: config.windowSizeMs,
    });
  }

  private currentWindowKey(identifier: string): { key: string; windowEnd: number } {
    const windowSizeSec = Math.ceil(this.config.windowSizeMs / 1000);
    const currentWindow = Math.floor(Date.now() / 1000 / windowSizeSec);
    const key = keyFor(identifier, `fixed:${currentWindow}`);
    const windowEnd = (currentWindow + 1) * windowSizeSec;
    return { key, windowEnd };
  }

  async check(identifier: string): Promise<RateLimitResult> {
    const { key, windowEnd } = this.currentWindowKey(identifier);
    const windowTtl = Math.ceil(this.config.windowSizeMs / 1000);

    const pipeline = this.redis.pipeline();
    pipeline.incr(key);
    pipeline.expire(key, windowTtl);

    const results = await pipeline.exec();

    const count = (results![0][1] as number) ?? 0;
    const allowed = count <= this.config.maxRequests;
    const remaining = Math.max(0, this.config.maxRequests - count);

    if (!allowed) {
      logger.info("Rate limit exceeded (fixed)", { identifier, count, limit: this.config.maxRequests });
    }

    return { allowed, limit: this.config.maxRequests, remaining, resetAt: windowEnd };
  }

  async reset(identifier: string): Promise<void> {
    const { key } = this.currentWindowKey(identifier);
    await this.redis.del(key);
  }

  async close(): Promise<void> {
    this.redis.disconnect();
  }
}

// ── Fastify Middleware ─────────────────────────────────────────────

type FastifyRequest = { ip: string; headers: Record<string, string | undefined> };
type FastifyReply = {
  code: (statusCode: number) => FastifyReply;
  headers: (headers: Record<string, string>) => FastifyReply;
  send: (payload: unknown) => FastifyReply;
};

export function rateLimitHook(
  limiter: SlidingWindowRateLimiter | FixedWindowRateLimiter,
  identifierFn?: (req: FastifyRequest) => string,
) {
  const getIdentifier = identifierFn ?? ((req: FastifyRequest) => req.headers["x-api-key"] ?? req.ip);

  return async (req: FastifyRequest, reply: FastifyReply) => {
    const identifier = getIdentifier(req);
    const result = await limiter.check(identifier);
    const headers = buildHeaders(result);

    reply.headers(headers);

    if (!result.allowed) {
      reply.code(429).send({
        error: "Too Many Requests",
        retryAfter: result.resetAt - Math.floor(Date.now() / 1000),
      });
    }
  };
}

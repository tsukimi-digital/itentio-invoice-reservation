import { z } from 'zod';

/// Values that ship in `.env.example` so that `cp .env.example .env` yields a
/// runnable service. That convenience is deliberate; what must not happen is
/// one of them reaching production. A length rule alone cannot stop that — the
/// placeholder below is 34 characters — so these values are refused by
/// identity, not by length, when NODE_ENV is production.
const KNOWN_DEV_SECRETS = new Set(['dev-only-insecure-secret-change-me']);

const PRODUCTION_MIN_SECRET_LENGTH = 32;

/**
 * Environment is validated once at boot. A missing or malformed variable fails
 * startup loudly rather than surfacing as an undefined value deep in a request.
 */
export const envSchema = z
  .object({
    NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
    PORT: z.coerce.number().int().positive().default(3000),

    DATABASE_URL: z.string().url(),

    KAFKA_BROKERS: z
      .string()
      .min(1)
      .transform((value) => value.split(',').map((broker) => broker.trim())),
    KAFKA_CLIENT_ID: z.string().min(1),
    KAFKA_GROUP_ID: z.string().min(1),

    JWT_SECRET: z.string().min(16, 'JWT_SECRET must be at least 16 characters'),
    JWT_EXPIRES_IN: z.string().min(1).default('1h'),
    REFRESH_TOKEN_TTL_DAYS: z.coerce.number().int().positive().default(30),

    // Number of proxy hops to trust when resolving the client IP. 0 means
    // "trust nothing", which is correct for a directly-exposed process and is
    // the safe default; behind one load balancer this is 1. Rate limiting is
    // only as sound as this value.
    TRUST_PROXY_HOPS: z.coerce.number().int().nonnegative().default(0),

    // Currency used to derive a cross rate when no direct or inverse rate
    // exists. Declared here rather than read from process.env at module load,
    // so a typo is a boot failure instead of a pivot with no rates that only
    // fails at request time.
    FX_PIVOT_CURRENCY: z.string().length(3).toUpperCase().default('USD'),

    // Upper bound on how stale a rate may be and still be used for a
    // conversion. Without it, the newest row in `fx_rate` is used no matter
    // how old — a three-year-old rate would price a reservation silently.
    //
    // The 7-day default is chosen for local use, where `prisma db seed` runs
    // once and the database then sits idle for days; a real deployment with a
    // live rate feed should tighten this to single-digit hours. The guard's
    // purpose is to make "arbitrarily old" impossible, not to encode a
    // treasury-grade tolerance.
    FX_MAX_RATE_AGE_HOURS: z.coerce.number().positive().default(168),
  })
  .superRefine((env, ctx) => {
    if (env.NODE_ENV !== 'production') return;

    if (
      KNOWN_DEV_SECRETS.has(env.JWT_SECRET) ||
      env.JWT_SECRET.length < PRODUCTION_MIN_SECRET_LENGTH
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['JWT_SECRET'],
        message:
          `must be a unique value of at least ${PRODUCTION_MIN_SECRET_LENGTH} characters in ` +
          'production — the placeholder from .env.example is refused',
      });
    }
  });

export type Env = z.infer<typeof envSchema>;

export function validateEnv(raw: Record<string, unknown>): Env {
  const parsed = envSchema.safeParse(raw);

  if (!parsed.success) {
    const details = parsed.error.issues
      .map((issue) => `  - ${issue.path.join('.')}: ${issue.message}`)
      .join('\n');
    throw new Error(`Invalid environment configuration:\n${details}`);
  }

  return parsed.data;
}

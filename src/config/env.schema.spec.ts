import { validateEnv } from './env.schema';

const baseEnv = {
  DATABASE_URL: 'postgresql://user:pass@localhost:5432/db?schema=public',
  KAFKA_BROKERS: 'localhost:9092',
  KAFKA_CLIENT_ID: 'test-client',
  KAFKA_GROUP_ID: 'test-group',
  JWT_SECRET: 'a-sufficiently-long-secret',
};

describe('validateEnv', () => {
  it('applies defaults for optional variables', () => {
    const env = validateEnv({ ...baseEnv });

    expect(env.NODE_ENV).toBe('development');
    expect(env.PORT).toBe(3000);
    expect(env.JWT_EXPIRES_IN).toBe('1h');
  });

  it('splits a comma-separated broker list into an array', () => {
    const env = validateEnv({ ...baseEnv, KAFKA_BROKERS: 'a:9092, b:9092 ,c:9092' });

    expect(env.KAFKA_BROKERS).toEqual(['a:9092', 'b:9092', 'c:9092']);
  });

  it('coerces PORT from its string representation', () => {
    const env = validateEnv({ ...baseEnv, PORT: '8080' });

    expect(env.PORT).toBe(8080);
  });

  it('rejects a JWT secret that is too short to be meaningful', () => {
    expect(() => validateEnv({ ...baseEnv, JWT_SECRET: 'short' })).toThrow(
      /JWT_SECRET must be at least 16 characters/,
    );
  });

  it('rejects a malformed database URL', () => {
    expect(() => validateEnv({ ...baseEnv, DATABASE_URL: 'not-a-url' })).toThrow(
      /Invalid environment configuration/,
    );
  });

  describe('production secret guard', () => {
    // The placeholder shipped in .env.example. It is 34 characters long, so
    // the plain min(16) rule accepted it and a production process would boot
    // signing tokens with a key published in this repository.
    const PLACEHOLDER = 'dev-only-insecure-secret-change-me';

    it('accepts the .env.example placeholder outside production', () => {
      const env = validateEnv({ ...baseEnv, JWT_SECRET: PLACEHOLDER });

      expect(env.JWT_SECRET).toBe(PLACEHOLDER);
    });

    it('refuses the .env.example placeholder in production despite its length', () => {
      expect(PLACEHOLDER.length).toBeGreaterThan(16);
      expect(() =>
        validateEnv({ ...baseEnv, NODE_ENV: 'production', JWT_SECRET: PLACEHOLDER }),
      ).toThrow(/JWT_SECRET/);
    });

    it('refuses a secret shorter than 32 characters in production', () => {
      expect(() =>
        validateEnv({ ...baseEnv, NODE_ENV: 'production', JWT_SECRET: 'a'.repeat(31) }),
      ).toThrow(/at least 32 characters in production/);
    });

    it('accepts a long unique secret in production', () => {
      const env = validateEnv({
        ...baseEnv,
        NODE_ENV: 'production',
        JWT_SECRET: 'x'.repeat(48),
      });

      expect(env.NODE_ENV).toBe('production');
    });
  });

  describe('FX and proxy configuration', () => {
    it('defaults the pivot currency and normalises its case', () => {
      expect(validateEnv({ ...baseEnv }).FX_PIVOT_CURRENCY).toBe('USD');
      expect(validateEnv({ ...baseEnv, FX_PIVOT_CURRENCY: 'eur' }).FX_PIVOT_CURRENCY).toBe('EUR');
    });

    it('rejects a pivot currency that is not a 3-letter code', () => {
      expect(() => validateEnv({ ...baseEnv, FX_PIVOT_CURRENCY: 'EURO' })).toThrow(
        /Invalid environment configuration/,
      );
    });

    it('defaults trust-proxy hops to zero so no proxy header is believed', () => {
      expect(validateEnv({ ...baseEnv }).TRUST_PROXY_HOPS).toBe(0);
    });
  });
});

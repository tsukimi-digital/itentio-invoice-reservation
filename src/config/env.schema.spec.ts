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
});

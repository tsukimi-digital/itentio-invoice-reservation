import { scryptSync, randomBytes, timingSafeEqual, type ScryptOptions } from 'node:crypto';

/// node:crypto only — no native build dependency, so `npm ci` needs no
/// toolchain. See docs/DECISIONS.md ADR-11.

const ALGORITHM_TAG = 'scrypt';
const FIELD_SEPARATOR = '$';

/// scrypt work factors, written into the stored string so a later cost
/// increase is detectable per hash. ADR-13 records that the verify path does
/// not yet rehash on a mismatch.
const COST = 16_384;
const BLOCK_SIZE = 8;
const PARALLELISM = 1;
const SCRYPT_OPTIONS: ScryptOptions = { N: COST, r: BLOCK_SIZE, p: PARALLELISM };

const SALT_BYTES = 16;
const KEY_BYTES = 64;

/// Index of each field in `scrypt$N$r$p$salt$hash`.
const SALT_FIELD = 4;
const HASH_FIELD = 5;

export function hashPassword(plain: string): string {
  const salt = randomBytes(SALT_BYTES);
  const hash = scryptSync(plain, salt, KEY_BYTES, SCRYPT_OPTIONS);
  return [
    ALGORITHM_TAG,
    COST,
    BLOCK_SIZE,
    PARALLELISM,
    salt.toString('base64'),
    hash.toString('base64'),
  ].join(FIELD_SEPARATOR);
}

export function verifyPassword(plain: string, stored: string): boolean {
  const fields = stored.split(FIELD_SEPARATOR);
  const saltB64 = fields[SALT_FIELD];
  const hashB64 = fields[HASH_FIELD];
  if (!saltB64 || !hashB64) return false;
  const salt = Buffer.from(saltB64, 'base64');
  const expected = Buffer.from(hashB64, 'base64');
  const actual = scryptSync(plain, salt, expected.length, SCRYPT_OPTIONS);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

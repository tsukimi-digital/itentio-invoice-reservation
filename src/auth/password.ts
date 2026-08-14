import { scryptSync, randomBytes, timingSafeEqual } from 'node:crypto';

/// node:crypto only — no native build dependency, so `npm ci` needs no
/// toolchain. See docs/DECISIONS.md ADR-009.
export function hashPassword(plain: string): string {
  const salt = randomBytes(16);
  const hash = scryptSync(plain, salt, 64);
  return `scrypt$16384$8$1$${salt.toString('base64')}$${hash.toString('base64')}`;
}

export function verifyPassword(plain: string, stored: string): boolean {
  const [, , , , saltB64, hashB64] = stored.split('$');
  if (!saltB64 || !hashB64) return false;
  const salt = Buffer.from(saltB64, 'base64');
  const expected = Buffer.from(hashB64, 'base64');
  const actual = scryptSync(plain, salt, expected.length);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

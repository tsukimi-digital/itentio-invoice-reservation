/// Request-rate ceilings, kept in one place so the relationship between the
/// app-wide default and the stricter login limit stays visible. The throttler
/// counts in process memory, so these are per-replica — see ADR-13.

export const RATE_LIMIT_WINDOW_MS = 60_000;

/// Applies to every route unless a handler overrides it.
export const DEFAULT_RATE_LIMIT = 100;

/// `POST /auth/login` is the only endpoint reachable without a token and is
/// therefore the brute-force target, so it gets a far tighter ceiling.
export const LOGIN_RATE_LIMIT = 5;

/// Name of the throttler these limits configure. `@Throttle` overrides address
/// the throttler by name, so the string has to agree with the registration.
export const DEFAULT_THROTTLER = 'default';

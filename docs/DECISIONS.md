# Architecture Decision Log

This records the reasoning behind the service's architecture: what was
decided, and why. The code is written like production code, but it's still a take-home
exercise.

## ADR-01: Technology choices

- The database is **PostgreSQL**. The core requirement is an invariant that
  has to hold under concurrent writes, and that guarantee belongs in the
  database itself, through transactions and row-level locking, rather than in
  checks scattered across the application.
- **Prisma** provides the typed client and migrations for everything else,
  but the one place where correctness genuinely depends on database
  behaviour drops down to raw SQL, because that behaviour isn't something a
  query builder can guarantee.
- There's no **Redis**, no schema registry, and no metrics backend.
  Background work stays inside Postgres so it remains transactionally
  consistent with the data it operates on, at the cost of a dispatch layer
  that can't scale beyond a single instance (ADR-13).

## ADR-02: Money representation and rounding

- Money is stored as an exact decimal value scaled to its own currency,
  never as a float or a currency-agnostic integer. Different currencies use
  different numbers of decimal places, so one fixed scale can't represent
  all of them correctly.
- Currency conversion introduces exactly one rounding step, using a method
  that doesn't bias figures in either direction over a large number of
  conversions.
- An amount that's more precise than its currency allows is treated as
  invalid input and rejected, rather than silently corrected. A system whose
  whole job is tracking a credit limit can't afford to guess at what a
  client meant.

## ADR-03: FX policy

- The exchange rate for a cross-currency reservation is fixed at the moment
  capacity is committed, and repayment reverses exactly that same amount.
  Nothing about a repayment is repriced against a later rate.
- The rate that applies is always determined by server time, never by a
  value the client supplies, so a caller can never effectively choose which
  historical rate applies to its own request.
- A rate that hasn't been refreshed recently enough is treated as unusable
  rather than applied anyway, since nothing else in the system guarantees
  rates stay current.

## ADR-04: Concurrency and the capacity invariant

- Capacity limits are checked and enforced inside the same database
  statement that updates the balance, so there's never a moment where two
  concurrent requests can both believe the same capacity is still available.
- Because a programme's balance can see heavy contention, the system blocks
  concurrent writers rather than letting them race and retry; retries would
  waste more work overall than waiting does.
- The invariant that reserved capacity never exceeds the limit is also
  enforced independently at the database level, as a safety net beneath the
  application logic that normally guarantees it.

## ADR-05: Kafka delivery semantics

- Messages from the treasury system are assumed to arrive more than once,
  and the system is built around that assumption rather than trusting the
  **broker** to guarantee single delivery.
- Duplicate messages are recognised by an identity the **producer** assigns,
  not by the broker's own positional bookkeeping, which isn't a reliable way
  to recognise the same logical event twice.
- A large reconciliation payload is handed off to run in the background
  rather than processed inline, so a slow snapshot can't destabilise the
  consumer's connection to the broker.

## ADR-06: Reconciliation, baseline and replay

- Reconciliation assumes the treasury system will eventually acknowledge
  what it's been told, and treats that acknowledgement, not our own
  records, as the signal for how much local history is already accounted
  for.
- A reconciliation update is layered on top of existing local activity
  rather than replacing it outright, so work that happened locally and
  hasn't yet been acknowledged is never silently lost.
- Reconciliation updates are versioned, so one that arrives out of order or
  gets delivered twice can never undo a more recent update.

## ADR-07: Treasury lowers the limit below reserved capacity: flag, don't clamp

- When the treasury system reports a limit that's already been exceeded,
  the system doesn't force consistency by discarding real reservations. It
  accepts the state as reported and flags it for a person to resolve.
- A programme is allowed to remain over its limit rather than being silently
  corrected, since correcting it would mean inventing numbers that don't
  correspond to any real reservation.
- Different categories of mismatch are tracked separately, because the right
  response to a currency mismatch isn't the right response to a missing
  invoice or a drifted amount.

## ADR-08: Idempotency

- An idempotent replay only ever returns the response to the exact request
  that was made. Reusing an **idempotency key** against a different resource
  is treated as a new, conflicting request, not as a valid replay of the
  original.
- The system's real protection against double-processing doesn't depend on
  the client sending anything correctly. The guarantee holds even if the
  idempotency mechanism itself is bypassed entirely.
- Repeating an operation with different terms than the original is treated
  as a conflict, not confirmed as if it had already succeeded. A caller
  should never be told something happened that didn't.

## ADR-09: The transactional outbox

- Every state change that needs to reach an external system is recorded in
  the same **transaction** as the change itself, so the two can never
  disagree with each other.
- Publishing to the broker happens independently of the request that
  triggered it, so a slow or unavailable broker never blocks a
  client-facing operation.
- When delivery is uncertain, the system prefers to send a message again
  rather than risk losing it. A duplicate can be filtered out downstream; a
  message that was never sent can't be recovered.

## ADR-10: Failure handling and background work

- An error that can't be classified is treated as permanent rather than
  temporary by default. A system that always assumes "try again" can stall
  indefinitely on something that will never resolve itself.
- Anything that can't be processed is set aside for inspection, instead of
  being retried forever or silently dropped, so one bad message can't take
  the rest of the pipeline down with it.
- The system assumes any given delivery might be partial or repeated,
  rather than treating that as an edge case; that assumption is built into
  how completion is judged, not added on afterward.

## ADR-11: Authentication and authorisation

- Every endpoint requires authentication by default, with only a small,
  explicit list of exceptions. A new route is secure unless someone
  deliberately opts it out, never the other way round.
- A refresh mechanism was added specifically so a session doesn't simply end
  when the **access token** expires. It also means a compromised session can
  actually be shut down, rather than just waiting to time out on its own.
- Production is prevented from ever starting with a development-grade
  secret. A working default that ships in the repository is exactly the
  kind of thing that quietly ends up running for real.

## ADR-12: Wiring, the HTTP contract, and CI

- Application configuration lives in one place that both the running
  service and the automated tests go through, so the tests can never
  validate a setup different from what actually runs in production.
- Failures are translated into a status that reflects what actually
  happened, rather than defaulting to a generic error. A caller should be
  able to trust that the response describes the real situation.
- The **CI** environment mirrors local development as closely as possible,
  including how dependencies are checked for known vulnerabilities, so a
  passing build is a meaningful signal rather than a formality.

## ADR-13: Deliberately not implemented

These are gaps, not decisions: things a production deployment would want
that this exercise doesn't have. Everything here is known and considered,
and none of it affects the correctness of capacity accounting or running
the service locally.

- There's no observability layer. Production would need one, to make
  debugging and incident response practical instead of guesswork.
- There's no tenant isolation. Production would need it, so one
  customer's programmes and balances stay separate from another's.
- Release only supports releasing an invoice's full amount. Production
  would need partial release too, since real repayments are often made
  in instalments.
- An access token can't be revoked before it expires. Production would
  need that, so a compromised or deactivated account can be shut out
  immediately rather than waiting out the token's lifetime.
- Login throttling only limits by IP address, not by account, and each
  replica counts independently since there's no shared store like Redis.
  Production would need both: per-account limits and a shared counter
  across the fleet.
- Idempotency keys are never expired or cleaned up. Production would
  need that, to keep the table from growing without bound.
- A message that lands in the dead-letter table has no way to be
  reprocessed. Production would need that, so a fixable failure doesn't
  require a manual database intervention to recover.
- Background dispatch can't safely run on more than one replica. Production
  handling real volume would need that, both for throughput and to survive
  a single instance going down.
- The usual HTTP security headers aren't set. Production would need
  them as a baseline defence, even though the immediate risk here is low.
- Exchange rates come from a table seeded once by hand, not from a live
  feed. Production would need an ingestion pipeline from a real rate
  source, since a manually maintained table doesn't track actual market
  movement.
- A currency conversion routed through a third currency doesn't preserve
  both intermediate rates. Production would need that level of detail for a
  complete audit trail.
- There's no retention policy for the inbox table that deduplicates
  Kafka messages. Production would need one, sized against the topic's
  own retention, so an offset reset can't cause double-processing.
- The locking strategy needs a stable session per transaction, which a
  transaction-pooling connection pooler doesn't provide. Production
  behind one would need the capacity and reconciliation paths on a
  direct connection to the primary, or a pooler configured in
  session-pooling mode.
- There's no caching layer. Production would benefit from caching
  read-mostly data, such as FX rate resolution and programme/currency
  metadata, while leaving the live reserved/limit balance uncached.

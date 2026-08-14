# Architecture Decision Log

Each entry: context, decision, alternatives rejected, consequences. Numbered
sequentially in the order the decision was made, not by implementation order.

## ADR-000: Domain-skill selection

**Context:** Five community skills (`currencies-and-fx`, `kafka-streams-programming`,
`kafka-schema-registry`, `kafka-consumer-lag`, `alpaca-broker-reconciliation-idempotency`)
were installed locally and read in full to check for transferable guidance before
designing the capacity/reconciliation domain from scratch.

**Decision:** Keep `kafka-schema-registry`, `kafka-consumer-lag`, and
`alpaca-broker-reconciliation-idempotency`. Remove `currencies-and-fx` and
`kafka-streams-programming`.

**Alternatives rejected:** Keeping all five "just in case" — rejected because
`kafka-streams-programming`'s WarpStream section recommends
`enable.idempotence=false`, which is actively wrong on plain Kafka and a real
risk if copied uncritically later.

**Consequences:** `currencies-and-fx` is portfolio-FX-on-floats (forwards, carry
trade) with zero coverage of decimal storage or rate versioning — money handling
here is designed from scratch (ADR-001, ADR-003). `kafka-streams-programming` is
JVM Kafka Streams; ~70% has no kafkajs analogue — the Kafka consumer here
(Task 10) is designed against kafkajs's actual API, not ported from it.

## ADR-001: Money representation — `numeric(24,4)` + `Decimal`, not integer minor units

**Context:** ISO-4217 currencies have different minor-unit exponents: JPY/KRW 0
decimal places, GBP/USD/EUR 2, KWD/BHD/JOD 3, CLF 4. The assignment requires
multi-currency support, so a single hardcoded `× 100` cannot be correct.

**Decision:** Store money as `numeric(24,4)` in Postgres (scale 4 covers every
live ISO-4217 exponent exactly) and operate on it in code via `Prisma.Decimal`
(decimal.js, already bundled with `@prisma/client`).

**Alternatives rejected:**
- *BigInt minor units* (`1823` for £18.23) — exact and fast, but not
  self-describing: the same integer means something different depending on a
  currency column that lives elsewhere. Every ad-hoc SQL query, log line, and
  downstream export must correctly apply the right exponent or silently
  mis-states money by 100×–1000×. In a system whose entire job is enforcing a
  credit limit, a silent 100× is the worst available failure mode.
- *`float`/`double`* — IEEE-754 binary floats cannot represent most decimal
  fractions exactly; rejected outright, not just for scale reasons.

**Consequences:** `CHECK (reserved_amount <= total_limit)` and
`SUM(delta_reserved)` work directly in SQL. decimal.js's default precision (20
significant digits) would silently truncate FX division, so the app clones a
dedicated `Dec` constructor (`precision: 34, rounding: ROUND_HALF_EVEN`) rather
than calling the global `Decimal.set()`, which would otherwise also change how
Prisma itself deserialises query results. API responses emit amounts as JSON
strings (`"1234.56"`), never numbers, because JSON numbers are IEEE-754 doubles.

## ADR-002: Locale — UK market, `en-GB` separators

**Context:** The service targets the UK market. `en-GB` uses `.` as the decimal
separator and `,` as the thousands separator (`1,234.56`) — the opposite of
continental European convention (`1.234,56`), so this cannot be assumed generic.

**Decision:** The default/example currency is GBP (2 decimal places). Any
human-facing formatting uses `Intl.NumberFormat('en-GB', { style: 'currency',
currency: 'GBP' })`. The API itself never accepts or emits locale-formatted
decimal strings — amounts cross the wire as plain, unambiguous decimal strings
(`"1234.56"`, no thousands separator) precisely to avoid the `,`-vs-`.` question
at the boundary. If free-text decimal input is ever accepted from a human, it
must be parsed strictly against `en-GB` conventions, never with `parseFloat`.

**Consequences:** No thousands separators appear in API payloads at all —
ambiguity is designed out rather than handled.

## ADR-003: FX rate frozen at reservation time

**Context:** An invoice may be denominated in a different currency than its
program. A rate must be chosen somewhere in the reserve→release lifecycle.

**Decision:** Convert at reservation time using the rate then in effect; store
the resulting program-currency amount, the rate, its source, and its
`as_of` on the invoice. Release returns exactly that stored amount — it is never
recomputed at a new rate.

**Alternatives rejected:** Recomputing at release time — more "current," but
capacity would not return to exactly its prior level, leaving an FX residue
that has to be booked somewhere every single release. Frozen-rate makes
capacity return to zero drift by construction.

**Consequences:** Rounding happens exactly once (ADR-010), at the point of
conversion; release does no arithmetic at all, just replays a stored number.

## ADR-004 / ADR-005: Reconciliation baseline+replay, and the topology assumption

**Context:** The assignment says bulk reconciliation messages "bring a program's
full state up to date," but does not say whether the treasury system is aware
of reservations made through our API — i.e., whether `reserved_amount` in a
snapshot already includes reservations we made since the last snapshot.

**Decision:** Assume a **bidirectional** contract: every reservation publishes
an outbound event carrying its local ledger `seq` (via a transactional outbox,
Task 9); treasury's bulk snapshot echoes back `acknowledged_local_seq`, the
highest local `seq` it has folded into its reported `reserved_amount`. On
reconciliation, replay = local ledger entries with `seq > acknowledged_local_seq`,
summed and added to the snapshot's `reserved_amount` to get the new balance.
The correction is written as one `RECONCILE_BASELINE` ledger entry — the
existing ledger is never rewritten or deleted.

**Alternatives rejected:**
- *Snapshot wins unconditionally* — simplest, but silently discards concurrent
  reservations made through our API; in a financial system that's lost money,
  not just a UX rough edge.
- *Reject stale snapshots* — safe but stops reconciliation from working at all
  once reservations happen more often than snapshots arrive.
- *Time-based replay* (`occurred_at > as_of`) — the fallback used only when a
  snapshot omits the ack watermark (Task 12); biased to over-replay
  deliberately, because over-replaying under-states available capacity (a
  false rejection, recoverable) while under-replaying risks overcommitting the
  credit line (a real loss).

**Consequences:** This is a genuine assumption about a contract the assignment
doesn't specify, made explicit here rather than silently baked into the code,
specifically so it can be challenged in review. If the real topology is
"treasury never sees our reservations," the replay rule is simpler (replay
*everything* since the last baseline) and must be reconfigured, not
auto-detected.

## ADR-006: Pessimistic locking on the program row

**Context:** Concurrent reservations against the same program must never
together exceed its limit.

**Decision:** `SELECT ... FOR NO KEY UPDATE` on the program row inside a
`READ COMMITTED` transaction, before any capacity check.

**Alternatives rejected:** Optimistic locking (version column + retry) — under
sustained contention on a single hot row, optimistic retries do more total
work and burn more connections than blocking does; pessimistic locking is
exactly the tool for "one row, contended, low-to-moderate arrival rate."
`SERIALIZABLE` isolation was also rejected: it adds predicate-lock overhead for
a guarantee the row lock already provides, and turns the same contention into
a `40001` retry the application would have to handle anyway.

**Consequences:** `FOR NO KEY UPDATE` specifically (not plain `FOR UPDATE`) —
it doesn't conflict with the `FOR KEY SHARE` lock Postgres takes for FK checks
on every `invoice`/`capacity_ledger_entry` insert, so those aren't serialized
behind reservations unnecessarily.

## ADR-007 / ADR-008: At-least-once Kafka, and why the consumer never applies a snapshot inline

**Context:** The sink for Kafka messages is Postgres, not another Kafka topic.
Kafka exactly-once semantics only guarantee atomicity between topics and the
consumer-offsets topic — they say nothing about an external database write.

**Decision:** Consume at-least-once; make every DB write idempotent via a
dedup table keyed on the producer-supplied `event_id`. Separately: a bulk
snapshot is never processed inside the Kafka consumer's message handler — it's
persisted to a `reconciliation_job` row (and chunked `reconciliation_snapshot_position`
rows) and processed by an independent worker loop.

**Alternatives rejected:** Chasing Kafka EOS for the consumer — would add
real cost (lower throughput, mandatory short commit intervals, 3-broker
minimum) for a guarantee that doesn't reach the actual sink. Applying the
snapshot synchronously inside `eachMessage`/`eachBatch` — kafkajs has no
`max.poll.interval.ms` (verified against its type definitions; it's not a
port of the Java client's poll loop), so a long single-message DB transaction
instead starves the heartbeat until `sessionTimeout` elapses, the consumer is
evicted, the group rebalances, and the same slow message is redelivered into
the same slow handler — a rebalance loop.

**Consequences:** The consumer's job is to be fast (single-digit
milliseconds per message) and durable (inbox row + job enqueue in one
transaction); the worker's job is to be correct and can safely take longer,
because nothing about Kafka group membership depends on it.

## ADR-009: Auth — JWT HS256, seeded users

**Context:** "All endpoints must be authenticated" and "the service should be
runnable locally" — no external identity provider, no network dependency.

**Decision:** `@nestjs/jwt` with HS256, a `POST /auth/login` endpoint against
seeded users (password hashed with `node:crypto.scrypt` — no native
dependency, so `npm ci` doesn't require a build toolchain), a global
`JwtAuthGuard` with an explicit `@Public()` override for `/health` only.

**Consequences:** `JWT_SECRET` in `.env.example` is a placeholder rejected by
env validation once too short; documented already in the scaffold's README.

## ADR-010: Rounding — `ROUND_HALF_EVEN`, exactly once

**Context:** FX conversion must round somewhere; a biased rounding mode
compounds across thousands of reservations into a systematic drift.

**Decision:** `ROUND_HALF_EVEN` (banker's rounding), applied exactly once in
`FxService.convert`, at the point the converted amount is quantised to the
target currency's minor-unit scale. The rate itself is never rounded; where a
stored rate must be inverted, the code divides rather than multiplying by a
materialised reciprocal, avoiding a second rounding pass.

**Alternatives rejected:** `ROUND_HALF_UP` — biases every tied conversion
upward, a small systematic drift across a large reservation volume for no
benefit, since the guarded `UPDATE` already makes overcommit impossible
regardless of rounding direction.

## ADR-011: Treasury lowers the limit below already-reserved capacity — flag, don't clamp

**Context:** A bulk reconciliation snapshot may report a `total_limit` lower
than the program's current `reserved_amount` (treasury cut the programme's
credit line after we already reserved against the old, higher limit). The
`program_no_overcommit` CHECK constraint would otherwise block reconciliation
from applying at all.

**Decision:** Apply the new limit and the recomputed `reserved_amount` as-is,
set `over_commit_acknowledged = true` (the one escape hatch the CHECK
constraint allows, writable only from the reconciliation path — the API
reservation path can never set it), and record a `reconciliation_discrepancy`
row of kind `OVER_LIMIT_AFTER_BASELINE`. The books stay true; the
inconsistency is visible and auditable rather than hidden.

**Alternatives rejected:** Clamping `reserved_amount` down to the new limit —
simpler, but the clamped number would then correspond to no real sum of
reservations, directly contradicting the ledger's role as the source of truth
(`reserved_amount` must always equal `SUM(delta_reserved)` over the ledger,
which `v_program_ledger_drift` from Task 1 can verify at any time).

**Consequences:** A program can sit in an over-limit state until a human
resolves it (renegotiate the limit, or release enough reservations). This is
a business decision surfaced to operators, not a system that silently forces
consistency by discarding real reservations.

## ADR-012: Known operational limitations — out of scope for this assignment

**Context:** Three operational-maturity gaps were identified during design
review and deliberately left undocumented-but-unbuilt, because they matter
for sustained production operation (dashboards, alerting, connection-pooler
topology) rather than for a locally-run demonstration of the core domain.

**Decision:** Document, don't implement, for this submission:

- **Dedup-table retention vs. Kafka topic retention.** `processed_message`
  rows are never pruned in this implementation. In a long-running deployment,
  the pruning TTL must exceed the Kafka topic's own retention plus the
  maximum expected consumer-group reset window — pruning too early risks
  double-processing on an offset reset.
- **Snapshot staleness alerting.** If treasury stops sending bulk
  reconciliation snapshots, `treasury_baseline_as_of` simply stops advancing;
  nothing currently alerts on that. A production deployment would monitor
  `now() - treasury_baseline_as_of` per program and page above a threshold.
- **PgBouncer transaction-pooling mode is incompatible with this design.**
  `SET LOCAL lock_timeout` and Prisma's interactive transactions require a
  stable session for the transaction's duration. Session pooling or a direct
  connection is required; this doesn't affect local Docker Compose use, where
  Prisma connects directly to Postgres.

**Consequences:** Explicitly named here so a reviewer sees they were
considered and consciously deferred, not missed.

# Architecture Decision Log

This is the primary technical write-up for this submission. It records the
decisions taken while building the service — why each approach, technology and
constraint was chosen, and what was rejected on the way.

**How to read it.** Each entry is context, decision, alternatives rejected,
consequences. The file describes the system **as it stands**: it is a record of
the decisions in force, not a changelog. Numbers are stable identifiers cited
from source comments (`// see docs/DECISIONS.md ADR-0xx`), not a reading order.

**On scope.** The brief says "treat this as production code — if you make
assumptions or trade-offs, document them briefly". This service is written that
way, but it is still an exercise rather than a deployed system, and a handful of
things a production deployment genuinely needs are deliberately absent. Those
are not omissions by accident: **ADR-012** and **ADR-034** name every one of
them, say what production would require, and say why it was left out here. If
something looks missing, it should be in one of those two entries — and if it
is not, that is a fair thing to hold against the submission.

**Where the interesting decisions are, if you are short of time:**

| Theme | Entries |
|---|---|
| Technology choices and what they cost | ADR-035 |
| Money, precision and rounding | ADR-001, ADR-010, ADR-020, ADR-002 |
| FX policy | ADR-003, ADR-019 |
| Preventing overcommit under concurrency | ADR-006 |
| Kafka delivery semantics and failure handling | ADR-007/008, ADR-023, ADR-025, ADR-027 |
| Reconciliation, and the contract it assumes | ADR-004/005, ADR-026, ADR-028, ADR-011 |
| Idempotency and the outbox | ADR-021, ADR-022, ADR-024 |
| Auth | ADR-009, ADR-016, ADR-017, ADR-018, ADR-031, ADR-032 |
| Background work: leases, retries, backoff | ADR-029, ADR-024 |
| Deliberately not built | ADR-012, ADR-034 |

## ADR-035: Technology choices

**Context:** The brief fixes the domain and the integration style (HTTP API,
Kafka ingestion, multi-currency, authenticated, runnable locally) but not the
stack.

**Decision and reasoning, component by component:**

- **PostgreSQL.** The central requirement is an invariant — reserved capacity
  must never exceed a limit — under concurrency. That calls for real
  transactions, row-level locking and `CHECK` constraints, so the guarantee can
  live in the database rather than in application politeness (ADR-006). Every
  other choice here follows from picking a database that can enforce the
  domain's core rule itself.
- **Prisma**, for typed migrations and a typed client — with a deliberate
  exception: the capacity path drops to `$queryRaw`, because `FOR NO KEY UPDATE`
  and a conditional `UPDATE … WHERE … RETURNING` are not expressible through the
  query builder, and those two statements *are* the concurrency design.
  *Rejected:* raw `pg` throughout (more control, but hand-rolled migrations and
  no type safety across the schema); TypeORM (heavier, and its migration story
  is weaker).
- **NestJS.** Dependency injection and module boundaries keep the capacity
  domain, the Kafka consumer, the reconciliation worker and the HTTP API
  independently testable. More decisively, guards, pipes and filters are
  first-class and global, which is what makes "all endpoints are authenticated"
  a default with an explicit `@Public()` opt-out rather than a rule enforced
  per-route and eventually forgotten (ADR-009). *Rejected:* bare Express or
  Fastify — lighter for three endpoints, but auth and validation would become
  per-handler discipline.
- **kafkajs directly**, not `@nestjs/microservices`' Kafka transport. The
  transport abstracts away `eachBatch`, manual offset resolution and heartbeat
  control — precisely the levers the delivery-semantics design depends on
  (ADR-007/008). An abstraction that hides the thing you are reasoning about is
  a liability.
- **zod for wire and environment data; class-validator for HTTP DTOs.** Both,
  on purpose. Inbound HTTP bodies are classes with decorators, which is what
  Nest's `ValidationPipe` consumes; Kafka messages and `process.env` are plain
  data, where zod parses and narrows in one step and gives a discriminated union
  for a multi-schema topic.
- **decimal.js**, reached through `Prisma.Decimal` — exact decimal arithmetic
  with no new dependency, since it already ships inside the Prisma client
  (ADR-001).
- **Jest with Testcontainers** for the tests that matter. The invariants under
  test are database constraints and row locks; a mocked database would prove
  nothing about either, so the concurrency and constraint specs run against a
  real, disposable Postgres.

**Deliberately not added:** Redis (nothing needs a second datastore — the outbox
and job queue live in Postgres, which keeps them transactional with the data
they describe); a Schema Registry (the zod schemas pin the contract for this
consumer, and a registry is infrastructure the exercise cannot stand up);
OpenTelemetry or a metrics backend (ADR-034).

**Consequences:** The stack is heavier than three endpoints justify. That is a
conscious reading of "treat this as production code": the shape is chosen for
where the system would go, not for the smallest thing that satisfies the brief.

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
  mis-state money by 100×–1000×. In a system whose entire job is enforcing a
  credit limit, a silent 100× is the worst available failure mode.
- *`float`/`double`* — IEEE-754 binary floats cannot represent most decimal
  fractions exactly; rejected outright, not just for scale reasons.

**Consequences:** `CHECK (reserved_amount <= total_limit)` and
`SUM(delta_reserved)` work directly in SQL. decimal.js's default precision (20
significant digits) would silently truncate FX division, so the app clones a
dedicated `Dec` constructor (`precision: 34, rounding: ROUND_HALF_EVEN`,
widened `toExpNeg`/`toExpPos` so `.toString()` cannot emit exponential notation
into a `::numeric` cast) rather than calling the global `Decimal.set()`, which
would also change how Prisma itself deserialises query results.

**Formatting rule at the boundary:** `Prisma.Decimal#toString()` does *not*
preserve trailing zeros — `30.00` renders as `"30"`. Every money field in an
HTTP response is therefore formatted with `.toFixed(minorUnits)`: via
`Money#toString()` in the domain layer, or explicitly where a raw
`Prisma.Decimal` is read from a query result (`CapacityService.toView()`,
`getAvailability()`, `ReconciliationController.list()`). Bare `.toString()`
remains in internal use — feeding `Dec` arithmetic, and in ledger `metadata` /
discrepancy `detail` JSON — but never for a value that reaches a response body.
This also protects against `serializeBigInts`, which walks response objects
with `Object.entries` and would render a `Decimal` as its internals (ADR-034).

## ADR-002: Locale — UK market, `en-GB` separators

**Context:** The service targets the UK market. `en-GB` uses `.` as the decimal
separator and `,` as the thousands separator (`1,234.56`) — the opposite of
continental European convention (`1.234,56`), so this cannot be assumed generic.

**Decision:** The default/example currency is GBP (2 decimal places). Any
human-facing formatting uses `Intl.NumberFormat('en-GB', { style: 'currency',
currency: 'GBP' })`. The API itself never accepts or emits locale-formatted
decimal strings — amounts cross the wire as plain, unambiguous decimal strings
(`"1234.56"`, no thousands separator) precisely to avoid the `,`-vs-`.`
question at the boundary. If free-text decimal input is ever accepted from a
human, it must be parsed strictly against `en-GB` conventions, never with
`parseFloat`.

**Consequences:** No thousands separators appear in API payloads at all —
ambiguity is designed out rather than handled.

## ADR-003: FX rate frozen at reservation time

**Context:** An invoice may be denominated in a different currency than its
programme. A rate must be chosen somewhere in the reserve→release lifecycle.

**Decision:** Convert at reservation time using the rate then in effect; store
the resulting programme-currency amount, the rate, its source and its `as_of`
on the invoice. Release returns exactly that stored amount — it is never
recomputed at a new rate.

**Alternatives rejected:** Recomputing at release time — more "current", but
capacity would not return to exactly its prior level, leaving an FX residue
that has to be booked somewhere on every single release. A frozen rate makes
capacity return to zero drift by construction.

**Consequences:** Rounding happens exactly once (ADR-010, ADR-020), at the
point of conversion; release does no arithmetic at all, just replays a stored
number. This is why a treasury delta cannot be currency-converted either — it
has no invoice to freeze a rate against (ADR-028).

## ADR-004 / ADR-005: Reconciliation baseline+replay, and the topology assumption

**Context:** The assignment says bulk reconciliation messages "bring a
programme's full state up to date", but does not say whether the treasury
system is aware of reservations made through our API — i.e. whether
`reserved_amount` in a snapshot already includes reservations we made since
the last snapshot.

**Decision:** Assume a **bidirectional** contract. Every capacity mutation made
through the API publishes an outbound event carrying its local ledger `seq`
via a transactional outbox — reservations *and* releases, without exception
(ADR-022), because the watermark below is only meaningful if the sequence
treasury sees is contiguous. Treasury's bulk snapshot echoes back
`acknowledged_local_seq`, the highest local `seq` it has folded into its
reported `reserved_amount`. On reconciliation, replay = local ledger entries
above that watermark, summed and added to the snapshot's `reserved_amount` to
get the new balance. The correction is written as one `RECONCILE_BASELINE`
ledger entry; the existing ledger is never rewritten or deleted.

Treasury-origin entries are fenced by their own watermark,
`included_through_event_seq`, compared against the `treasury_event_seq`
recorded on each entry (ADR-026).

**Alternatives rejected:**
- *Snapshot wins unconditionally* — simplest, but silently discards concurrent
  reservations made through our API; in a financial system that is lost money,
  not a UX rough edge.
- *Reject stale snapshots* — safe, but stops reconciliation working at all once
  reservations happen more often than snapshots arrive.

**Fallback when a watermark is absent:** a snapshot may omit
`acknowledged_local_seq` or `included_through_event_seq`. Entries are then
replayed on the basis of `occurred_at > as_of - 5 minutes`. The skew margin is
deliberate: over-replaying under-states available capacity (a false rejection,
recoverable), while under-replaying risks overcommitting the credit line (a
real loss), so the bias is placed firmly on the recoverable side. The
`RECONCILE_BASELINE` entry's metadata records which strategy was used
(`replayStrategy: 'watermark' | 'time-fallback'`) so the choice is auditable
after the fact.

**Consequences:** This is a genuine assumption about a contract the assignment
does not specify, made explicit here rather than baked silently into the code,
specifically so it can be challenged. If the real topology is "treasury never
sees our reservations", the replay rule is simpler — replay *everything* since
the last baseline — and must be reconfigured, not auto-detected.

## ADR-006: Pessimistic locking on the programme row

**Context:** Concurrent reservations against the same programme must never
together exceed its limit.

**Decision:** `SELECT ... FOR NO KEY UPDATE` on the programme row inside a
`READ COMMITTED` transaction, before any capacity check. The check itself
lives in the `WHERE` clause of the debiting `UPDATE`, so there is no
read-modify-write window at all; zero affected rows *is* the rejection signal.

**Alternatives rejected:** Optimistic locking (version column + retry) — under
sustained contention on a single hot row, optimistic retries do more total work
and burn more connections than blocking does; pessimistic locking is exactly
the tool for "one row, contended, low-to-moderate arrival rate". `SERIALIZABLE`
isolation was also rejected: it adds predicate-lock overhead for a guarantee
the row lock already provides, and turns the same contention into a `40001`
retry the application would have to handle anyway.

**Consequences:** `FOR NO KEY UPDATE` specifically, not plain `FOR UPDATE` — it
does not conflict with the `FOR KEY SHARE` lock Postgres takes for FK checks on
every `invoice`/`capacity_ledger_entry` insert, so those are not serialised
behind reservations unnecessarily.

**Scope note:** this applies to every lock taken on `program`/`invoice` rows for
a capacity mutation. The exceptions are job-queue claims —
`ReconciliationWorker.claimNextJob()` and the outbox relay's batch claim — which
use `FOR UPDATE SKIP LOCKED`. That is a different pattern (a worker grabbing the
next unclaimed row and skipping rows others hold), so the FK-contention
rationale above does not apply and the standard idiom is correct there.

## ADR-007 / ADR-008: At-least-once Kafka, and why the consumer never applies a snapshot inline

**Context:** The sink for Kafka messages is Postgres, not another Kafka topic.
Kafka exactly-once semantics only guarantee atomicity between topics and the
consumer-offsets topic — they say nothing about an external database write.

**Decision:** Consume at-least-once; make every DB write idempotent via an
inbox table keyed on the producer-supplied `event_id`. The inbox insert and the
state change share one transaction, and the offset is resolved only after that
transaction commits, which makes the pipeline effectively-once at the sink.

Separately, a bulk snapshot is never processed inside the message handler — it
is persisted as a `reconciliation_job` row (with chunked
`reconciliation_snapshot_position` rows) and processed by an independent worker
loop.

**Alternatives rejected:** Chasing Kafka EOS for the consumer — real cost (lower
throughput, mandatory short commit intervals, 3-broker minimum) for a guarantee
that does not reach the actual sink. Applying the snapshot synchronously inside
`eachBatch` — kafkajs has no `max.poll.interval.ms` (verified against its type
definitions; it is not a port of the Java client's poll loop), so a long
single-message DB transaction starves the heartbeat until `sessionTimeout`
elapses, the consumer is evicted, the group rebalances, and the same slow
message is redelivered into the same slow handler: a rebalance loop.

**Consequences:** The consumer's job is to be fast (single-digit milliseconds
per message) and durable (inbox row plus job enqueue in one transaction); the
worker's job is to be correct and may take longer, because nothing about Kafka
group membership depends on it.

## ADR-009: Auth — JWT HS256, seeded users

**Context:** "All endpoints must be authenticated" and "the service should be
runnable locally" — no external identity provider, no network dependency.

**Decision:** `@nestjs/jwt` with HS256, a `POST /auth/login` endpoint against
seeded users (password hashed with `node:crypto.scrypt` — no native dependency,
so `npm ci` needs no build toolchain), and a global `JwtAuthGuard` with an
explicit `@Public()` override. Exactly three routes are public: `/health`
(a liveness probe exposing no business data), `POST /auth/login`, and
`POST /auth/refresh` (a refresh token is not a bearer JWT, so `JwtAuthGuard`
does not apply).

**Consequences:** `.env.example` ships a working development secret so that
`cp .env.example .env` yields a runnable service. What prevents that value
reaching production is an explicit refusal at boot, not the absence of the
example — see ADR-032.

Login always runs the key-derivation function, verifying against a fixed dummy
hash when the e-mail is unknown, so response latency does not distinguish an
existing account from a missing one (ADR-031).

## ADR-010: Rounding — `ROUND_HALF_EVEN`, exactly once

**Context:** FX conversion must round somewhere; a biased rounding mode
compounds across thousands of reservations into a systematic drift.

**Decision:** `ROUND_HALF_EVEN` (banker's rounding), applied exactly once per
conversion, in `FxService.convert`, at the point the converted amount is
quantised to the target currency's minor-unit scale. The rate itself is never
rounded; where a stored rate must be inverted, the code divides rather than
multiplying by a materialised reciprocal, avoiding a second rounding pass.

An inbound API amount is *not* a rounding opportunity: an amount carrying more
precision than its own currency permits is rejected as a `400` rather than
quietly re-scaled (ADR-020).

**Alternatives rejected:** `ROUND_HALF_UP` — biases every tied conversion
upward, a small systematic drift across a large reservation volume for no
benefit, since the guarded `UPDATE` already makes overcommit impossible
regardless of rounding direction.

**Consequences:** The mode is verified on genuine ties — `money.spec.ts`
asserts `18.235 → 18.24`, `18.245 → 18.24` and JPY `2.5 → 2`, `3.5 → 4`, all of
which fail under `ROUND_HALF_UP`. A non-tie value such as `18.239` proves
quantisation but says nothing about the rounding policy, so it is not relied on
for that.

## ADR-011: Treasury lowers the limit below already-reserved capacity — flag, don't clamp

**Context:** A bulk reconciliation snapshot may report a `total_limit` lower
than the programme's current `reserved_amount` (treasury cut the credit line
after we already reserved against the old, higher one). The
`program_no_overcommit` CHECK constraint would otherwise block reconciliation
from applying at all.

**Decision:** Apply the new limit and the recomputed `reserved_amount` as-is,
set `over_commit_acknowledged = true` (the one escape hatch the CHECK allows,
writable only from the reconciliation path — the API reservation path can never
set it), and record a `reconciliation_discrepancy` row of kind
`OVER_LIMIT_AFTER_BASELINE`.

**Alternatives rejected:** Clamping `reserved_amount` down to the new limit —
simpler, but the clamped number would correspond to no real sum of
reservations, directly contradicting the ledger's role as source of truth
(`reserved_amount` must always equal `SUM(delta_reserved)` over the ledger,
which `v_program_ledger_drift` can verify at any time).

**Consequences:** A programme can sit in an over-limit state until a human
resolves it (renegotiate the limit, or release enough reservations). That is a
business decision surfaced to operators, not a system that silently forces
consistency by discarding real reservations — so it has to actually reach an
operator. Every discrepancy is logged at `warn` as it is recorded, and
`GET /programs/:programRef/discrepancies` (ADMIN only) exposes them over HTTP.

Discrepancies are raised for three kinds: `OVER_LIMIT_AFTER_BASELINE` above,
`UNKNOWN_INVOICE` for a snapshot position with no local counterpart, and
`AMOUNT_DRIFT` where the amounts disagree. A position whose currency differs
from the invoice's is raised as `CURRENCY_MISMATCH` rather than compared as a
bare number, since comparing amounts across currencies is meaningless.

## ADR-012: Operational limitations of the running system

**Context:** Three constraints bind a long-running deployment rather than a
locally-run demonstration. They are properties of how the service behaves over
time and under a real topology, so unlike ADR-034's list they cannot be closed
by writing more code alone — each needs an operational decision to go with it.

**Decision:** Document, don't implement:

- **Dedup-table retention vs. Kafka topic retention.** `processed_message` rows
  are never pruned. In a long-running deployment the pruning TTL must exceed the
  Kafka topic's own retention plus the maximum expected consumer-group reset
  window — pruning too early risks double-processing on an offset reset.
- **Snapshot staleness alerting.** If treasury stops sending bulk snapshots,
  `treasury_baseline_as_of` simply stops advancing and nothing alerts on it. A
  production deployment would monitor `now() - treasury_baseline_as_of` per
  programme and page above a threshold.
- **PgBouncer transaction-pooling mode is incompatible with this design.**
  `SET LOCAL lock_timeout` and Prisma's interactive transactions require a
  stable session for the transaction's duration. Session pooling or a direct
  connection is required; this does not affect local Docker Compose use, where
  Prisma connects directly to Postgres.

**Consequences:** Each is a property a reviewer could reasonably probe, so each
is named rather than left to be discovered. The first two would be caught by the
observability work ADR-034 defers; the third is a deployment-topology
constraint that the design accepts in exchange for `SET LOCAL lock_timeout` and
interactive transactions, which the concurrency guarantee depends on.

## ADR-013: The Kafka consumer self-provisions its topic

**Context:** A consumer subscribing to a topic nothing has ever produced to
races the broker's auto-create and throws an uncaught (though technically
"retriable") `KafkaJSProtocolError: UNKNOWN_TOPIC_OR_PARTITION`. In a real
deployment the treasury system owns and provisions `treasury.capacity-events`
out-of-band (Terraform, an admin script). Locally, nothing does.

**Decision:** The consumer calls `kafka.admin().createTopics(...)` for its topic
before subscribing, catching and logging any error rather than throwing.
`createTopics` is idempotent — a no-op if the topic already exists — so this is
always safe to run, including against a treasury-managed topic.

**Alternatives rejected:** Requiring the topic to be created out-of-band before
first run — correct for production, but leaves the service unable to start
cleanly from a freshly reset local environment without a manual step, which
conflicts with the assignment's "runnable locally" requirement.

**Consequences:** Combined with ADR-027, a broker that is unreachable at startup
does not prevent the service from serving HTTP; provisioning and subscription
are retried in the background.

## ADR-014: CI runs both Postgres and Kafka services

**Context:** The e2e suite drives real Kafka: `test/kafka-consumer.e2e-spec.ts`
and `test/reconciliation.e2e-spec.ts` publish messages and assert on the
resulting database state, and every full-`AppModule` spec boots the consumer.

**Decision:** `.github/workflows/ci.yml` runs a `kafka` service alongside
`postgres`, using the exact same single-node KRaft configuration as
`docker-compose.yml`, so CI matches local development rather than drifting into
a second, subtly different configuration to maintain. Both services carry a
health check, so readiness is a gate rather than a function of how long `npm ci`
happens to take.

**Consequences:** CI runs lint → build → unit → e2e. Because the e2e suite now
exercises the Kafka path rather than merely requiring a broker to boot, a green
build is evidence about the consumer, the dead-letter path and reconciliation,
not only about the HTTP API.

## ADR-015: `app.enableShutdownHooks()`

**Context:** NestJS `OnModuleDestroy` hooks (Kafka consumer/producer disconnect,
the background workers' intervals) only run on an explicit `app.close()` unless
`enableShutdownHooks()` is called — a real `SIGTERM` (container restart,
`docker stop`, orchestrator rolling deploy) would otherwise bypass them
entirely, leaving the consumer to sit in its group until it times out.

**Decision:** Call `app.enableShutdownHooks()` immediately after
`NestFactory.create(AppModule)` in `main.ts`.

**Consequences:** Both background loops also *drain* rather than merely stopping
their timers: `onModuleDestroy` awaits the in-flight batch before disconnecting
the producer, so a shutdown cannot leave work publishing into a torn-down client
and recording failures against messages that may have been delivered.

## ADR-016: Rate limiting, and `tokenVersion` left inert

**Context:** `POST /auth/login` is the one endpoint reachable without a token
and is therefore the brute-force target. Separately, `User.tokenVersion` is
carried in every JWT's claims.

**Decision (rate limiting):** `@nestjs/throttler`, registered as a global
`APP_GUARD` alongside `JwtAuthGuard` — NestJS supports multiple concurrent
global guards, they simply all run per-request. A `'default'` named throttler
(100 req/min) covers every route; `POST /auth/login` overrides it with
`@Throttle({ default: { limit: 5, ttl: 60_000 } })`.

Throttling keys on `req.ip`, which is only meaningful if Express is told how
many proxy hops to trust — hence `TRUST_PROXY_HOPS` (ADR-033). The store is
in-process, so limits are per-replica and reset on deploy; a shared store would
be required for a multi-replica deployment to hold a global limit.

**Decision (`tokenVersion`):** No enforcement logic. It is populated into the
JWT at login and is otherwise inert — a deliberately unbuilt revocation
mechanism, not an oversight. Server-side revocation exists and is real, but it
works through the `refresh_token` table (ADR-018, ADR-031), not by bumping a
JWT claim. The consequence is that an access token remains valid for its full
`JWT_EXPIRES_IN` after a role change or deactivation.

**Decision (e2e concurrency):** `test:e2e` always runs `--runInBand`. Several
independently-booted `AppModule` instances, each with its own kafkajs consumer
and admin client, contend on a single-node local broker; serial execution is
simpler than tuning worker counts or adding kafkajs retry/backoff, matches how a
single-broker CI service should be exercised anyway, and the suite runs in under
a minute.

## ADR-017: Role enforcement (RBAC)

**Context:** `JwtAuthGuard` verifies that a token is valid but says nothing
about what its bearer may do.

**Decision:** `@Roles(...roles: UserRole[])` (`SetMetadata`, the same pattern as
`@Public()`) plus a `RolesGuard` reading required roles via
`Reflector.getAllAndOverride` and comparing against `request.user.role`.
Registered as a global `APP_GUARD` in the same `AuthModule.providers` array as
`JwtAuthGuard`, listed after it — guard array order is what guarantees
`request.user` is populated before `RolesGuard` reads it, and relying on
cross-module `APP_GUARD` resolution order for that would be fragile.

Role assignment: `@Roles('ADMIN', 'OPERATOR')` on `reserve`/`release`;
`@Roles('ADMIN')` on the discrepancies query, which exposes reconciliation
internals and cross-system disagreement — operator information, not client
information. Availability (`GET`) stays open to any authenticated role,
including `READER`: a query endpoint carries no overcommit risk.

**Consequences:** The role is read from the signed JWT, not from a
client-supplied header, so RBAC cannot be bypassed by spoofing. `test/rbac.e2e-spec.ts`
creates its own `READER`/`OPERATOR`/`ADMIN` users directly via Prisma rather
than through `prisma/seed.ts` — seed data stays production-representative (one
admin), test fixtures stay test-local. Test specs generate a fresh
`programRef`/`invoiceRef`/`Idempotency-Key` per run via `randomUUID()`, so the
suite is safe to re-run against a persistent local database.

## ADR-018: Refresh tokens — opaque, hashed, rotate-on-use

**Context:** The access token expires after `JWT_EXPIRES_IN` (1h default).
Without a renewal path a client must re-send the password on every expiry.

**Decision:** `POST /auth/login` returns `{ accessToken, refreshToken }`. The
refresh token is an opaque 256-bit random value (`randomBytes(32)`, base64url),
not a second JWT; only its SHA-256 hash is stored, in `refresh_token`
(`userId`, unique `tokenHash`, `expiresAt`, nullable `revokedAt`).
`POST /auth/refresh` hashes the presented token, looks it up, and rejects with
401 if it is missing, revoked, expired or belongs to an inactive user. On
success it rotates: the presented row is revoked and a new access+refresh pair
is issued, atomically (ADR-031).

TTL is `REFRESH_TOKEN_TTL_DAYS` (default 30), a plain integer-days variable
rather than a duration string like `JWT_EXPIRES_IN`: `@nestjs/jwt` parses
duration strings internally, but computing `expiresAt` as a `Date` here needs a
millisecond value directly, and adding a duration-parsing dependency for one
field is not worth it.

**Alternatives rejected:**
- *A second, longer-lived JWT as the refresh token* — self-contained JWTs cannot
  be revoked without a version or blocklist check, which is exactly the
  `tokenVersion` mechanism this project chose not to build (ADR-016). An opaque,
  hashed, DB-backed token is revoked by marking the row — real server-side
  revocation, which is the entire point of a refresh token distinct from the
  access token.
- *Storing the raw refresh token* — only the hash is persisted, so a database
  read (backup leak, compromised replica) cannot be turned into a usable token;
  the same reasoning already applied to `passwordHash`.
- *No rotation (a reusable refresh token)* — simpler, but a leaked token would
  then work until its TTL with no way to detect the leak.
- *A `POST /auth/logout` endpoint* — not built. Revoking a whole token family
  does happen, but as a response to detected reuse rather than as a client-driven
  operation (ADR-031).

## ADR-019: The FX valuation instant is server time, never a request field

**Context:** `reserve` accepts a `requestedAt` in its body, and the rate
provider selects a rate by an `as_of` cutoff. If the client supplies that
cutoff, the client chooses the rate.

**Decision:** The valuation instant is server time. `requestedAt` remains part
of the API and is recorded as the ledger entry's `occurred_at`, but it does not
influence pricing. The DTO rejects a future `requestedAt`, which would otherwise
corrupt the audit trail.

**Alternatives rejected:** *A bounded window* (accept `requestedAt` within ±24h
of now) — preserves backdating, but keeps a pricing lever in the client's hands
for a capability nothing in the brief asks for.

**Consequences:** Because the selected rate is frozen onto the invoice and
replayed at release (ADR-003), a client-chosen rate would be a permanent
mispricing, not a transient one. Server time removes the lever entirely.

## ADR-020: Face amount is quantised at the invoice currency's scale

**Context:** A reservation involves two currencies with two different
minor-unit scales: the invoice's and the programme's. Each amount must be
quantised at the scale of the currency it is denominated in.

**Decision:** The face amount is quantised using the **invoice** currency's
`minor_units`; the converted amount is quantised using the **programme**
currency's, once, inside `FxService.convert`. An inbound amount carrying more
precision than its own currency permits is a `400` — `1000.5` JPY is malformed
input, not a rounding opportunity, and must not be booked as a different number.

**Consequences:** Using the programme's scale for both would round a GBP invoice
of `1234.56` to `1235` against a JPY programme before conversion, consuming
capacity nobody asked for and persisting the rounded figure as
`invoice.face_amount` so the audit trail would agree with the error. The
`invoice_face_quantised` CHECK cannot catch that, because `1235` is a perfectly
valid GBP amount — which is why the scale must be right in the application, not
merely validated afterwards.

This path is covered end to end by `test/fx-cross-currency.e2e-spec.ts`
(JPY programme, GBP invoice, and the reverse), because the multi-currency
requirement is the one the assignment calls out explicitly and it is invisible
to any test that uses the same currency on both sides.

## ADR-021: The idempotency fingerprint covers method and concrete URL

**Context:** An `Idempotency-Key` is scoped by whatever goes into its
fingerprint. `programRef` is a path parameter, so two reservations against
different programmes can carry byte-identical bodies.

**Decision:** The fingerprint is `sha256` over
`{method, originalUrl-without-query, canonicalised body}`. Uniqueness in the
database stays `(client_id, key)`; a mismatched fingerprint yields `409` rather
than a replay.

Canonicalisation is a recursive key sort. The replacer-*array* form of
`JSON.stringify` is specifically avoided: it is a key allow-list applied at
every nesting depth, correct only by accident for flat DTOs, and it would
silently drop nested fields from the hash the moment one was added — making two
materially different requests hash identically.

**Consequences:** `req.route.path` cannot substitute for `originalUrl`: it is
the route *template* (`/programs/:programRef/reserve`), identical for every
programme. Without the URL in the fingerprint, reusing a key against a second
programme returns the first programme's `201` while debiting nothing — the
caller believes a reservation exists that does not. The `method` and `path`
columns on `idempotency_key` are stored for diagnostics; the fingerprint is what
is compared.

## ADR-022: Every capacity mutation publishes an outbox event

**Context:** ADR-004/005's watermark means "the highest local `seq` treasury has
folded into its reported `reserved_amount`". That is sound only if every local
`seq` reaches treasury.

**Decision:** Both `reserve` and `release` write an outbox row —
`invoice.reserved` and `invoice.released` respectively — in the same
transaction as their ledger append, keyed by programme so a programme's events
keep their relative order on the wire.

**Consequences:** If releases were unpublished, treasury could acknowledge
`seq 3` having never seen the release at `seq 2`; reconciliation would then skip
`seq 2` as already-included and restore capacity that had genuinely been
returned — permanently, since the invoice is already `RELEASED` and a second
release replays rather than re-executing. Contiguity of the published sequence
is not a nicety here, it is the precondition the watermark rests on.

## ADR-023: Kafka offsets are not an identity

**Context:** Inbox deduplication needs a key that is stable across
redeliveries *and* across topic recreation.

**Decision:** Deduplicate on the producer-supplied `event_id`, with a unique
constraint on that column and an `ON CONFLICT (event_id) DO NOTHING` insert.
`(topic, partition, offset)` is retained as a plain index for diagnostics, not
as a uniqueness constraint.

**Consequences:** `ON CONFLICT` addresses exactly one conflict target, so a
second unique constraint on the offset triple would raise `23505` outside the
clause, abort the transaction and wedge the partition — and offsets restart at
zero whenever a topic is recreated, which for a local environment is a routine
`docker compose down`. Kafka therefore also has a named volume and a health
check in `docker-compose.yml`, matching Postgres: an asymmetry where only one of
the two survives a restart is itself a source of this class of bug.

## ADR-024: The outbox relay claims work, bounds attempts and backs off

**Context:** A polling relay that merely selects `status = 'PENDING'` has no
protection against a second instance, against overlapping runs of its own timer,
or against a row that can never be published.

**Decision:** A single `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)`
claims a batch, increments `attempts` and pushes `available_at` forward by an
exponential backoff, atomically. Publication marks the row `SENT` guarded on
`status = 'PENDING'`. After `MAX_ATTEMPTS` the row is parked in `FAILED`. A
re-entrancy flag prevents a slow batch overlapping itself, and messages sharing
a partition key are published in order, abandoning the rest of that key's group
on the first failure.

**Consequences:** The design can duplicate a message (send precedes the SENT
mark) but cannot lose one, which is the correct direction — the producer is
configured `idempotent: true` and consumers deduplicate. Ordering within a key
matters specifically because these payloads carry the watermark sequence of
ADR-022: publishing `N+1` after `N` failed would let treasury acknowledge past a
message it never received.

Incrementing `attempts` at *claim* time rather than on failure means a crash
mid-publish still counts as an attempt, so a row that reliably kills the process
backs off instead of spinning.

## ADR-025: A dead-letter writer cannot fail, and errors are classified

**Context:** The dead-letter table exists to hold messages that could not be
processed — including messages that could not be parsed. And a consumer that
treats every error as transient will retry a deterministic failure forever.

**Decision:** The dead-letter writer never parses unguarded: an unparseable body
is stored as `{__unparsed: true, reason, raw: <base64>}`, and a tombstone gets
its own marker. Errors are classified by `isTransientDbError`: permanent
failures are dead-lettered and the offset advances; transient ones are rethrown
for kafkajs to redeliver, but only up to `MAX_TRANSIENT_ATTEMPTS`, after which
the message is dead-lettered regardless.

**The default is "permanent".** An unknown deterministic error retried forever
wedges a partition and every message behind it; the same error parked in an
inspectable table costs exactly one message.

**Deliberate trade-off:** if the dead-letter write *itself* fails, the message is
skipped and the failure logged at `error`. Blocking the partition on it would
convert the loss of one message into the loss of all subsequent ones, and the
message is unprocessable regardless of the dead-letter table's state. That log
line is the only remaining trace, by design.

**Consequences:** Deterministic failures that reach this path include a CHECK
violation, an unknown programme, a currency contradiction (ADR-028) and a
malformed numeric. None of them can stop consumption.

## ADR-026: `event_seq` on treasury deltas, and a NULL-safe replay

**Context:** Reconciliation fences treasury-origin ledger entries with
`treasury_event_seq > included_through_event_seq`. That requires the delta event
to carry a sequence and the ledger entry to record it.

**Decision:** `event_seq` is part of the delta schema as an **optional** field
(`nullable().default(null)`) and is persisted to
`capacity_ledger_entry.treasury_event_seq`. The replay predicate treats a NULL
sequence as "unknown whether treasury folded this in" and replays the entry.
Where the snapshot omits its watermark entirely, the time-based fallback of
ADR-004/005 applies.

**Alternatives rejected:** *A required `event_seq`* — cleaner, but it would
dead-letter every delta from a producer that does not emit one, turning a
tolerable accounting imprecision into a total ingestion outage.

**Assumption stated explicitly:** `event_seq` is monotonic **per programme**.
Comparing it against `included_through_event_seq` is meaningless otherwise.

**Consequences:** `NULL > n` evaluates to NULL, not false-and-move-on, so a
sequence that is never written makes the entire TREASURY branch of the replay
unsatisfiable — silently, and undetectably by `v_program_ledger_drift`, since
the correcting entry keeps counter and ledger consistent on the wrong number.
Treating NULL as "replay it" is the same over-replay bias ADR-004/005 commits
to.

## ADR-027: Broker unavailability must not stop the HTTP API

**Context:** `reserve`, `release` and availability touch only Postgres. An
`onModuleInit` that rejects aborts `NestFactory.create`, and `main.ts` then
exits the process.

**Decision:** The Kafka consumer and the outbox producer connect in the
background with retry. Neither can prevent the application from serving HTTP.
The consumer exposes a `ready` promise that resolves once it has joined its
group, which integration tests await before publishing (with
`fromBeginning: false`, anything sent before the join could be skipped).

**Consequences:** This is what makes the outbox's stated purpose true — "a
broker outage delays acknowledgement to treasury, never blocks a reservation".
An unguarded connect at startup would contradict it directly, since the process
would not start at all.

## ADR-028: `delta.currency` is an assertion, not an instruction

**Context:** A treasury delta modifies `program.reserved_amount`, a column
denominated in the programme's currency.

**Decision:** Compare `delta.currency` with the programme's currency; a mismatch
is a permanent error and is dead-lettered. The delta's UPDATE also carries the
same `[0, total_limit]` guard the API path uses, so a delta that would move the
programme outside those bounds is dead-lettered rather than walking into the
`program_no_overcommit` CHECK.

**Alternatives rejected:** *Converting through `FxService`* — the event carries
no rate, so the conversion would use a rate of our choosing at a moment of our
choosing, booking an FX residue with no invoice to freeze it against. That
contradicts ADR-003 head-on. An amount added to a programme-currency column can
only sensibly already be in that currency, which makes the field a statement
about the producer's view of the world rather than a unit to convert from.

**Consequences:** Validating the field and then ignoring it would let a USD
amount be added one-for-one to a GBP programme, recorded in an append-only
ledger with no currency column to detect it afterwards. `applyBaseline`
performs the equivalent check for snapshots; the two paths agree.

## ADR-029: Background jobs have a lease, a bounded retry and a re-entrancy guard

**Context:** `ReconciliationWorker` claims a job, commits `CLAIMED` and then
works outside that transaction — position sync in particular runs across many
chunks and can outlast the tick interval.

**Decision:**
- A `CLAIMED` job untouched for longer than a five-minute lease may be
  re-claimed. Which phase it resumes at is derived from durable state:
  `applyBaseline` stamps `program_id` in the same transaction that sets
  `BASELINE_APPLIED`, so a non-null `program_id` means the baseline is already
  committed and must not run again.
- A failure returns the job to its durable phase with exponential backoff via
  `available_at`, becoming terminal `FAILED` only after `MAX_JOB_ATTEMPTS`.
- Both timer loops carry a re-entrancy flag and drain in-flight work on
  shutdown.
- Snapshot completeness is `chunks_received >= chunk_count`.

**Consequences:** Without the lease, a crash mid-`applyBaseline` strands a job in
a state no selector matches. Without the backoff, a single `SET LOCAL
lock_timeout` trip — a long reservation holding the programme row is enough —
abandons that programme's snapshot permanently, and ADR-012 already notes that
nothing alerts on a stale `treasury_baseline_as_of`. Without the re-entrancy
guard, a large snapshot's position sync is re-claimed by the next tick and runs
twice concurrently, duplicating discrepancy rows and moving `positionCursor`
backwards — on a single instance.

`>=` rather than `=` for chunk completeness: a producer resending a chunk under
a fresh `event_id` pushes the counter past the total, and equality then never
holds again, parking a complete snapshot forever. Per-`chunk_index` uniqueness
is **not** implemented, so a duplicated chunk can still be counted twice; that
costs per-invoice attribution only, because the capacity figure comes from the
snapshot header rather than from positions.

## ADR-030: Snapshot positions resolve invoices within their programme

**Context:** `invoice.external_ref` is unique only per programme.

**Decision:** Position sync looks invoices up on the `(programId, externalRef)`
unique key. Amounts are compared with `Decimal#equals`, not by string.

**Consequences:** An unscoped lookup would match an arbitrary same-ref invoice
belonging to a different programme, raising drift alerts against the wrong
figures and writing another programme's amounts into this programme's
discrepancy record — which, since those records are readable over HTTP
(ADR-011), would be a cross-programme disclosure rather than merely a wrong
number. String comparison is avoided because, as ADR-001 notes, `toString()`
does not preserve trailing zeros.

## ADR-031: Refresh rotation is atomic, and reuse revokes the family

**Context:** Rotation is only a security control if exactly one caller can win
it, and a rotated-away token reappearing is evidence of a leak.

**Decision:** The revoke is `updateMany({ where: { id, revokedAt: null } })`
inside a transaction with the replacement's creation; `count !== 1` means a
concurrent request won and this one fails with 401. Presenting an
already-revoked token revokes every unrevoked token for that user and logs a
warning.

**Consequences:** A read followed by an unconditional write would let two
concurrent refreshes both observe `revokedAt = null` and both receive valid,
independent chains — "usable at most once" would not survive concurrency.
Rotation alone, without reuse detection, leaves a thief who rotates first with a
self-renewing session while the victim sees only an unexplained 401; revoking
the family turns that signal into an action.

Login applies the same principle to a different oracle: it verifies against a
fixed dummy hash when the e-mail is unknown, so both paths pay the full KDF
cost. The status code was never the leak — an ~80× latency difference was.

## ADR-032: Production refuses known-development configuration

**Context:** `.env.example` deliberately contains a working development secret
so `cp .env.example .env` produces a runnable service (ADR-009). What must not
happen is that value reaching production.

**Decision:** A `superRefine` on the env schema rejects, when
`NODE_ENV=production`, both the exact `.env.example` placeholder and any secret
under 32 characters. `prisma/seed.ts` will not create its admin in production
unless `SEED_ADMIN_PASSWORD` is supplied, imports `hashPassword` rather than
duplicating it, and exits non-zero on failure.

Swagger is mounted only outside production. `SwaggerModule.setup` registers
handlers on the HTTP adapter rather than as Nest controllers, so global guards
never run for `/docs` and `/docs-json` — leaving the full API surface readable
without a token, which contradicts "all endpoints are authenticated".

**Consequences:** The defect this guards against is not the example file; it is
the absence of a refusal. A length rule alone does not help, because the
placeholder is 34 characters long — the check has to know the value.

## ADR-033: One application configuration, shared by the process and the tests

**Context:** `app.useGlobalPipes` and friends are calls on the application
instance. A test that builds the app through
`Test.createTestingModule(...).createNestApplication()` inherits nothing
installed inside `bootstrap()`.

**Decision:** `configureApp(app)` in `src/bootstrap.ts` is the single place that
installs pipes, interceptors, the exception filter, `trust proxy` and Swagger.
`main.ts` and every full-app e2e spec call it.

**The exception filter** maps Prisma and domain failures onto HTTP statuses:
`P2025 → 404`, `P2002 → 409`, `P2003 → 400`, CHECK violation (`23514`) `→ 422`,
`FxRateUnavailableError → 422`, `CurrencyMismatchError → 400`. Anything
unrecognised stays a 500 with a generic body — the goal is to stop *expected*
conditions masquerading as server faults, not to dress every failure as a 4xx.

**`trust proxy`** is configurable via `TRUST_PROXY_HOPS`, defaulting to 0 (trust
nothing). Rate limiting keys on `req.ip`, and behind a load balancer without it
every client shares one bucket — which would make the 5/min login limit a
system-wide denial of service while leaving distributed brute force untouched.

**Consequences:** If the tests configured the app differently from the process,
they would be testing a different application — and specifically, no e2e test
would exercise request validation at all. This is what makes the status-code
assertions in `fx-cross-currency.e2e-spec.ts` meaningful.

## ADR-034: Deliberately not implemented

**Context:** A production deployment of this service would need more than the
exercise does. Building all of it would mean a great deal of code with no way to
exercise it here — no real treasury producer, no cluster, no operations team, no
incident to respond to. The judgement applied throughout: **build everything the
core domain's correctness depends on; name the rest instead of quietly leaving
it out.**

**Decision:** The following are known, considered and not built. Each says what
production would want, why it is absent, and what closing it would take. None of
them affects the correctness of the capacity accounting or running the service
locally.

**Observability.** No metrics, no structured JSON logging, no correlation-ID
propagation, no tracing. In production this is the difference between diagnosing
an incident and guessing: the numbers that would matter are consumer lag,
dead-letter rate, outbox depth and age, and reconciliation duration.
`JobHeader.correlation_id` exists in the type as the hook for it. *Absent
because* a metrics backend is infrastructure the exercise cannot stand up, and
counters with nothing scraping them would be theatre. *Would take:* a
`prom-client` registry, a `/metrics` endpoint, and a Nest interceptor stamping a
request id.

**Readiness probing.** `/health` is a liveness probe that returns a literal —
it checks neither Postgres nor the Kafka consumer's group membership. An
orchestrator therefore keeps routing write traffic to an instance whose database
has failed over or whose consumer has stopped. `@nestjs/terminus` is a declared
dependency and is imported nowhere, which is the honest marker of where this was
going to go. *Absent because* a meaningful readiness signal needs a policy
decision — is a broker outage "not ready", when the HTTP API deliberately keeps
serving without one (ADR-027)? *Would take:* Terminus with a Prisma indicator
plus the consumer's existing `ready` promise, split into `/health/live` and
`/health/ready`.

**HTTP security headers.** No `helmet`: no HSTS, `X-Content-Type-Options`,
`X-Frame-Options` or referrer policy. `X-Powered-By` is disabled, and CORS is
off by default, which is the safe posture for a pure JSON API. *Absent because*
the impact on a JSON API consumed by a server-side client is small, and the one
browser-rendered surface, Swagger, is confined to non-production (ADR-032).
*Would take:* one middleware registration in `configureApp`.

**Dead-letter re-drive.** `published_to_dlq` and `resolved_at` are columns
nothing sets. Failed messages land in the table and stay there; a production
system needs a way to inspect, fix and replay them. *Absent because* it is an
operations tool, not domain logic. *Would take:* an admin endpoint or CLI that
republishes a row and marks it resolved.

**Idempotency-key expiry.** `expires_at` is written and indexed but not read,
and there is no sweeper, so a key replays indefinitely and the table grows
without bound. *Absent because* the durable guarantee does not depend on it: the
business-key uniqueness on `(programId, invoiceRef)` is what actually prevents
double-reservation, and the header is a latency optimisation on top. *Would
take:* a scheduled delete plus an expiry check in the claim — the index is
already there for it.

**Per-account login throttling.** The limit is per-IP only, so brute force
against one account is bounded by an attacker's IP diversity rather than by the
account. *Absent because* doing it properly means a shared store and a lockout
policy (how long, who unlocks, how to avoid a denial-of-service against a real
user) — policy decisions the brief does not supply. The related limitation, that
the throttler counts in process memory and therefore resets per replica and per
deploy, is the same story: correct behaviour needs Redis.

**Access-token revocation.** `User.tokenVersion` is issued in every JWT and
checked by nothing, so a role change or a deactivation takes effect only when
the access token expires (default 1h). *Absent by explicit scope decision* — see
ADR-016. Refresh-token revocation is real and independent of it (ADR-031).

**scrypt cost migration.** The cost parameters are encoded into the stored hash
string and ignored on verification, so the format looks migratable but is not,
and the cost factor is Node's default rather than current OWASP guidance.
*Absent because* a rehash-on-login migration path is a chunk of work for a
demo's single seeded user. *Would take:* parsing the encoded parameters on
verify, then rehashing at the current cost when they differ.

**Cross-rate FX audit fidelity.** A pivot-derived conversion stores only the
quote leg's `rate_id` and `as_of`, so the two legs are not individually
reproducible from the audit fields. The effective rate itself *is* stored, so the
amount is reproducible; it is the provenance that is lossy. *Would take:* a
join table, or two nullable leg columns on `invoice`.

**Producer-contract violation detection.** `processed_message.payload_hash` is
written and never compared, so a producer reusing an `event_id` for different
content is deduplicated silently rather than flagged. *Absent because* it is a
defence against a misbehaving upstream that does not exist here. *Would take:*
comparing the hash on conflict and dead-lettering a mismatch.

**Programme administration.** There is no endpoint to create or amend a
programme; it is treated as back-office data entry, outside the
reserve/release/query API the brief describes. This is why
`LedgerEntryType.LIMIT_INCREASE`, `LIMIT_DECREASE` and `MANUAL_ADJUSTMENT`, and
`InvoiceStatus.CANCELLED`, are modelled with no code path — the ledger's shape
anticipates limit changes even though nothing issues them yet. Same for
`ReconciliationStatus.POSITIONS_SYNCED` and `Invoice.treasuryRef` /
`treasuryAckedAt`; `treasuryRef` carries an index on a column that is always
NULL, which is a small cost paid for a schema that reads coherently.

**Two loose ends in code, named rather than hidden.**
`claimIdempotencyKey` returns an `in_flight` variant its callers do not handle;
it is unreachable while the claim and the settle share one transaction, but it
is an unhandled case of a sum type and would become a double-execution path if
that ever changed. And `serializeBigInts` walks response objects with
`Object.entries`, so it would render a `Prisma.Decimal` as its internals —
latent only because every money field is formatted to a string at the boundary
first (ADR-001).

**Consequences:** This list is the honest answer to "what would you do next".
Nothing on it is load-bearing for the invariant the brief is actually about —
that a programme's reserved capacity never exceeds its limit, across concurrent
API calls, treasury deltas and bulk reconciliation.

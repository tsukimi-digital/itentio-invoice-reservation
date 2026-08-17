# Program Capacity & Invoice Reservation

Tracks a financing program's credit capacity in real time: invoices approved for
early payment reserve part of a program's limit, repayments release it back, and
clients can query current availability at any time. Capacity also arrives from an
external treasury system over Kafka, including periodic bulk-reconciliation
messages that bring a program's full state up to date. Programs and invoices may
be denominated in different currencies. All endpoints are authenticated.

Every conscious design decision — money representation, locking strategy, the
reconciliation algorithm, delivery semantics — is recorded with its rationale and
rejected alternatives in **[`docs/DECISIONS.md`](docs/DECISIONS.md)**. That file
is the primary technical write-up for this submission; this README covers how to
run it and how the pieces fit together.

## Stack

- **NestJS** (TypeScript, strict) — DI and module boundaries keep the capacity
  domain, the Kafka consumer, the reconciliation worker, and the HTTP API
  independently testable.
- **PostgreSQL + Prisma** — capacity accounting needs real transactions and
  row-level locking (`FOR NO KEY UPDATE`) plus `CHECK` constraints as a
  database-level backstop; Prisma gives typed migrations on top of that.
- **kafkajs** — consumes treasury capacity events (deltas and bulk
  reconciliation snapshots) and publishes reservation acknowledgements via a
  transactional outbox.
- **zod** — validates environment configuration and every inbound Kafka
  message against an explicit schema.
- **decimal.js** (via `Prisma.Decimal`, cloned) — exact-precision money
  arithmetic; see `docs/DECISIONS.md` ADR-02.
- **Jest + Testcontainers** — unit tests plus real-Postgres integration tests
  (DB constraints, 50-way concurrent reservation stress test) that spin up
  disposable containers rather than mocking the database.

Why each of these, what was rejected, and what deliberately is *not* in the
stack (no Redis, no schema registry, no metrics backend): ADR-01.

## Architecture

```
                         ┌─────────────────────────────┐
  HTTP client ──JWT──▶   │  CapacityController          │
                         │  POST reserve / release       │
                         │  GET  availability             │
                         └──────────────┬───────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │  CapacityService              │
                         │  FOR NO KEY UPDATE row lock    │
                         │  guarded UPDATE (no read-      │
                         │  modify-write window)          │
                         │  append-only ledger entry       │
                         └──────┬──────────────┬─────────┘
                                │              │
                          ledger + outbox   FX freeze
                                │              │
                                ▼              ▼
                         Postgres ledger    invoice.reservedProgramAmount
                                │
                                ▼
                    OutboxRelayService ──▶ Kafka (capacity.reservation-events)


  Kafka (treasury.capacity-events)
           │
           ▼
  CapacityConsumerService (eachBatch + heartbeat)
           │
           ├── inbox dedup (processed_message, unique on event_id)
           │
           ├── incremental delta ──▶ CapacityService.applyTreasuryDelta
           │                          (same lock/ledger discipline as the API path)
           │
           └── bulk snapshot ──▶ enqueue ReconciliationJob (never applied inline —
                                  a long snapshot must never risk a consumer rebalance)
                                            │
                                            ▼
                              ReconciliationWorker (2s poll, SKIP LOCKED)
                                            │
                              ┌─────────────┴─────────────┐
                              ▼                             ▼
                    applyBaseline                  syncPositions
                    (fenced CAS on snapshot_seq,    (chunked, restartable,
                     baseline + replay of un-        flags AMOUNT_DRIFT /
                     acknowledged local entries,      UNKNOWN_INVOICE as
                     ONE correcting ledger entry)     discrepancies)
```

## Running locally

```bash
cp .env.example .env
docker compose up -d              # Postgres on :5432, Kafka on :9092
npm install
npm run prisma:generate
npx prisma migrate deploy         # apply all migrations
npx prisma db seed                # currencies, FX rates, admin user, 2 programmes
npm run start:dev                 # http://localhost:3000, Swagger at /docs
```

Default seeded user: `admin@itentio.dev` / `dev-only-password-change-me`.

The seed creates two programmes so the API is exercisable immediately:
`PRG-1` (GBP, £10,000,000 limit) and `PRG-2` (JPY, ¥1,500,000,000) — the second
one exists so the cross-currency path can be driven without adding data by hand.
There is no endpoint that creates a programme; that is treated as back-office
data entry, outside the reserve/release/query API the brief describes.

## Trying it out

```bash
# 1. Log in
TOKEN=$(curl -s -X POST localhost:3000/auth/login \
  -H 'content-type: application/json' \
  -d '{"email":"admin@itentio.dev","password":"dev-only-password-change-me"}' \
  | node -pe 'JSON.parse(require("fs").readFileSync(0)).accessToken')

# 2. Reserve capacity for an invoice
curl -X POST localhost:3000/programs/PRG-1/reserve \
  -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"invoiceRef":"INV-1","amount":"1234.56","currency":"GBP","requestedAt":"2026-01-01T00:00:00Z"}'

# 3. Check availability
curl -H "Authorization: Bearer $TOKEN" localhost:3000/programs/PRG-1

# 4. Release on repayment
curl -X POST localhost:3000/programs/PRG-1/release \
  -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"invoiceRef":"INV-1"}'

# 5. Cross-currency: a GBP invoice against the JPY programme, converted at the
#    seeded rate and quantised to whole yen
curl -X POST localhost:3000/programs/PRG-2/reserve \
  -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"invoiceRef":"INV-2","amount":"1000.00","currency":"GBP","requestedAt":"2026-01-01T00:00:00Z"}'
```

Full interactive API docs (with request/response schemas) at `/docs`.

Retrying the reserve call with the **same** `Idempotency-Key` and body returns
the original response unchanged rather than double-reserving — the header is a
latency optimisation; the underlying guarantee is a business-key uniqueness
constraint on `(programId, invoiceRef)`.

The key's fingerprint covers the HTTP method and the concrete URL as well as
the body, so the same key used against a different programme is a `409` rather
than a replay of the first programme's response (ADR-08). Note that
`idempotency_key.expires_at` is recorded but **not** enforced and there is no
sweeper, so in practice a key replays indefinitely — see ADR-13.

```bash
# 6. Reconciliation discrepancies (ADMIN only)
curl -H "Authorization: Bearer $TOKEN" localhost:3000/programs/PRG-1/discrepancies
```

## Development

```bash
npm run lint
npm run audit       # fails the build on a high-severity advisory
npm run build
npm run test:unit   # fast, mocked
npm run test:e2e    # real HTTP, real Postgres, real Kafka
npm test            # unit then e2e, in sequence — what CI runs
```

`test:e2e` needs `docker compose up -d` already running with migrations applied:
most specs boot the whole `AppModule` against the compose Postgres and Kafka.
Two of them — `concurrency` and `db-constraints` — instead spin up their own
disposable Postgres through Testcontainers, because there the database itself is
the subject under test.

What `test:e2e` actually exercises:

- **Concurrency** — `test/concurrency.e2e-spec.ts` runs 50 simultaneous
  reservations against a real Postgres and asserts that exactly the affordable
  number succeed and that the ledger sum matches the counter. The capacity
  guard is proven under real contention, not in a single-threaded mock.
- **Kafka** — `test/kafka-consumer.e2e-spec.ts` and
  `test/reconciliation.e2e-spec.ts` publish real messages and assert on the
  resulting database state, including the dead-letter path and the
  snapshot-versus-delta replay. The broker is driven, not merely required in
  order to boot.
- **Multi-currency** — `test/fx-cross-currency.e2e-spec.ts` covers reserve →
  release across two currencies with different minor-unit scales, plus rate
  staleness and hostile input. This is the path `DbFxRateProvider` lives on and
  the one the assignment calls out explicitly.
- **Idempotency scope** — `test/idempotency-scope.e2e-spec.ts` covers replay,
  conflict, and the same key used against a second programme.

Every full-app e2e spec calls `configureApp()` from `src/bootstrap.ts` — the
same function `main.ts` uses — so the suite exercises the application that
actually ships, global pipes and exception filter included (ADR-12).

## Known limitations

**ADR-13 is the full list** of what is deliberately not built and why. The ones
worth knowing before reading the code:

- **No tenancy.** Authorisation is role-based only — a programme has no owner,
  so any OPERATOR may act on any programme.
- **No partial release.** Release is all-or-nothing on the frozen amount.
- **No readiness probe or metrics.** `/health` is a liveness literal.
- **Access tokens cannot be revoked** before they expire; refresh tokens can.
- **Unenforced idempotency-key TTL** and an unread `payload_hash`, plus schema
  columns and enum values that model behaviour with no code path yet.
- **Operational limits** of a long-running deployment: dedup-table retention
  isn't bounded against Kafka topic retention, nothing alerts on a stale
  reconciliation snapshot, and the locking strategy needs a direct Postgres
  connection rather than a transaction-pooling PgBouncer.
- The throttler counts in process memory, so limits are per-replica and reset on
  deploy; `TRUST_PROXY_HOPS` must match the real hop count for `req.ip` — and
  therefore rate limiting — to mean anything behind a load balancer.

None of these affect running the service locally.

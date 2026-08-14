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
  arithmetic; see `docs/DECISIONS.md` ADR-001.
- **Jest + Testcontainers** — unit tests plus real-Postgres integration tests
  (DB constraints, 50-way concurrent reservation stress test) that spin up
  disposable containers rather than mocking the database.

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
npx prisma db seed                # currencies, seed FX rates, admin user
npm run start:dev                 # http://localhost:3000, Swagger at /docs
```

Default seeded user: `admin@itentio.dev` / `dev-only-password-change-me`.

## Trying it out

```bash
# 1. Log in
TOKEN=$(curl -s -X POST localhost:3000/auth/login \
  -H 'content-type: application/json' \
  -d '{"email":"admin@itentio.dev","password":"dev-only-password-change-me"}' \
  | node -pe 'JSON.parse(require("fs").readFileSync(0)).accessToken')

# 2. Create a program (via Prisma Studio: npx prisma studio — no admin
#    endpoint is exposed; program creation is treated as back-office data
#    entry, out of scope for this assignment's reservation/release/query API)

# 3. Reserve capacity for an invoice
curl -X POST localhost:3000/programs/PRG-1/reserve \
  -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"invoiceRef":"INV-1","amount":"1234.56","currency":"GBP","requestedAt":"2026-01-01T00:00:00Z"}'

# 4. Check availability
curl -H "Authorization: Bearer $TOKEN" localhost:3000/programs/PRG-1

# 5. Release on repayment
curl -X POST localhost:3000/programs/PRG-1/release \
  -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"invoiceRef":"INV-1"}'
```

Full interactive API docs (with request/response schemas) at `/docs`.

Retrying the reserve call with the **same** `Idempotency-Key` and body returns
the original response unchanged rather than double-reserving — the header is a
latency optimisation; the underlying guarantee is a business-key uniqueness
constraint on `(programId, invoiceRef)` that outlives the header's 24h TTL.

## Development

```bash
npm run lint
npm run build
npm run test:unit   # fast, mocked
npm run test:e2e    # real Postgres (Testcontainers) + real HTTP + real Kafka
npm test            # both, in sequence — what CI runs
```

`test:e2e` includes a 50-way concurrent-reservation stress test
(`test/concurrency.e2e-spec.ts`) that proves the capacity guard holds under
real contention, not just in a single-threaded mock.

## Known limitations

Documented in full, with rationale, in `docs/DECISIONS.md` ADR-012: dedup-table
retention isn't bounded against Kafka topic retention, there's no alerting on
stale reconciliation snapshots, and the locking strategy assumes a direct
Postgres connection (not a transaction-pooling PgBouncer). None of these affect
running the service locally.

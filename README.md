# Program Capacity & Invoice Reservation

Tracks a financing program's credit capacity in real time: invoices reserve part
of a program's limit on approval and release it back on repayment, with capacity
also flowing in from an external treasury system over Kafka, including periodic
bulk-reconciliation snapshots. Programs and invoices may be denominated in
different currencies, and every endpoint is authenticated. Every conscious
design decision is recorded with its rationale and rejected alternatives in
**[`docs/DECISIONS.md`](docs/DECISIONS.md)**, the primary technical write-up for
this submission; this README covers how to run it and how the pieces fit
together.

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
  arithmetic.
- **Jest + Testcontainers** — unit tests plus real-Postgres integration tests
  (DB constraints, 50-way concurrent reservation stress test) that spin up
  disposable containers rather than mocking the database.

The reasoning behind each of these, and what deliberately is *not* in the
stack (no Redis, no schema registry, no metrics backend), is in
[`docs/DECISIONS.md`](docs/DECISIONS.md).

## Architecture

```
                    ┌─────────────────────────┐
HTTP client ─JWT──▶ │ CapacityController      │
                    │ POST reserve / release  │
                    │ GET  availability       │
                    └────────────┬────────────┘
                                 ▼
                    ┌──────────────────────────────────────┐
                    │ CapacityService, one transaction:    │
                    │ 1. idempotency claim                 │
                    │ 2. FOR NO KEY UPDATE row lock        │
                    │ 3. guarded UPDATE (no read-modify-   │
                    │    write window)                     │
                    └───┬─────────────┬─────────────┬──────┘
                        ▼             ▼             ▼
                  ledger entry   outbox row    FX freeze on
                  (Postgres)     (Postgres)    invoice row
                                      │
                                      ▼
                   OutboxRelayService (1s poll, SKIP LOCKED)
                                      │
                                      ▼
                    Kafka (capacity.reservation-events)


Kafka (treasury.capacity-events)
        │
        ▼
CapacityConsumerService (eachBatch + heartbeat)
        │
        ▼
inbox dedup (processed_message, unique on event_id) — duplicates skipped
        │ new event
        ├── incremental delta ──▶ CapacityService.applyTreasuryDelta
        │                          (same lock/ledger discipline as the API path)
        │
        └── bulk snapshot ──▶ enqueue ReconciliationJob (never applied inline —
                                a long snapshot must never risk a consumer rebalance)
                                          │
                                          ▼
                            ReconciliationWorker (2s poll, SKIP LOCKED)
                                          │
                                one job, two sequential phases:
                                          │
                                          ▼
                           1. applyBaseline — fenced CAS on snapshot_seq,
                              baseline + replay of unacknowledged local
                              entries, ONE correcting ledger entry
                                          │
                                          ▼
                           2. syncPositions — chunked, restartable, flags
                              AMOUNT_DRIFT / UNKNOWN_INVOICE as discrepancies
```

## Running locally

```bash
cp .env.example .env
docker compose up -d              # Postgres on :5432, Kafka on :9092
npm install
npm run prisma:generate
npx prisma migrate deploy         # apply all migrations
npx prisma db seed                # currencies, FX rates, admin user, 2 programmes
npm run start:dev                 # http://localhost:3000
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

Full interactive API docs (Swagger UI, with request/response schemas) at the
`GET /docs` endpoint: `http://localhost:3000/docs`.

Retrying the reserve call with the **same** `Idempotency-Key` and body returns
the original response unchanged rather than double-reserving — the header is a
latency optimisation; the underlying guarantee is a business-key uniqueness
constraint on `(programId, invoiceRef)`.

### Testing the Kafka side by hand

There's no HTTP surface for the treasury feed — it's a real Kafka topic. To
push a delta into it directly (`kafkajs` is already a dependency, so no extra
install):

```bash
node -e '
const { Kafka } = require("kafkajs");
(async () => {
  const kafka = new Kafka({ clientId: "manual-test", brokers: ["localhost:9092"] });
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic: "treasury.capacity-events",
    messages: [{
      key: "PRG-1",
      value: JSON.stringify({
        event_id: crypto.randomUUID(),
        event_type: "program.capacity.delta",
        schema_version: 1,
        produced_at: new Date().toISOString(),
        program_ref: "PRG-1",
        event_seq: 1,
        delta: { amount: "500.00", currency: "GBP", direction: "RESERVE" },
      }),
    }],
  });
  await producer.disconnect();
})();
'
```

`GET /programs/PRG-1` should reflect the delta within a second or two. The
full wire contract (deltas and bulk snapshots alike) is
[`src/kafka/schemas/capacity-event.schema.ts`](src/kafka/schemas/capacity-event.schema.ts).


## Development

```bash
npm run lint
npm run audit
npm run build
npm run test:unit   # unit tests, fast and mocked
npm run test:e2e    # full stack: real HTTP, real Postgres, real Kafka
npm test            # unit then e2e, in sequence (compliant with CI)
```

`test:e2e` needs `docker compose up -d` already running with migrations applied:
most specs boot the whole `AppModule` against the compose Postgres and Kafka.
Two of them — `concurrency` and `db-constraints` — instead spin up their own
disposable Postgres through Testcontainers, because there the database itself is
the subject under test.

`test:e2e` covers 50-way concurrent reservations against a real Postgres,
real Kafka delivery (dead-letter path, snapshot-versus-delta replay),
cross-currency reserve/release with rate staleness, and idempotency-key scope.

Every full-app e2e spec calls `configureApp()` from `src/bootstrap.ts` — the
same function `main.ts` uses — so the suite exercises the application that
actually ships, global pipes and exception filter included.

## Known limitations

**[ADR-13](docs/DECISIONS.md#adr-13-deliberately-not-implemented)** has the
full, detailed list of what is deliberately not built and why. The ones worth
knowing before reading the code:

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

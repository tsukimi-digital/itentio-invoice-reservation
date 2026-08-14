# Program Capacity & Invoice Reservation

Tracks a financing program's credit capacity in real time: invoices approved for
early payment reserve part of a program's limit, repayments release it back, and
clients can query current availability at any time. Capacity also arrives from an
external treasury system over Kafka, including periodic bulk-reconciliation
messages that overwrite a program's full state.

## Status

Scaffold stage: project skeleton, local infra (Postgres + Kafka), config
validation, health check, CI. Domain logic (reservations, releases, Kafka
consumer, currency conversion, auth) lands in subsequent commits.

## Stack

- **NestJS** (TypeScript) — DI and module boundaries make the reservation
  domain, the Kafka consumer, and the HTTP API separable and independently
  testable.
- **PostgreSQL + Prisma** — capacity accounting needs real transactions and
  row-level locking; Prisma gives typed migrations on top of that.
- **kafkajs** — consumes treasury capacity events and bulk reconciliation
  messages.

## Running locally

```bash
cp .env.example .env
docker compose up -d          # Postgres on :5432, Kafka on :9092
npm install
npm run prisma:generate
npm run start:dev             # http://localhost:3000, Swagger at /docs
```

## Development

```bash
npm run lint
npm test          # unit tests
npm run test:e2e  # end-to-end tests
npm run build
```

## Assumptions & trade-offs

Documented here as they're made, so the reasoning behind each shortcut is
explicit rather than left for the reader to guess.

- **Kafka via `apache/kafka` KRaft image, single broker, single node.** No
  Zookeeper, no replication. Sufficient for local dev and for demonstrating
  the consumer; a real deployment would use a managed, replicated cluster.
- **`JWT_SECRET` ships a placeholder default in `.env.example`.** It is
  intentionally rejected as too short by config validation once real values
  are swapped in for anything beyond local dev — see `src/config/env.schema.ts`.
- **`/health` is unauthenticated.** It's a liveness probe, not a business
  endpoint. Every endpoint that exposes program or invoice data requires
  authentication, per the assignment's requirement.

Further assumptions (locking strategy for concurrent reservations, currency
conversion source and rounding, reconciliation-vs-live-update conflict
resolution) will be added here as those parts are implemented.

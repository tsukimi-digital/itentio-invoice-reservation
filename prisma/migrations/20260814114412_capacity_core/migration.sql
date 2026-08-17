-- CreateEnum
CREATE TYPE "ProgramStatus" AS ENUM ('ACTIVE', 'SUSPENDED', 'CLOSED');

-- CreateEnum
CREATE TYPE "InvoiceStatus" AS ENUM ('REGISTERED', 'RESERVED', 'RELEASED', 'CANCELLED');

-- CreateEnum
CREATE TYPE "LedgerEntryType" AS ENUM ('RESERVE', 'RELEASE', 'LIMIT_INCREASE', 'LIMIT_DECREASE', 'RECONCILE_BASELINE', 'MANUAL_ADJUSTMENT');

-- CreateEnum
CREATE TYPE "LedgerOrigin" AS ENUM ('API', 'TREASURY', 'RECONCILIATION', 'SYSTEM');

-- CreateEnum
CREATE TYPE "FxRateSource" AS ENUM ('SEED', 'IDENTITY', 'ECB', 'TREASURY');

-- CreateEnum
CREATE TYPE "IdempotencyStatus" AS ENUM ('IN_FLIGHT', 'COMPLETED', 'FAILED');

-- CreateEnum
CREATE TYPE "ReconciliationStatus" AS ENUM ('PENDING', 'CLAIMED', 'BASELINE_APPLIED', 'POSITIONS_SYNCED', 'DONE', 'SUPERSEDED', 'FAILED');

-- CreateEnum
CREATE TYPE "UserRole" AS ENUM ('ADMIN', 'OPERATOR', 'READER');

-- CreateTable
CREATE TABLE "currency" (
    "code" CHAR(3) NOT NULL,
    "minor_units" INTEGER NOT NULL,
    "name" VARCHAR(64) NOT NULL,

    CONSTRAINT "currency_pkey" PRIMARY KEY ("code")
);

-- CreateTable
CREATE TABLE "program" (
    "id" UUID NOT NULL,
    "external_ref" VARCHAR(64) NOT NULL,
    "name" VARCHAR(200) NOT NULL,
    "status" "ProgramStatus" NOT NULL DEFAULT 'ACTIVE',
    "currency_code" CHAR(3) NOT NULL,
    "total_limit" DECIMAL(24,4) NOT NULL,
    "reserved_amount" DECIMAL(24,4) NOT NULL DEFAULT 0,
    "over_commit_acknowledged" BOOLEAN NOT NULL DEFAULT false,
    "next_ledger_seq" BIGINT NOT NULL DEFAULT 1,
    "treasury_snapshot_seq" BIGINT,
    "treasury_baseline_as_of" TIMESTAMPTZ(6),
    "treasury_acked_local_seq" BIGINT,
    "treasury_included_event_seq" BIGINT,
    "version" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "program_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "invoice" (
    "id" UUID NOT NULL,
    "program_id" UUID NOT NULL,
    "external_ref" VARCHAR(64) NOT NULL,
    "status" "InvoiceStatus" NOT NULL DEFAULT 'REGISTERED',
    "face_amount" DECIMAL(24,4) NOT NULL,
    "currency_code" CHAR(3) NOT NULL,
    "reserved_program_amount" DECIMAL(24,4),
    "fx_rate_id" UUID,
    "fx_rate" DECIMAL(24,12),
    "fx_rate_source" "FxRateSource",
    "fx_rate_as_of" TIMESTAMPTZ(6),
    "fx_inverted" BOOLEAN NOT NULL DEFAULT false,
    "fx_pivot" CHAR(3),
    "treasury_ref" VARCHAR(64),
    "treasury_acked_at" TIMESTAMPTZ(6),
    "reserved_at" TIMESTAMPTZ(6),
    "released_at" TIMESTAMPTZ(6),
    "version" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "invoice_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "capacity_ledger_entry" (
    "id" UUID NOT NULL,
    "program_id" UUID NOT NULL,
    "seq" BIGINT NOT NULL,
    "entry_type" "LedgerEntryType" NOT NULL,
    "origin" "LedgerOrigin" NOT NULL,
    "delta_reserved" DECIMAL(24,4) NOT NULL DEFAULT 0,
    "delta_limit" DECIMAL(24,4) NOT NULL DEFAULT 0,
    "balance_reserved_after" DECIMAL(24,4) NOT NULL,
    "balance_limit_after" DECIMAL(24,4) NOT NULL,
    "invoice_id" UUID,
    "occurred_at" TIMESTAMPTZ(6) NOT NULL,
    "recorded_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "treasury_event_seq" BIGINT,
    "event_id" VARCHAR(64),
    "idempotency_key_id" UUID,
    "correlation_id" VARCHAR(64),
    "metadata" JSONB,

    CONSTRAINT "capacity_ledger_entry_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "fx_rate" (
    "id" UUID NOT NULL,
    "base_currency_code" CHAR(3) NOT NULL,
    "quote_currency_code" CHAR(3) NOT NULL,
    "rate" DECIMAL(24,12) NOT NULL,
    "source" "FxRateSource" NOT NULL DEFAULT 'SEED',
    "asOf" TIMESTAMPTZ(6) NOT NULL,
    "valid_until" TIMESTAMPTZ(6),
    "ingested_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "fx_rate_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "processed_message" (
    "id" UUID NOT NULL,
    "event_id" VARCHAR(64) NOT NULL,
    "topic" VARCHAR(200) NOT NULL,
    "partition" INTEGER NOT NULL,
    "offset" BIGINT NOT NULL,
    "event_type" VARCHAR(80) NOT NULL,
    "program_ref" VARCHAR(64),
    "payload_hash" CHAR(64) NOT NULL,
    "schema_version" INTEGER NOT NULL,
    "received_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "processed_at" TIMESTAMPTZ(6),

    CONSTRAINT "processed_message_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "dead_letter_message" (
    "id" UUID NOT NULL,
    "event_id" VARCHAR(64),
    "topic" VARCHAR(200) NOT NULL,
    "partition" INTEGER NOT NULL,
    "offset" BIGINT NOT NULL,
    "key_text" VARCHAR(256),
    "payload" JSONB NOT NULL,
    "headers" JSONB NOT NULL,
    "error_class" VARCHAR(120) NOT NULL,
    "error_message" TEXT NOT NULL,
    "attempts" INTEGER NOT NULL DEFAULT 1,
    "published_to_dlq" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "resolved_at" TIMESTAMPTZ(6),

    CONSTRAINT "dead_letter_message_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "reconciliation_job" (
    "id" UUID NOT NULL,
    "program_id" UUID,
    "program_ref" VARCHAR(64) NOT NULL,
    "snapshot_id" VARCHAR(64) NOT NULL,
    "snapshot_seq" BIGINT NOT NULL,
    "asOf" TIMESTAMPTZ(6) NOT NULL,
    "chunk_count" INTEGER NOT NULL DEFAULT 1,
    "chunks_received" INTEGER NOT NULL DEFAULT 0,
    "position_count" INTEGER NOT NULL DEFAULT 0,
    "status" "ReconciliationStatus" NOT NULL DEFAULT 'PENDING',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "available_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "claimed_at" TIMESTAMPTZ(6),
    "claimed_by" VARCHAR(80),
    "position_cursor" VARCHAR(64),
    "header" JSONB NOT NULL,
    "lastError" TEXT,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "completed_at" TIMESTAMPTZ(6),

    CONSTRAINT "reconciliation_job_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "reconciliation_snapshot_position" (
    "id" UUID NOT NULL,
    "job_id" UUID NOT NULL,
    "invoice_ref" VARCHAR(64) NOT NULL,
    "local_ledger_seq" BIGINT,
    "reserved_amount" DECIMAL(24,4) NOT NULL,
    "currency_code" CHAR(3) NOT NULL,
    "status_text" VARCHAR(40) NOT NULL,
    "occurred_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "reconciliation_snapshot_position_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "reconciliation_discrepancy" (
    "id" UUID NOT NULL,
    "job_id" UUID NOT NULL,
    "program_id" UUID NOT NULL,
    "kind" VARCHAR(60) NOT NULL,
    "invoice_ref" VARCHAR(64),
    "expected" DECIMAL(24,4),
    "actual" DECIMAL(24,4),
    "detail" JSONB NOT NULL,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "reconciliation_discrepancy_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "idempotency_key" (
    "id" UUID NOT NULL,
    "client_id" VARCHAR(64) NOT NULL,
    "key" VARCHAR(255) NOT NULL,
    "method" VARCHAR(10) NOT NULL,
    "path" VARCHAR(255) NOT NULL,
    "request_hash" CHAR(64) NOT NULL,
    "status" "IdempotencyStatus" NOT NULL DEFAULT 'IN_FLIGHT',
    "response_status" INTEGER,
    "response_body" JSONB,
    "resource_id" UUID,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "completed_at" TIMESTAMPTZ(6),
    "expires_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "idempotency_key_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "user" (
    "id" UUID NOT NULL,
    "email" VARCHAR(255) NOT NULL,
    "password_hash" VARCHAR(255) NOT NULL,
    "display_name" VARCHAR(120) NOT NULL,
    "role" "UserRole" NOT NULL DEFAULT 'READER',
    "is_active" BOOLEAN NOT NULL DEFAULT true,
    "token_version" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "last_login_at" TIMESTAMPTZ(6),

    CONSTRAINT "user_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "program_external_ref_key" ON "program"("external_ref");

-- CreateIndex
CREATE INDEX "program_status_idx" ON "program"("status");

-- CreateIndex
CREATE INDEX "invoice_program_id_status_idx" ON "invoice"("program_id", "status");

-- CreateIndex
CREATE INDEX "invoice_treasury_ref_idx" ON "invoice"("treasury_ref");

-- CreateIndex
CREATE UNIQUE INDEX "invoice_program_id_external_ref_key" ON "invoice"("program_id", "external_ref");

-- CreateIndex
CREATE UNIQUE INDEX "capacity_ledger_entry_event_id_key" ON "capacity_ledger_entry"("event_id");

-- CreateIndex
CREATE INDEX "capacity_ledger_entry_program_id_origin_seq_idx" ON "capacity_ledger_entry"("program_id", "origin", "seq");

-- CreateIndex
CREATE INDEX "capacity_ledger_entry_program_id_recorded_at_idx" ON "capacity_ledger_entry"("program_id", "recorded_at");

-- CreateIndex
CREATE INDEX "capacity_ledger_entry_invoice_id_idx" ON "capacity_ledger_entry"("invoice_id");

-- CreateIndex
CREATE UNIQUE INDEX "capacity_ledger_entry_program_id_seq_key" ON "capacity_ledger_entry"("program_id", "seq");

-- CreateIndex
CREATE INDEX "fx_rate_base_currency_code_quote_currency_code_asOf_idx" ON "fx_rate"("base_currency_code", "quote_currency_code", "asOf" DESC);

-- CreateIndex
CREATE UNIQUE INDEX "fx_rate_base_currency_code_quote_currency_code_source_asOf_key" ON "fx_rate"("base_currency_code", "quote_currency_code", "source", "asOf");

-- CreateIndex
CREATE UNIQUE INDEX "processed_message_event_id_key" ON "processed_message"("event_id");

-- CreateIndex
CREATE INDEX "processed_message_received_at_idx" ON "processed_message"("received_at");

-- CreateIndex
CREATE UNIQUE INDEX "processed_message_topic_partition_offset_key" ON "processed_message"("topic", "partition", "offset");

-- CreateIndex
CREATE INDEX "dead_letter_message_topic_created_at_idx" ON "dead_letter_message"("topic", "created_at");

-- CreateIndex
CREATE UNIQUE INDEX "reconciliation_job_snapshot_id_key" ON "reconciliation_job"("snapshot_id");

-- CreateIndex
CREATE INDEX "reconciliation_job_status_available_at_idx" ON "reconciliation_job"("status", "available_at");

-- CreateIndex
CREATE INDEX "reconciliation_job_program_ref_snapshot_seq_idx" ON "reconciliation_job"("program_ref", "snapshot_seq");

-- CreateIndex
CREATE UNIQUE INDEX "reconciliation_snapshot_position_job_id_invoice_ref_key" ON "reconciliation_snapshot_position"("job_id", "invoice_ref");

-- CreateIndex
CREATE INDEX "reconciliation_discrepancy_program_id_created_at_idx" ON "reconciliation_discrepancy"("program_id", "created_at");

-- CreateIndex
CREATE INDEX "idempotency_key_expires_at_idx" ON "idempotency_key"("expires_at");

-- CreateIndex
CREATE UNIQUE INDEX "idempotency_key_client_id_key_key" ON "idempotency_key"("client_id", "key");

-- CreateIndex
CREATE UNIQUE INDEX "user_email_key" ON "user"("email");

-- AddForeignKey
ALTER TABLE "program" ADD CONSTRAINT "program_currency_code_fkey" FOREIGN KEY ("currency_code") REFERENCES "currency"("code") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "invoice" ADD CONSTRAINT "invoice_program_id_fkey" FOREIGN KEY ("program_id") REFERENCES "program"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "invoice" ADD CONSTRAINT "invoice_currency_code_fkey" FOREIGN KEY ("currency_code") REFERENCES "currency"("code") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "invoice" ADD CONSTRAINT "invoice_fx_rate_id_fkey" FOREIGN KEY ("fx_rate_id") REFERENCES "fx_rate"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "capacity_ledger_entry" ADD CONSTRAINT "capacity_ledger_entry_program_id_fkey" FOREIGN KEY ("program_id") REFERENCES "program"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "capacity_ledger_entry" ADD CONSTRAINT "capacity_ledger_entry_invoice_id_fkey" FOREIGN KEY ("invoice_id") REFERENCES "invoice"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "fx_rate" ADD CONSTRAINT "fx_rate_base_currency_code_fkey" FOREIGN KEY ("base_currency_code") REFERENCES "currency"("code") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "fx_rate" ADD CONSTRAINT "fx_rate_quote_currency_code_fkey" FOREIGN KEY ("quote_currency_code") REFERENCES "currency"("code") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "reconciliation_job" ADD CONSTRAINT "reconciliation_job_program_id_fkey" FOREIGN KEY ("program_id") REFERENCES "program"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "reconciliation_snapshot_position" ADD CONSTRAINT "reconciliation_snapshot_position_job_id_fkey" FOREIGN KEY ("job_id") REFERENCES "reconciliation_job"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- Hand-edited additions below this line: DB-level guarantees not expressible
-- in the Prisma schema DSL.

ALTER TABLE program
  ADD CONSTRAINT program_reserved_non_negative CHECK (reserved_amount >= 0),
  ADD CONSTRAINT program_limit_non_negative    CHECK (total_limit >= 0),
  ADD CONSTRAINT program_no_overcommit
    CHECK (reserved_amount <= total_limit OR over_commit_acknowledged);

ALTER TABLE capacity_ledger_entry
  ADD CONSTRAINT ledger_balance_non_negative CHECK (balance_reserved_after >= 0),
  ADD CONSTRAINT ledger_seq_positive         CHECK (seq > 0),
  ADD CONSTRAINT ledger_delta_sign CHECK (
    (entry_type = 'RESERVE'        AND delta_reserved > 0 AND delta_limit = 0) OR
    (entry_type = 'RELEASE'        AND delta_reserved < 0 AND delta_limit = 0) OR
    (entry_type = 'LIMIT_INCREASE' AND delta_limit > 0    AND delta_reserved = 0) OR
    (entry_type = 'LIMIT_DECREASE' AND delta_limit < 0    AND delta_reserved = 0) OR
    (entry_type IN ('RECONCILE_BASELINE', 'MANUAL_ADJUSTMENT'))
  );

CREATE OR REPLACE FUNCTION capacity_ledger_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'capacity_ledger_entry is append-only (attempted %)', TG_OP
    USING ERRCODE = 'restrict_violation';
END $$;

CREATE TRIGGER trg_capacity_ledger_append_only
  BEFORE UPDATE OR DELETE OR TRUNCATE ON capacity_ledger_entry
  FOR EACH STATEMENT EXECUTE FUNCTION capacity_ledger_append_only();

-- NOTE: this cannot use scale(amount) <= minor_units — once a value is
-- assigned to a numeric(24,4) column, Postgres always normalises its stored
-- scale to 4 (padding with trailing zeros), so scale() would read back 4
-- unconditionally and the check would reject every row regardless of the
-- actual input precision. Comparing against ROUND(amount, minor_units)
-- correctly detects "more precision than the currency allows" instead,
-- because numeric equality ignores trailing-zero scale differences.
CREATE OR REPLACE FUNCTION assert_quantised(amount numeric, ccy char(3))
RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT amount = ROUND(amount, (SELECT minor_units FROM currency WHERE code = ccy))
$$;

ALTER TABLE invoice
  ADD CONSTRAINT invoice_face_quantised CHECK (assert_quantised(face_amount, currency_code));

ALTER TABLE fx_rate
  ADD CONSTRAINT fx_rate_positive CHECK (rate > 0),
  ADD CONSTRAINT fx_rate_not_identity CHECK (base_currency_code <> quote_currency_code);

CREATE OR REPLACE VIEW v_program_ledger_drift AS
  SELECT p.id AS program_id, p.reserved_amount,
         COALESCE(l.sum_delta, 0) AS ledger_sum,
         p.reserved_amount - COALESCE(l.sum_delta, 0) AS drift
  FROM program p
  LEFT JOIN (
    SELECT program_id, SUM(delta_reserved) AS sum_delta
    FROM capacity_ledger_entry GROUP BY program_id
  ) l ON l.program_id = p.id
  WHERE p.reserved_amount <> COALESCE(l.sum_delta, 0);

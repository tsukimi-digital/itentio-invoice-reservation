-- DB-level guarantees layered on the core schema.

-- ---------------------------------------------------------------------------
-- Outbox claim/backoff marker.
-- The relay claims a row by pushing available_at forward, bounds attempts and
-- backs off. Without it two instances publish duplicates and 50
-- permanently-failing rows starve every newer message behind them (the batch
-- is ordered by created_at and capped).
-- ---------------------------------------------------------------------------
ALTER TABLE "outbox_message"
  ADD COLUMN "available_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP;

DROP INDEX "outbox_message_status_created_at_idx";
CREATE INDEX "outbox_message_status_available_at_created_at_idx"
  ON "outbox_message"("status", "available_at", "created_at");

-- ---------------------------------------------------------------------------
-- Kafka offsets are not a stable identity.
-- The inbox insert uses `ON CONFLICT (event_id) DO NOTHING`, which covers one
-- unique constraint only. A second unique on (topic, partition, offset) means
-- that after any offset reset — including a routine `docker compose down`,
-- since only Postgres has a volume — the first new event collides on the
-- triple, raises 23505 outside the ON CONFLICT clause, aborts the transaction
-- and wedges the partition permanently.
-- Dedup stays keyed on the producer-supplied event_id, which survives topic
-- recreation. The triple is retained as a diagnostic index only.
-- ---------------------------------------------------------------------------
DROP INDEX "processed_message_topic_partition_offset_key";
CREATE INDEX "processed_message_topic_partition_offset_idx"
  ON "processed_message"("topic", "partition", "offset");

-- ---------------------------------------------------------------------------
-- Money invariants. `ledger_delta_sign` catches a negative RESERVE only three
-- statements after the guarded UPDATE has already lowered
-- program.reserved_amount, and it surfaces as a 500. These make the invoice
-- row itself unable to hold a nonsensical amount.
-- ---------------------------------------------------------------------------
ALTER TABLE "invoice"
  ADD CONSTRAINT invoice_face_amount_positive
    CHECK (face_amount > 0),
  ADD CONSTRAINT invoice_reserved_program_amount_positive
    CHECK (reserved_program_amount IS NULL OR reserved_program_amount > 0),
  -- CapacityService.release dereferences reserved_program_amount with a
  -- non-null assertion on the strength of status = 'RESERVED'. Make the
  -- database guarantee what the code already assumes.
  ADD CONSTRAINT invoice_reserved_requires_amount
    CHECK (status <> 'RESERVED' OR reserved_program_amount IS NOT NULL);

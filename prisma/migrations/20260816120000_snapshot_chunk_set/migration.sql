-- Snapshot completeness is judged on the SET of chunk indexes received rather
-- than on a count of messages. Delivery is at-least-once, so the same chunk can
-- arrive more than once; a counter cannot distinguish three copies of chunk 0
-- from chunks 0, 1 and 2, which lets a snapshot missing two thirds of its
-- positions be treated as complete and applied.
ALTER TABLE "reconciliation_job"
    ADD COLUMN "received_chunks" INTEGER[] NOT NULL DEFAULT '{}';

-- Jobs already judged complete under the old counter keep that verdict: their
-- full index range is materialised so the new predicate agrees. A job still
-- accumulating cannot have its arrived indexes reconstructed, so it restarts
-- collection — at worst it waits for a redelivery, which is the safe direction.
UPDATE "reconciliation_job"
   SET "received_chunks" = ARRAY(SELECT generate_series(0, "chunk_count" - 1))
 WHERE "chunks_received" >= "chunk_count";

ALTER TABLE "reconciliation_job" DROP COLUMN "chunks_received";

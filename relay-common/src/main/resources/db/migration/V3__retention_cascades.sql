-- Retention: finished deliveries older than the retention period are deleted by the API's housekeeping job.
-- Their attempts and outbox rows go with them, and a replay keeps existing when its original is purged.
ALTER TABLE delivery_attempts
    DROP CONSTRAINT delivery_attempts_delivery_id_fkey,
    ADD CONSTRAINT delivery_attempts_delivery_id_fkey
        FOREIGN KEY (delivery_id) REFERENCES deliveries (id) ON DELETE CASCADE;

ALTER TABLE outbox
    DROP CONSTRAINT outbox_delivery_id_fkey,
    ADD CONSTRAINT outbox_delivery_id_fkey
        FOREIGN KEY (delivery_id) REFERENCES deliveries (id) ON DELETE CASCADE;

ALTER TABLE deliveries
    DROP CONSTRAINT deliveries_replay_of_fkey,
    ADD CONSTRAINT deliveries_replay_of_fkey
        FOREIGN KEY (replay_of) REFERENCES deliveries (id) ON DELETE SET NULL;

CREATE INDEX deliveries_completed_idx ON deliveries (completed_at) WHERE completed_at IS NOT NULL;
CREATE INDEX events_created_idx ON events (created_at);

-- Operations API: list an endpoint's deliveries newest first, and find replays of a delivery.
CREATE INDEX deliveries_endpoint_created_idx ON deliveries (endpoint_id, created_at DESC, id DESC);
CREATE INDEX deliveries_created_idx ON deliveries (created_at DESC, id DESC);
CREATE INDEX deliveries_replay_of_idx ON deliveries (replay_of) WHERE replay_of IS NOT NULL;
CREATE INDEX deliveries_endpoint_dlt_idx ON deliveries (endpoint_id, dlt_at) WHERE dlt_at IS NOT NULL;

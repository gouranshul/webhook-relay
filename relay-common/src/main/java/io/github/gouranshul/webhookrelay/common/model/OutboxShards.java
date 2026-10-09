package io.github.gouranshul.webhookrelay.common.model;

/**
 * The outbox is split into a fixed number of shards by {@code hashtext(endpoint_id)}. A relay must hold a
 * shard's advisory lock to publish it, so per-endpoint order survives multiple relay instances.
 *
 * <p>The count is a constant, not configuration: rows carry their shard, and changing the count while rows
 * are pending would strand them. It must match {@code SHARD_SQL}.
 */
public final class OutboxShards {

    public static final int COUNT = 16;

    /** SQL expression computing the shard of {@code endpoint_id}. */
    public static final String SHARD_SQL = "((hashtext(endpoint_id::text) & 2147483647) % " + COUNT + ")";

    /** Namespace for {@code pg_try_advisory_xact_lock(int, int)} so these locks never collide with others. */
    public static final int ADVISORY_LOCK_NAMESPACE = 0x5EB0;

    private OutboxShards() {
    }
}

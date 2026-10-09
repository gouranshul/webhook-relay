package io.github.gouranshul.webhookrelay.e2e;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Samples the outbox once a second during a scenario and keeps the peak. A Kafka outage shows up as a growing
 * backlog (the API keeps accepting while the relay cannot publish); recovery shows up as it draining.
 */
final class BacklogSampler implements AutoCloseable {

    final AtomicLong peakPending = new AtomicLong();
    final AtomicLong peakLagSeconds = new AtomicLong();
    private final Thread thread;
    private volatile boolean running = true;

    BacklogSampler(JdbcClient jdbc) {
        thread = Thread.ofVirtual().start(() -> {
            while (running) {
                try {
                    jdbc.sql("""
                                    SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::bigint
                                      FROM outbox WHERE published_at IS NULL""")
                            .query((rs, n) -> {
                                peakPending.accumulateAndGet(rs.getLong(1), Math::max);
                                peakLagSeconds.accumulateAndGet(rs.getLong(2), Math::max);
                                return null;
                            })
                            .list();
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                } catch (RuntimeException e) {
                    // The database may be the thing under test; keep sampling.
                }
            }
        });
    }

    @Override
    public void close() {
        running = false;
        thread.interrupt();
    }

    @Override
    public String toString() {
        return "peakOutboxPending=" + peakPending.get() + " peakOutboxLag=" + peakLagSeconds.get() + "s";
    }
}

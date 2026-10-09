package io.github.gouranshul.webhookrelay.api.outbox;

import io.github.gouranshul.webhookrelay.api.config.RelayApiProperties;
import io.github.gouranshul.webhookrelay.api.outbox.OutboxRepository.PendingRow;
import io.github.gouranshul.webhookrelay.api.tracing.TraceContext;
import io.github.gouranshul.webhookrelay.common.Topics;
import io.github.gouranshul.webhookrelay.common.model.OutboxShards;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * Polls the outbox and publishes to {@code webhook.deliveries}, keyed by endpoint id.
 *
 * <p>Per shard, in one transaction: take the shard's advisory lock, claim a batch with
 * {@code FOR UPDATE SKIP LOCKED}, send every record and wait for the broker's acks, then mark the rows
 * published and commit. If the send or the commit fails the rows stay unpublished and are sent again, so the
 * guarantee is at-least-once; the dispatcher dedupes on delivery id. Safe to run on any number of instances.
 */
public class OutboxRelay implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final Duration MAX_ERROR_BACKOFF = Duration.ofSeconds(5);

    private final String name;
    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final TransactionTemplate tx;
    private final RelayApiProperties.Outbox config;
    private final Counter published;
    private final Counter failures;
    private final Timer batchTimer;

    private final List<Thread> workers = new ArrayList<>();
    private volatile boolean running;

    public OutboxRelay(String name, OutboxRepository repository, KafkaTemplate<String, String> kafka,
            ObjectMapper mapper, TransactionTemplate tx, RelayApiProperties.Outbox config, MeterRegistry registry) {
        this.name = name;
        this.repository = repository;
        this.kafka = kafka;
        this.mapper = mapper;
        this.tx = tx;
        this.config = config;
        this.published = Counter.builder("outbox.published").description("Outbox rows published to Kafka")
                .register(registry);
        this.failures = Counter.builder("outbox.publish.failures").description("Failed outbox batches (rolled back)")
                .register(registry);
        this.batchTimer = Timer.builder("outbox.batch.duration").description("Claim + publish + mark, per batch")
                .register(registry);
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        for (int i = 0; i < config.workers(); i++) {
            workers.add(Thread.ofPlatform().name(name + "-" + i).daemon(true).start(this::loop));
        }
        log.info("Outbox relay {} started with {} workers", name, config.workers());
    }

    @Override
    public synchronized void stop() {
        running = false;
        for (Thread worker : workers) {
            worker.interrupt();
        }
        for (Thread worker : workers) {
            try {
                worker.join(config.publishTimeout().plusSeconds(5).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        workers.clear();
        log.info("Outbox relay {} stopped", name);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void loop() {
        Duration errorBackoff = config.pollInterval();
        while (running) {
            try {
                if (drainOnce() == 0) {
                    sleep(config.pollInterval());
                }
                errorBackoff = config.pollInterval();
            } catch (RuntimeException e) {
                if (!running) {
                    return;
                }
                failures.increment();
                log.warn("Outbox relay {} failed to publish a batch; retrying in {}: {}", name, errorBackoff, e.toString());
                sleep(errorBackoff);
                errorBackoff = min(errorBackoff.multipliedBy(2), MAX_ERROR_BACKOFF);
            }
        }
    }

    /** One pass over every shard, starting at a random one so instances do not contend in lockstep. */
    int drainOnce() {
        if (!repository.hasPending()) {
            return 0;
        }
        int start = ThreadLocalRandom.current().nextInt(OutboxShards.COUNT);
        int total = 0;
        for (int i = 0; i < OutboxShards.COUNT && running; i++) {
            total += publishShard((start + i) % OutboxShards.COUNT);
        }
        return total;
    }

    int publishShard(int shard) {
        Integer count = tx.execute(status -> {
            if (!repository.tryLockShard(shard)) {
                return 0;
            }
            List<PendingRow> rows = repository.lockBatch(shard, config.batchSize());
            if (rows.isEmpty()) {
                return 0;
            }
            long started = System.nanoTime();
            List<CompletableFuture<?>> sends = new ArrayList<>(rows.size());
            for (PendingRow row : rows) {
                sends.add(kafka.send(toRecord(row)));
            }
            awaitAcks(sends);
            repository.markPublished(rows.stream().map(PendingRow::id).toList());
            batchTimer.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
            return rows.size();
        });
        int n = count == null ? 0 : count;
        published.increment(n);
        return n;
    }

    private ProducerRecord<String, String> toRecord(PendingRow row) {
        var record = new ProducerRecord<>(Topics.DELIVERIES, row.message().endpointId().toString(),
                mapper.writeValueAsString(row.message()));
        if (row.traceparent() != null) {
            record.headers().add(TraceContext.TRACEPARENT, row.traceparent().getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    private void awaitAcks(List<CompletableFuture<?>> sends) {
        try {
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                    .get(config.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Kafka acks", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Kafka rejected an outbox record", e.getCause());
        } catch (TimeoutException e) {
            throw new IllegalStateException("Timed out waiting for Kafka acks", e);
        }
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            if (!running) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }
}

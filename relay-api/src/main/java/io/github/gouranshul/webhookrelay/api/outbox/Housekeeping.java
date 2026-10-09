package io.github.gouranshul.webhookrelay.api.outbox;

import io.github.gouranshul.webhookrelay.api.config.RelayApiProperties;
import io.github.gouranshul.webhookrelay.api.event.IdempotencyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Deletes published outbox rows past their retention and expired idempotency keys, in bounded batches so a
 * large backlog never turns into one long-running delete. Idempotent; safe on every instance.
 */
@Component
public class Housekeeping {

    private static final Logger log = LoggerFactory.getLogger(Housekeeping.class);
    private static final int BATCH = 5_000;
    private static final int MAX_BATCHES_PER_RUN = 20;

    private final OutboxRepository outbox;
    private final IdempotencyRepository idempotency;
    private final RelayApiProperties properties;

    public Housekeeping(OutboxRepository outbox, IdempotencyRepository idempotency, RelayApiProperties properties) {
        this.outbox = outbox;
        this.idempotency = idempotency;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${relay.housekeeping.interval:60s}", initialDelayString = "${relay.housekeeping.interval:60s}")
    public void run() {
        try {
            int outboxRows = 0;
            int keys = 0;
            for (int i = 0; i < MAX_BATCHES_PER_RUN; i++) {
                int deleted = outbox.deletePublishedOlderThan(properties.outbox().retention(), BATCH);
                outboxRows += deleted;
                if (deleted < BATCH) {
                    break;
                }
            }
            for (int i = 0; i < MAX_BATCHES_PER_RUN; i++) {
                int deleted = idempotency.purgeExpired(BATCH);
                keys += deleted;
                if (deleted < BATCH) {
                    break;
                }
            }
            if (outboxRows + keys > 0) {
                log.info("Housekeeping removed {} published outbox rows and {} expired idempotency keys", outboxRows, keys);
            }
        } catch (RuntimeException e) {
            log.warn("Housekeeping run failed: {}", e.toString());
        }
    }
}

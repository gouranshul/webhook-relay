package io.github.gouranshul.webhookrelay.dispatcher.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory;
import org.springframework.kafka.retrytopic.DestinationTopicResolver;
import org.springframework.stereotype.Component;

/**
 * Forwards a record to its next retry topic, or to the DLT, from outside the listener.
 *
 * <p>Attempts run on their own threads and finish after the listener has returned, so their failures cannot
 * go through the container's error handler. This uses the same {@link DeadLetterPublishingRecoverer} that
 * {@code @RetryableTopic} would: same destination resolution (attempt count, exception classification), same
 * retry headers, and the same per-message due-time function from {@link RetryScheduling}. So a record forwarded
 * here is indistinguishable from one forwarded by the framework. The send is synchronous; if it fails, the
 * caller keeps the delivery's lease and the {@code LeaseSweeper} re-enqueues it.
 */
@Component
public class RetryForwarder {

    /** Must match the {@code @KafkaListener} id in {@link DeliveryListener}. */
    static final String MAIN_LISTENER_ID = "webhook-dispatcher";

    private final DestinationTopicResolver resolver;
    private final RetryScheduling scheduling;
    private volatile DeadLetterPublishingRecoverer recoverer;

    public RetryForwarder(DestinationTopicResolver resolver, RetryScheduling scheduling) {
        this.resolver = resolver;
        this.scheduling = scheduling;
    }

    /** Publishes the record to where {@code cause} sends it next. Throws if the broker does not ack. */
    public void forward(ConsumerRecord<?, ?> record, Exception cause) {
        recoverer().accept(record, null, cause);
    }

    private DeadLetterPublishingRecoverer recoverer() {
        DeadLetterPublishingRecoverer current = recoverer;
        if (current == null) {
            synchronized (this) {
                current = recoverer;
                if (current == null) {
                    // Created lazily: the resolver only knows the topic chain once the listener is registered.
                    DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(resolver);
                    scheduling.configure(factory);
                    current = factory.create(MAIN_LISTENER_ID);
                    recoverer = current;
                }
            }
        }
        return current;
    }
}

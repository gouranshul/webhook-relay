package io.github.gouranshul.webhookrelay.dispatcher.kafka;

import io.github.gouranshul.webhookrelay.common.Topics;
import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.dispatcher.delivery.DeadLetterHandler;
import io.github.gouranshul.webhookrelay.dispatcher.delivery.DeliveryProcessor;
import io.github.gouranshul.webhookrelay.dispatcher.delivery.PermanentDeliveryException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Consumes {@code webhook.deliveries} and, through {@code @RetryableTopic}, its retry topics
 * ({@code webhook.deliveries-retry-0..n}) and the DLT ({@code webhook.deliveries-dlt}).
 *
 * <p>A failed attempt never blocks the partition: the record is re-published to the next retry topic, whose
 * consumer pauses only that partition until the record is due. {@link PermanentDeliveryException} skips the
 * retry topics and goes straight to the DLT. Per-message jitter and {@code Retry-After} are applied by
 * {@link RetryScheduling}.
 */
@Component
public class DeliveryListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryListener.class);

    private final DeliveryProcessor processor;
    private final DeadLetterHandler deadLetters;
    private final ObjectMapper mapper;

    public DeliveryListener(DeliveryProcessor processor, DeadLetterHandler deadLetters, ObjectMapper mapper) {
        this.processor = processor;
        this.deadLetters = deadLetters;
        this.mapper = mapper;
    }

    @RetryableTopic(
            attempts = "${relay.retry.attempts:6}",
            backOff = @BackOff(
                    delayString = "${relay.retry.initial-delay:1s}",
                    multiplierString = "${relay.retry.multiplier:5}",
                    maxDelayString = "${relay.retry.max-delay:10m}"),
            exclude = PermanentDeliveryException.class,
            traversingCauses = "true",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
            dltStrategy = DltStrategy.ALWAYS_RETRY_ON_ERROR,
            numPartitions = "${relay.kafka.retry-partitions:12}",
            replicationFactor = "${relay.kafka.replication-factor:1}",
            concurrency = "${relay.dispatcher.retry-concurrency:2}")
    @KafkaListener(id = "webhook-dispatcher", topics = Topics.DELIVERIES,
            concurrency = "${relay.dispatcher.concurrency:16}")
    public void onDelivery(ConsumerRecord<String, String> record) {
        processor.process(record, decode(record));
    }

    @DltHandler
    public void onDeadLetter(ConsumerRecord<String, String> record,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String reason) {
        DeliveryMessage message;
        try {
            message = decode(record);
        } catch (PermanentDeliveryException unreadable) {
            log.error("Unreadable record reached the DLT at {}-{}@{}; nothing to mark", record.topic(),
                    record.partition(), record.offset());
            return;
        }
        deadLetters.handle(message, reason);
    }

    private DeliveryMessage decode(ConsumerRecord<String, String> record) {
        try {
            DeliveryMessage message = mapper.readValue(record.value(), DeliveryMessage.class);
            if (message.deliveryId() == null || message.endpointId() == null || message.webhookId() == null) {
                throw new PermanentDeliveryException("Delivery message is missing ids");
            }
            return message;
        } catch (JacksonException | IllegalArgumentException e) {
            throw new PermanentDeliveryException("Unreadable delivery message", e);
        }
    }
}

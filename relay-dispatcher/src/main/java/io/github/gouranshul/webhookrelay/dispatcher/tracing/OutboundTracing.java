package io.github.gouranshul.webhookrelay.dispatcher.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * Produces the W3C trace headers for the outbound webhook request. The Kafka listener observation has already
 * continued the trace from the record's {@code traceparent} header (set by the outbox relay from the original
 * {@code POST /v1/events}), so the receiver sees the same trace id the publisher did.
 */
@Component
public class OutboundTracing {

    private final ObjectProvider<Tracer> tracer;
    private final ObjectProvider<Propagator> propagator;

    public OutboundTracing(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    public Map<String, String> currentHeaders() {
        Tracer t = tracer.getIfAvailable();
        Propagator p = propagator.getIfAvailable();
        if (t == null || p == null) {
            return Map.of();
        }
        Span span = t.currentSpan();
        if (span == null) {
            return Map.of();
        }
        Map<String, String> headers = new HashMap<>();
        p.inject(span.context(), headers, Map::put);
        return headers;
    }
}

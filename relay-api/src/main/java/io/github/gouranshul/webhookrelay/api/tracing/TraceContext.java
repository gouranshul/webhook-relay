package io.github.gouranshul.webhookrelay.api.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * Captures the W3C {@code traceparent} of the current request so it can be stored with the event. The outbox
 * breaks the call stack (the relay publishes later, on another thread), so the context has to travel as data:
 * request -> events.traceparent -> Kafka header -> dispatcher span -> outbound webhook request.
 */
@Component
public class TraceContext {

    public static final String TRACEPARENT = "traceparent";

    private final ObjectProvider<Tracer> tracer;
    private final ObjectProvider<Propagator> propagator;

    public TraceContext(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /** Returns the current traceparent, or null when tracing is unavailable or there is no active span. */
    public String currentTraceparent() {
        Tracer t = tracer.getIfAvailable();
        Propagator p = propagator.getIfAvailable();
        if (t == null || p == null) {
            return null;
        }
        Span span = t.currentSpan();
        if (span == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        p.inject(span.context(), carrier, Map::put);
        return carrier.get(TRACEPARENT);
    }
}

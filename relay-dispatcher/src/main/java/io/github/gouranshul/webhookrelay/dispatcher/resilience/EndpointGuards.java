package io.github.gouranshul.webhookrelay.dispatcher.resilience;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.gouranshul.webhookrelay.dispatcher.config.DispatcherProperties;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * One circuit breaker and one bulkhead per endpoint, created on first use.
 *
 * <p>They are <b>not</b> registered in a Resilience4j registry: registries never forget an entry, so a system
 * with churning endpoints would leak. Instead guards live in a size-capped Caffeine cache (W-TinyLFU, close to
 * LRU) that also expires idle entries. Evicting a guard forgets its failure history and, briefly, its
 * concurrency count; both are safe to lose.
 *
 * <p>Metrics are aggregated by state rather than tagged per endpoint, to keep cardinality bounded.
 */
@Component
public class EndpointGuards {

    public record Guard(CircuitBreaker circuitBreaker, Bulkhead bulkhead) {
    }

    private final Cache<UUID, Guard> guards;
    private final CircuitBreakerConfig circuitBreakerConfig;
    private final BulkheadConfig bulkheadConfig;
    private final MeterRegistry registry;

    public EndpointGuards(DispatcherProperties properties, MeterRegistry registry) {
        DispatcherProperties.Resilience resilience = properties.resilience();
        DispatcherProperties.CircuitBreaker cb = resilience.circuitBreaker();
        this.circuitBreakerConfig = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(cb.slidingWindowSize())
                .minimumNumberOfCalls(cb.minimumCalls())
                .failureRateThreshold(cb.failureRateThreshold())
                .slowCallRateThreshold(cb.slowCallRateThreshold())
                .slowCallDurationThreshold(cb.slowCallDuration())
                .waitDurationInOpenState(cb.openDuration())
                .permittedNumberOfCallsInHalfOpenState(cb.halfOpenCalls())
                .build();
        this.bulkheadConfig = BulkheadConfig.custom()
                .maxConcurrentCalls(resilience.bulkheadPerEndpoint())
                .maxWaitDuration(Duration.ZERO)
                .build();
        this.registry = registry;
        this.guards = Caffeine.newBuilder()
                .maximumSize(resilience.maxEndpoints())
                .expireAfterAccess(resilience.guardIdleExpiry())
                .recordStats()
                .build();

        for (CircuitBreaker.State state : new CircuitBreaker.State[] {
            CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN, CircuitBreaker.State.HALF_OPEN}) {
            Gauge.builder("webhook.circuitbreaker.state", this, g -> g.countInState(state))
                    .description("Endpoints whose circuit breaker is in this state")
                    .tag("state", state.name().toLowerCase())
                    .register(registry);
        }
        Gauge.builder("webhook.endpoint.guards", guards, Cache::estimatedSize)
                .description("Per-endpoint guards held in memory (bounded)")
                .register(registry);
    }

    public Guard forEndpoint(UUID endpointId) {
        return guards.get(endpointId, this::create);
    }

    private Guard create(UUID endpointId) {
        String name = endpointId.toString();
        CircuitBreaker circuitBreaker = CircuitBreaker.of(name, circuitBreakerConfig);
        circuitBreaker.getEventPublisher().onStateTransition(event -> Counter.builder("webhook.circuitbreaker.transitions")
                .description("Circuit breaker state transitions")
                .tag("to", event.getStateTransition().getToState().name().toLowerCase())
                .register(registry)
                .increment());
        return new Guard(circuitBreaker, Bulkhead.of(name, bulkheadConfig));
    }

    long countInState(CircuitBreaker.State state) {
        return guards.asMap().values().stream().filter(g -> g.circuitBreaker().getState() == state).count();
    }

    long size() {
        guards.cleanUp();
        return guards.estimatedSize();
    }
}

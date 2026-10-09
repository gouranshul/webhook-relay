package io.github.gouranshul.webhookrelay.e2e;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes events at a steady rate, the way a well-behaved producer should: every logical event has its own
 * {@code Idempotency-Key}, and a failed or timed-out request is retried with the same key until it gets a 202.
 * So a chaos run must end with exactly {@code sent} events stored, no more, no fewer.
 */
final class Traffic implements AutoCloseable {

    private static final Duration GIVE_UP_AFTER = Duration.ofMinutes(4);

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor();
    private final String apiUrl;
    private final String token;

    final AtomicInteger sent = new AtomicInteger();
    final AtomicInteger accepted = new AtomicInteger();
    final AtomicInteger clientRetries = new AtomicInteger();
    final AtomicInteger gaveUp = new AtomicInteger();
    /** Slowest single HTTP response seen, in ms: shows injected latency reached the producer. */
    final AtomicLong slowestResponseMs = new AtomicLong();

    Traffic(String apiUrl, String token) {
        this.apiUrl = apiUrl;
        this.token = token;
    }

    /** Sends {@code perSecond} events per second for {@code duration}, without waiting for responses. */
    void run(String type, int perSecond, Duration duration) throws InterruptedException {
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / perSecond;
        long end = System.nanoTime() + duration.toNanos();
        long next = System.nanoTime();
        int seq = 0;
        while (System.nanoTime() < end) {
            int n = seq++;
            sent.incrementAndGet();
            senders.submit(() -> publishUntilAccepted(type, n));
            next += intervalNanos;
            long sleep = next - System.nanoTime();
            if (sleep > 0) {
                TimeUnit.NANOSECONDS.sleep(sleep);
            }
        }
    }

    private void publishUntilAccepted(String type, int seq) {
        String key = "e2e-" + UUID.randomUUID();
        String body = "{\"type\":\"" + type + "\",\"data\":{\"seq\":" + seq + ",\"order_id\":\"ord_" + seq + "\"}}";
        long deadline = System.nanoTime() + GIVE_UP_AFTER.toNanos();
        long backoffMs = 200;
        while (System.nanoTime() < deadline) {
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(apiUrl + "/v1/events"))
                        .timeout(Duration.ofSeconds(60))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + token)
                        .header("Idempotency-Key", key)
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
                long started = System.nanoTime();
                int status = http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
                slowestResponseMs.accumulateAndGet(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started), Math::max);
                if (status == 202) {
                    accepted.incrementAndGet();
                    return;
                }
            } catch (Exception e) {
                // fall through to retry
            }
            clientRetries.incrementAndGet();
            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            backoffMs = Math.min(backoffMs * 2, 5_000);
        }
        gaveUp.incrementAndGet();
    }

    /** Waits until every send has either been accepted or given up. */
    void awaitResponses(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (accepted.get() + gaveUp.get() < sent.get() && System.nanoTime() < deadline) {
            Thread.sleep(200);
        }
    }

    @Override
    public void close() {
        senders.shutdownNow();
    }
}

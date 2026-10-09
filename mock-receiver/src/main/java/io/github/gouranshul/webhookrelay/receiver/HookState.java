package io.github.gouranshul.webhookrelay.receiver;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-hook behaviour and counters. Memory is bounded: at most {@code maxRecorded} recent requests and
 * successful webhook ids are kept (oldest evicted).
 */
public final class HookState {

    /** One received request, for test assertions. */
    public record Received(String webhookId, Instant receivedAt, int status, String signature, String traceparent) {
    }

    public record Stats(long requests, long succeeded, long uniqueSucceeded, long duplicates, long signatureFailures,
            long failuresReturned) {
    }

    private volatile Behavior behavior;
    private final AtomicInteger scriptIndex = new AtomicInteger();
    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong signatureFailures = new AtomicLong();
    private final AtomicLong failuresReturned = new AtomicLong();
    private final int maxRecorded;
    private final Map<String, Boolean> succeededIds;
    private final Deque<Received> recent = new ArrayDeque<>();

    HookState(Behavior behavior, int maxRecorded) {
        this.behavior = behavior;
        this.maxRecorded = maxRecorded;
        this.succeededIds = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > HookState.this.maxRecorded;
            }
        });
    }

    public Behavior behavior() {
        return behavior;
    }

    void behavior(Behavior newBehavior) {
        this.behavior = newBehavior;
        this.scriptIndex.set(0);
    }

    /** Next scripted status, or null once the script is used up. */
    Integer nextScripted() {
        List<Integer> script = behavior.script();
        int i = scriptIndex.getAndIncrement();
        return i < script.size() ? script.get(i) : null;
    }

    long onRequest() {
        return requests.incrementAndGet();
    }

    void onSignatureFailure() {
        signatureFailures.incrementAndGet();
    }

    void onFailureReturned() {
        failuresReturned.incrementAndGet();
    }

    /** Returns true if this webhook id had already been answered 2xx (i.e. the relay sent a duplicate). */
    boolean onSuccess(String webhookId) {
        succeeded.incrementAndGet();
        boolean duplicate = succeededIds.putIfAbsent(webhookId, Boolean.TRUE) != null;
        if (duplicate) {
            duplicates.incrementAndGet();
        }
        return duplicate;
    }

    /** Whether this webhook id was answered 2xx (within the bounded memory of recent ids). */
    public boolean hasSucceeded(String webhookId) {
        return succeededIds.containsKey(webhookId);
    }

    synchronized void record(Received received) {
        recent.addLast(received);
        while (recent.size() > maxRecorded) {
            recent.removeFirst();
        }
    }

    public synchronized List<Received> recent() {
        return new ArrayList<>(recent);
    }

    public Stats stats() {
        return new Stats(requests.get(), succeeded.get(), succeeded.get() - duplicates.get(), duplicates.get(),
                signatureFailures.get(), failuresReturned.get());
    }
}

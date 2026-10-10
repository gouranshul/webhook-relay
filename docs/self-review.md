# Self-review (staff-engineer pass)

A deliberate pass over the code looking for the failure classes that matter in a delivery system. Each finding
lists what was wrong (or what was checked and is fine) and the fix. Findings marked **found by testing** came
out of the load and chaos runs rather than from reading the code.

## Lost-message paths

| # | Path | Verdict |
|---|---|---|
| L1 | Event accepted but outbox row missing | Fine. Event, deliveries, outbox rows and idempotency key commit in one transaction (`AtomicityTest` injects a failure on the outbox insert). |
| L2 | Relay publishes, then fails to mark rows | Fine. Rows stay unpublished and are sent again (at-least-once); the dispatcher dedupes on the delivery row. |
| L3 | Dispatcher commits the Kafka offset before the attempt finishes | **Fixed by design.** Since attempts are detached (ADR 0004) this is the normal case. The delivery row stays `IN_FLIGHT` under a lease and only leaves it once the outcome is recorded **and** any retry record is acked by Kafka; `LeaseSweeper` re-enqueues expired leases through the outbox. **Found by testing:** a 45 s Kafka kill made 150 forwards fail; all 150 were swept and delivered (`load/results/chaos-final.txt`). |
| L4 | Endpoint resumed while a delivery is being parked | Fine. Parking share-locks the endpoint row and resuming locks it `FOR UPDATE` before un-parking, so one of the two always sees the other. |
| L5 | DLT bookkeeping fails (DB down) | Fine. `DltStrategy.ALWAYS_RETRY_ON_ERROR` keeps re-processing the DLT record. |
| L6 | Deferral (circuit open, bulkhead full) cannot be forwarded because Kafka is down | Fine. It is thrown on the consumer thread, so the container's own recoverer fails and the record is re-polled. |
| L7 | Single Kafka broker with replication factor 1 | **Limitation, documented.** A broker disk loss loses records that the outbox already marked published. Production needs RF ≥ 3 and `min.insync.replicas=2` (producer already uses `acks=all`). |
| L8 | A delivery that crashes the dispatcher on every attempt | **Fixed.** The sweeper would have re-enqueued it forever; after 20 expired leases it is now failed (`LeaseSweeperTest.failsAPoisonPill...`). |
| L9 | Retry records routed to a partition that does not exist | **Fixed, found by benchmarking.** The recoverer forwards to the same partition *number* in the retry topic; with 48 main and 12 retry partitions, records from partitions 12-47 could not be forwarded and only survived through the lease sweeper, so a 1 s retry took hours (`load/results/retry-partition-bug.md`). Forwarding is now keyed (no explicit partition). `RetryPartitionMismatchTest` reproduces the exact `TimeoutException` without the fix. |

## Double-send paths

| # | Path | Verdict |
|---|---|---|
| D1 | Same record consumed twice (rebalance, outbox republish, retry copy) | Fine. Only a claim (`PENDING/RETRYING → IN_FLIGHT`, one `UPDATE ... RETURNING`) may send. `DeliveryFlowTest.redeliveredRecordsNeverCauseASecondSend` publishes the same record three times; the receiver sees one request. |
| D2 | Lease expires while the attempt is still recording (slow DB) | **Fixed.** The lease was request timeout + token wait + 5 s, and recording three statements under the injected 5 s Postgres latency could exceed it, letting the sweeper re-enqueue a delivery that was about to be marked `SUCCEEDED`. The margin is now 30 s. |
| D3 | Receiver processed the request but the response was lost | Inherent to HTTP; at-least-once, receivers dedupe on `Webhook-Id` (ADR 0002). Chaos runs report receiver-side duplicates: 0 in every scenario. |

## Unbounded memory

| # | Item | Verdict |
|---|---|---|
| M1 | Per-endpoint circuit breakers/bulkheads | Fine. Caffeine, size-capped (10,000) and idle-expiring; not in a Resilience4j registry. |
| M2 | Endpoint config cache (holds decrypted secrets) | Fine. Size-capped, 5 s TTL, `toString()` redacts the secret. |
| M3 | Concurrent attempts | Fine. Global in-flight semaphore (2,000) plus per-endpoint bulkhead; the platform-mode pool's queue is bounded by the semaphore. |
| M4 | Request bodies without `Content-Length` (chunked) | **Fixed.** Only `Content-Length` was checked; a chunked body was buffered by Jackson without limit. The stream is now counted and cut off at the limit (413). |
| M5 | Tables growing forever | **Fixed.** Published outbox rows (1 h) and expired idempotency keys were purged, but deliveries, attempts and events were not. Finished deliveries (with cascading attempts) and orphaned events are now purged after 30 days, in batches. |
| M6 | Metric cardinality | Fine. Tags are outcome, attempt number (capped at "7+"), reason and breaker state, never endpoint id. |
| M7 | Error bodies from receivers | Fine. At most 512 bytes are read and stored. |

## Secret handling

| # | Item | Verdict |
|---|---|---|
| S1 | Signing secrets at rest | AES-256-GCM, key from env, row id as AAD (a ciphertext copied to another row fails to decrypt), key id stored for rotation. Shown once on creation, never returned again. |
| S2 | Secrets in logs | Checked: no log statement prints an endpoint config, a secret or a payload. Records logged by topic/partition/offset or id only. |
| S3 | Secrets in git | `.env` is ignored; tests generate a random key per JVM; gitleaks (history) is clean and runs in CI. CI uploads only text test summaries, not surefire XML (which can contain per-run test keys). |
| S4 | Management API auth | Bearer token compared in constant time. Actuator exposes only `health`, `info`, `prometheus` (removed `metrics`). |
| S5 | SSRF | URL policy rejects private/loopback/link-local/CGNAT targets, credentials in URLs and non-HTTP schemes, at registration **and before every send** (DNS can change). Redirects are never followed. Residual risk: the JDK client re-resolves DNS after the check (TOCTOU); a resolver-pinned client is on the roadmap. |

## Thread-safety

`RetryForwarder` lazy init (volatile + double-checked), `OutboxRelay` start/stop (synchronized), the rate limiter's
log throttle (CAS), `OutboxMetrics` (volatile snapshot) and the mock receiver's per-hook state (synchronized/atomics)
were checked. Resilience4j and Caffeine objects are thread-safe. Kafka `ConsumerRecord`s handed to attempt threads
are immutable.

**Found by testing:** sharing the bounded attempt pool with the JDK `HttpClient` as its executor deadlocked under load
(`load/results/pt-100-deadlock.md`). Fixed by giving the client its own executor; `PlatformExecutorTest` reproduces it.

## Retry storms and timeouts

| # | Item | Verdict |
|---|---|---|
| R1 | Synchronized retries | **Fixed earlier.** Spring Kafka draws retry-topic delays once per topic; per-message jitter (+0–20 %) is added through the backoff header (ADR 0003). |
| R2 | Hammering a dead receiver | Circuit breaker opens and records skip the HTTP call; half-open allows 3 probes. `Retry-After` honoured, capped at 1 h. |
| R3 | Timeouts | HTTP connect 2 s / request 10 s; Redis 500 ms; Kafka producer `max.block.ms` 10 s, `request.timeout.ms` 10 s, `delivery.timeout.ms` 30 s (added to the dispatcher, whose forwards relied on the 120 s default); Hikari connection 5–10 s; **pgjdbc `connectTimeout`/`socketTimeout` added** (a hung socket previously blocked a thread forever); outbox publish 10 s. |
| R4 | Infrastructure errors burning delivery attempts | `DataAccessException` uses blocking retries (up to 2 min) instead of moving along the retry chain. |

## Remaining known limitations

- Deferrals (breaker open, bulkhead full, rate limited) consume retry-topic slots (ADR 0004); a delay topic is on the roadmap.

- The mock receiver's admin API is unauthenticated: it is a test tool and is not part of a deployment.

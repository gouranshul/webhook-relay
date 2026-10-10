# First full benchmark batch: INVALID, kept as evidence of a retry-routing bug

Six runs (vt-100, pt-100, vt-60, vt-slow-50, pt-slow-50, vt-200-overload) on 2026-10-09/10 completed their k6
phase with every threshold passing, but several never drained. Nine hours after the overload run, deliveries were
still cycling:

```
IN_FLIGHT=2 RETRYING=73 (attempt_count <= 5, updated_at = "now")
one delivery: attempt 1 (503) at 18:20:07, attempt 2 (503) at 03:26:16 the next day, for a 1 s retry delay
dispatcher log: TimeoutException: Partition 43 of topic webhook.deliveries-retry-0 with partition count 12 is
                not present in metadata after 10000 ms.
```

Cause: the main topic has 48 partitions, the retry topics 12, and the dead-letter recoverer forwards a failed record
to the same partition NUMBER in the next topic. Records from main partitions 12-47 could never be forwarded; their
lease expired, the lease sweeper re-enqueued them onto the same main partition, and the cycle repeated. No event
was lost (the sweeper kept every one alive), but retries effectively stopped for three quarters of the endpoints.

The tests never caught it because every test topology used equal partition counts. Fix: forward with no explicit
partition so the producer partitions by key (endpoint id). Regression test: `RetryPartitionMismatchTest` (fails
without the fix with the same TimeoutException, passes with it). All benchmark numbers in docs/benchmarks.md come
from the batch re-run after the fix.

# Benchmark run: vt-slow-50

- Date: 2026-10-10T12:35:47Z
- Window (UTC): 2026-10-10T12:29:48Z to 2026-10-10T12:35:47Z (load + drain; a valid run has no host sleep in this window)
- Settings: RATE=50/s RAMP=30s HOLD=3m ENDPOINTS_PER_TYPE=4 (3 event types)
- Retry max delay: 2m
- Dispatcher: VIRTUAL_THREADS_ENABLED=true EXECUTOR=virtual PLATFORM_POOL=200 BULKHEAD=50
- Receiver: failure rate 0.10, latency 1000-2000 ms
- k6 exit code: 0 (non-zero means a threshold failed)
- Drain after load: 146 s (complete)

## k6
```
run: vt-slow-50  target rate: 50/s  hold: 3m
requests: 9860  rate: 46.8/s  accepted: 9824
failed: 0.000%  dropped iterations: 0
publish latency ms: p50=10.6 p95=37.0 p99=81.9 max=445.2
thresholds: http_req_duration{name:publish} OK, dropped_iterations OK, checks OK, http_req_failed OK
```

## Outcome (from the database)
```
events: 9824
deliveries by status: SUCCEEDED=39296
http attempts: 43773  by outcome: RETRYABLE=4477 SUCCESS=39296
deliveries succeeded/s during hold: 200.2
accept->delivered latency, first-attempt successes (ms) p50/p95/p99/max: 1560 / 2004 / 2068 / 2420
accept->delivered latency, all successes incl. retries (ms) p50/p95/p99: 1616 / 4579 / 10801
receiver: duplicates:0,uniqueSucceeded:39296,requests:43773
```

## Dispatcher resources (Prometheus, over the run)
```
max heap used (MB): 259.02542877197266
max non-heap used (MB): 137.20380401611328
max live platform threads: 364
max attempts in flight: 374
max process CPU (cores): 4.64516129032258
max container memory (docker stats, dispatcher): 564.6MiB 
```

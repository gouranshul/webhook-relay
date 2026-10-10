# Benchmark run: vt-200-overload

- Date: 2026-10-10T12:55:35Z
- Window (UTC): 2026-10-10T12:46:52Z to 2026-10-10T12:55:35Z (load + drain; a valid run has no host sleep in this window)
- Settings: RATE=200/s RAMP=30s HOLD=3m ENDPOINTS_PER_TYPE=4 (3 event types)
- Retry max delay: 2m
- Dispatcher: VIRTUAL_THREADS_ENABLED=true EXECUTOR=virtual PLATFORM_POOL=200 BULKHEAD=50
- Receiver: failure rate 0.10, latency 50-200 ms
- k6 exit code: 99 (non-zero means a threshold failed)
- Drain after load: 308 s (complete)

## k6
```
run: vt-200-overload  target rate: 200/s  hold: 3m
requests: 38252  rate: 180.3/s  accepted: 38150
failed: 0.173%  dropped iterations: 1083
publish latency ms: p50=64.7 p95=2206.3 p99=3466.6 max=5422.4
thresholds: checks OK, http_req_failed OK, dropped_iterations FAILED, http_req_duration{name:publish} FAILED
```

## Outcome (from the database)
```
events: 38150
deliveries by status: FAILED=1 SUCCEEDED=152599
http attempts: 169373  by outcome: RETRYABLE=16774 SUCCESS=152599
deliveries succeeded/s during hold: 416.7
accept->delivered latency, first-attempt successes (ms) p50/p95/p99/max: 59207 / 175039 / 208443 / 222495
accept->delivered latency, all successes incl. retries (ms) p50/p95/p99: 59520 / 175235 / 209189
receiver: uniqueSucceeded:152599,requests:169373
```

## Dispatcher resources (Prometheus, over the run)
```
max heap used (MB): 1444.8744277954102
max non-heap used (MB): 138.7331314086914
max live platform threads: 352
max attempts in flight: 390
max process CPU (cores): 3.9415907919601443
max container memory (docker stats, dispatcher): 595.8MiB 
```

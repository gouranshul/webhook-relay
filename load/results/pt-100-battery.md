# Benchmark run: pt-100 (ON BATTERY: host CPU throttled; superseded by AC runs, kept for transparency)

- Date: 2026-10-10T11:38:25Z
- Settings: RATE=100/s RAMP=30s HOLD=3m ENDPOINTS_PER_TYPE=4 (3 event types)
- Retry max delay: 2m
- Dispatcher: VIRTUAL_THREADS_ENABLED=false EXECUTOR=platform PLATFORM_POOL=200 BULKHEAD=50
- Receiver: failure rate 0.10, latency 50-200 ms
- k6 exit code: 99 (non-zero means a threshold failed)
- Drain after load: 463 s (complete)

## k6
```
run: pt-100  target rate: 100/s  hold: 3m
requests: 19562  rate: 92.2/s  accepted: 19526
failed: 0.000%  dropped iterations: 123
publish latency ms: p50=221.7 p95=1070.4 p99=1692.7 max=3669.4
thresholds: dropped_iterations FAILED, checks OK, http_req_duration{name:publish} FAILED, http_req_failed OK
```

## Outcome (from the database)
```
events: 19526
deliveries by status: SUCCEEDED=78104
http attempts: 86623  by outcome: RETRYABLE=8519 SUCCESS=78104
deliveries succeeded/s during hold: 145.4
accept->delivered latency, first-attempt successes (ms) p50/p95/p99/max: 92988 / 267758 / 292781 / 299699
accept->delivered latency, all successes incl. retries (ms) p50/p95/p99: 93224 / 267985 / 293114
receiver: uniqueSucceeded:78104,duplicates:0,signatureFailures:0
```

## Dispatcher resources (Prometheus, over the run)
```
max heap used (MB): 221.36627197265625
max non-heap used (MB): 138.5211410522461
max live platform threads: 434
max attempts in flight: 310
max process CPU (cores): 6.821052631578947
max container memory (docker stats, dispatcher): 582.6MiB 
```

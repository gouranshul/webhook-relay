# Benchmark run: vt-60 (ON BATTERY: host CPU throttled; superseded by AC runs, kept for transparency)

- Date: 2026-10-10T11:15:30Z
- Settings: RATE=60/s RAMP=30s HOLD=3m ENDPOINTS_PER_TYPE=4 (3 event types)
- Retry max delay: 2m
- Dispatcher: VIRTUAL_THREADS_ENABLED=true EXECUTOR=virtual PLATFORM_POOL=200 BULKHEAD=50
- Receiver: failure rate 0.10, latency 50-200 ms
- k6 exit code: 0 (non-zero means a threshold failed)
- Drain after load: 165 s (complete)

## k6
```
run: vt-60  target rate: 60/s  hold: 3m
requests: 11825  rate: 56.1/s  accepted: 11789
failed: 0.000%  dropped iterations: 0
publish latency ms: p50=13.9 p95=68.8 p99=149.1 max=620.7
thresholds: checks OK, dropped_iterations OK, http_req_duration{name:publish} OK, http_req_failed OK
```

## Outcome (from the database)
```
events: 11789
deliveries by status: SUCCEEDED=47156
http attempts: 52415  by outcome: RETRYABLE=5259 SUCCESS=47156
deliveries succeeded/s during hold: 240.4
accept->delivered latency, first-attempt successes (ms) p50/p95/p99/max: 205 / 648 / 1292 / 2115
accept->delivered latency, all successes incl. retries (ms) p50/p95/p99: 215 / 1772 / 6924
receiver: duplicates:0,uniqueSucceeded:47156,requests:52415
```

## Dispatcher resources (Prometheus, over the run)
```
max heap used (MB): 138.3690643310547
max non-heap used (MB): 138.10526275634766
max live platform threads: 345
max attempts in flight: 77
max process CPU (cores): 4.215700141442715
max container memory (docker stats, dispatcher): 417.9MiB 
```

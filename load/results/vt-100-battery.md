# Benchmark run: vt-100 (ON BATTERY: host CPU throttled; superseded by AC runs, kept for transparency)

- Date: 2026-10-10T11:25:12Z
- Settings: RATE=100/s RAMP=30s HOLD=3m ENDPOINTS_PER_TYPE=4 (3 event types)
- Retry max delay: 2m
- Dispatcher: VIRTUAL_THREADS_ENABLED=true EXECUTOR=virtual PLATFORM_POOL=200 BULKHEAD=50
- Receiver: failure rate 0.10, latency 50-200 ms
- k6 exit code: 99 (non-zero means a threshold failed)
- Drain after load: 304 s (complete)

## k6
```
run: vt-100  target rate: 100/s  hold: 3m
requests: 19162  rate: 90.7/s  accepted: 18816
failed: 1.618%  dropped iterations: 523
publish latency ms: p50=1091.5 p95=4305.2 p99=5068.7 max=5700.5
thresholds: checks FAILED, http_req_failed FAILED, dropped_iterations FAILED, http_req_duration{name:publish} FAILED
```

## Outcome (from the database)
```
events: 18816
deliveries by status: SUCCEEDED=75264
http attempts: 83615  by outcome: RETRYABLE=8351 SUCCESS=75264
deliveries succeeded/s during hold: 116.4
accept->delivered latency, first-attempt successes (ms) p50/p95/p99/max: 128555 / 245577 / 274636 / 289805
accept->delivered latency, all successes incl. retries (ms) p50/p95/p99: 128795 / 245731 / 274831
receiver: signatureFailures:0
```

## Dispatcher resources (Prometheus, over the run)
```
max heap used (MB): 216.2439422607422
max non-heap used (MB): 136.6496124267578
max live platform threads: 139
max attempts in flight: 370
max process CPU (cores): 4.7424994603928345
max container memory (docker stats, dispatcher): 535.1MiB 
```

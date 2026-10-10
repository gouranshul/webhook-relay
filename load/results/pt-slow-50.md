# Benchmark run: pt-slow-50

- Date: 2026-10-10T12:45:50Z
- Window (UTC): 2026-10-10T12:36:31Z to 2026-10-10T12:45:50Z (load + drain; a valid run has no host sleep in this window)
- Settings: RATE=50/s RAMP=30s HOLD=3m ENDPOINTS_PER_TYPE=4 (3 event types)
- Retry max delay: 2m
- Dispatcher: VIRTUAL_THREADS_ENABLED=false EXECUTOR=platform PLATFORM_POOL=200 BULKHEAD=50
- Receiver: failure rate 0.10, latency 1000-2000 ms
- k6 exit code: 0 (non-zero means a threshold failed)
- Drain after load: 345 s (complete)

## k6
```
run: pt-slow-50  target rate: 50/s  hold: 3m
requests: 9860  rate: 46.8/s  accepted: 9824
failed: 0.000%  dropped iterations: 0
publish latency ms: p50=13.0 p95=34.3 p99=67.4 max=482.9
thresholds: http_req_duration{name:publish} OK, checks OK, http_req_failed OK, dropped_iterations OK
```

## Outcome (from the database)
```
events: 9824
deliveries by status: FAILED=184 SUCCEEDED=39112
http attempts: 43483  by outcome: RETRYABLE=4371 SUCCESS=39112
deliveries succeeded/s during hold: 116.9
accept->delivered latency, first-attempt successes (ms) p50/p95/p99/max: 27287 / 214675 / 325478 / 336164
accept->delivered latency, all successes incl. retries (ms) p50/p95/p99: 38842 / 223569 / 335240
receiver: hooks:load-0-0:requests:3600,succeeded:3234,uniqueSucceeded:3234,duplicates:0,signatureFailures:0,failuresReturned:366,load-0-1:requests:3569,succeeded:3227,uniqueSucceeded:3227,duplicates:0,signatureFailures:0,failuresReturned:342,load-0-2:requests:3613,succeeded:3226,uniqueSucceeded:3226,duplicates:0,signatureFailures:0,failuresReturned:387,load-0-3:requests:3579,succeeded:3227,uniqueSucceeded:3227,duplicates:0,signatureFailures:0,failuresReturned:352,load-1-0:requests:3595,succeeded:3225,uniqueSucceeded:3225,duplicates:0,signatureFailures:0,failuresReturned:370,load-1-1:requests:3602,succeeded:3227,uniqueSucceeded:3227,duplicates:0,signatureFailures:0,failuresReturned:375,load-1-2:requests:3602,succeeded:3221,uniqueSucceeded:3221,duplicates:0,signatureFailures:0,failuresReturned:381,load-1-3:requests:3589,succeeded:3230,uniqueSucceeded:3230,duplicates:0,signatureFailures:0,failuresReturned:359,load-2-0:requests:3673,succeeded:3322,uniqueSucceeded:3322,duplicates:0,signatureFailures:0,failuresReturned:351,load-2-1:requests:3665,succeeded:3328,uniqueSucceeded:3328,duplicates:0,signatureFailures:0,failuresReturned:337,load-2-2:requests:3700,succeeded:3322,uniqueSucceeded:3322,duplicates:0,signatureFailures:0,failuresReturned:378,load-2-3:requests:3696,succeeded:3323,uniqueSucceeded:3323,duplicates:0,signatureFailures:0,failuresReturned:373,signatureFailures:0,duplicates:0,uniqueSucceeded:39112,requests:43483
```

## Dispatcher resources (Prometheus, over the run)
```
max heap used (MB): 3187.935935974121
max non-heap used (MB): 138.68126678466797
max live platform threads: 395
max attempts in flight: 600
max process CPU (cores): 5
max container memory (docker stats, dispatcher): 4.409GiB 
```

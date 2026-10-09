# pt-100 (first attempt) - INVALID RUN, kept as evidence of a starvation deadlock

Settings: VIRTUAL_THREADS_ENABLED=false, delivery executor = fixed pool of 200 platform threads, RATE=100/s, 3 min hold.
The same 200-thread pool was also the JDK HttpClient's executor. With all 200 threads blocked in HttpClient.send(),
no thread was left to complete responses, so every call timed out, deliveries were dead-lettered, and endpoint
auto-pause parked the rest. Snapshot taken ~15 min after the load ended:

```
deliveries: PARKED=76813 FAILED=128 SUCCEEDED=1655
dispatcher: successful attempts/s = 0, attempts in flight = 600 (12 endpoints x bulkhead 50), live threads = 347
```

k6 (ingest was unaffected):
```
run: pt-100  target rate: 100/s  hold: 3m
requests: 19685  rate: 93.3/s  accepted: 19649
failed: 0.000%  dropped iterations: 0
publish latency ms: p50=10.2 p95=27.1 p99=55.4 max=181.8
thresholds: http_req_duration{name:publish} OK, dropped_iterations OK, http_req_failed OK, checks OK
```

Fix: the HttpClient gets its own executor (commit 'fix(dispatcher): stop sharing the attempt pool with HttpClient').

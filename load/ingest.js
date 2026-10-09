// Ingest load test for webhook-relay. Runs in the grafana/k6 container on the compose network:
//   load/run-benchmark.sh <name>      (recommended: resets state, runs, drains, collects numbers)
// or directly:
//   docker run --rm --network webhook-relay_default -v "$PWD/load:/scripts" -e TOKEN=... grafana/k6:2.3.0 run /scripts/ingest.js
//
// Env: BASE_URL (http://relay-api:8080), RECEIVER_URL (http://mock-receiver:8090), TOKEN (required),
//      RATE (events/s at plateau, default 200), RAMP (default 30s), HOLD (default 3m), ENDPOINTS_PER_TYPE (default 4)
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://relay-api:8080';
const RECEIVER_URL = __ENV.RECEIVER_URL || 'http://mock-receiver:8090';
const TOKEN = __ENV.TOKEN;
const RATE = parseInt(__ENV.RATE || '200', 10);
const RAMP = __ENV.RAMP || '30s';
const HOLD = __ENV.HOLD || '3m';
const ENDPOINTS_PER_TYPE = parseInt(__ENV.ENDPOINTS_PER_TYPE || '4', 10);
const TYPES = ['order.created', 'user.signed_up', 'invoice.paid'];

const accepted = new Counter('events_accepted');

export const options = {
  setupTimeout: '60s',
  scenarios: {
    ingest: {
      executor: 'ramping-arrival-rate',
      startRate: Math.max(1, Math.floor(RATE / 10)),
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 400,
      stages: [
        { target: RATE, duration: RAMP },
        { target: RATE, duration: HOLD },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:publish}': ['p(95)<250', 'p(99)<500'],
    checks: ['rate>0.99'],
    // Fails the run if the system could not sustain the requested arrival rate.
    dropped_iterations: ['count<' + Math.ceil(RATE * 0.01 * 60)],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const auth = { 'Content-Type': 'application/json', Authorization: `Bearer ${TOKEN}` };

// Registers ENDPOINTS_PER_TYPE endpoints per event type, each pointing at its own mock-receiver hook, and hands
// each hook its signing secret so the receiver verifies every request. Average fan-out is ENDPOINTS_PER_TYPE.
export function setup() {
  if (!TOKEN) {
    throw new Error('TOKEN is required');
  }
  const endpoints = [];
  TYPES.forEach((type, t) => {
    for (let i = 0; i < ENDPOINTS_PER_TYPE; i++) {
      const hook = `load-${t}-${i}`;
      const res = http.post(`${BASE_URL}/v1/endpoints`, JSON.stringify({
        url: `${RECEIVER_URL}/hooks/${hook}`, eventTypes: [type], rateLimitPerSec: 5000,
        description: `k6 ${type} #${i}`,
      }), { headers: auth });
      if (res.status !== 201) {
        throw new Error(`endpoint registration failed: ${res.status} ${res.body}`);
      }
      const body = res.json();
      // The receiver uses its default (env-configured) failure rate and latency, plus this secret.
      const defaults = http.get(`${RECEIVER_URL}/admin/defaults`).json();
      defaults.secret = body.secret;
      http.put(`${RECEIVER_URL}/admin/hooks/${hook}`, JSON.stringify(defaults),
        { headers: { 'Content-Type': 'application/json' } });
      endpoints.push(body.id);
    }
  });
  return { endpoints };
}

export default function () {
  const type = TYPES[Math.floor(Math.random() * TYPES.length)];
  const id = `${__VU}-${__ITER}-${Date.now()}`;
  const payload = JSON.stringify({
    type,
    data: { id, amount_cents: Math.floor(Math.random() * 100000), currency: 'EUR', note: 'k6 load test' },
  });
  const res = http.post(`${BASE_URL}/v1/events`, payload, {
    headers: Object.assign({ 'Idempotency-Key': `k6-${id}-${Math.random()}` }, auth),
    tags: { name: 'publish' },
  });
  const ok = check(res, { 'status is 202': (r) => r.status === 202 });
  if (ok) {
    accepted.add(1);
  }
}

export function handleSummary(data) {
  const out = {};
  out['/scripts/results/' + (__ENV.RUN_NAME || 'ingest') + '.json'] = JSON.stringify(data, null, 2);
  out.stdout = textSummary(data);
  return out;
}

function textSummary(data) {
  const m = data.metrics;
  const d = m['http_req_duration{name:publish}'] || m.http_req_duration;
  const fmt = (v) => (v === undefined ? 'n/a' : v.toFixed(1));
  return [
    `run: ${__ENV.RUN_NAME || 'ingest'}  target rate: ${RATE}/s  hold: ${HOLD}`,
    `requests: ${m.http_reqs.values.count}  rate: ${fmt(m.http_reqs.values.rate)}/s  accepted: ${m.events_accepted ? m.events_accepted.values.count : 0}`,
    `failed: ${(m.http_req_failed.values.rate * 100).toFixed(3)}%  dropped iterations: ${m.dropped_iterations ? m.dropped_iterations.values.count : 0}`,
    `publish latency ms: p50=${fmt(d.values.med)} p95=${fmt(d.values['p(95)'])} p99=${fmt(d.values['p(99)'])} max=${fmt(d.values.max)}`,
    `thresholds: ${Object.entries(m).filter(([, v]) => v.thresholds).map(([k, v]) => `${k} ${Object.values(v.thresholds).every((t) => t.ok) ? 'OK' : 'FAILED'}`).join(', ')}`,
    '',
  ].join('\n');
}

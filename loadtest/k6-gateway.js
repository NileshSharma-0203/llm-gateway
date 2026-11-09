// k6 load test for a deployed gateway. Not run in the environment that produced
// BENCHMARKS.md, because k6 was unavailable there.
//
//   k6 run -e BASE_URL=http://localhost:8080 -e API_KEY=gw-... -e MODEL=default loadtest/k6-gateway.js
import http from 'k6/http';
import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const cacheHits = new Rate('cache_hit');
const ttfb = new Trend('ttfb_ms', true);

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const MODEL = __ENV.MODEL || 'default';
const STREAM = __ENV.STREAM === 'true';

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 200),
      timeUnit: '1s',
      duration: __ENV.DURATION || '2m',
      preAllocatedVUs: 200,
      maxVUs: 2000,
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'] },
};

export default function () {
  const prompt = Math.floor(Math.pow(Math.random(), 3) * 1000);
  const res = http.post(
    `${BASE}/v1/chat/completions`,
    JSON.stringify({
      model: MODEL,
      temperature: 0,
      stream: STREAM,
      max_tokens: 64,
      messages: [{ role: 'user', content: `Summarize item ${prompt}` }],
    }),
    { headers: { Authorization: `Bearer ${__ENV.API_KEY}`, 'Content-Type': 'application/json' } },
  );
  check(res, { 'status is 200': (r) => r.status === 200 });
  ttfb.add(res.timings.waiting);
  cacheHits.add(res.headers['X-Cache'] === 'HIT');
}

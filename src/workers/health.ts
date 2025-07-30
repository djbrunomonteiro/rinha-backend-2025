import { interval, mergeMap, from, startWith } from "rxjs";
import axios from "axios";
import Redis from "ioredis";

const redis = new Redis({
  host: 'redis',
  port: 6379,
});

const APIS = [
  { name: 'default', baseUrl: process.env.PAYMENT_PROCESSOR_URL_DEFAULT || 'http://payment-processor-default:8080' },
  { name: 'fallback', baseUrl: process.env.PAYMENT_PROCESSOR_URL_FALLBACK || 'http://payment-processor-fallback:8080' },
];

function checkHealth() {
  interval(5000).pipe(
    startWith(0),
    mergeMap(() => from(APIS)),
    mergeMap(async ({ baseUrl }) => {
      const url = `${baseUrl}/payments/service-health`;

      try {
        const { data } = await axios.get(url, { timeout: 5000 });
        console.log(data)
        await updateHealthStatus(baseUrl, !data.failing, data.minResponseTime);
      } catch (err) {
        console.log(`❌ Falha em ${baseUrl}: ${err.message}`);
        await updateHealthStatus(baseUrl, false, 9999);
      }
    }, 2)
  ).subscribe();
}

async function updateHealthStatus(fullUrl: string, isHealthy: boolean, minResponseTime: number) {
  const { protocol, host } = new URL(fullUrl);
  const url = `${protocol}//${host}`;
  const key = `health:${url}`;

  const payload = {
    url,
    is_healthy: isHealthy ? '1' : '0',
    min_response_time: String(minResponseTime),
    checked_at: new Date().toISOString(),
  };
  await redis.hmset(key, payload);
}

checkHealth();

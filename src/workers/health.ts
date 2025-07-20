import { interval, mergeMap, from, startWith } from "rxjs";

const axios = require('axios');

const { Client } = require('pg');

const client = new Client({
  host: 'db',
  user: 'postgres',
  password: 'postgres',
  database: 'payments',
});


const APIS = [
  { name: 'default', baseUrl: process.env.PAYMENT_PROCESSOR_URL_DEFAULT || 'http://payment-processor-default:8080' },
  { name: 'fallback', baseUrl: process.env.PAYMENT_PROCESSOR_URL_FALLBACK || 'http://payment-processor-fallback:8080' },
];

async function checkHealth() {
  await client.connect();

    interval(5000).pipe(
        startWith(0),
        mergeMap(() => from(APIS)),
        mergeMap(async ({baseUrl}) => {
          const url = `${baseUrl}/payments/service-health`;
            try {
            const {data} = await axios.get(url, { timeout: 5000 });
            await updateHealthStatus(baseUrl, !data.failing, data.minResponseTime)
            } catch (err) {
            console.log(`❌ Falha em ${baseUrl}: ${err.message}`);
            }
        }, 2) // Máximo 2 simultâneos
    ).subscribe();


}

async function updateHealthStatus(fullUrl, isHealthy, minResponseTime) {
  const { protocol, host } = new URL(fullUrl);
  const url = `${protocol}//${host}`; // Garantido: http://host:port

  const query = `
    UPDATE health_checks
       SET is_healthy = $1,
           min_response_time = $2,
           checked_at = NOW()
     WHERE url = $3
  `;

  await client.query(query, [isHealthy, minResponseTime, url]);
}

checkHealth();


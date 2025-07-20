import { catchError, firstValueFrom, from, interval, mergeMap, of, startWith, toArray } from "rxjs";
const axios = require('axios');

const { Client } = require('pg');

let healthyApis = [];

const client = new Client({
  host: 'db',
  user: 'postgres',
  password: 'postgres',
  database: 'payments',
});

const maxSize = 10


async function main() {
  await client.connect();

  listenHealth();
}

async function listenHealth() {
  interval(5000).pipe(
    startWith(0),
    mergeMap(() =>
      from(
        client.query(`
          SELECT id, url, is_healthy, min_response_time
          FROM health_checks
          WHERE url LIKE '%default%' OR url LIKE '%fallback%'
        `)
      ).pipe(
        catchError((err) => {
          return of({ rows: [] }); // retorna vazio pra não quebrar
        })
      )
    )
  ).subscribe(async (res: any) => {
    const apis = res.rows;

    if (apis.length === 0) {
      healthyApis = [];
      return;
    }

    const healthy = apis
      .filter(api => api.is_healthy)
      .sort((a, b) => a.min_response_time - b.min_response_time) // mais rápido primeiro
      .map(api => api.url);

    healthyApis = healthy;

    const payments = await fetchAndLockPayments();
    if(payments.length)
    processBatchRx(payments).subscribe()

  });
}

function processBatchRx(payments: any[]) {
  return from(payments).pipe(
    mergeMap(paymentRef => {
      const payment = {
        correlationId: paymentRef.correlation_id,
        amount: Number(paymentRef.amount),
        requestedAt: paymentRef.requested_at
      };

      // Tenta enviar para o primeiro endpoint
      return from(axios.post(`${healthyApis[0]}/payments`, payment, {timeout: 3000})).pipe(
        mergeMap(() =>
          from(client.query(`
            UPDATE payments_queue
            SET status = 'approved',
                updated_at = NOW(),
                url = $2
            WHERE id = $1
          `, [paymentRef.id, healthyApis[0]]))
        ),
        catchError(() => {
          // Se falhar, tenta o segundo endpoint
          return from(axios.post(`${healthyApis[1]}/payments`, payment, {timeout: 3000})).pipe(
            mergeMap(() =>
              from(client.query(`
                UPDATE payments_queue
                SET status = 'approved',
                    updated_at = NOW(),
                    url = $2
                WHERE id = $1
              `, [paymentRef.id, healthyApis[1]]))
            ),
            catchError(async () => {
              // Se falhar novamente, incrementa retries e atualiza status
              return from(client.query(`
                UPDATE payments_queue
                SET
                  retries = retries + 1,
                  status = CASE
                    WHEN retries + 1 > 10 THEN 'failed'
                    ELSE 'pending'
                  END,
                  updated_at = NOW()
                WHERE id = $1
              `, [paymentRef.id]));
            })
          );
        })
      );
    }, maxSize)
  );
}



async function fetchAndLockPayments() {
  
  
  let payments: any[] = [];

  // Inicia a transação
  await client.query('BEGIN');

  try {
    const res = await client.query(`
      SELECT * FROM payments_queue
      WHERE status = 'pending'
        AND retries <= 10
      ORDER BY created_at ASC
      FOR UPDATE SKIP LOCKED
      LIMIT $1
    `, [maxSize]);

    payments = res.rows;

    if (payments.length === 0) {
      await client.query('COMMIT');
      return payments;
    }

    // Atualiza o status para 'processing' para indicar que estão reservados
    const ids = payments.map(p => p.id);
    await client.query(`
      UPDATE payments_queue
      SET status = 'processing', updated_at = NOW()
      WHERE id = ANY($1)
    `, [ids]);

    await client.query('COMMIT');

    return payments;
  } catch (err) {
    await client.query('ROLLBACK');
    return payments
  }
}


main().catch(console.error);

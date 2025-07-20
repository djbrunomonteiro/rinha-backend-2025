/* eslint-disable @typescript-eslint/no-unsafe-argument */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable prettier/prettier */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { HttpService } from '@nestjs/axios';
import { HttpStatus, Injectable, Res } from '@nestjs/common';
import {
  bufferCount,
  bufferTime,
  catchError,
  delay,
  filter,
  firstValueFrom,
  forkJoin,
  from,
  interval,
  map,
  mergeMap,
  of,
  retry,
  startWith,
  Subject,
  switchMap,
  tap,
  timeout,
} from 'rxjs';
import { paymentDTO } from './app.controller';
import { Response } from 'express';

import { Client } from 'pg';
import { Worker } from 'worker_threads';
import { join } from 'path';

@Injectable()
export class AppService {

  private client: Client;
  private worker: Worker;


  defaultUrl = process.env.PAYMENT_PROCESSOR_URL_DEFAULT || 'http://payment-processor-default:8080';
  fallbackUrl = process.env.PAYMENT_PROCESSOR_URL_FALLBACK || 'http://payment-processor-fallback:8080';
  healthyApis: string[] = [];

  queue$ = new Subject<paymentDTO>();



  constructor(private http: HttpService) {
    this.listenQueue()
  }

  async enqueue(payment: paymentDTO, @Res() res: Response){
    this.queue$.next(payment);
    const response$ = of(res.status(HttpStatus.ACCEPTED).json({ message: 'Pagamento enfileirado' })).pipe(delay(3500))
    return await firstValueFrom(response$);

  }

  listenQueue(){
    this.queue$.pipe(
      bufferTime(5000, undefined, 30),
      filter(payments => payments.length > 0),
      mergeMap(payments => this.dispatchPayments(payments)
      )
    ).subscribe()
  }

  dispatchPayments(batch: paymentDTO[]) {

    if(!this.healthyApis.length){
      for (let index = 0; index < batch.length; index++) {
        const payment = batch[index];
        this.insertPayment(payment, 'pending')
      }
      return of()
    }
    
    

    // Divide o lote igualmente entre as APIs
    const result$: any[] = [];

    const chunkSize = Math.ceil(batch.length / this.healthyApis.length);

    for (let i = 0; i < this.healthyApis.length; i++) {
      const subBatch = batch.slice(i * chunkSize, (i + 1) * chunkSize);
      const url = this.healthyApis[i];

      const apiRequests$ = from(subBatch).pipe(
        mergeMap(payment => this.sendPayment(payment, url))
      );

      result$.push(apiRequests$);
    }

    // Processa tudo em paralelo
    return from(result$).pipe(mergeMap(stream => stream));
  }




  async onModuleInit() {
    this.client = new Client({
      host: 'db',
      port: 5432,
      user: 'postgres',
      password: 'postgres',
      database: 'payments',
    });

    await this.client.connect();

    this.listenHealth();

    const workerPath = join(__dirname, 'workers', 'process-payments.js');
    this.worker = new Worker(workerPath);
  }

  async onModuleDestroy() {
    await this.client.end();
    if (this.worker) {
      this.worker.terminate();
    }
  }

  async listenHealth() {
    interval(5000).pipe(
      startWith(0),
      mergeMap(() =>
        from(
          this.client.query(`
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
    ).subscribe((res) => {
      const apis = res.rows;

      if (apis.length === 0) {
        this.healthyApis = [];
        return;
      }

      const healthyApis = apis.filter(api => api.is_healthy);

      let sortedApis: typeof healthyApis;

      const maxResponseTime = Math.max(...healthyApis.map(api => api.min_response_time));
      const minResponseTime = Math.min(...healthyApis.map(api => api.min_response_time));
      const diff = maxResponseTime - minResponseTime;

      if (diff > 500) {
        // Ordena pelo tempo de resposta se diferença for significativa
        sortedApis = [...healthyApis].sort((a, b) => a.min_response_time - b.min_response_time);
      } else {
        // Mantém ordem original
        sortedApis = healthyApis;
      }

      this.healthyApis = sortedApis;
      console.log(this.healthyApis)
      
    });
  }





  // async processPayment(payment: paymentDTO, @Res() res: Response) {
  //   try {
  //     await this.sendPayment(payment); // função que tenta enviar e salvar conforme falhas
  //     return res.status(HttpStatus.ACCEPTED).json({ message: 'Pagamento enfileirado' });
  //   } catch (error) {
  //     // Mesmo em caso de erro inesperado, retornamos 202 para não bloquear cliente
  //     return res.status(HttpStatus.ACCEPTED).json({ message: 'Pagamento enfileirado' });
  //   }
  // }

  sendPayment(payment: any, url: string) {
    if (!url) {
      // Sem APIs disponíveis, salva direto como pendente
      return from(this.insertPayment(payment, 'pending'));
    }

    return this.http.post(`${url}/payments`, { ...payment, amount: Number(payment.amount) }).pipe(
      retry(3),
      timeout(1500),
      switchMap((res) =>
        // Se sucesso, insere como aprovado e retorna o resultado
        from(this.insertPayment(payment, 'approved'))
      ),
      catchError(() => {
        return from(this.insertPayment(payment, 'pending'));
      })
    );
  }
  
  

  async insertPayment(payment: paymentDTO, status = 'pending') {
    const query = `
      INSERT INTO payments_queue (correlation_id, amount, requested_at, status)
      VALUES ($1, $2, $3, $4)
      RETURNING *;
    `;

    const values = [payment.correlationId, payment.amount, payment?.requestedAt, status];
    const result = await this.client.query(query, values);
    return result.rows[0];
  }
  


  async purgePayment() {
    // Limpa a tabela primeiro
    await this.client.query(`DELETE FROM payments_queue`);
    await this.client.query(`
      UPDATE payments_summary
      SET total_requests = 0,
          total_amount = 0
    `);
  
    // Observable que faz chamadas POST para ambos os endpoints em paralelo
    const purgeRequests$ = forkJoin([
      this.http.post(`${this.defaultUrl}/admin/purge-payments`, {}, {
        headers: { 'X-Rinha-Token': '123' },
      }),
      this.http.post(`${this.fallbackUrl}/admin/purge-payments`, {}, {
        headers: { 'X-Rinha-Token': '123' },
      })
    ]).pipe(
      catchError(err => {
        console.error('Erro ao purgar pagamentos:', err);
        // Aqui você pode decidir o que retornar em caso de erro
        throw err;
      }),
      map(responses => {
        // Retorna os dados da resposta do default e fallback juntos
        return {
          defaultResponse: responses[0].data,
          fallbackResponse: responses[1].data,
        };
      }),
    );
  
    // Espera todas as requisições terminarem e retorna o resultado
    const response = await firstValueFrom(purgeRequests$);
    return response;
  }


  async getPaymentsSummary() {
    const result = await this.client.query(`
      SELECT url_type, total_requests, total_amount
      FROM payments_summary
      WHERE url_type IN ('default', 'fallback')
    `);
  
    const rows = result.rows as { url_type: string; total_requests: number; total_amount: string }[];
  
    // Transforma para o formato esperado, cuidando de conversão numérica
    const summary = {
      default: { totalRequests: 0, totalAmount: 0 },
      fallback: { totalRequests: 0, totalAmount: 0 },
    };
  
    for (const row of rows) {
      summary[row.url_type] = {
        totalRequests: Number(row.total_requests),
        totalAmount: Number(row.total_amount),
      };
    }

    return summary;
  }
  
  
}

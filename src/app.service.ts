import { HttpService } from '@nestjs/axios';
import { HttpStatus, Injectable, Res } from '@nestjs/common';
import {
  BehaviorSubject,
  catchError,
  concatMap,
  delay,
  filter,
  firstValueFrom,
  forkJoin,
  from,
  interval,
  map,
  mergeMap,
  of,
  Subject,
  tap,
  withLatestFrom,
} from 'rxjs';
import { paymentDTO } from './app.controller';
import { Response } from 'express';
import Decimal from 'decimal.js';
import Redis from 'ioredis';

@Injectable()
export class AppService {

  private redis: Redis;
  private redisSub: Redis;

  private defaultUrl = process.env.PAYMENT_PROCESSOR_URL_DEFAULT || 'http://payment-processor-default:8080';
  private fallbackUrl = process.env.PAYMENT_PROCESSOR_URL_FALLBACK || 'http://payment-processor-fallback:8080';
  private healthyApis: any[] = [
    {
      url: this.defaultUrl,
      is_healthy: true,
      min_response_time: 0,
      checked_at: ''
    },
    {
      url: this.fallbackUrl,
      is_healthy: true,
      min_response_time: 100,
      checked_at: ''
    }
  ];

  private queue$ = new Subject<paymentDTO>();
  private pauser$ = new BehaviorSubject<boolean>(true);

  constructor(private http: HttpService) {}

  async onModuleInit() {
    this.redis = new Redis({ host: 'redis', port: 6379 });
    this.redisSub = new Redis({ host: 'redis', port: 6379 });
    await this.redis.del('summary');

    this.listenQueue()
    this.listenHealth();
  }

  listenPause() {
    this.redisSub.subscribe('payments:queue:pause');
    this.redisSub.on('message', (channel, message) => {
      if (channel === 'payments:queue:pause') {
        if (message === 'pause') {
          this.pauser$.next(false);
        } else if (message === 'resume') {
          this.pauser$.next(true);
        }
      }
    });
  }

  listenQueue() {
    this.queue$.pipe(
      withLatestFrom(this.pauser$),
      filter(([_, isRunning]) => isRunning), 
      map(([payment]) => payment),
      concatMap((payment) => from(this.sendPayment(payment)))
    ).subscribe();
  }

  async listenHealth() {
    interval(5000).pipe(
      mergeMap(async () => {
        const keys = await this.redis.keys('health:*');
        const results = await Promise.all(
          keys.map(async (key) => {
            const data = await this.redis.hgetall(key);

            return {
              url: data.url,
              is_healthy: data.is_healthy === '1',
              min_response_time: Number(data.min_response_time),
              checked_at: data.checked_at,
            };
          })
        );

        return results;
      })
    ).subscribe((res) => {
      this.healthyApis = res.sort((a, b) => {
        const aIsPreferred = a.url.includes('default') && a.min_response_time <= 500;
        const bIsPreferred = b.url.includes('default') && b.min_response_time <= 500;
        if (aIsPreferred && !bIsPreferred) return -1;
        if (!aIsPreferred && bIsPreferred) return 1;
        return a.min_response_time - b.min_response_time;
      });
    });
  }


  async enqueue(payment: paymentDTO, @Res() res: Response) {
    this.queue$.next({ ...payment});
    const response$ = of(res.status(HttpStatus.ACCEPTED).json({ message: 'Pagamento enfileirado' }))
    return await firstValueFrom(response$);

  }

  async sendPayment(payment: any) {
    const now = new Date();
    const requestedAt = new Date(now.getTime() - 3000);
    const body = { ...payment, requestedAt };

    let fast = this.defaultUrl;
    let slow = this.fallbackUrl;
    let sort: any[] = []


    const isHealthy = this.healthyApis.filter(({ is_healthy }) => is_healthy);

    if (!isHealthy.length) {
      sort = this.healthyApis.sort(
        (a, b) => a?.min_response_time - b?.min_response_time
      );

      const waitTime = 500
      await new Promise(resolve => setTimeout(resolve, waitTime));

    }

    fast = sort[0]?.url ?? this.defaultUrl
    slow = sort[1]?.url ?? this.fallbackUrl


    const postPayment = (url: string) => this.http.post(`${url}/payments`, body)

    const response$ = postPayment(fast).pipe(
      tap((response) => {
        if (response.data?.message.includes('successfully')) {
          this.addToSummary(fast, body)
        }
      }),
      catchError(() =>
        postPayment(slow).pipe(
          tap((response) => {
            if (response.data?.message.includes('successfully')) {
              this.addToSummary(slow, body)
            }
          }),
          catchError(() => {
            this.queue$.next(body);
            return of(null);
          })
        )
      )
    );

    return await firstValueFrom(response$);
  }

  async purgePayment() {
    await this.redis.del('payments', 'payments:ids');
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
        throw err;
      }),
      map(responses => {
        return {
          defaultResponse: responses[0].data,
          fallbackResponse: responses[1].data,
        };
      }),
    );

    return await firstValueFrom(purgeRequests$);
  }


  async addToSummary(url: string, payment: any) {
    const origin = url.includes('default') ? 'default' : 'fallback';
    const amount = Number(payment.amount || 0);
    const correlationId = payment.correlationId;
  
    const exists = await this.redis.sismember('payments:ids', correlationId);
    if (exists) {
      return;
    }
  
    await this.redis.multi()
      .sadd('payments:ids', correlationId)
      .rpush(
        'payments',
        JSON.stringify({
          ...payment,
          origin,
          amount,
        }),
      )
      .exec();
  }
  
  async getPaymentsSummary(from, to) {
    await this.redis.publish('payments:queue:pause', 'pause');
    const raw = await this.redis.lrange('payments', 0, -1);
  
    const summary = {
      default: {
        totalRequests: 0,
        totalAmount: new Decimal(0),
      },
      fallback: {
        totalRequests: 0,
        totalAmount: new Decimal(0),
      }
    };
  
    const now = Date.now();
    const fromDate = new Date(from).getTime();
    const toDate = Math.min(new Date(to).getTime(), now);
  
    for (const item of raw) {
      const payment = JSON.parse(item);
      let { origin, amount, requestedAt } = payment;
  
      const requestedAtTime = new Date(requestedAt).getTime();
      if (requestedAtTime < fromDate || requestedAtTime > toDate) {
        continue;
      }
  
      if (!summary[origin]) continue;
  
      summary[origin].totalRequests++;
      summary[origin].totalAmount = summary[origin].totalAmount.plus(new Decimal(amount));
    }
  

    const results =  {
      default: {
        totalRequests: summary.default.totalRequests,
        totalAmount: Number(summary.default.totalAmount.toFixed(3)),
      },
      fallback: {
        totalRequests: summary.fallback.totalRequests,
        totalAmount: Number(summary.fallback.totalAmount.toFixed(3)),
      },
    };

    await this.redis.publish('payments:queue:pause', 'resume');
    return results
  }


  async onModuleDestroy() {
    await this.redis.del('payments', 'payments:ids');
  }
}

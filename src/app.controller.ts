import {
  Body,
  Controller,
  Get,
  HttpCode,
  Post,
  Query,
  Res,
} from '@nestjs/common';
import { AppService } from './app.service';


export interface paymentDTO {
  correlationId: string;
  amount: number;
  requestedAt?: any;
  retry?: number
}

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Post('payments')
  @HttpCode(202)
  async createPayment(@Body() paymentData: any, @Res() res ) {
    return await this.appService.enqueue(paymentData, res);

  }

  @Post('purge-payments')
  @HttpCode(200)
  async purgePayment() {
    return await this.appService.purgePayment();
  }

  @Get('payments-summary')
  getSummary(@Query('from') from: string, @Query('to') to: string) {
    return this.appService.getPaymentsSummary(from, to);
  }
}

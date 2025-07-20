/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-return */
import {
  BadRequestException,
  Body,
  Controller,
  Get,
  HttpCode,
  Post,
  Query,
  Res,
} from '@nestjs/common';
import { AppService } from './app.service';
import { Response } from 'express';

export interface paymentDTO {
  correlationId: string;
  amount: number;
  requestedAt?: any;
}

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Post('payments')
  @HttpCode(202)
  async createPayment(@Body() paymentData: paymentDTO, @Res() res: Response) {
    const requestedAt = new Date()
    return this.appService.enqueue({...paymentData, requestedAt}, res);
  }

  @Post('purge-payments')
  @HttpCode(200)
  async purgePayment() {
    return await this.appService.purgePayment();
  }

  @Get('payments-summary')
  getSummary(@Query('from') from: string, @Query('to') to: string) {
    return this.appService.getPaymentsSummary();
  }
}

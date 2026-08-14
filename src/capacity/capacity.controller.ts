import { Body, Controller, Param, Post } from '@nestjs/common';
import { ApiBearerAuth, ApiOperation, ApiTags } from '@nestjs/swagger';
import { CapacityService } from './capacity.service';
import { ReserveDto } from './dto/reserve.dto';
import { IdempotencyCtx } from '../idempotency/idempotency-key.decorator';
import type { IdempotencyContext } from '../idempotency/idempotency';

@ApiTags('capacity')
@ApiBearerAuth()
@Controller('programs/:programRef')
export class CapacityController {
  constructor(private readonly capacity: CapacityService) {}

  @Post('reserve')
  @ApiOperation({ summary: 'Reserve program capacity for an invoice' })
  reserve(
    @Param('programRef') programRef: string,
    @Body() dto: ReserveDto,
    @IdempotencyCtx() idem: IdempotencyContext,
  ) {
    return this.capacity.reserve(
      {
        programRef,
        invoiceRef: dto.invoiceRef,
        amount: dto.amount,
        currency: dto.currency,
        requestedAt: new Date(dto.requestedAt),
      },
      idem,
    );
  }
}

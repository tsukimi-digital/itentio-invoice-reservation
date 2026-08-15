import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { ApiBearerAuth, ApiOperation, ApiTags } from '@nestjs/swagger';
import { CapacityService } from './capacity.service';
import { ReserveDto } from './dto/reserve.dto';
import { ReleaseDto } from './dto/release.dto';
import { IdempotencyCtx } from '../idempotency/idempotency-key.decorator';
import type { IdempotencyContext } from '../idempotency/idempotency';
import { Roles } from '../auth/roles.decorator';

@ApiTags('capacity')
@ApiBearerAuth()
@Controller('programs/:programRef')
export class CapacityController {
  constructor(private readonly capacity: CapacityService) {}

  // Availability (GET, below) is open to any authenticated role, including
  // READER — only capacity-mutating actions are role-restricted.
  @Roles('ADMIN', 'OPERATOR')
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

  @Roles('ADMIN', 'OPERATOR')
  @Post('release')
  @ApiOperation({ summary: 'Release a program reservation on invoice repayment' })
  release(
    @Param('programRef') programRef: string,
    @Body() dto: ReleaseDto,
    @IdempotencyCtx() idem: IdempotencyContext,
  ) {
    return this.capacity.release({ programRef, invoiceRef: dto.invoiceRef }, idem);
  }

  @Get()
  @ApiOperation({ summary: 'Current capacity availability for a program' })
  getAvailability(@Param('programRef') programRef: string) {
    return this.capacity.getAvailability(programRef);
  }
}

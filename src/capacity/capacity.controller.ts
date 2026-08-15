import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { ApiBearerAuth, ApiOperation, ApiTags } from '@nestjs/swagger';
import { CapacityService } from './capacity.service';
import { ReserveDto } from './dto/reserve.dto';
import { ReleaseDto } from './dto/release.dto';
import { ProgramParamsDto } from './dto/program-params.dto';
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
    @Param() params: ProgramParamsDto,
    @Body() dto: ReserveDto,
    @IdempotencyCtx() idem: IdempotencyContext,
  ) {
    return this.capacity.reserve(
      {
        programRef: params.programRef,
        invoiceRef: dto.invoiceRef,
        amount: dto.amount,
        currency: dto.currency,
        requestedAt: dto.requestedAt,
      },
      idem,
    );
  }

  @Roles('ADMIN', 'OPERATOR')
  @Post('release')
  @ApiOperation({ summary: 'Release a program reservation on invoice repayment' })
  release(
    @Param() params: ProgramParamsDto,
    @Body() dto: ReleaseDto,
    @IdempotencyCtx() idem: IdempotencyContext,
  ) {
    return this.capacity.release(
      { programRef: params.programRef, invoiceRef: dto.invoiceRef },
      idem,
    );
  }

  @Get()
  @ApiOperation({ summary: 'Current capacity availability for a program' })
  getAvailability(@Param() params: ProgramParamsDto) {
    return this.capacity.getAvailability(params.programRef);
  }
}

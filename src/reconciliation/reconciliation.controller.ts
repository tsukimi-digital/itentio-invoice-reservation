import { Controller, Get, NotFoundException, Param, Query } from '@nestjs/common';
import { ApiBearerAuth, ApiOperation, ApiTags } from '@nestjs/swagger';
import { PrismaService } from '../prisma/prisma.service';
import { Roles } from '../auth/roles.decorator';
import { ProgramParamsDto } from '../capacity/dto/program-params.dto';
import { ListDiscrepanciesDto } from './dto/list-discrepancies.dto';

const DEFAULT_LIMIT = 50;

/// ADR-011 says a programme that ends up over its limit after reconciliation
/// is left "visible and auditable rather than hidden". It was neither: the
/// `reconciliation_discrepancy` table was written by the worker and read by
/// nothing — no endpoint, no log, no alert — so "auditable" meant "open a
/// psql session". This endpoint, plus the warn-level log the worker now
/// emits, is what makes that ADR true.
///
/// ADMIN-only: discrepancies expose reconciliation internals and cross-system
/// disagreement, which is operator information, not client information.
@ApiTags('reconciliation')
@ApiBearerAuth()
@Controller('programs/:programRef')
export class ReconciliationController {
  constructor(private readonly prisma: PrismaService) {}

  @Roles('ADMIN')
  @Get('discrepancies')
  @ApiOperation({ summary: 'Reconciliation discrepancies recorded for a program (newest first)' })
  async list(@Param() params: ProgramParamsDto, @Query() query: ListDiscrepanciesDto) {
    const program = await this.prisma.program.findUnique({
      where: { externalRef: params.programRef },
      include: { currency: true },
    });
    if (!program) throw new NotFoundException('program');

    const rows = await this.prisma.reconciliationDiscrepancy.findMany({
      where: { programId: program.id },
      orderBy: { createdAt: 'desc' },
      take: query.limit ?? DEFAULT_LIMIT,
    });

    const minorUnits = program.currency.minorUnits;
    return {
      programRef: program.externalRef,
      currency: program.currencyCode,
      // Amounts are formatted here rather than handed over as Prisma.Decimal:
      // the global BigInt interceptor walks response objects with
      // Object.entries and would serialise a Decimal as its internals
      // ({"s":1,"e":2,"d":[...]}), bypassing toJSON. ADR-001's rule — money
      // leaves as a fixed-scale string, never a number — is enforced at the
      // boundary.
      discrepancies: rows.map((row) => ({
        id: row.id,
        kind: row.kind,
        invoiceRef: row.invoiceRef,
        expected: row.expected?.toFixed(minorUnits) ?? null,
        actual: row.actual?.toFixed(minorUnits) ?? null,
        detail: row.detail,
        createdAt: row.createdAt.toISOString(),
      })),
    };
  }
}

import { Type } from 'class-transformer';
import { IsInt, IsOptional, Max, Min } from 'class-validator';

/// Page bounds live with the DTO that enforces them, so the ceiling and the
/// fallback cannot drift apart. An unbounded list endpoint over a table that
/// grows with every reconciliation is a denial-of-service waiting to happen.
export const MIN_DISCREPANCY_LIMIT = 1;
export const MAX_DISCREPANCY_LIMIT = 200;
export const DEFAULT_DISCREPANCY_LIMIT = 50;

export class ListDiscrepanciesDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(MIN_DISCREPANCY_LIMIT)
  @Max(MAX_DISCREPANCY_LIMIT)
  limit?: number;
}

import { IsString, Matches } from 'class-validator';
import { EXTERNAL_REF } from './reserve.dto';

/// Path parameters are validated before they reach Prisma. Not an injection
/// vector — every query is parameterised — but anything longer than the
/// VarChar(64) column, or otherwise unmatched, surfaces as a 500 instead of a
/// 400, and an authenticated caller can generate those without limit.
export class ProgramParamsDto {
  @IsString()
  @Matches(EXTERNAL_REF, {
    message: 'programRef must be 1-64 chars of letters, digits, dot, dash, underscore or slash',
  })
  programRef!: string;
}

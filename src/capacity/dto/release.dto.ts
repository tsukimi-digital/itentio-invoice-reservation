import { IsString, Matches } from 'class-validator';
import { EXTERNAL_REF } from './reserve.dto';

export class ReleaseDto {
  @IsString()
  @Matches(EXTERNAL_REF, {
    message: 'invoiceRef must be 1-64 chars of letters, digits, dot, dash, underscore or slash',
  })
  invoiceRef!: string;
}

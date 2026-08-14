import { IsISO8601, IsNumberString, IsString, Length } from 'class-validator';

export class ReserveDto {
  @IsString()
  invoiceRef!: string;

  @IsNumberString()
  amount!: string;

  @IsString()
  @Length(3, 3)
  currency!: string;

  @IsISO8601()
  requestedAt!: string;
}

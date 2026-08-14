import { IsString } from 'class-validator';

export class ReleaseDto {
  @IsString()
  invoiceRef!: string;
}

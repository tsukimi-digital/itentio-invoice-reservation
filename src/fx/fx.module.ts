import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { FxService } from './fx.service';
import { FX_RATE_PROVIDER } from './fx-rate.provider';
import { DbFxRateProvider } from './db-fx-rate.provider';

@Module({
  imports: [PrismaModule],
  providers: [FxService, { provide: FX_RATE_PROVIDER, useClass: DbFxRateProvider }],
  exports: [FxService],
})
export class FxModule {}

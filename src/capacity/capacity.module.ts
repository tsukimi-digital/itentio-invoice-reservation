import { Module } from '@nestjs/common';
import { FxModule } from '../fx/fx.module';
import { CapacityService } from './capacity.service';
import { CapacityController } from './capacity.controller';

@Module({
  imports: [FxModule],
  controllers: [CapacityController],
  providers: [CapacityService],
  exports: [CapacityService],
})
export class CapacityModule {}

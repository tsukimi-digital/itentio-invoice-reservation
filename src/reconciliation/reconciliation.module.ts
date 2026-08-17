import { Module } from '@nestjs/common';
import { ReconciliationWorker } from './reconciliation.worker';
import { ReconciliationController } from './reconciliation.controller';

@Module({
  controllers: [ReconciliationController],
  providers: [ReconciliationWorker],
})
export class ReconciliationModule {}

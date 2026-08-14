import { Module } from '@nestjs/common';
import { ReconciliationWorker } from './reconciliation.worker';

@Module({ providers: [ReconciliationWorker] })
export class ReconciliationModule {}

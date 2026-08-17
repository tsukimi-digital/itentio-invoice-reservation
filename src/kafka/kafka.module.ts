import { Module } from '@nestjs/common';
import { CapacityModule } from '../capacity/capacity.module';
import { CapacityConsumerService } from './capacity-consumer.service';
import { DeadLetterService } from './dead-letter.service';

@Module({ imports: [CapacityModule], providers: [CapacityConsumerService, DeadLetterService] })
export class KafkaModule {}

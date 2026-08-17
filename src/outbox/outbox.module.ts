import { Module } from '@nestjs/common';
import { OutboxRelayService } from './outbox-relay.service';

@Module({ providers: [OutboxRelayService] })
export class OutboxModule {}

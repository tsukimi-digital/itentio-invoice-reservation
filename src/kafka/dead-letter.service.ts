import { Injectable } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import type { KafkaMessage } from 'kafkajs';

@Injectable()
export class DeadLetterService {
  constructor(private readonly prisma: PrismaService) {}

  async record(
    topic: string,
    partition: number,
    message: KafkaMessage,
    error: Error,
  ): Promise<void> {
    await this.prisma.deadLetterMessage.create({
      data: {
        eventId: message.headers?.event_id?.toString(),
        topic,
        partition,
        offset: BigInt(message.offset),
        keyText: message.key?.toString() ?? null,
        payload: JSON.parse(message.value?.toString() ?? 'null') as Prisma.InputJsonValue,
        headers: Object.fromEntries(
          Object.entries(message.headers ?? {}).map(([k, v]) => [k, v?.toString()]),
        ),
        errorClass: error.constructor.name,
        errorMessage: error.message,
      },
    });
  }
}

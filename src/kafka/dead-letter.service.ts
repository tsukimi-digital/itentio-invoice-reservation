import { Injectable } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import type { KafkaMessage } from 'kafkajs';

const MAX_ERROR_MESSAGE = 4_000;

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
        payload: toStorablePayload(message),
        headers: Object.fromEntries(
          Object.entries(message.headers ?? {}).map(([k, v]) => [k, v?.toString()]),
        ),
        errorClass: error.constructor.name,
        errorMessage: error.message.slice(0, MAX_ERROR_MESSAGE),
      },
    });
  }
}

/// The dead-letter table exists precisely to hold messages that could not be
/// parsed — so parsing must not be able to fail here.
///
/// A throw inside the DLQ writer escapes `handleOne` and `eachBatch`, kafkajs
/// retries the batch forever, the offset never advances, and one byte of
/// malformed input stops all consumption permanently — with no dead-letter row
/// to show for it. Every unparseable shape therefore has a storable
/// representation: a non-JSON payload, a tombstone (`value: null`) and a
/// literal JSON `null` (which the JSONB NOT NULL constraint rejects) all
/// become an `__unparsed` envelope. See docs/DECISIONS.md ADR-10.
function toStorablePayload(message: KafkaMessage): Prisma.InputJsonValue {
  if (message.value === null || message.value === undefined) {
    return { __unparsed: true, reason: 'tombstone', raw: null };
  }
  const raw = message.value.toString('utf8');
  try {
    const parsed: unknown = JSON.parse(raw);
    // `JSON.parse('null')` is valid JSON but violates the NOT NULL column.
    if (parsed === null) {
      return { __unparsed: true, reason: 'json-null', raw };
    }
    return parsed as Prisma.InputJsonValue;
  } catch {
    return {
      __unparsed: true,
      reason: 'invalid-json',
      raw: message.value.toString('base64'),
    };
  }
}

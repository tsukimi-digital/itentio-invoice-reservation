import { ConflictException } from '@nestjs/common';

export class InsufficientCapacityException extends ConflictException {
  constructor(details: {
    programRef: string;
    requested: { toString(): string };
    available: string;
  }) {
    super({ error: 'INSUFFICIENT_CAPACITY', ...details, requested: details.requested.toString() });
  }
}

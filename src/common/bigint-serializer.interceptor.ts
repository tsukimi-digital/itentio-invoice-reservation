import { CallHandler, ExecutionContext, Injectable, NestInterceptor } from '@nestjs/common';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

/// BigInt has no toJSON, so JSON.stringify throws on any snapshot_seq /
/// ledger seq that reaches a response body unconverted. Applied globally
/// rather than per-DTO so a newly added BigInt field can't slip through.
export function serializeBigInts<T>(value: T): T {
  if (typeof value === 'bigint') return value.toString() as unknown as T;
  if (Array.isArray(value)) return value.map(serializeBigInts) as unknown as T;
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([k, v]): [string, unknown] => [k, serializeBigInts(v)]),
    ) as T;
  }
  return value;
}

@Injectable()
export class BigIntSerializerInterceptor implements NestInterceptor {
  intercept(_context: ExecutionContext, next: CallHandler): Observable<unknown> {
    return next.handle().pipe(map((data: unknown) => serializeBigInts(data)));
  }
}

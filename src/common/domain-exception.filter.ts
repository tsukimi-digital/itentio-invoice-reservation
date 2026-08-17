import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { Prisma } from '@prisma/client';
import type { Request, Response } from 'express';
import { FxRateUnavailableError } from '../fx/fx-rate.provider';
import { CurrencyMismatchError } from '../money/money';

/// Postgres SQLSTATE codes that Prisma surfaces inside `meta.code` on P2010
/// (raw-query failures). A CHECK constraint doing its job — the last line of
/// defence for money invariants — is an expected condition, so it must reach
/// the client as a 4xx rather than as an opaque 500 that also reads as a
/// server fault in the logs.
const SQLSTATE = {
  CHECK_VIOLATION: '23514',
  FOREIGN_KEY_VIOLATION: '23503',
  UNIQUE_VIOLATION: '23505',
  INVALID_TEXT_REPRESENTATION: '22P02',
  NOT_NULL_VIOLATION: '23502',
  /// Contention outcomes. `55P03` is what `SET LOCAL lock_timeout` in
  /// CapacityService produces when a competing reservation holds the programme
  /// row; `40001`/`40P01` are the serialisation and deadlock equivalents. All
  /// three mean "try again shortly", not "the server is broken".
  LOCK_NOT_AVAILABLE: '55P03',
  SERIALIZATION_FAILURE: '40001',
  DEADLOCK_DETECTED: '40P01',
} as const;

/// Retry hint sent with a contention response, in seconds.
const CONTENTION_RETRY_AFTER = '1';

interface Mapped {
  status: number;
  error: string;
  message: string;
  headers?: Record<string, string>;
}

/// Shape of the `http-errors` objects Express body parsers throw (a rejected
/// payload carries `status`, and `type` names the reason). They are plain
/// errors rather than HttpExceptions, so without an explicit branch they fall
/// through to the unhandled path and a rejected request body is reported as a
/// server fault.
interface HttpErrorLike {
  status?: unknown;
  statusCode?: unknown;
  type?: unknown;
}

const BODY_PARSER_ERRORS: Record<string, string> = {
  'entity.too.large': 'PAYLOAD_TOO_LARGE',
  'entity.parse.failed': 'MALFORMED_BODY',
  'entity.verify.failed': 'MALFORMED_BODY',
  'request.aborted': 'REQUEST_ABORTED',
  'request.size.invalid': 'INVALID_REQUEST',
  'encoding.unsupported': 'UNSUPPORTED_ENCODING',
  'charset.unsupported': 'UNSUPPORTED_ENCODING',
  'parameters.too.many': 'INVALID_REQUEST',
};

/// Maps domain and persistence failures onto HTTP statuses. Registered
/// globally in `configureApp` so the HTTP contract is identical in production
/// and in the e2e suite.
///
/// Deliberately narrow: anything not recognised here stays a 500 with a
/// generic body. The point is to stop *expected* conditions (unknown program,
/// no FX rate, violated CHECK) from masquerading as server faults — not to
/// dress every failure up as a 4xx.
@Catch()
export class DomainExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(DomainExceptionFilter.name);

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();

    // Anything already expressed as an HttpException (including everything
    // thrown deliberately by the services and by ValidationPipe) passes
    // through untouched.
    if (exception instanceof HttpException) {
      response.status(exception.getStatus()).json(exception.getResponse());
      return;
    }

    const mapped = this.map(exception);
    if (mapped) {
      for (const [header, value] of Object.entries(mapped.headers ?? {})) {
        response.setHeader(header, value);
      }
      response.status(mapped.status).json({
        statusCode: mapped.status,
        error: mapped.error,
        message: mapped.message,
      });
      return;
    }

    this.logger.error(
      `Unhandled error on ${request.method} ${request.url}: ${String(exception)}`,
      exception instanceof Error ? exception.stack : undefined,
    );
    response.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
      statusCode: HttpStatus.INTERNAL_SERVER_ERROR,
      error: 'INTERNAL_ERROR',
      message: 'Internal server error',
    });
  }

  private map(exception: unknown): Mapped | null {
    if (exception instanceof FxRateUnavailableError) {
      return {
        status: HttpStatus.UNPROCESSABLE_ENTITY,
        error: 'FX_RATE_UNAVAILABLE',
        message: exception.message,
      };
    }
    if (exception instanceof CurrencyMismatchError) {
      return {
        status: HttpStatus.BAD_REQUEST,
        error: 'CURRENCY_MISMATCH',
        message: exception.message,
      };
    }
    if (exception instanceof Prisma.PrismaClientKnownRequestError) {
      return this.mapPrisma(exception);
    }
    if (exception instanceof Prisma.PrismaClientValidationError) {
      return {
        status: HttpStatus.BAD_REQUEST,
        error: 'INVALID_REQUEST',
        message: 'Request could not be applied to the data model',
      };
    }
    return this.mapBodyParser(exception);
  }

  /// A body rejected before it ever reached a handler — too large, malformed,
  /// wrongly encoded. The status the parser chose is authoritative and is
  /// honoured as long as it is a client error; a 5xx from this source would be
  /// a genuine fault and is left to the unhandled path.
  private mapBodyParser(exception: unknown): Mapped | null {
    if (typeof exception !== 'object' || exception === null) return null;
    const candidate = exception as HttpErrorLike;
    const raw = candidate.status ?? candidate.statusCode;
    if (typeof raw !== 'number' || !Number.isInteger(raw) || raw < 400 || raw > 499) {
      return null;
    }
    const type = typeof candidate.type === 'string' ? candidate.type : undefined;
    const error = (type && BODY_PARSER_ERRORS[type]) ?? undefined;
    if (!error) return null;

    return {
      status: raw,
      error,
      message:
        error === 'PAYLOAD_TOO_LARGE'
          ? 'Request body exceeds the maximum accepted size'
          : 'Request body could not be read',
    };
  }

  private mapPrisma(e: Prisma.PrismaClientKnownRequestError): Mapped | null {
    switch (e.code) {
      case 'P2025': // findUniqueOrThrow / update on a row that does not exist
        return {
          status: HttpStatus.NOT_FOUND,
          error: 'NOT_FOUND',
          message: 'Resource not found',
        };
      case 'P2002': // unique constraint
        return {
          status: HttpStatus.CONFLICT,
          error: 'CONFLICT',
          message: 'Resource already exists',
        };
      case 'P2003': // foreign key — e.g. an unknown currency code
        return {
          status: HttpStatus.BAD_REQUEST,
          error: 'INVALID_REFERENCE',
          message: 'Request references an unknown related resource',
        };
      case 'P2010':
        return this.mapRawSqlState(e);
      case 'P2034': // write conflict / deadlock on a non-raw operation
        return this.contention();
      default:
        return null;
    }
  }

  /// Losing a race for the programme row is the concurrency design working as
  /// intended, so the response says "retry" rather than reporting a fault. It
  /// is reached only after `withRetry` has exhausted its in-process attempts.
  private contention(): Mapped {
    return {
      status: HttpStatus.SERVICE_UNAVAILABLE,
      error: 'CONTENTION',
      message: 'The programme is busy with another capacity change; retry shortly',
      headers: { 'Retry-After': CONTENTION_RETRY_AFTER },
    };
  }

  /// P2010 wraps a raw-query failure; the real SQLSTATE lives in `meta.code`.
  /// This is the path every hand-written `$queryRaw` in CapacityService takes.
  private mapRawSqlState(e: Prisma.PrismaClientKnownRequestError): Mapped | null {
    const sqlState = (e.meta as { code?: string } | undefined)?.code;
    switch (sqlState) {
      case SQLSTATE.CHECK_VIOLATION:
        return {
          status: HttpStatus.UNPROCESSABLE_ENTITY,
          error: 'INVARIANT_VIOLATION',
          message: 'Request violates a data integrity rule',
        };
      case SQLSTATE.FOREIGN_KEY_VIOLATION:
        return {
          status: HttpStatus.BAD_REQUEST,
          error: 'INVALID_REFERENCE',
          message: 'Request references an unknown related resource',
        };
      case SQLSTATE.UNIQUE_VIOLATION:
        return {
          status: HttpStatus.CONFLICT,
          error: 'CONFLICT',
          message: 'Resource already exists',
        };
      case SQLSTATE.INVALID_TEXT_REPRESENTATION:
      case SQLSTATE.NOT_NULL_VIOLATION:
        return {
          status: HttpStatus.BAD_REQUEST,
          error: 'INVALID_REQUEST',
          message: 'Request contains a malformed value',
        };
      case SQLSTATE.LOCK_NOT_AVAILABLE:
      case SQLSTATE.SERIALIZATION_FAILURE:
      case SQLSTATE.DEADLOCK_DETECTED:
        return this.contention();
      default:
        return null;
    }
  }
}

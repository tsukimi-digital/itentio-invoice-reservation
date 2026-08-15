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
/// (raw-query failures). Without this mapping a CHECK constraint doing its job
/// — the last line of defence for money invariants — reaches the client as an
/// opaque 500, and every rejected request pollutes the error log as if the
/// server had malfunctioned.
const SQLSTATE = {
  CHECK_VIOLATION: '23514',
  FOREIGN_KEY_VIOLATION: '23503',
  UNIQUE_VIOLATION: '23505',
  INVALID_TEXT_REPRESENTATION: '22P02',
  NOT_NULL_VIOLATION: '23502',
} as const;

interface Mapped {
  status: number;
  error: string;
  message: string;
}

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
    return null;
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
      default:
        return null;
    }
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
      default:
        return null;
    }
  }
}

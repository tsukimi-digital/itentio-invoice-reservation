import { Controller, Get } from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { Public } from '../auth/public.decorator';

/**
 * Unauthenticated on purpose: this is the container liveness probe, not a
 * business endpoint. Every endpoint that exposes program or invoice data is
 * authenticated.
 */
@ApiTags('health')
@Controller('health')
export class HealthController {
  @Public()
  @Get()
  @ApiOperation({ summary: 'Liveness probe' })
  check(): { status: string } {
    return { status: 'ok' };
  }
}

import { Body, Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { Throttle } from '@nestjs/throttler';
import { Public } from './public.decorator';
import { AuthService } from './auth.service';
import { LoginDto } from './dto/login.dto';
import { RefreshDto } from './dto/refresh.dto';
import { DEFAULT_THROTTLER, LOGIN_RATE_LIMIT, RATE_LIMIT_WINDOW_MS } from '../config/rate-limits';

@ApiTags('auth')
@Controller('auth')
export class AuthController {
  constructor(private readonly auth: AuthService) {}

  @Public()
  // Stricter than the app-wide default (100/min) — this is the one endpoint
  // reachable without a token, so it's the brute-force target.
  @Throttle({ [DEFAULT_THROTTLER]: { limit: LOGIN_RATE_LIMIT, ttl: RATE_LIMIT_WINDOW_MS } })
  @Post('login')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Exchange credentials for a JWT + refresh token' })
  login(@Body() dto: LoginDto) {
    return this.auth.login(dto.email, dto.password);
  }

  // Not a credential-guessing target — the refresh token is a 256-bit random
  // value, not a password — so the app-wide default throttle is sufficient.
  @Public()
  @Post('refresh')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Rotate a refresh token for a new access token + refresh token' })
  refresh(@Body() dto: RefreshDto) {
    return this.auth.refresh(dto.refreshToken);
  }
}

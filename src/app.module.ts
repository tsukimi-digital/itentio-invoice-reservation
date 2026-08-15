import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { APP_GUARD } from '@nestjs/core';
import { ThrottlerGuard, ThrottlerModule } from '@nestjs/throttler';
import { validateEnv } from './config/env.schema';
import { HealthModule } from './health/health.module';
import { AuthModule } from './auth/auth.module';
import { PrismaModule } from './prisma/prisma.module';
import { CapacityModule } from './capacity/capacity.module';
import { OutboxModule } from './outbox/outbox.module';
import { KafkaModule } from './kafka/kafka.module';
import { ReconciliationModule } from './reconciliation/reconciliation.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      validate: validateEnv,
    }),
    // Named 'default' throttler applies to every route unless overridden
    // with @Throttle({ default: {...} }) — see AuthController.login for the
    // stricter override (brute-force protection on the one endpoint that
    // doesn't require a token to hit).
    ThrottlerModule.forRoot([{ name: 'default', ttl: 60_000, limit: 100 }]),
    PrismaModule,
    HealthModule,
    AuthModule,
    CapacityModule,
    OutboxModule,
    KafkaModule,
    ReconciliationModule,
  ],
  providers: [{ provide: APP_GUARD, useClass: ThrottlerGuard }],
})
export class AppModule {}

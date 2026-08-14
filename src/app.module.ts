import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { validateEnv } from './config/env.schema';
import { HealthModule } from './health/health.module';
import { AuthModule } from './auth/auth.module';
import { PrismaModule } from './prisma/prisma.module';
import { CapacityModule } from './capacity/capacity.module';
import { OutboxModule } from './outbox/outbox.module';
import { KafkaModule } from './kafka/kafka.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      validate: validateEnv,
    }),
    PrismaModule,
    HealthModule,
    AuthModule,
    CapacityModule,
    OutboxModule,
    KafkaModule,
  ],
})
export class AppModule {}

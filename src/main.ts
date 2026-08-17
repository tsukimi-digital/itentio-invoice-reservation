import { ConfigService } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';
import { configureApp } from './bootstrap';
import type { Env } from './config/env.schema';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  // Without this, onModuleDestroy (Kafka consumer/producer disconnect, the
  // reconciliation worker's interval) only runs on an explicit app.close()
  // call — a real SIGTERM (container restart) would not trigger it, leaving
  // the consumer to time out of its group instead of leaving cleanly.
  app.enableShutdownHooks();

  // Shared with the e2e suite — see configureApp's doc comment.
  configureApp(app);

  const configService = app.get(ConfigService<Env, true>);
  const port = configService.get('PORT', { infer: true });

  await app.listen(port);
}

bootstrap().catch((error: unknown) => {
  new Logger('Bootstrap').error(
    `Failed to start application: ${String(error)}`,
    error instanceof Error ? error.stack : undefined,
  );
  process.exit(1);
});

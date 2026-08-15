import { INestApplication, ValidationPipe } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { BigIntSerializerInterceptor } from './common/bigint-serializer.interceptor';
import { DomainExceptionFilter } from './common/domain-exception.filter';
import type { Env } from './config/env.schema';

interface ExpressLike {
  set(key: string, value: unknown): void;
  disable(key: string): void;
}

/// Everything that shapes the HTTP contract lives here rather than in
/// `bootstrap()`, because `app.useGlobalPipes`/`useGlobalFilters` are calls on
/// the application instance — a test that builds the app through
/// `Test.createTestingModule(...).createNestApplication()` does NOT inherit
/// them. Before this was extracted, no e2e spec exercised ValidationPipe at
/// all: the suite was testing a differently-configured application than the
/// one that ships.
export function configureApp(app: INestApplication): void {
  const config = app.get(ConfigService<Env, true>);
  const nodeEnv = config.get('NODE_ENV', { infer: true });

  const httpAdapter = app.getHttpAdapter().getInstance() as ExpressLike;

  // ThrottlerGuard keys on `req.ip`. Without this, Express reports the socket
  // peer address — which behind any load balancer is the proxy, so every
  // client in the world shares one rate-limit bucket and the 5/min login
  // limit becomes a system-wide denial of service. Default 0 (trust nothing)
  // is the safe choice; deployments behind a proxy set the real hop count.
  httpAdapter.set('trust proxy', config.get('TRUST_PROXY_HOPS', { infer: true }));
  httpAdapter.disable('x-powered-by');

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );
  app.useGlobalInterceptors(new BigIntSerializerInterceptor());
  app.useGlobalFilters(new DomainExceptionFilter());

  // Swagger mounts its handlers directly on the HTTP adapter, not as Nest
  // controllers, so the global JwtAuthGuard/RolesGuard/ThrottlerGuard never
  // run for /docs and /docs-json. That makes the full API surface readable
  // without a token — acceptable locally, not in production.
  if (nodeEnv !== 'production') {
    const document = SwaggerModule.createDocument(
      app,
      new DocumentBuilder()
        .setTitle('Program Capacity & Invoice Reservation')
        .setDescription('Tracks program credit capacity and invoice reservations in real time')
        .setVersion('0.1.0')
        .addBearerAuth()
        .build(),
    );
    SwaggerModule.setup('docs', app, document);
  }
}

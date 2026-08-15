import { CanActivate, ExecutionContext, ForbiddenException, Injectable } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import type { UserRole } from '@prisma/client';
import { ROLES_KEY } from './roles.decorator';
import type { JwtClaims } from './auth.service';

/// Runs after JwtAuthGuard (see AuthModule's providers array — order there
/// determines global-guard execution order), so request.user is already
/// populated. A route with no @Roles() metadata is unrestricted by role.
@Injectable()
export class RolesGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const required = this.reflector.getAllAndOverride<UserRole[] | undefined>(ROLES_KEY, [
      context.getHandler(),
      context.getClass(),
    ]);
    if (!required || required.length === 0) return true;

    const request = context.switchToHttp().getRequest<{ user?: JwtClaims }>();
    const role = request.user?.role;
    if (!role || !required.includes(role as UserRole)) {
      throw new ForbiddenException(`Requires one of role: ${required.join(', ')}`);
    }
    return true;
  }
}

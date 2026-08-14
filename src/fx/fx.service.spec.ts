import { Test } from '@nestjs/testing';
import { FxService } from './fx.service';
import { FX_RATE_PROVIDER, FxRateProvider } from './fx-rate.provider';
import { Money } from '../money/money';
import { Dec } from '../money/decimal';

describe('FxService', () => {
  it('returns the input unchanged for same-currency conversion', async () => {
    const resolve = jest.fn();
    const provider: FxRateProvider = { resolve };
    const module = await Test.createTestingModule({
      providers: [FxService, { provide: FX_RATE_PROVIDER, useValue: provider }],
    }).compile();
    const fx = module.get(FxService);

    const result = await fx.convert(Money.of('10.00', 'GBP', 2), 'GBP', 2, new Date());
    expect(result.amount.toString()).toBe('10.00');
    expect(resolve).not.toHaveBeenCalled();
  });

  it('converts using a direct rate and quantises once', async () => {
    const provider: FxRateProvider = {
      resolve: jest.fn().mockResolvedValue({
        rateId: 'r1',
        rate: new Dec('0.855000000000'),
        inverted: false,
        pivot: null,
        source: 'SEED',
        asOf: new Date(),
      }),
    };
    const module = await Test.createTestingModule({
      providers: [FxService, { provide: FX_RATE_PROVIDER, useValue: provider }],
    }).compile();
    const fx = module.get(FxService);

    const result = await fx.convert(Money.of('100.00', 'EUR', 2), 'GBP', 2, new Date());
    expect(result.amount.toString()).toBe('85.50');
    expect(result.audit.effectiveRate.toString()).toBe('0.855');
  });

  it('divides rather than multiplying by a materialised inverse', async () => {
    // stored rate is GBP->EUR; converting EUR->GBP must divide, not invert-then-multiply
    const provider: FxRateProvider = {
      resolve: jest.fn().mockResolvedValue({
        rateId: 'r2',
        rate: new Dec('1.169590643274'),
        inverted: true,
        pivot: null,
        source: 'SEED',
        asOf: new Date(),
      }),
    };
    const module = await Test.createTestingModule({
      providers: [FxService, { provide: FX_RATE_PROVIDER, useValue: provider }],
    }).compile();
    const fx = module.get(FxService);

    const result = await fx.convert(Money.of('100.00', 'EUR', 2), 'GBP', 2, new Date());
    expect(result.amount.toString()).toBe('85.50');
  });
});

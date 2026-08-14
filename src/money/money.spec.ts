import { Money, CurrencyMismatchError } from './money';

describe('Money', () => {
  it('quantises on construction to the currency minor units', () => {
    const m = Money.of('18.239', 'GBP', 2);
    expect(m.toString()).toBe('18.24'); // HALF_EVEN: 18.239 -> 18.24
  });

  it('adds same-currency amounts exactly', () => {
    const a = Money.of('10.10', 'GBP', 2);
    const b = Money.of('5.05', 'GBP', 2);
    expect(a.plus(b).toString()).toBe('15.15');
  });

  it('refuses cross-currency arithmetic', () => {
    const gbp = Money.of('10.00', 'GBP', 2);
    const usd = Money.of('10.00', 'USD', 2);
    expect(() => gbp.plus(usd)).toThrow(CurrencyMismatchError);
  });

  it('serialises to a JSON string, never a number', () => {
    const m = Money.of('1234.56', 'GBP', 2);
    expect(JSON.stringify(m)).toBe('{"amount":"1234.56","currency":"GBP"}');
  });
});

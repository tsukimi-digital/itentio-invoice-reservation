import { serializeBigInts } from './bigint-serializer.interceptor';

describe('serializeBigInts', () => {
  it('converts a top-level BigInt to a string', () => {
    expect(serializeBigInts({ seq: 42n })).toEqual({ seq: '42' });
  });

  it('converts nested and array BigInts', () => {
    expect(serializeBigInts({ a: [{ b: 1n }, { b: 2n }] })).toEqual({
      a: [{ b: '1' }, { b: '2' }],
    });
  });

  it('leaves non-BigInt values untouched', () => {
    expect(serializeBigInts({ a: 'x', b: 1, c: null })).toEqual({ a: 'x', b: 1, c: null });
  });
});

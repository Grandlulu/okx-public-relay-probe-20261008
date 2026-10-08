import test from 'node:test';
import assert from 'node:assert/strict';
import { isAuthorized, routeFor, isAllowedClientFrame } from '../policy.mjs';
const token = 'test-only-probe-token-never-used-on-cloud';
const frame = (channel, op = 'subscribe', fields = {}) => JSON.stringify({ op, args: [{ channel, instId: 'BTC-USDT' }], ...fields });

test('accepts the exact configured token and rejects missing/incorrect credentials', () => {
  assert.equal(isAuthorized(token, token), true);
  for (const supplied of [undefined, '', token + 'x', ['bad', token]]) assert.equal(isAuthorized(supplied, token), false);
  assert.equal(isAuthorized('short', 'short'), false);
});
test('routes only exact public paths without query or private endpoints', () => {
  assert.equal(routeFor('/okx/public').upstream, 'wss://ws.okx.com/ws/v5/public');
  assert.equal(routeFor('/okx/business').upstream, 'wss://ws.okx.com/ws/v5/business');
  assert.equal(routeFor('/okx/time').upstream, 'https://openapi.okx.com/api/v5/public/time');
  for (const path of ['/okx/private', '/okx/public?url=https://evil.invalid', '/okx/public/', '/anything']) assert.equal(routeFor(path), null);
});
test('forwards only application ping and approved public subscriptions', () => {
  assert.equal(isAllowedClientFrame('/okx/public', 'ping', false), true);
  assert.equal(isAllowedClientFrame('/okx/public', frame('bbo-tbt'), false), true);
  assert.equal(isAllowedClientFrame('/okx/public', frame('bbo-tbt', 'unsubscribe', { id: 'probe-1' }), false), true);
});
test('business allows only the two candle channels for BTC-USDT', () => {
  assert.equal(isAllowedClientFrame('/okx/business', frame('candle5m'), false), true);
  assert.equal(isAllowedClientFrame('/okx/business', frame('candle15m'), false), true);
  assert.equal(isAllowedClientFrame('/okx/business', frame('bbo-tbt'), false), false);
  assert.equal(isAllowedClientFrame('/okx/public', frame('candle5m'), false), false);
});
test('rejects login/orders and credential-carrying frames before forwarding', () => {
  for (const op of ['login', 'order', 'cancel-order', 'batch-orders']) assert.equal(isAllowedClientFrame('/okx/business', frame('candle5m', op), false), false);
  for (const fields of [{ apiKey: 'must-not-forward' }, { authorization: 'must-not-forward' }, { url: 'https://evil.invalid' }]) assert.equal(isAllowedClientFrame('/okx/public', frame('bbo-tbt', 'subscribe', fields), false), false);
});
test('rejects other instruments, oversized/binary/invalid frames', () => {
  assert.equal(isAllowedClientFrame('/okx/public', frame('bbo-tbt').replace('BTC-USDT', 'ETH-USDT'), false), false);
  assert.equal(isAllowedClientFrame('/okx/public', frame('bbo-tbt'), true), false);
  for (const value of ['{', 'null', '[]', 'pong', 'x'.repeat(4097)]) assert.equal(isAllowedClientFrame('/okx/public', value, false), false);
  assert.equal(isAllowedClientFrame('/okx/public', JSON.stringify({ op: 'subscribe', args: [{ channel: 'bbo-tbt', instId: 'BTC-USDT', apiKey: 'must-not-forward' }] }), false), false);
});

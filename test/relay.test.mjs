import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import WebSocket, { WebSocketServer } from 'ws';
import { createProbeRelay } from '../relay.mjs';

const token = 'test-only-probe-token-never-used-on-cloud';
async function fixture(t, { rejectOrigin = false } = {}) {
  const originHttp = http.createServer();
  const origin = new WebSocketServer({ noServer: true, perMessageDeflate: false });
  const received = [], opened = [], originUrls = [], originOptions = [], timeCalls = [];
  originHttp.on('upgrade', (request, socket, head) => {
    if (rejectOrigin) { socket.end('HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n'); return; }
    origin.handleUpgrade(request, socket, head, client => {
      opened.push(client);
      client.on('message', data => received.push(data.toString()));
    });
  });
  originHttp.listen(0, '127.0.0.1'); await once(originHttp, 'listening');
  let relay;
  t.after(async () => { await relay?.close(); for (const c of origin.clients) c.terminate(); await new Promise(r => origin.close(r)); await new Promise(r => originHttp.close(r)); });
  relay = createProbeRelay({
    token,
    openWebSocket: (url, options) => {
      originUrls.push(url); originOptions.push(options);
      return new WebSocket('ws://127.0.0.1:' + originHttp.address().port, { perMessageDeflate: false });
    },
    readPublicTime: async url => { timeCalls.push(url); return { code: '0', data: [{ ts: '1791444600000' }] }; },
    log: () => {},
  });
  const address = await relay.listen(0, '127.0.0.1');
  return { relay, origin, received, opened, originUrls, originOptions, timeCalls, base: 'http://127.0.0.1:' + address.port };
}
function client(base, path, headers = {}) { const c = new WebSocket(base.replace('http:', 'ws:') + path, { headers, perMessageDeflate: false }); c.on('error', () => {}); return c; }
async function waitUntil(predicate) { for (let i = 0; i < 100; i++) { if (predicate()) return; await delay(10); } throw Error('Expected local protocol event did not arrive'); }

test('HTTP guards reject unapproved paths/methods before any upstream connection', async t => {
  const f = await fixture(t);
  assert.equal((await fetch(f.base + '/healthz')).status, 200);
  assert.equal((await fetch(f.base + '/okx/public')).status, 426);
  assert.equal((await fetch(f.base + '/okx/private')).status, 404);
  assert.equal((await fetch(f.base + '/okx/public?url=https://evil.invalid')).status, 404);
  assert.equal((await fetch(f.base + '/okx/public', { method: 'POST' })).status, 405);
  assert.equal(f.originUrls.length, 0);
});
test('unauthorized WebSocket cannot open an upstream connection', async t => {
  const f = await fixture(t), c = client(f.base, '/okx/public', { 'X-Probe-Token': 'bad' });
  const status = await new Promise(r => c.once('unexpected-response', (_, response) => { response.resume(); c.terminate(); r(response.statusCode); }));
  assert.equal(status, 401); assert.equal(f.originUrls.length, 0);
});
test('malformed upgrades cannot consume an upstream connection even with probe token', async t => {
  const f = await fixture(t);
  for (const extra of [{ Upgrade: 'h2c', 'Sec-WebSocket-Key': 'MDEyMzQ1Njc4OWFiY2RlZg==' }, { Upgrade: 'websocket', 'Sec-WebSocket-Key': 'invalid' }]) {
    const status = await new Promise((resolve, reject) => {
      const request = http.get(f.base + '/okx/public', { headers: { Connection: 'Upgrade', 'X-Probe-Token': token, 'Sec-WebSocket-Version': '13', ...extra } }, response => { response.resume(); resolve(response.statusCode); });
      request.on('error', reject);
    });
    assert.equal(status, 400); assert.equal(f.originUrls.length, 0);
  }
});
test('valid client gets exact fixed upstream and never forwards client credentials', async t => {
  const f = await fixture(t), c = client(f.base, '/okx/public', { 'X-Probe-Token': token, Authorization: 'must-not-forward', Cookie: 'must-not-forward', 'OK-ACCESS-KEY': 'must-not-forward' });
  await once(c, 'open');
  assert.deepEqual(f.originUrls, ['wss://ws.okx.com/ws/v5/public']);
  assert.equal(f.originOptions[0].headers, undefined);
  const subscription = JSON.stringify({ op: 'subscribe', args: [{ channel: 'bbo-tbt', instId: 'BTC-USDT' }] });
  c.send(subscription); await waitUntil(() => f.received.includes(subscription));
  const output = once(c, 'message'); f.opened[0].send('{"arg":{"channel":"bbo-tbt"},"data":[{"askPx":"1"}]}');
  assert.match((await output)[0].toString(), /askPx/);
  c.close();
});
test('text ping waits for real upstream pong and is never answered locally', async t => {
  const f = await fixture(t), c = client(f.base, '/okx/business', { 'X-Probe-Token': token });
  await once(c, 'open'); const messages = []; c.on('message', m => messages.push(m.toString()));
  c.send('ping'); await waitUntil(() => f.received.includes('ping')); await delay(75);
  assert.deepEqual(messages, []);
  const reply = once(c, 'message'); f.opened[0].send('pong');
  assert.equal((await reply)[0].toString(), 'pong'); c.close();
});
test('private login frame closes client and does not reach upstream', async t => {
  const f = await fixture(t), c = client(f.base, '/okx/business', { 'X-Probe-Token': token });
  await once(c, 'open'); const closed = once(c, 'close');
  c.send('{"op":"login","args":[{"apiKey":"must-not-forward"}]}');
  assert.equal((await closed)[0], 1008); assert.equal(f.received.length, 0);
});
test('binary client frame is refused before forwarding', async t => {
  const f = await fixture(t), c = client(f.base, '/okx/public', { 'X-Probe-Token': token });
  await once(c, 'open'); const closed = once(c, 'close'); c.send(Buffer.from('ping'));
  assert.equal((await closed)[0], 1008); assert.equal(f.received.length, 0);
});
test('failed upstream upgrade returns502 rather than pretending frontend101 succeeded', async t => {
  const f = await fixture(t, { rejectOrigin: true }), c = client(f.base, '/okx/public', { 'X-Probe-Token': token });
  const status = await new Promise(r => c.once('unexpected-response', (_, response) => { response.resume(); c.terminate(); r(response.statusCode); }));
  assert.equal(status, 502);
});
test('public time requires probe auth, fixed upstream and no-store', async t => {
  const f = await fixture(t); assert.equal((await fetch(f.base + '/okx/time')).status, 401);
  assert.equal(f.timeCalls.length, 0);
  const response = await fetch(f.base + '/okx/time', { headers: { 'X-Probe-Token': token } });
  assert.equal(response.status, 200); assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.deepEqual(await response.json(), { code: '0', data: [{ ts: '1791444600000' }] });
  assert.deepEqual(f.timeCalls, ['https://openapi.okx.com/api/v5/public/time']);
});
test('disconnect tears down origin and releases session', async t => {
  const f = await fixture(t), c = client(f.base, '/okx/public', { 'X-Probe-Token': token }); await once(c, 'open');
  c.close(); await waitUntil(() => f.opened[0].readyState === WebSocket.CLOSED);
  assert.equal(f.relay.activeSessions, 0);
});

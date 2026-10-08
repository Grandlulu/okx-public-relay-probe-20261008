import http from 'node:http';
import https from 'node:https';
import WebSocket, { WebSocketServer } from 'ws';
import contract from './contract.json' with { type: 'json' };
import { isAuthorized, isAllowedClientFrame, routeFor } from './policy.mjs';

function readTime(url) {
  return new Promise((resolve, reject) => {
    const agent = new https.Agent({ keepAlive: false });
    let bytes = 0, body = '';
    const request = https.get(url, { agent, headers: { Accept: 'application/json', 'Cache-Control': 'no-cache' } }, response => {
      response.on('data', chunk => {
        bytes += chunk.length;
        if (bytes > 65536) { request.destroy(new Error('Public response too large')); return; }
        body += chunk.toString();
      });
      response.on('end', () => {
        clearTimeout(timer); agent.destroy();
        if (response.statusCode !== 200) { reject(new Error('Public time rejected')); return; }
        try { resolve(JSON.parse(body)); } catch { reject(new Error('Invalid public time')); }
      });
      response.on('error', error => request.destroy(error));
    });
    const timer = setTimeout(() => request.destroy(new Error('Public time deadline')), contract.limits.connectTimeoutMs);
    request.on('error', error => { clearTimeout(timer); agent.destroy(); reject(error); });
  });
}

export function createProbeRelay({ token, openWebSocket, readPublicTime = readTime, log = value => console.log(JSON.stringify(value)) } = {}) {
  if (!isAuthorized(token, token)) throw new Error('A valid independent PROBE_TOKEN is required');
  const sessions = new Set();
  const websocketServer = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: contract.limits.clientFrameBytes });
  websocketServer.on('headers', headers => headers.push('X-Probe-Relay-Version: 1'));
  const counters = { accepted: 0, rejectedFrames: 0, fromClientBytes: 0, fromUpstreamBytes: 0, upstreamFailures: 0 };
  const json = (response, status, value) => {
    response.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); response.end(JSON.stringify(value));
  };
  const server = http.createServer({ maxHeaderSize: 8192 }, async (request, response) => {
    const route = routeFor(request.url);
    if (!route) { json(response, 404, { error: 'NOT_FOUND' }); return; }
    if (request.method !== route.method) { response.setHeader('Allow', route.method); json(response, 405, { error: 'GET_REQUIRED' }); return; }
    if (request.url === '/healthz') { json(response, 200, { ok: true, service: 'okx-public-network-probe', version: 1 }); return; }
    if (route.upgrade) { json(response, 426, { error: 'WEBSOCKET_REQUIRED' }); return; }
    if (!isAuthorized(request.headers['x-probe-token'], token)) { json(response, 401, { error: 'UNAUTHORIZED' }); return; }
    try {
      const value = await readPublicTime(route.upstream);
      const ts = value?.data?.[0]?.ts;
      if (value?.code !== '0' || typeof ts !== 'string' || !/^\d{13}$/.test(ts)) throw new Error('Invalid public time');
      json(response, 200, { code: '0', data: [{ ts }] });
    } catch {
      counters.upstreamFailures++; json(response, 502, { error: 'PUBLIC_TIME_UNAVAILABLE' });
    }
  });
  server.on('upgrade', (request, socket, head) => {
    const reject = status => socket.end('HTTP/1.1 ' + status + '\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
    const route = routeFor(request.url);
    if (!route?.upgrade) { reject('404 Not Found'); return; }
    if (request.method !== 'GET') { reject('405 Method Not Allowed'); return; }
    if (!isAuthorized(request.headers['x-probe-token'], token)) { reject('401 Unauthorized'); return; }
    if (request.headers.upgrade?.toLowerCase() !== 'websocket' ||
        request.headers['sec-websocket-version'] !== '13' ||
        typeof request.headers['sec-websocket-key'] !== 'string' ||
        !/^[+/0-9A-Za-z]{22}==$/.test(request.headers['sec-websocket-key'])) {
      reject('400 Bad Request'); return;
    }
    if (sessions.size >= contract.limits.clients) { reject('503 Capacity Limit'); return; }
    const session = { client: null, upstream: null, agent: null, stopping: false };
    sessions.add(session);
    function end(code = 1011, reason = 'Upstream unavailable') {
      if (session.stopping) return;
      session.stopping = true; clearTimeout(deadline); sessions.delete(session);
      if (session.client?.readyState === WebSocket.OPEN) session.client.close(code, reason);
      else if (!session.client && !socket.destroyed) reject('502 Upstream Unavailable');
      if (session.upstream?.readyState === WebSocket.OPEN) session.upstream.close(1000, 'Probe session ended');
      else if (session.upstream?.readyState !== WebSocket.CLOSED) session.upstream?.terminate();
      const force = setTimeout(() => { session.client?.terminate(); session.upstream?.terminate(); session.agent?.destroy(); }, 1000);
      force.unref();
    }
    const deadline = setTimeout(() => end(), contract.limits.connectTimeoutMs);
    socket.on('error', () => end());
    socket.once('close', () => end(1000, 'Client disconnected'));
    try {
      session.agent = new https.Agent({ keepAlive: false });
      const options = { agent: session.agent, handshakeTimeout: contract.limits.connectTimeoutMs, maxPayload: contract.limits.upstreamFrameBytes, perMessageDeflate: false, followRedirects: false, rejectUnauthorized: true };
      const upstream = openWebSocket ? openWebSocket(route.upstream, options) : new WebSocket(route.upstream, options);
      session.upstream = upstream;
      upstream.on('error', () => { counters.upstreamFailures++; end(); });
      upstream.on('unexpected-response', (_, response) => { response.resume(); counters.upstreamFailures++; end(); });
      upstream.on('close', code => end([1000, 1001].includes(code) ? code : 1011, 'Upstream disconnected'));
      upstream.once('open', () => {
        if (session.stopping || socket.destroyed) { end(); return; }
        clearTimeout(deadline);
        websocketServer.handleUpgrade(request, socket, head, client => {
          session.client = client; counters.accepted++;
          client.on('error', () => end(1011, 'Client transport error'));
          client.on('close', () => end(1000, 'Client disconnected'));
          client.on('message', (data, binary) => {
            if (!isAllowedClientFrame(request.url, data, binary)) {
              counters.rejectedFrames++; end(1008, 'Public probe frames only'); return;
            }
            if (upstream.readyState !== WebSocket.OPEN) { end(); return; }
            if (upstream.bufferedAmount > contract.limits.bufferedBytes) { end(1013, 'Slow upstream'); return; }
            counters.fromClientBytes += data.length;
            upstream.send(data, { binary: false });
          });
          upstream.on('message', (data, binary) => {
            if (binary || client.readyState !== WebSocket.OPEN) { if (binary) end(1011, 'Unexpected upstream frame'); return; }
            if (client.bufferedAmount > contract.limits.bufferedBytes) { end(1013, 'Slow client'); return; }
            counters.fromUpstreamBytes += data.length;
            client.send(data, { binary: false });
          });
          log({ event: 'probe-connected', route: request.url, active: sessions.size });
        });
      });
    } catch { counters.upstreamFailures++; end(); }
    session.stop = end;
  });
  let closing;
  return {
    get activeSessions() { return sessions.size; },
    get counters() { return { ...counters }; },
    listen: (port = 8080, host = '0.0.0.0') => new Promise((resolve, reject) => {
      server.once('error', reject); server.listen(port, host, () => { server.off('error', reject); resolve(server.address()); });
    }),
    close: () => closing ??= new Promise(resolve => {
      for (const session of sessions) session.stop(1001, 'Probe shutdown');
      server.closeAllConnections();
      server.close(() => websocketServer.close(resolve));
    }),
  };
}

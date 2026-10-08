import { createProbeRelay } from './relay.mjs';
const port = Number(process.env.PORT || 8080);
if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('Invalid PORT');
const relay = createProbeRelay({ token: process.env.PROBE_TOKEN?.trim() });
await relay.listen(port);
console.log(JSON.stringify({ event: 'probe-listening', port, publicOnly: true }));
let stopping = false;
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, async () => {
  if (stopping) return; stopping = true;
  const deadline = setTimeout(() => process.exit(1), 5000); deadline.unref();
  await relay.close(); clearTimeout(deadline); process.exit(0);
});

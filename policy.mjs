import { timingSafeEqual } from 'node:crypto';
import contract from './contract.json' with { type: 'json' };

export function isAuthorized(supplied, configured) {
  if (typeof supplied !== 'string' || typeof configured !== 'string' ||
      Buffer.byteLength(configured.trim()) < contract.token.minimumUtf8Bytes) return false;
  const left = Buffer.from(supplied), right = Buffer.from(configured);
  return left.length === right.length && timingSafeEqual(left, right);
}

export function routeFor(rawPath) {
  return typeof rawPath === 'string' && Object.hasOwn(contract.routes, rawPath)
    ? contract.routes[rawPath] : null;
}

export function isAllowedClientFrame(path, raw, isBinary = false) {
  const route = routeFor(path);
  if (!route?.channels || isBinary || !(typeof raw === 'string' || Buffer.isBuffer(raw)) ||
      Buffer.byteLength(raw) > contract.limits.clientFrameBytes) return false;
  const text = raw.toString();
  if (text === 'ping') return true;
  let message;
  try { message = JSON.parse(text); } catch { return false; }
  if (!message || typeof message !== 'object' || Array.isArray(message) ||
      Object.keys(message).some(k => !['op', 'args', 'id'].includes(k)) ||
      !contract.clientFrames.operations.includes(message.op) ||
      !Array.isArray(message.args) || message.args.length < 1 ||
      message.args.length > contract.clientFrames.maximumArgs) return false;
  if (message.id !== undefined && (typeof message.id !== 'string' ||
      !/^[a-zA-Z0-9_-]{1,32}$/.test(message.id))) return false;
  return message.args.every(arg => arg && typeof arg === 'object' && !Array.isArray(arg) &&
    Object.keys(arg).every(k => ['channel', 'instId'].includes(k)) &&
    route.channels.includes(arg.channel) && arg.instId === contract.clientFrames.instrument);
}

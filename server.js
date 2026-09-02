'use strict';

const crypto = require('crypto');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const AUTH_TOKEN = process.env.AUTH_TOKEN;
const PUBLIC_AUTH_TOKEN = process.env.PUBLIC_AUTH_TOKEN;
const REQUEST_TIMEOUT_MS = positiveInt(process.env.REQUEST_TIMEOUT_MS, 30_000);
const MAX_BODY_BYTES = positiveInt(process.env.MAX_BODY_BYTES, 10 * 1024 * 1024);
const WS_PATH = '/_tunnel/connect';
const HEALTH_PATH = '/_tunnel/health';

if (!AUTH_TOKEN || AUTH_TOKEN.length < 24) {
  console.error('AUTH_TOKEN is required and must contain at least 24 characters.');
  process.exit(1);
}

let localClientWs = null;
const pendingRequests = new Map();

function positiveInt(value, fallback) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function safeEqual(actual, expected) {
  if (typeof actual !== 'string' || typeof expected !== 'string') return false;
  const actualBuffer = Buffer.from(actual);
  const expectedBuffer = Buffer.from(expected);
  return actualBuffer.length === expectedBuffer.length &&
    crypto.timingSafeEqual(actualBuffer, expectedBuffer);
}

function bearerToken(header) {
  const match = /^Bearer\s+(.+)$/i.exec(header || '');
  return match?.[1];
}

function removeHopByHopHeaders(headers) {
  const cleaned = { ...headers };
  const connectionTokens = String(cleaned.connection || '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);

  for (const name of [
    'connection',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'te',
    'trailer',
    'transfer-encoding',
    'upgrade',
    ...connectionTokens
  ]) {
    delete cleaned[name];
  }
  return cleaned;
}

function sendText(res, statusCode, text) {
  if (res.headersSent || res.writableEnded) return;
  res.writeHead(statusCode, {
    'content-type': 'text/plain; charset=utf-8',
    'cache-control': 'no-store'
  });
  res.end(text);
}

function failPendingForClient(ws, reason) {
  for (const [id, entry] of pendingRequests) {
    if (entry.ws !== ws) continue;
    clearTimeout(entry.timeout);
    pendingRequests.delete(id);
    sendText(entry.res, 502, reason);
  }
}

const server = http.createServer((req, res) => {
  const requestUrl = new URL(req.url || '/', 'http://tunnel.invalid');

  if (requestUrl.pathname === HEALTH_PATH) {
    res.writeHead(200, {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store'
    });
    return res.end(JSON.stringify({ ok: true, clientConnected: localClientWs?.readyState === WebSocket.OPEN }));
  }

  if (PUBLIC_AUTH_TOKEN && !safeEqual(req.headers['x-tunnel-token'], PUBLIC_AUTH_TOKEN)) {
    return sendText(res, 401, 'Unauthorized');
  }

  const ws = localClientWs;
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return sendText(res, 502, 'Bad Gateway: local tunnel client is not connected.');
  }

  const contentLength = Number(req.headers['content-length']);
  if (Number.isFinite(contentLength) && contentLength > MAX_BODY_BYTES) {
    return sendText(res, 413, 'Payload Too Large');
  }

  const chunks = [];
  let bodySize = 0;
  let rejected = false;

  req.on('data', (chunk) => {
    bodySize += chunk.length;
    if (bodySize > MAX_BODY_BYTES) {
      rejected = true;
      req.resume();
      sendText(res, 413, 'Payload Too Large');
      return;
    }
    chunks.push(chunk);
  });

  req.on('error', () => sendText(res, 400, 'Bad Request'));

  req.on('end', () => {
    if (rejected || res.writableEnded) return;

    const id = crypto.randomUUID();
    const timeout = setTimeout(() => {
      pendingRequests.delete(id);
      sendText(res, 504, 'Gateway Timeout');
    }, REQUEST_TIMEOUT_MS);

    pendingRequests.set(id, { res, timeout, ws });
    res.on('close', () => {
      const entry = pendingRequests.get(id);
      if (!entry) return;
      clearTimeout(entry.timeout);
      pendingRequests.delete(id);
    });

    const headers = removeHopByHopHeaders(req.headers);
    delete headers.host;
    delete headers['x-tunnel-token'];

    const payload = JSON.stringify({
      id,
      method: req.method,
      path: requestUrl.pathname + requestUrl.search,
      headers,
      body: Buffer.concat(chunks).toString('base64')
    });

    ws.send(payload, (error) => {
      if (!error) return;
      const entry = pendingRequests.get(id);
      if (!entry) return;
      clearTimeout(entry.timeout);
      pendingRequests.delete(id);
      sendText(res, 502, 'Bad Gateway: failed to reach local tunnel client.');
    });
  });
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: Math.ceil(MAX_BODY_BYTES * 1.5) + 64 * 1024
});

server.on('upgrade', (request, socket, head) => {
  const requestUrl = new URL(request.url || '/', 'http://tunnel.invalid');
  const token = bearerToken(request.headers.authorization);

  if (requestUrl.pathname !== WS_PATH) {
    socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
    return socket.destroy();
  }
  if (!safeEqual(token, AUTH_TOKEN)) {
    socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');
    return socket.destroy();
  }

  wss.handleUpgrade(request, socket, head, (ws) => wss.emit('connection', ws));
});

wss.on('connection', (ws) => {
  if (localClientWs) localClientWs.close(1012, 'Replaced by a new tunnel client');
  localClientWs = ws;
  ws.isAlive = true;
  console.log('Local tunnel client connected.');

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (message) => {
    try {
      const response = JSON.parse(message.toString());
      const entry = pendingRequests.get(response.id);
      if (!entry || entry.ws !== ws) return;
      clearTimeout(entry.timeout);
      pendingRequests.delete(response.id);

      const statusCode = Number(response.statusCode);
      const safeStatusCode = statusCode >= 100 && statusCode <= 599 ? statusCode : 502;
      const headers = removeHopByHopHeaders(response.headers || {});
      entry.res.writeHead(safeStatusCode, headers);
      entry.res.end(Buffer.from(response.body || '', 'base64'));
    } catch (error) {
      console.error('Invalid tunnel response:', error.message);
    }
  });

  ws.on('error', (error) => console.error('Tunnel client WebSocket error:', error.message));
  ws.on('close', () => {
    if (localClientWs === ws) localClientWs = null;
    failPendingForClient(ws, 'Bad Gateway: local tunnel client disconnected.');
    console.log('Local tunnel client disconnected.');
  });
});

const heartbeat = setInterval(() => {
  const ws = localClientWs;
  if (!ws) return;
  if (!ws.isAlive) return ws.terminate();
  ws.isAlive = false;
  ws.ping();
}, 20_000);
heartbeat.unref();

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Tunnel server listening on port ${PORT}`);
});

function shutdown() {
  clearInterval(heartbeat);
  if (localClientWs) localClientWs.close(1001, 'Server shutting down');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10_000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

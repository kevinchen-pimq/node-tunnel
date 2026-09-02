'use strict';

const crypto = require('crypto');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const AUTH_TOKEN = process.env.AUTH_TOKEN;
const PUBLIC_AUTH_TOKEN = process.env.PUBLIC_AUTH_TOKEN;
const REQUEST_TIMEOUT_MS = positiveInt(process.env.REQUEST_TIMEOUT_MS, 30_000);
const MAX_BODY_BYTES = positiveInt(process.env.MAX_BODY_BYTES, 10 * 1024 * 1024);
const STREAM_CHUNK_BYTES = Math.min(positiveInt(process.env.STREAM_CHUNK_BYTES, 64 * 1024), 256 * 1024);
const STREAM_WINDOW_FRAMES = Math.min(positiveInt(process.env.STREAM_WINDOW_FRAMES, 8), 64);
const WS_PATH = '/_tunnel/connect';
const HEALTH_PATH = '/_tunnel/health';
const REQUEST_BODY_FRAME = 1;
const RESPONSE_BODY_FRAME = 2;
const FRAME_HEADER_BYTES = 17;

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
  return actualBuffer.length === expectedBuffer.length && crypto.timingSafeEqual(actualBuffer, expectedBuffer);
}

function bearerToken(header) {
  return /^Bearer\s+(.+)$/i.exec(header || '')?.[1];
}

function idToBuffer(id) {
  const hex = id.replaceAll('-', '');
  if (!/^[a-f0-9]{32}$/i.test(hex)) throw new Error('Invalid request ID');
  return Buffer.from(hex, 'hex');
}

function bufferToId(buffer) {
  const hex = buffer.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function removeHopByHopHeaders(headers) {
  const cleaned = { ...headers };
  const connectionTokens = String(cleaned.connection || '').split(',').map((value) => value.trim().toLowerCase()).filter(Boolean);
  for (const name of [
    'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
    'te', 'trailer', 'transfer-encoding', 'upgrade', ...connectionTokens
  ]) delete cleaned[name];
  return cleaned;
}

function sendText(res, statusCode, text) {
  if (res.headersSent || res.writableEnded) return res.destroy();
  res.writeHead(statusCode, {
    'content-type': 'text/plain; charset=utf-8',
    'cache-control': 'no-store'
  });
  res.end(text);
}

function sendControl(ws, message, callback = () => {}) {
  if (ws.readyState !== WebSocket.OPEN) return callback(new Error('WebSocket is not open'));
  ws.send(JSON.stringify(message), callback);
}

function makeBodyFrame(kind, idBuffer, chunk) {
  const frame = Buffer.allocUnsafe(FRAME_HEADER_BYTES + chunk.length);
  frame[0] = kind;
  idBuffer.copy(frame, 1);
  chunk.copy(frame, FRAME_HEADER_BYTES);
  return frame;
}

function sendRequestFrames(entry, idBuffer, chunk) {
  for (let offset = 0; offset < chunk.length; offset += STREAM_CHUNK_BYTES) {
    const end = Math.min(offset + STREAM_CHUNK_BYTES, chunk.length);
    const frame = makeBodyFrame(REQUEST_BODY_FRAME, idBuffer, chunk.subarray(offset, end));
    entry.requestInFlight += 1;
    entry.ws.send(frame, { binary: true }, (error) => {
      if (error && pendingRequests.has(entry.id)) {
        failPending(entry.id, 502, 'Bad Gateway: failed to stream request body.', false);
      }
    });
  }
}

function removePending(id) {
  const entry = pendingRequests.get(id);
  if (!entry) return null;
  clearTimeout(entry.timeout);
  pendingRequests.delete(id);
  return entry;
}

function failPending(id, statusCode, message, notifyClient = true) {
  const entry = removePending(id);
  if (!entry) return;
  if (notifyClient) sendControl(entry.ws, { type: 'request-abort', id });
  sendText(entry.res, statusCode, message);
}

function failPendingForClient(ws, reason) {
  for (const [id, entry] of pendingRequests) {
    if (entry.ws !== ws) continue;
    removePending(id);
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

  const id = crypto.randomUUID();
  const idBuffer = idToBuffer(id);
  let bodySize = 0;
  let rejected = false;
  const timeout = setTimeout(() => failPending(id, 504, 'Gateway Timeout'), REQUEST_TIMEOUT_MS);
  const entry = { id, req, res, timeout, ws, requestInFlight: 0, responseStarted: false, responseBytes: 0 };
  pendingRequests.set(id, entry);

  res.on('close', () => {
    if (res.writableEnded || !pendingRequests.has(id)) return;
    removePending(id);
    sendControl(ws, { type: 'request-abort', id });
  });

  const headers = removeHopByHopHeaders(req.headers);
  delete headers.host;
  delete headers['x-tunnel-token'];

  sendControl(ws, {
    type: 'request-start',
    id,
    method: req.method,
    path: requestUrl.pathname + requestUrl.search,
    headers
  }, (error) => {
    if (error) failPending(id, 502, 'Bad Gateway: failed to reach local tunnel client.', false);
  });

  req.on('data', (chunk) => {
    if (rejected || !pendingRequests.has(id)) return;
    bodySize += chunk.length;
    if (bodySize > MAX_BODY_BYTES) {
      rejected = true;
      req.resume();
      return failPending(id, 413, 'Payload Too Large');
    }

    sendRequestFrames(entry, idBuffer, chunk);
    if (entry.requestInFlight >= STREAM_WINDOW_FRAMES) req.pause();
  });

  req.on('end', () => {
    if (!rejected && pendingRequests.has(id)) {
      sendControl(ws, { type: 'request-end', id }, (error) => {
        if (error) failPending(id, 502, 'Bad Gateway: failed to finish request.', false);
      });
    }
  });
  req.on('error', () => failPending(id, 400, 'Bad Request'));
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: Math.max(64 * 1024, STREAM_CHUNK_BYTES + FRAME_HEADER_BYTES)
});

server.on('upgrade', (request, socket, head) => {
  const requestUrl = new URL(request.url || '/', 'http://tunnel.invalid');
  if (requestUrl.pathname !== WS_PATH) {
    socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
    return socket.destroy();
  }
  if (!safeEqual(bearerToken(request.headers.authorization), AUTH_TOKEN)) {
    socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');
    return socket.destroy();
  }
  wss.handleUpgrade(request, socket, head, (ws) => wss.emit('connection', ws));
});

function handleResponseControl(ws, message) {
  const entry = pendingRequests.get(message.id);
  if (!entry || entry.ws !== ws) return;

  if (message.type === 'request-body-ack') {
    entry.requestInFlight = Math.max(0, entry.requestInFlight - 1);
    if (entry.req.isPaused() && entry.requestInFlight < STREAM_WINDOW_FRAMES) entry.req.resume();
    return;
  }

  if (message.type === 'response-start') {
    if (entry.responseStarted) return failPending(message.id, 502, 'Invalid duplicate tunnel response.');
    const statusCode = Number(message.statusCode);
    const safeStatusCode = statusCode >= 100 && statusCode <= 599 ? statusCode : 502;
    const headers = removeHopByHopHeaders(message.headers || {});
    const responseLength = Number(headers['content-length']);
    if (Number.isFinite(responseLength) && responseLength > MAX_BODY_BYTES) {
      return failPending(message.id, 502, 'Local response exceeded size limit.');
    }
    try {
      entry.res.writeHead(safeStatusCode, headers);
      entry.responseStarted = true;
    } catch {
      failPending(message.id, 502, 'Local server returned invalid response headers.');
    }
    return;
  }

  if (message.type === 'response-end') {
    const completed = removePending(message.id);
    if (!completed) return;
    if (!completed.responseStarted) return sendText(completed.res, 502, 'Invalid tunnel response.');
    completed.res.end();
    return;
  }

  if (message.type === 'response-abort') {
    failPending(message.id, 502, message.reason || 'Local response stream failed.', false);
  }
}

function handleResponseBody(ws, data) {
  if (data.length < FRAME_HEADER_BYTES || data[0] !== RESPONSE_BODY_FRAME) return;
  const id = bufferToId(data.subarray(1, FRAME_HEADER_BYTES));
  const entry = pendingRequests.get(id);
  if (!entry || entry.ws !== ws || !entry.responseStarted) return;

  const chunk = data.subarray(FRAME_HEADER_BYTES);
  entry.responseBytes += chunk.length;
  if (entry.responseBytes > MAX_BODY_BYTES) {
    return failPending(id, 502, 'Local response exceeded size limit.');
  }
  const acknowledge = () => sendControl(ws, { type: 'response-body-ack', id });
  if (entry.res.write(chunk)) acknowledge();
  else entry.res.once('drain', acknowledge);
}

wss.on('connection', (ws) => {
  if (localClientWs) localClientWs.close(1012, 'Replaced by a new tunnel client');
  localClientWs = ws;
  ws.isAlive = true;
  console.log('Local tunnel client connected.');

  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (data, isBinary) => {
    try {
      if (isBinary) return handleResponseBody(ws, data);
      handleResponseControl(ws, JSON.parse(data.toString()));
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

server.listen(PORT, '0.0.0.0', () => console.log(`Tunnel server listening on port ${PORT}`));

function shutdown() {
  clearInterval(heartbeat);
  if (localClientWs) localClientWs.close(1001, 'Server shutting down');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10_000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

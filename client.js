'use strict';

const http = require('http');
const https = require('https');
const WebSocket = require('ws');

const REMOTE_SERVER_URL = process.env.REMOTE_SERVER_URL;
const AUTH_TOKEN = process.env.AUTH_TOKEN;
const LOCAL_TARGET = process.env.LOCAL_TARGET || 'http://127.0.0.1:3000';
const LOCAL_REQUEST_TIMEOUT_MS = positiveInt(process.env.LOCAL_REQUEST_TIMEOUT_MS, 30_000);
const MAX_BODY_BYTES = positiveInt(process.env.MAX_BODY_BYTES, 10 * 1024 * 1024);

if (!REMOTE_SERVER_URL || !AUTH_TOKEN) {
  console.error('REMOTE_SERVER_URL and AUTH_TOKEN are required.');
  process.exit(1);
}

let remoteUrl;
let localTarget;
try {
  remoteUrl = new URL(REMOTE_SERVER_URL);
  localTarget = new URL(LOCAL_TARGET);
} catch (error) {
  console.error(`Invalid URL configuration: ${error.message}`);
  process.exit(1);
}

if (!['ws:', 'wss:'].includes(remoteUrl.protocol)) {
  console.error('REMOTE_SERVER_URL must use ws:// or wss://.');
  process.exit(1);
}
if (!['http:', 'https:'].includes(localTarget.protocol)) {
  console.error('LOCAL_TARGET must use http:// or https://.');
  process.exit(1);
}

remoteUrl.pathname = '/_tunnel/connect';
remoteUrl.search = '';
remoteUrl.hash = '';

let reconnectAttempt = 0;
let reconnectTimer = null;
let shuttingDown = false;
let activeWs = null;

function positiveInt(value, fallback) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function removeHopByHopHeaders(headers) {
  const cleaned = { ...headers };
  const connectionTokens = String(cleaned.connection || '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  for (const name of [
    'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
    'te', 'trailer', 'transfer-encoding', 'upgrade', ...connectionTokens
  ]) delete cleaned[name];
  return cleaned;
}

function sendResponse(ws, response) {
  if (ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify(response), (error) => {
    if (error) console.error('Failed to send tunnel response:', error.message);
  });
}

function errorResponse(ws, id, statusCode, message) {
  sendResponse(ws, {
    id,
    statusCode,
    headers: { 'content-type': 'text/plain; charset=utf-8' },
    body: Buffer.from(message).toString('base64')
  });
}

function forwardRequest(ws, request) {
  const { id, method, path, body } = request;
  if (typeof id !== 'string' || typeof path !== 'string' || !path.startsWith('/')) {
    return errorResponse(ws, id, 400, 'Invalid tunnel request');
  }

  let pathUrl;
  try {
    pathUrl = new URL(path, 'http://tunnel.invalid');
  } catch {
    return errorResponse(ws, id, 400, 'Invalid request path');
  }

  const headers = removeHopByHopHeaders(request.headers || {});
  delete headers.host;
  const transport = localTarget.protocol === 'https:' ? https : http;
  const basePath = localTarget.pathname.replace(/\/$/, '');

  const localReq = transport.request({
    protocol: localTarget.protocol,
    hostname: localTarget.hostname,
    port: localTarget.port || undefined,
    method: method || 'GET',
    path: basePath + pathUrl.pathname + pathUrl.search,
    headers,
    timeout: LOCAL_REQUEST_TIMEOUT_MS
  }, (localRes) => {
    const chunks = [];
    let size = 0;
    let tooLarge = false;

    localRes.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        tooLarge = true;
        errorResponse(ws, id, 502, 'Local response exceeded size limit');
        localRes.destroy();
        return;
      }
      chunks.push(chunk);
    });
    localRes.on('end', () => {
      if (tooLarge) return;
      sendResponse(ws, {
        id,
        statusCode: localRes.statusCode,
        headers: removeHopByHopHeaders(localRes.headers),
        body: Buffer.concat(chunks).toString('base64')
      });
    });
    localRes.on('error', (error) => {
      if (!tooLarge) errorResponse(ws, id, 502, `Local response error: ${error.message}`);
    });
  });

  localReq.on('timeout', () => localReq.destroy(new Error('Local request timed out')));
  localReq.on('error', (error) => errorResponse(ws, id, 502, `Local server error: ${error.message}`));

  try {
    if (body) localReq.write(Buffer.from(body, 'base64'));
    localReq.end();
  } catch (error) {
    localReq.destroy();
    errorResponse(ws, id, 400, `Invalid request body: ${error.message}`);
  }
}

function scheduleReconnect() {
  if (shuttingDown || reconnectTimer) return;
  const delay = Math.min(30_000, 1000 * (2 ** Math.min(reconnectAttempt, 5)));
  reconnectAttempt += 1;
  console.log(`Reconnecting in ${Math.round(delay / 1000)} second(s)...`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, delay);
}

function connect() {
  console.log(`Connecting to ${remoteUrl.origin}...`);
  const ws = new WebSocket(remoteUrl, {
    headers: { authorization: `Bearer ${AUTH_TOKEN}` },
    handshakeTimeout: 10_000,
    maxPayload: Math.ceil(MAX_BODY_BYTES * 1.5) + 64 * 1024
  });
  activeWs = ws;

  ws.on('open', () => {
    reconnectAttempt = 0;
    console.log('Connected to Railway tunnel server.');
  });
  ws.on('message', (data) => {
    try {
      forwardRequest(ws, JSON.parse(data.toString()));
    } catch (error) {
      console.error('Invalid message from tunnel server:', error.message);
    }
  });
  ws.on('error', (error) => console.error('WebSocket error:', error.message));
  ws.on('close', (code) => {
    if (activeWs === ws) activeWs = null;
    if (!shuttingDown) {
      console.log(`Tunnel connection closed (${code}).`);
      scheduleReconnect();
    }
  });
}

function shutdown() {
  shuttingDown = true;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (activeWs) activeWs.close(1000, 'Client shutting down');
  setTimeout(() => process.exit(0), 250).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

connect();

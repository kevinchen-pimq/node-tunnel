'use strict';

const http = require('http');
const https = require('https');
const WebSocket = require('ws');

const REMOTE_SERVER_URL = process.env.REMOTE_SERVER_URL;
const AUTH_TOKEN = process.env.AUTH_TOKEN;
const LOCAL_TARGET = process.env.LOCAL_TARGET || 'http://127.0.0.1:3000';
const LOCAL_REQUEST_TIMEOUT_MS = positiveInt(process.env.LOCAL_REQUEST_TIMEOUT_MS, 30_000);
const MAX_BODY_BYTES = positiveInt(process.env.MAX_BODY_BYTES, 10 * 1024 * 1024);
const STREAM_CHUNK_BYTES = Math.min(positiveInt(process.env.STREAM_CHUNK_BYTES, 64 * 1024), 256 * 1024);
const STREAM_WINDOW_FRAMES = Math.min(positiveInt(process.env.STREAM_WINDOW_FRAMES, 8), 64);
const REQUEST_BODY_FRAME = 1;
const RESPONSE_BODY_FRAME = 2;
const FRAME_HEADER_BYTES = 17;

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
const activeRequests = new Map();

function positiveInt(value, fallback) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
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

function sendBodyChunk(ws, kind, idBuffer, chunk, callback, offset = 0) {
  if (offset >= chunk.length) return callback();
  const end = Math.min(offset + STREAM_CHUNK_BYTES, chunk.length);
  const frame = makeBodyFrame(kind, idBuffer, chunk.subarray(offset, end));
  if (ws.readyState !== WebSocket.OPEN) return callback(new Error('WebSocket is not open'));
  ws.send(frame, { binary: true }, (error) => {
    if (error) return callback(error);
    sendBodyChunk(ws, kind, idBuffer, chunk, callback, end);
  });
}

function sendResponseFrames(ws, state, chunk) {
  for (let offset = 0; offset < chunk.length; offset += STREAM_CHUNK_BYTES) {
    const end = Math.min(offset + STREAM_CHUNK_BYTES, chunk.length);
    const frame = makeBodyFrame(RESPONSE_BODY_FRAME, state.idBuffer, chunk.subarray(offset, end));
    state.responseInFlight += 1;
    ws.send(frame, { binary: true }, (error) => {
      if (error) abortState(state.id);
    });
  }
}

function abortState(id) {
  const state = activeRequests.get(id);
  if (!state) return;
  activeRequests.delete(id);
  state.localReq.destroy();
  state.localRes?.destroy();
}

function sendSimpleResponse(ws, id, statusCode, message) {
  const body = Buffer.from(message);
  let idBuffer;
  try { idBuffer = idToBuffer(id); } catch { return; }
  sendControl(ws, {
    type: 'response-start',
    id,
    statusCode,
    headers: {
      'content-type': 'text/plain; charset=utf-8',
      'content-length': String(body.length)
    }
  }, (startError) => {
    if (startError) return;
    sendBodyChunk(ws, RESPONSE_BODY_FRAME, idBuffer, body, (bodyError) => {
      if (!bodyError) sendControl(ws, { type: 'response-end', id });
    });
  });
}

function handleRequestStart(ws, request) {
  const { id, method, path } = request;
  if (typeof id !== 'string' || typeof path !== 'string' || !path.startsWith('/') || activeRequests.has(id)) {
    return sendSimpleResponse(ws, id, 400, 'Invalid tunnel request');
  }

  let pathUrl;
  let idBuffer;
  try {
    pathUrl = new URL(path, 'http://tunnel.invalid');
    idBuffer = idToBuffer(id);
  } catch {
    return sendSimpleResponse(ws, id, 400, 'Invalid tunnel request');
  }

  const headers = removeHopByHopHeaders(request.headers || {});
  delete headers.host;
  const contentLength = Number(headers['content-length']);
  if (Number.isFinite(contentLength) && contentLength > MAX_BODY_BYTES) {
    return sendSimpleResponse(ws, id, 413, 'Payload Too Large');
  }

  const transport = localTarget.protocol === 'https:' ? https : http;
  const basePath = localTarget.pathname.replace(/\/$/, '');
  const state = { id, idBuffer, requestBytes: 0, responseBytes: 0, responseStarted: false, responseInFlight: 0, localReq: null, localRes: null };

  const localReq = transport.request({
    protocol: localTarget.protocol,
    hostname: localTarget.hostname,
    port: localTarget.port || undefined,
    method: method || 'GET',
    path: basePath + pathUrl.pathname + pathUrl.search,
    headers,
    timeout: LOCAL_REQUEST_TIMEOUT_MS
  }, (localRes) => {
    state.localRes = localRes;
    const responseLength = Number(localRes.headers['content-length']);
    if (Number.isFinite(responseLength) && responseLength > MAX_BODY_BYTES) {
      localRes.destroy();
      activeRequests.delete(id);
      return sendSimpleResponse(ws, id, 502, 'Local response exceeded size limit');
    }

    state.responseStarted = true;
    sendControl(ws, {
      type: 'response-start',
      id,
      statusCode: localRes.statusCode,
      headers: removeHopByHopHeaders(localRes.headers)
    }, (error) => {
      if (error) abortState(id);
    });

    localRes.on('data', (chunk) => {
      state.responseBytes += chunk.length;
      if (state.responseBytes > MAX_BODY_BYTES) {
        localRes.destroy();
        activeRequests.delete(id);
        return sendControl(ws, { type: 'response-abort', id, reason: 'Local response exceeded size limit' });
      }
      sendResponseFrames(ws, state, chunk);
      if (state.responseInFlight >= STREAM_WINDOW_FRAMES) localRes.pause();
    });
    localRes.on('end', () => {
      activeRequests.delete(id);
      sendControl(ws, { type: 'response-end', id });
    });
    localRes.on('error', (error) => {
      activeRequests.delete(id);
      if (state.responseBytes <= MAX_BODY_BYTES) {
        sendControl(ws, { type: 'response-abort', id, reason: `Local response error: ${error.message}` });
      }
    });
  });

  state.localReq = localReq;
  activeRequests.set(id, state);
  localReq.on('timeout', () => localReq.destroy(new Error('Local request timed out')));
  localReq.on('error', (error) => {
    if (!activeRequests.has(id)) return;
    activeRequests.delete(id);
    if (state.responseStarted) sendControl(ws, { type: 'response-abort', id, reason: error.message });
    else sendSimpleResponse(ws, id, 502, `Local server error: ${error.message}`);
  });
}

function handleRequestBody(ws, data) {
  if (data.length < FRAME_HEADER_BYTES || data[0] !== REQUEST_BODY_FRAME) return;
  const id = bufferToId(data.subarray(1, FRAME_HEADER_BYTES));
  const state = activeRequests.get(id);
  if (!state) return;

  const chunk = data.subarray(FRAME_HEADER_BYTES);
  state.requestBytes += chunk.length;
  if (state.requestBytes > MAX_BODY_BYTES) {
    abortState(id);
    return sendSimpleResponse(ws, id, 413, 'Payload Too Large');
  }
  const acknowledge = () => sendControl(ws, { type: 'request-body-ack', id });
  if (state.localReq.write(chunk)) acknowledge();
  else state.localReq.once('drain', acknowledge);
}

function handleRequestControl(ws, message) {
  if (message.type === 'request-start') return handleRequestStart(ws, message);
  const state = activeRequests.get(message.id);
  if (!state) return;
  if (message.type === 'response-body-ack') {
    state.responseInFlight = Math.max(0, state.responseInFlight - 1);
    if (state.localRes?.isPaused() && state.responseInFlight < STREAM_WINDOW_FRAMES) state.localRes.resume();
    return;
  }
  if (message.type === 'request-end') return state.localReq.end();
  if (message.type === 'request-abort') abortState(message.id);
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
    maxPayload: Math.max(64 * 1024, STREAM_CHUNK_BYTES + FRAME_HEADER_BYTES)
  });
  activeWs = ws;

  ws.on('open', () => {
    reconnectAttempt = 0;
    console.log('Connected to Railway tunnel server.');
  });
  ws.on('message', (data, isBinary) => {
    try {
      if (isBinary) return handleRequestBody(ws, data);
      handleRequestControl(ws, JSON.parse(data.toString()));
    } catch (error) {
      console.error('Invalid message from tunnel server:', error.message);
    }
  });
  ws.on('error', (error) => console.error('WebSocket error:', error.message));
  ws.on('close', (code) => {
    for (const id of activeRequests.keys()) abortState(id);
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
  for (const id of activeRequests.keys()) abortState(id);
  if (activeWs) activeWs.close(1000, 'Client shutting down');
  setTimeout(() => process.exit(0), 250).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

connect();

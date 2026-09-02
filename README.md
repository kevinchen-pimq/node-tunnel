# Railway Node Tunnel

A small single-client HTTP tunnel: Railway receives public HTTP requests, sends them over an authenticated WebSocket to `client.js`, and the client forwards them to a local web server.

## Railway server

Required variable:

- `AUTH_TOKEN`: a random secret of at least 24 characters. It authenticates the local tunnel client.

Optional variables:

- `PUBLIC_AUTH_TOKEN`: when set, public callers must include `X-Tunnel-Token: <value>`.
- `MAX_BODY_BYTES`: maximum buffered request/response size (default `10485760`).
- `REQUEST_TIMEOUT_MS`: public request timeout (default `30000`).

The server health endpoint is `/_tunnel/health`. Only one local client can be connected at a time; a newer connection replaces the old one.

## Local client

Install dependencies, then run:

```bash
npm install
REMOTE_SERVER_URL=wss://YOUR_DOMAIN \
AUTH_TOKEN='THE_SAME_RAILWAY_SECRET' \
LOCAL_TARGET=http://127.0.0.1:3000 \
npm run client
```

`REMOTE_SERVER_URL` may be supplied without a path; the client always connects to the protected `/_tunnel/connect` endpoint.

If `PUBLIC_AUTH_TOKEN` is enabled, callers use:

```bash
curl -H 'X-Tunnel-Token: YOUR_PUBLIC_TOKEN' https://YOUR_DOMAIN/path
```

## Scope and limitations

- HTTP requests and responses are buffered in memory; this is not intended for large uploads or streaming.
- End-user WebSocket/SSE tunneling is not supported.
- The Railway URL exposes your local web app to the internet unless `PUBLIC_AUTH_TOKEN` or application-level authentication is enabled.
- Use HTTPS/WSS for the Railway endpoint and never commit tokens to source control.

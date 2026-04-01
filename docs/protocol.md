# Protocol

Protocol version: `1`

All packets are single UDP datagrams containing one UTF-8 JSON object.

## Common fields

- `v`: protocol version integer
- `type`: message type string
- `session`: session identifier string when applicable
- `token`: rendezvous-issued random session token when applicable
- `ts`: unix milliseconds sender timestamp

## Rendezvous messages

### `register`

Sent by host to rendezvous.

```json
{
  "v": 1,
  "type": "register",
  "service": "demo-shell",
  "meta": {"impl": "go-host"}
}
```

### `register_ok`

Sent by rendezvous to host.

```json
{
  "v": 1,
  "type": "register_ok",
  "service": "demo-shell",
  "public_addr": "203.0.113.10:45000",
  "expires_in_ms": 30000
}
```

### `connect_request`

Sent by client to rendezvous.

```json
{
  "v": 1,
  "type": "connect_request",
  "service": "demo-shell",
  "meta": {"impl": "python-client"}
}
```

### `connect_intro`

Sent by rendezvous to both peers.

```json
{
  "v": 1,
  "type": "connect_intro",
  "role": "host",
  "service": "demo-shell",
  "session": "sess-...",
  "token": "randhex",
  "peer_addr": "198.51.100.7:55001",
  "peer_meta": {"impl": "python-client"},
  "timeout_ms": 15000
}
```

For the client intro, `role` is `client` and `peer_addr` points at the host.

### `error`

Sent by rendezvous on failures.

```json
{
  "v": 1,
  "type": "error",
  "code": "service_not_found",
  "message": "requested service is not registered"
}
```

## Peer-to-peer messages

### `punch`

Short datagram sent repeatedly during hole punching.

```json
{
  "v": 1,
  "type": "punch",
  "session": "sess-...",
  "token": "randhex"
}
```

### `hello`

Peer authentication and session activation.

```json
{
  "v": 1,
  "type": "hello",
  "session": "sess-...",
  "token": "randhex",
  "role": "client"
}
```

### `hello_ack`

Confirms the peer session is active.

```json
{
  "v": 1,
  "type": "hello_ack",
  "session": "sess-...",
  "token": "randhex"
}
```

### `stdin`

Client to host shell input.

```json
{
  "v": 1,
  "type": "stdin",
  "session": "sess-...",
  "token": "randhex",
  "data": "base64-bytes"
}
```

### `stdout`

Host to client shell output.

```json
{
  "v": 1,
  "type": "stdout",
  "session": "sess-...",
  "token": "randhex",
  "data": "base64-bytes"
}
```

### `resize`

Client to host terminal resize hint.

```json
{
  "v": 1,
  "type": "resize",
  "session": "sess-...",
  "token": "randhex",
  "cols": 120,
  "rows": 40
}
```

This version carries the resize event for forward compatibility. The `script`-based shell wrapper does not yet force PTY resize in every implementation.

### `keepalive`

Bidirectional idle heartbeat.

```json
{
  "v": 1,
  "type": "keepalive",
  "session": "sess-...",
  "token": "randhex"
}
```

### `close`

Bidirectional session shutdown.

```json
{
  "v": 1,
  "type": "close",
  "session": "sess-...",
  "token": "randhex",
  "reason": "shell_exit"
}
```

## Timeouts

- Host registration refresh: every 10 seconds
- Host registration expiry: 30 seconds
- Punch/hello retry interval: 500 milliseconds
- Punch/hello phase timeout: 15 seconds
- Keepalive interval: 5 seconds
- Session idle timeout: 20 seconds

## Interoperability rules

- Unknown fields must be ignored.
- Unknown message types should be dropped.
- `data` payloads are base64 encoded raw bytes.
- JSON object keys are case-sensitive.
- All implementations must validate `v`, `session`, and `token` before processing session messages.

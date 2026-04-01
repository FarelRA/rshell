# P2P Remote Shell System

Peer-to-peer remote shell over UDP with NAT hole punching through a shared Go rendezvous server.

## Components

- `rendezvous/`: shared Go coordination server
- `go/`: Go host and client
- `bun/`: Bun host and client
- `python/`: Python host and client
- `php/`: PHP host and client

## Features

- Separate host and client programs in every language
- Shared cross-language JSON-over-UDP protocol
- NAT traversal via rendezvous-based UDP hole punching
- Interactive shell relay using a PTY-backed `script` wrapper
- Multiple concurrent client sessions per host process
- Keepalive, timeout, and close handling

## Docs

- `docs/architecture.md`
- `docs/protocol.md`
- `docs/build-and-run.md`
- `docs/examples.md`
- `docs/interoperability.md`

## Current limitations

- Primary target is Linux
- Symmetric NATs may fail without a relay server
- Traffic is authenticated by session token but not encrypted end-to-end in this version
- Resize packets are defined and forwarded, but shell window resizing is best-effort in this version

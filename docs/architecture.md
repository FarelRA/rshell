# Architecture

## Components

1. `rendezvous/`
   - Shared Go coordination server.
   - Tracks registered shell hosts and introduces peers for UDP hole punching.

2. `*/host`
   - Runs on the machine exposing a shell.
   - Registers a service name with the rendezvous server.
   - Accepts peer introductions.
   - Spawns one PTY-backed shell session per client session.

3. `*/client`
   - Runs on the machine initiating a remote shell connection.
   - Requests a connection to a named host service.
   - Performs UDP hole punching with the host.
   - Relays terminal input/output.

## Connection flow

1. Host binds local UDP port and sends `register` to rendezvous.
2. Rendezvous records the host's observed public endpoint.
3. Client binds local UDP port and sends `connect_request` for a service name.
4. Rendezvous creates a session and sends `connect_intro` to both peers.
5. Both peers begin sending repeated `punch` and `hello` datagrams to each other's public endpoints.
6. Once `hello` is received and validated, the P2P session becomes active.
7. Client enters raw terminal mode and relays local input to host.
8. Host spawns a PTY-backed shell using `script` and relays shell output back to the client.

## Reliability model

- UDP is used for all traffic.
- Control messages are retried on a timer until success or timeout.
- Data messages are best-effort and low-latency.
- Heartbeats detect dead peers.
- Session state expires automatically on inactivity.

## Security model

- Rendezvous issues a random per-session token.
- Host and client validate the token before accepting a peer session.
- This prevents blind unsolicited packets from attaching to a live session.
- Traffic is not encrypted in this initial version; deploy over trusted environments or add an encryption layer in a future revision.

## Limits

- Linux is the primary supported target.
- Hole punching may fail on symmetric NATs or restrictive firewalls.
- There is no relay fallback in this version.

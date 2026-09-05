# Build And Run

## Common runtime requirements

- Linux
- UDP reachability to the rendezvous server
- `script` command available on both peer machines

## Build distributable artifacts

Create a `dist/` directory with bundled Bun entrypoints, Linux Go binaries for `386`, `amd64`, `arm`, and `arm64`, plus the Python/PHP runtime files:

```bash
npm run build
```

Or:

```bash
bash scripts/build_dist.sh
```

## Shared rendezvous server

```bash
go run ./rendezvous --listen :4000
```

## Go

Host:

```bash
go run ./go/host --rendezvous 203.0.113.10:4000 --service demo-shell
```

Client:

```bash
go run ./go/client --rendezvous 203.0.113.10:4000 --service demo-shell
```

## Bun

Host:

```bash
bun run bun/host.js --rendezvous=203.0.113.10:4000 --service=demo-shell
```

Client:

```bash
bun run bun/client.js --rendezvous=203.0.113.10:4000 --service=demo-shell
```

## Python

Host:

```bash
python3 python/host.py --rendezvous 203.0.113.10:4000 --service demo-shell
```

Client:

```bash
python3 python/client.py --rendezvous 203.0.113.10:4000 --service demo-shell
```

## PHP

Host:

```bash
php php/host.php --rendezvous=203.0.113.10:4000 --service=demo-shell
```

Client:

```bash
php php/client.php --rendezvous=203.0.113.10:4000 --service=demo-shell
```

## Recommended deployment sequence

1. Start the rendezvous server on a publicly reachable machine.
2. Start a host on the machine exposing a shell.
3. Start a client on the machine initiating the remote shell.
4. Wait for the `connect_intro`, punch, and `hello` exchange to complete.
5. Use the shell interactively until either side sends `close` or the session times out.

## Notes

- Hosts refresh registration every 10 seconds.
- Clients and hosts send keepalives every 5 seconds.
- Session idle timeout is 20 seconds.

## Local smoke matrix

Run the local interoperability smoke test:

```bash
bash scripts/smoke_matrix.sh
```

This rebuilds `dist/`, selects the native Go binary for the current machine from the multi-arch output, starts a local rendezvous server per test case, runs one host/client pair for each language combination, injects a simple shell command, and verifies the expected output marker.

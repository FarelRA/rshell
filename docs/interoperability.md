# Interoperability

## Design choices that enable cross-language operation

- One shared protocol in `docs/protocol.md`
- UTF-8 JSON messages over UDP
- Base64 payload encoding for shell data
- Shared message names, timing, and token validation rules
- Same rendezvous server for all implementations

## Intended compatibility matrix

Hosts:
- Go
- Bun
- Python
- PHP

Clients:
- Go
- Bun
- Python
- PHP

All host/client combinations are designed to interoperate as long as:

- the rendezvous server is the shared Go implementation
- both peers can send and receive UDP packets
- `script` exists on the host machine
- the local PHP build includes `pcntl`, `posix`, and `sockets`

## Practical caveats

- Some environments will not allow reliable hole punching.
- PHP and Bun shells depend on external `script` behavior on Linux.
- This repository currently includes smoke verification and static validation; full distributed NAT validation still depends on the deployment environment.

## Included verification

- Syntax/build validation for Go, Bun, Python, and PHP implementations
- Local automated `4 x 4` smoke matrix in `scripts/smoke_matrix.sh`
- One shared Go rendezvous implementation used for every matrix run

## Terminal behavior

- Go and Python hosts use a real PTY and propagate window size changes directly.
- Bun and PHP hosts keep compatibility with the same protocol and apply window size through shell-side `stty` updates.
- Hosts brand the remote terminal process and terminal environment as `rshell-<implementation>-host` where supported.

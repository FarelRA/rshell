# Usage Examples

## Same-language example

Start rendezvous:

```bash
go run ./rendezvous --listen :4000
```

Start a Go host:

```bash
go run ./go/host --rendezvous 127.0.0.1:4000 --service lab
```

Start a Go client:

```bash
go run ./go/client --rendezvous 127.0.0.1:4000 --service lab
```

## Cross-language example

Start a Python host:

```bash
python3 python/host.py --rendezvous 127.0.0.1:4000 --service mixed
```

Connect from a Bun client:

```bash
bun run bun/client.js --rendezvous=127.0.0.1:4000 --service=mixed
```

## Alternate shell

```bash
go run ./go/host --rendezvous 127.0.0.1:4000 --service zshell --shell /bin/zsh
```

## Explicit local bind

```bash
python3 python/client.py --rendezvous 203.0.113.10:4000 --service demo-shell --listen 0.0.0.0:50055
```

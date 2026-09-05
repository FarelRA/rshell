#!/usr/bin/env bash
set -euo pipefail

ROOT="$(dirname "$(dirname "$0")")"
DIST="$ROOT/dist"

bash "$ROOT/scripts/build_dist.sh"

case "$(uname -m)" in
  x86_64) GO_DIST_ARCH=amd64 ;;
  i386|i686) GO_DIST_ARCH=386 ;;
  armv7l|armv7|arm) GO_DIST_ARCH=arm ;;
  aarch64|arm64) GO_DIST_ARCH=arm64 ;;
  *)
    printf 'unsupported host architecture for smoke: %s\n' "$(uname -m)" >&2
    exit 1
    ;;
esac

host_cmd() {
  case "$1" in
    go) printf '"%s/go/%s/host" --rendezvous 127.0.0.1:%s --service %s' "$DIST" "$GO_DIST_ARCH" "$2" "$3" ;;
    bun) printf 'bun "%s/bun/host.js" --rendezvous=127.0.0.1:%s --service=%s' "$DIST" "$2" "$3" ;;
    python) printf 'python3 "%s/python/host.py" --rendezvous 127.0.0.1:%s --service %s' "$DIST" "$2" "$3" ;;
    php) printf 'php "%s/php/host.php" --rendezvous=127.0.0.1:%s --service=%s' "$DIST" "$2" "$3" ;;
  esac
}

client_cmd() {
  case "$1" in
    go) printf '"%s/go/%s/client" --rendezvous 127.0.0.1:%s --service %s' "$DIST" "$GO_DIST_ARCH" "$2" "$3" ;;
    bun) printf 'bun "%s/bun/client.js" --rendezvous=127.0.0.1:%s --service=%s' "$DIST" "$2" "$3" ;;
    python) printf 'python3 "%s/python/client.py" --rendezvous 127.0.0.1:%s --service %s' "$DIST" "$2" "$3" ;;
    php) printf 'php "%s/php/client.php" --rendezvous=127.0.0.1:%s --service=%s' "$DIST" "$2" "$3" ;;
  esac
}

test_pair() {
  local host_lang="$1"
  local client_lang="$2"
  local port="$3"
  local service="svc_${host_lang}_${client_lang}"
  local ready="READY_${host_lang}_${client_lang}"
  local host
  local rdv
  local output

  "$DIST/go/$GO_DIST_ARCH/rendezvous" --listen ":${port}" >"/tmp/rshell-rdv-${port}.log" 2>&1 &
  rdv=$!
  sleep 0.4
  bash -lc "$(host_cmd "$host_lang" "$port" "$service")" >"/tmp/rshell-host-${port}.log" 2>&1 &
  host=$!
  sleep 1.2

  output=$(READY_VALUE="$ready" bash -lc "{ printf 'printf %s\\n' \"\$READY_VALUE\"; sleep 1; printf '\\nexit\\n'; sleep 1; } | timeout 10s $(client_cmd "$client_lang" "$port" "$service")" 2>"/tmp/rshell-client-${port}.log" || true)

  kill "$host" "$rdv" 2>/dev/null || true
  wait "$host" 2>/dev/null || true
  wait "$rdv" 2>/dev/null || true

  if printf '%s' "$output" | rg -q "$ready"; then
    printf 'PASS %s->%s\n' "$host_lang" "$client_lang"
  else
    printf 'FAIL %s->%s\n' "$host_lang" "$client_lang"
  fi
}

port=4600
for host in go bun python php; do
  for client in go bun python php; do
    test_pair "$host" "$client" "$port"
    port=$((port + 1))
  done
done

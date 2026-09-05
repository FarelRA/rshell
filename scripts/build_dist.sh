#!/usr/bin/env bash
set -euo pipefail

ROOT="$(dirname "$(dirname "$0")")"
DIST="$ROOT/dist"
GO_ARCHES=(386 amd64 arm arm64)

rm -rf "$DIST"
mkdir -p "$DIST/bun" "$DIST/go" "$DIST/python" "$DIST/php"

for arch in "${GO_ARCHES[@]}"; do
  mkdir -p "$DIST/go/$arch"
  GOOS=linux GOARCH="$arch" go build -o "$DIST/go/$arch/rendezvous" ./rendezvous
  GOOS=linux GOARCH="$arch" go build -o "$DIST/go/$arch/host" ./go/host
  GOOS=linux GOARCH="$arch" go build -o "$DIST/go/$arch/client" ./go/client
done

bun build "$ROOT/bun/host.js" --target bun --outfile "$DIST/bun/host.js"
bun build "$ROOT/bun/client.js" --target bun --outfile "$DIST/bun/client.js"

cp "$ROOT/python/common.py" "$ROOT/python/host.py" "$ROOT/python/client.py" "$DIST/python/"
cp "$ROOT/php/common.php" "$ROOT/php/native.php" "$ROOT/php/host.php" "$ROOT/php/client.php" "$DIST/php/"

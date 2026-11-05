#!/bin/sh
set -e
dir=$(dirname "$0")
case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) echo "multigres-mcp: unsupported OS $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) arch=amd64 ;;
  arm64 | aarch64) arch=arm64 ;;
  *) echo "multigres-mcp: unsupported CPU $(uname -m)" >&2; exit 1 ;;
esac
bin="$dir/multigres-mcp-$os-$arch"
[ -x "$bin" ] || chmod +x "$bin" 2>/dev/null || true
exec "$bin" "$@"

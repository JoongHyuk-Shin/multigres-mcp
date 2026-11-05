#!/bin/sh
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
version=$(jq -r .version "$root/packaging/mcpb/manifest.json")
if [ "$(jq -r .version "$root/server.json")" != "$version" ]; then
  echo "server.json and packaging/mcpb/manifest.json name different versions" >&2
  exit 1
fi
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/server"
cp "$root/packaging/mcpb/manifest.json" "$work/"
cp "$root/packaging/mcpb/launch.sh" "$work/server/"
chmod +x "$work/server/launch.sh"
for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64; do
  os=${target%/*}
  arch=${target#*/}
  ext=""
  if [ "$os" = windows ]; then ext=.exe; fi
  (cd "$root" && CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath \
    -ldflags "-s -w -X main.version=$version" \
    -o "$work/server/multigres-mcp-$os-$arch$ext" ./cmd/multigres-mcp)
done
mkdir -p "$root/dist"
out="$root/dist/multigres-mcp-$version.mcpb"
npx -y @anthropic-ai/mcpb@2.1.2 pack "$work" "$out"
openssl dgst -sha256 "$out"

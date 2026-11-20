#!/bin/sh
# Re-creates internal/quicgo from a quic-go release plus internal/quicgo/PATCH.
#
#   scripts/vendor-quicgo.sh                  vendor the pinned version
#   scripts/vendor-quicgo.sh vX.Y.Z           vendor another version (then update the pin below)
#   scripts/vendor-quicgo.sh pristine DIR     export the unpatched, import-unchanged tree to DIR
#   scripts/vendor-quicgo.sh diff DIR         rewrite PATCH as the difference between the pristine tree and DIR
#   scripts/vendor-quicgo.sh verify           re-vendor into a temp dir and fail if internal/quicgo differs from it
#
# Never edit internal/quicgo by hand: change PATCH instead (see docs/guide.md, "Maintenance").
set -eu

pin=v0.65.0
pinsum=h1:BJQSjdTvLPLmWtaCupUphRbgxgpZhdO/GMpr9Eoe3B4=
upstream=github.com/quic-go/quic-go
module=github.com/ciranda-dev/kolo-transport
dirs=". internal/ackhandler internal/congestion internal/handshake internal/monotime
internal/protocol internal/qerr internal/utils internal/utils/linkedlist internal/utils/minheap
internal/utils/ringbuffer internal/wire qlog qlogwriter qlogwriter/jsontext quicvarint"

root=$(cd "$(dirname "$0")/.." && pwd)
dest=$root/internal/quicgo

mode=vendor
out=
case ${1:-} in
verify)
	mode=verify
	shift
	;;
pristine | diff)
	mode=$1
	out=${2:?usage: vendor-quicgo.sh $1 DIR [version]}
	shift 2
	;;
esac
version=${1:-$pin}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# go mod download checks the module against go.sum and the checksum database.
info=$(cd "$tmp" && GOWORK=off GOFLAGS=-mod=mod go mod download -json "$upstream@$version") || info=
src=$(printf '%s\n' "$info" | sed -n 's/^[[:space:]]*"Dir": "\(.*\)",$/\1/p')
sum=$(printf '%s\n' "$info" | sed -n 's/^[[:space:]]*"Sum": "\(.*\)",$/\1/p')
[ -d "$src" ] && [ -n "$sum" ] || { echo "vendor-quicgo: cannot download $upstream@$version" >&2; exit 1; }
if [ "$version" = "$pin" ] && [ "$sum" != "$pinsum" ]; then
	echo "vendor-quicgo: $upstream@$version has module sum $sum, the script pins $pinsum" >&2
	exit 1
fi

tree=$tmp/tree
mkdir -p "$tree"
for d in $dirs; do
	mkdir -p "$tree/$d"
	for f in "$src/$d"/*.go; do
		case $f in
		*_test.go | */mockgen.go) continue ;;
		esac
		cp "$f" "$tree/$d/"
	done
done
cp "$src/LICENSE" "$tree/LICENSE"
chmod -R u+w "$tree"

case $mode in
pristine)
	mkdir -p "$out"
	cp -R "$tree/." "$out/"
	exit 0
	;;
diff)
	mkdir "$tmp/b"
	cp -R "$out/." "$tmp/b/"
	mv "$tree" "$tmp/a"
	{
		sed '/^diff /,$d' "$dest/PATCH"
		(cd "$tmp" && diff -ruN a b || true) | sed -E 's/^(---|\+\+\+) ([^[:space:]]+)[[:space:]].*$/\1 \2/'
	} >"$tmp/PATCH"
	mv "$tmp/PATCH" "$dest/PATCH"
	exit 0
	;;
esac

(cd "$tree" && patch -p1 -F0 --no-backup-if-mismatch -s <"$dest/PATCH")

# Rewrite import paths only, never URLs in comments.
find "$tree" -name '*.go' | while read -r f; do
	sed "s|\"$upstream|\"$module/internal/quicgo|g" "$f" >"$f.new"
	mv "$f.new" "$f"
done
gofmt -l "$tree" | while read -r f; do gofmt -w "$f"; done

printf '# Vendored code is not ours to lint.\nchecks = ["-all"]\n' >"$tree/staticcheck.conf"
printf '%s\n%s\n' "$version" "$sum" >"$tree/VERSION"
cp "$dest/PATCH" "$tree/PATCH"
if command -v sha256sum >/dev/null; then sha=sha256sum; else sha="shasum -a 256"; fi
(cd "$tree" && find . -type f ! -name CHECKSUMS | sort | xargs $sha >CHECKSUMS)

if [ "$mode" = verify ]; then
	if diff -r "$tree" "$dest" >"$tmp/verify.diff"; then
		echo "verified: internal/quicgo is $upstream $version ($sum) plus PATCH"
		exit 0
	fi
	head -n 40 "$tmp/verify.diff" >&2
	echo "vendor-quicgo: internal/quicgo differs from $upstream $version plus PATCH: run scripts/vendor-quicgo.sh" >&2
	exit 1
fi

rm -rf "$dest.new"
mv "$tree" "$dest.new"
rm -rf "$dest"
mv "$dest.new" "$dest"
(cd "$root" && go build ./internal/quicgo/... && go vet ./internal/quicgo/...)
echo "vendored $upstream $version ($sum) into internal/quicgo"

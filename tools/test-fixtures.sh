#!/usr/bin/env bash
# test-fixtures: download & verify pinned test fixture tarballs.
# Re-running is a no-op when cached files match the manifest's sha256
# (mtime preserved so Go test caching stays valid across runs).
#
# Usage: tools/test-fixtures.sh [MANIFEST [CACHE_DIR]]
#   MANIFEST defaults to test-fixtures.json
#   CACHE_DIR defaults to test-fixtures-cache
set -euo pipefail

manifest="${1:-test-fixtures.json}"
cache="${2:-test-fixtures-cache}"

for tool in jq curl; do
	command -v "$tool" >/dev/null 2>&1 || { echo "test-fixtures: $tool not found in PATH" >&2; exit 1; }
done

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
	else
		shasum -a 256 "$1" | awk '{print $1}'
	fi
}

mkdir -p "$cache"

while IFS=$'\t' read -r name url want; do
	path="$cache/${name}.tar.gz"
	if [[ -f "$path" ]] && [[ "$(sha256 "$path")" == "$want" ]]; then
		echo "$name: cached (sha256 ${want:0:12})"
		continue
	fi
	echo "$name: downloading from $url"
	curl -fsSL -o "$path.tmp" "$url"
	got=$(sha256 "$path.tmp")
	if [[ "$got" != "$want" ]]; then
		rm -f "$path.tmp"
		echo "test-fixtures: $name: sha256 mismatch — want $want got $got" >&2
		exit 1
	fi
	mv "$path.tmp" "$path"
	echo "$name: ok"
done < <(jq -r 'to_entries | sort_by(.key) | .[] | "\(.key)\t\(.value.url)\t\(.value.sha256)"' "$manifest" | tr -d '\r')

#!/usr/bin/env bash
# test-fixtures: download, verify, and extract pinned test fixture tarballs.
#
# For each entry KEY in the manifest, ensures:
#   CACHE_DIR/KEY.tar.gz   downloaded & sha256-verified against the manifest
#   CACHE_DIR/KEY/         extracted contents
#   CACHE_DIR/KEY/.sha256  sentinel file recording the verified sha256
#
# Re-running is a no-op when the sentinel already matches; useful tarballs
# stay cached so re-extraction (e.g. after `make clean`) doesn't re-download.
#
# Usage: tools/test-fixtures.sh [MANIFEST [CACHE_DIR]]
#   MANIFEST defaults to test-fixtures.json
#   CACHE_DIR defaults to test-fixtures-cache
set -euo pipefail

manifest="${1:-test-fixtures.json}"
cache="${2:-test-fixtures-cache}"

for tool in jq curl tar; do
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
	tar_path="$cache/${name}.tar.gz"
	out_dir="$cache/${name}"
	sentinel="$out_dir/.sha256"

	if [[ -f "$sentinel" ]] && [[ "$(cat "$sentinel")" == "$want" ]]; then
		echo "$name: cached (sha256 ${want:0:12})"
		continue
	fi

	if [[ ! -f "$tar_path" ]] || [[ "$(sha256 "$tar_path")" != "$want" ]]; then
		echo "$name: downloading from $url"
		curl -fsSL -o "$tar_path.tmp" "$url"
		got=$(sha256 "$tar_path.tmp")
		if [[ "$got" != "$want" ]]; then
			rm -f "$tar_path.tmp"
			echo "test-fixtures: $name: sha256 mismatch — want $want got $got" >&2
			exit 1
		fi
		mv "$tar_path.tmp" "$tar_path"
	fi

	echo "$name: extracting"
	rm -rf "$out_dir.tmp" "$out_dir"
	mkdir -p "$out_dir.tmp"
	tar -xzf "$tar_path" -C "$out_dir.tmp"
	echo "$want" > "$out_dir.tmp/.sha256"
	mv "$out_dir.tmp" "$out_dir"
	echo "$name: ok"
done < <(jq -r 'to_entries | sort_by(.key) | .[] | "\(.key)\t\(.value.url)\t\(.value.sha256)"' "$manifest" | tr -d '\r')

#!/usr/bin/env bash
# test-fixtures: download, verify, and extract pinned test fixture tarballs.
#
# For each entry KEY in the manifest, ensures:
#   CACHE_DIR/KEY.tar.gz   downloaded & sha256-verified against the manifest
#   CACHE_DIR/KEY/         extracted contents
#   CACHE_DIR/KEY/.sha256  sentinel file recording the verified sha256
#
# Re-running is a no-op when the sentinel already matches. Tarballs are kept
# alongside the extracted dirs so re-extraction (after `make clean` or a
# manual rm of CACHE_DIR/KEY/) doesn't re-download. The trade-off is roughly
# 2x local disk usage vs. extracted-only; CI caches only *.tar.gz to stay
# under cache budget and re-extracts on each run.
#
# Usage: tools/test-fixtures.sh [MANIFEST [CACHE_DIR [KEY...]]]
#   MANIFEST defaults to test-fixtures.json
#   CACHE_DIR defaults to test-fixtures-cache
#   KEY...    optional manifest keys; default = all keys
set -euo pipefail

manifest="${1:-test-fixtures.json}"
cache="${2:-test-fixtures-cache}"
keys=("${@:3}")

for tool in jq curl tar; do
	command -v "$tool" >/dev/null 2>&1 || { echo "test-fixtures: $tool not found in PATH" >&2; exit 1; }
done
if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
	echo "test-fixtures: neither sha256sum nor shasum found in PATH" >&2
	exit 1
fi

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
	else
		shasum -a 256 "$1" | awk '{print $1}'
	fi
}

mkdir -p "$cache"

# Read & parse the manifest up-front so jq errors abort the script (process
# substitution would mask them under `set -e`).
if (( ${#keys[@]} == 0 )); then
	keys_json='[]'
else
	keys_json=$(printf '%s\n' "${keys[@]}" | jq -R . | jq -s .)
fi
if ! manifest_entries=$(jq -r --argjson keys "$keys_json" '
	to_entries
	| sort_by(.key)
	| map(select(($keys | length) == 0 or (.key as $k | $keys | index($k))))
	| .[]
	| "\(.key)\t\(.value.url)\t\(.value.sha256)"
' "$manifest" | tr -d '\r'); then
	echo "test-fixtures: failed to parse manifest $manifest" >&2
	exit 1
fi
if [[ -z "$manifest_entries" ]] && (( ${#keys[@]} > 0 )); then
	echo "test-fixtures: no manifest entries matched keys: ${keys[*]}" >&2
	exit 1
fi

# Clean up the in-progress iteration's tmp paths on any exit (including
# Ctrl-C / SIGTERM) so a leftover .tmp doesn't accumulate across runs.
tar_path=""
out_dir=""
trap 'rm -f "${tar_path}.tmp" 2>/dev/null; rm -rf "${out_dir}.tmp" 2>/dev/null' EXIT

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
		curl -fsSL --retry 3 --retry-all-errors --retry-delay 2 -o "$tar_path.tmp" "$url"
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
	tar --no-same-owner --no-same-permissions -xzf "$tar_path" -C "$out_dir.tmp"
	echo "$want" > "$out_dir.tmp/.sha256"
	mv "$out_dir.tmp" "$out_dir"
	echo "$name: ok"
done <<< "$manifest_entries"

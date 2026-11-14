#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/source/fixtures/keep" "$work/source/fixtures/drop" "$work/cache"
echo keep > "$work/source/fixtures/keep/a.json"
echo drop > "$work/source/fixtures/drop/b.json"
tar -czf "$work/cache/example.tar.gz" -C "$work/source" fixtures
if command -v sha256sum >/dev/null 2>&1; then
	want=$(sha256sum "$work/cache/example.tar.gz" | awk '{print $1}')
else
	want=$(shasum -a 256 "$work/cache/example.tar.gz" | awk '{print $1}')
fi
jq -n --arg sha "$want" '{example: {url: "unused", sha256: $sha}}' > "$work/manifest.json"

run() {
	bash "$repo/tools/test-fixtures.sh" "$@" "$work/manifest.json" "$work/cache" example
}

run --include fixtures/keep
[[ -f "$work/cache/example/fixtures/keep/a.json" ]]
[[ ! -e "$work/cache/example/fixtures/drop" ]]
touch "$work/cache/example/marker"
run --include fixtures/keep
[[ -f "$work/cache/example/marker" ]]

run --include fixtures/drop
[[ -f "$work/cache/example/fixtures/drop/b.json" ]]
[[ ! -e "$work/cache/example/fixtures/keep" ]]
[[ ! -e "$work/cache/example/marker" ]]

run
[[ -f "$work/cache/example/fixtures/keep/a.json" ]]
[[ -f "$work/cache/example/fixtures/drop/b.json" ]]
[[ $(cat "$work/cache/example/.sha256") == "$want" ]]
touch "$work/cache/example/marker"
run --include fixtures/keep
[[ -f "$work/cache/example/marker" ]]

rm -rf "$work/cache/example"
if run --include fixtures/missing; then
	echo 'missing archive path was accepted' >&2
	exit 1
fi
[[ ! -e "$work/cache/example/.sha256" ]]
echo 'test-fixtures checks passed'

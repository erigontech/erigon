#!/usr/bin/env bash
set -euo pipefail

here=$(cd "$(dirname -- "$0")" && pwd)
repo=$(cd "$here/../../.." && pwd)
script="$here/setup.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

mock_bin="$tmp/bin"
calls="$tmp/calls"
sources="$tmp/sources"
mkdir -p "$mock_bin" "$sources"

cat >"$mock_bin/mock-command" <<'EOF'
#!/usr/bin/env bash
case "${0##*/}" in
  sudo)
    exec "$@"
    ;;
  timeout)
    printf 'timeout %s\n' "$*" >>"$SETUP_KURTOSIS_CALLS"
    [ "$1" = "--kill-after=10s" ] || exit 98
    shift 2
    exec "$@"
    ;;
  apt-get)
    printf 'apt-get %s\n' "$*" >>"$SETUP_KURTOSIS_CALLS"
    case "$1" in
      update) exit "${MOCK_APT_UPDATE_EXIT:-0}" ;;
      install) exit "${MOCK_APT_INSTALL_EXIT:-0}" ;;
      *) exit 97 ;;
    esac
    ;;
  kurtosis)
    printf 'kurtosis %s\n' "$*" >>"$SETUP_KURTOSIS_CALLS"
    exit 0
    ;;
esac
EOF
for command in sudo timeout apt-get kurtosis; do
  cp "$mock_bin/mock-command" "$mock_bin/$command"
done
chmod +x "$mock_bin"/*

fail() {
  printf 'FAIL - %s\n' "$1"
  exit 1
}

contains() {
  grep -Fq -- "$2" "$3" || fail "$1"
}

rejects() {
  if grep -Fq -- "$2" "$3"; then fail "$1"; fi
}

reset_case() {
  : >"$calls"
  rm -rf "$sources"
  mkdir -p "$sources"
  printf 'deb https://packages.microsoft.com/example stable main\n' >"$sources/microsoft.list"
  printf 'deb http://archive.ubuntu.com/ubuntu noble main\n' >"$sources/ubuntu.list"
}

run_setup() {
  env PATH="$mock_bin:$PATH" \
    SETUP_KURTOSIS_CALLS="$calls" \
    KURTOSIS_VERSION=1.15.2 \
    KURTOSIS_APT_SOURCES_DIR="$sources" \
    "$@" bash "$script"
}

reset_case
run_setup || fail "successful setup"
contains "update has no hard deadline" "timeout --kill-after=10s 300 apt-get update" "$calls"
contains "install has no hard deadline" "timeout --kill-after=10s 600 apt-get install" "$calls"
contains "apt has no network bound" "Acquire::https::Timeout=15" "$calls"
contains "apt has no lock bound" "DPkg::Lock::Timeout=60" "$calls"
contains "version is not pinned" "kurtosis-cli=1.15.2" "$calls"
contains "engine did not start" "kurtosis engine start" "$calls"
[ ! -e "$sources/microsoft.list" ] || fail "broken third-party source remains"
[ -e "$sources/ubuntu.list" ] || fail "Ubuntu source was removed"

reset_case
if update_out=$(run_setup MOCK_APT_UPDATE_EXIT=1 2>&1); then
  fail "update failure did not stop setup"
fi
grep -Fq "apt-get update failed or did not finish within 300s" <<<"$update_out" \
  || fail "update failure was not diagnosed"
rejects "install ran after update failure" "apt-get install" "$calls"

for workflow in test-kurtosis-assertoor.yml test-kurtosis-gloas.yml; do
  path="$repo/.github/workflows/$workflow"
  contains "$workflow does not use shared setup" "uses: ./.github/actions/setup-kurtosis" "$path"
  contains "$workflow does not use the preinstalled Kurtosis action" \
    "uses: erigontech/kurtosis-assertoor-github-action@v1.1.7" "$path"
  rejects "$workflow still installs with apt directly" "sudo apt-get" "$path"
  rejects "$workflow uses an action that reinstalls Kurtosis" \
    "uses: ethpandaops/kurtosis-assertoor-github-action@" "$path"
done

echo "setup-kurtosis tests passed"

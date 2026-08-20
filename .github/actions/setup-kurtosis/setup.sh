#!/usr/bin/env bash
set -euo pipefail

version=${KURTOSIS_VERSION:?KURTOSIS_VERSION is required}
sources_dir=${KURTOSIS_APT_SOURCES_DIR:-/etc/apt/sources.list.d}

# Kurtosis uses none of the hosted runner's failure-prone third-party apt sources.
while IFS= read -r -d '' source; do
  if sudo grep -qE 'dl\.google\.com|packages\.microsoft\.com' "$source"; then
    sudo rm -f "$source"
  fi
done < <(sudo find "$sources_dir" -type f -print0 2>/dev/null)
printf 'deb [trusted=yes] https://apt.fury.io/kurtosis-tech/ /\n' \
  | sudo tee "$sources_dir/kurtosis.list" >/dev/null

apt_opts=(-o Acquire::Retries=3 -o Acquire::http::Timeout=15
          -o Acquire::https::Timeout=15 -o DPkg::Lock::Timeout=60)
if ! sudo timeout --kill-after=10s 300 apt-get update -q "${apt_opts[@]}"; then
  echo "::error::apt-get update failed or did not finish within 300s"
  exit 1
fi
if ! sudo timeout --kill-after=10s 600 apt-get install -qq -y \
  "${apt_opts[@]}" "kurtosis-cli=$version"; then
  echo "::error::apt-get install Kurtosis failed or did not finish within 600s"
  exit 1
fi

kurtosis analytics disable
for n in 1 2 3; do
  if kurtosis engine start; then
    exit 0
  fi
  echo "kurtosis engine start failed (attempt $n of 3)"
  kurtosis engine stop || true
  sleep $((10 * n))
done
exit 1

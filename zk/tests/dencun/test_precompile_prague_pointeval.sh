#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 \
  --rpc-url <url>

Options:
  --rpc-url         L2 RPC endpoint URL
  -h, --help        Show this help message and exit
EOF
  exit 1
}

declare RPC_URL=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --rpc-url)
      RPC_URL="$2"; shift 2;;
    -h|--help)
      usage;;
    *)
      echo "Unknown option: $1" >&2; usage;;
  esac
done

if [[ -z "$RPC_URL" ]]; then
  echo "Error: --rpc-url required." >&2
  usage
fi

TO_ADDR="0x000000000000000000000000000000000000000a"
GAS_LIMIT="100000"

# Input data for the point-eval precompile
DATA_HEX="0x010657f37554c781402a22917dee2f75def7ab966d7b770905398eba3c44401400000000000000000000000000000000000000000000000000000000000000050000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"

echo "Calling point-eval precompile…"

RES_RAW=$(cast call --rpc-url "$RPC_URL" --gas-limit "$GAS_LIMIT" --data "$DATA_HEX" "$TO_ADDR")

RES_RAW=${RES_RAW%\"}
RES_RAW=${RES_RAW#\"}
RES_HEX=${RES_RAW#0x}

# 3) verify length: 64 bytes == 128 hex chars
if [[ ${#RES_HEX} -ne 128 ]]; then
  echo "Error: response is ${#RES_HEX}/128 hex chars (got $(( ${#RES_HEX} / 2 )) bytes)" >&2
  exit 1
fi

echo "OK: response is 64 bytes (128 hex chars)"
echo "Response: 0x${RES_HEX}"

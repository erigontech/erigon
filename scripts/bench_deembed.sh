#!/usr/bin/env bash
# bench_deembed.sh — compare commitment lookup perf between embedded and
# de-embedded storage formats, both at the raw domain level and via the
# logical TrieContext.Branch API (which in de-embed mode reads meta + all
# populated children to reassemble a branch blob).
#
# Usage:
#   scripts/bench_deembed.sh --datadir /path/to/datadir [--chain mainnet] \
#     [--sample-size 100000] [--seed 42]
#
# Assumptions:
#   - `build/bin/integration` is built (run `make integration`).
#   - The datadir already contains commitment files produced under BOTH
#     formats (e.g., two sibling datadirs — one built with
#     COMMITMENT_DEEMBED=false and one with =true). Point --datadir-embedded
#     and --datadir-deembed at each.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
INTEGRATION_BIN="${REPO_ROOT}/build/bin/integration"

CHAIN="mainnet"
SAMPLE_SIZE=100000
SEED=42
DATADIR_EMBEDDED=""
DATADIR_DEEMBED=""

usage() {
    cat >&2 <<EOF
Usage: $0 --datadir-embedded DIR --datadir-deembed DIR [options]

Options:
  --datadir-embedded DIR   Datadir built with COMMITMENT_DEEMBED=false
  --datadir-deembed DIR    Datadir built with COMMITMENT_DEEMBED=true
  --chain NAME             Chain name (default: mainnet)
  --sample-size N          Bench sample size (default: 100000)
  --seed N                 RNG seed (default: 42)
  -h, --help               Show this help
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --datadir-embedded) DATADIR_EMBEDDED="$2"; shift 2 ;;
        --datadir-deembed)  DATADIR_DEEMBED="$2"; shift 2 ;;
        --chain)            CHAIN="$2"; shift 2 ;;
        --sample-size)      SAMPLE_SIZE="$2"; shift 2 ;;
        --seed)             SEED="$2"; shift 2 ;;
        -h|--help)          usage ;;
        *) echo "unknown argument: $1" >&2; usage ;;
    esac
done

if [[ -z "${DATADIR_EMBEDDED}" || -z "${DATADIR_DEEMBED}" ]]; then
    usage
fi

if [[ ! -x "${INTEGRATION_BIN}" ]]; then
    echo "integration binary not found at ${INTEGRATION_BIN}; run 'make integration' first" >&2
    exit 1
fi

run_bench() {
    local label="$1" datadir="$2" deembed_env="$3" via_trie_ctx="$4"
    local extra=()
    if [[ "${via_trie_ctx}" == "true" ]]; then
        extra+=(--via-trie-ctx)
    fi
    echo
    echo "===== ${label} ====="
    echo "COMMITMENT_DEEMBED=${deembed_env}"
    echo "datadir=${datadir}"
    echo "via-trie-ctx=${via_trie_ctx}"
    COMMITMENT_DEEMBED="${deembed_env}" "${INTEGRATION_BIN}" commitment bench-lookup \
        --datadir "${datadir}" \
        --chain "${CHAIN}" \
        --sample-size "${SAMPLE_SIZE}" \
        --seed "${SEED}" \
        "${extra[@]}"
}

run_bench "embedded / raw"            "${DATADIR_EMBEDDED}" "false" "false"
run_bench "embedded / via-trie-ctx"   "${DATADIR_EMBEDDED}" "false" "true"
run_bench "de-embedded / raw"         "${DATADIR_DEEMBED}"  "true"  "false"
run_bench "de-embedded / via-trie-ctx" "${DATADIR_DEEMBED}" "true"  "true"

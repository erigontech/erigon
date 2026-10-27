#!/usr/bin/env bash
# T-CI-2: start the eez-rollup0 Kurtosis devnet, follow it with Erigon and
# require Erigon's L2 to equal the reference's up to safe (T-NET-2), with a
# mid-wave restart (T-NET-6) and the sequencer overlay (T-NET-8).
#
#   ROLLUP0           eez-rollup0 checkout with its eez-core-protocol submodule
#   ERIGON, EEZ       erigon and eez binaries under test
#   OUT               directory for the logs and the diff report
#   KURTOSIS_ENCLAVE  enclave name, default eez-erigon
#   EEZ_SKIP_NODE_BUILD, EEZ_SKIP_PROOF_SIGNER_BUILD, EEZ_SKIP_DEPLOY_BUILD
#                     passed to start.sh; set to 1 when the images are pulled
#
# Requires kurtosis, docker, cast, forge, jq and curl.
set -euo pipefail

: "${ROLLUP0:?}" "${ERIGON:?}" "${EEZ:?}" "${OUT:?}"
export KURTOSIS_ENCLAVE="${KURTOSIS_ENCLAVE:-eez-erigon}"
K="$ROLLUP0/testing/kurtosis"
DATADIR="$OUT/erigon"
ERIGON_RPC=http://127.0.0.1:18545
mkdir -p "$OUT"

log() { echo "==> $(date -u +%T) $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

rpc() {
	curl -sf -X POST -H 'content-type: application/json' \
		--data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":$3}" "$1"
}

# block prints "<number> <hash>" of a block tag or hex number, "0 null" when
# there is none.
block() {
	local r
	r=$(rpc "$1" eth_getBlockByNumber "[\"$2\",false]" \
		| jq -r 'if .result == null then "0x0 null" else "\(.result.number) \(.result.hash)" end')
	printf '%d %s\n' "${r%% *}" "${r##* }"
}

hex() { printf '0x%x' "$1"; }

ERIGON_PID=
start_erigon() {
	"$ERIGON" --eez.enabled --datadir "$DATADIR" \
		--eez.l1.rpc "$EEZ_DEVNET_L1_RPC" --eez.l1.poll=1s --eez.sequencer.rpc "$EEZ_DEVNET_L2_RPC" \
		--http.api=eth,debug,eez --http.port 18545 --authrpc.port 18551 --private.api.addr 127.0.0.1:19090 \
		--port 30399 --torrent.port 42099 --log.console.verbosity=info >> "$OUT/erigon.log" 2>&1 &
	ERIGON_PID=$!
	for _ in $(seq 120); do
		rpc "$ERIGON_RPC" eez_followerStatus '[]' 2>/dev/null | jq -e '.result.safe' > /dev/null && return
		kill -0 "$ERIGON_PID" 2> /dev/null || { tail -30 "$OUT/erigon.log"; fail "erigon exited"; }
		sleep 2
	done
	fail "follower did not start"
}
stop_erigon() {
	[ -n "$ERIGON_PID" ] || return 0
	kill "$ERIGON_PID" 2> /dev/null || true
	wait "$ERIGON_PID" 2> /dev/null || true
	ERIGON_PID=
}
trap stop_erigon EXIT

# A wave that fails leaves fewer effects to compare; the 0x76 count below
# catches a run without any.
wave() {
	log "wave $1"
	EEZ_WAVE_MODE="$1" EEZ_WAVE_COUNT=3 bash "$K/scripts/cross-chain-wave.sh" >> "$OUT/waves.log" 2>&1 \
		|| echo "wave $1 failed, see waves.log" | tee -a "$OUT/waves.log"
}

sed -E 's/^([[:space:]]*enable_explorers:[[:space:]]*)true/\1false/' "$K/ci-args.yaml" > "$OUT/args.yaml"
grep -Eq '^[[:space:]]*enable_explorers:[[:space:]]*false' "$OUT/args.yaml" || fail "could not turn Blockscout off"
arg() { sed -nE "s/^[[:space:]]*$1:[[:space:]]*\"?([0-9]+)\"?.*/\1/p" "$OUT/args.yaml" | head -1; }
l1_ms=$(arg l1_block_time_ms)
l2_ms=$(arg l2_block_time_ms)
l1_chain=$(arg network_id)
[ -n "$l1_ms" ] && [ -n "$l2_ms" ] && [ -n "$l1_chain" ] || fail "args.yaml lacks the block times or network id"

log "starting the devnet"
bash "$K/start.sh" "$OUT/args.yaml" > "$OUT/start.log" 2>&1 || { tail -50 "$OUT/start.log"; fail "start.sh"; }
# shellcheck disable=SC1091
source "$K/ports.sh" > /dev/null
kurtosis files download "$KURTOSIS_ENCLAVE" eez-deployments "$OUT/deployments" > /dev/null
env_value() { grep -E "^$1=" "$OUT/deployments/deployments.env" | head -1 | cut -d= -f2- | tr -d '"'; }
registry=$(env_value EEZ_REGISTRY_ADDRESS)
deploy_block=$(env_value EEZ_REGISTRY_DEPLOY_BLOCK)
rollup_id=$(env_value EEZ_ROLLUP_ID)

log "erigon init: registry $registry from L1 block $deploy_block, rollup $rollup_id"
"$ERIGON" init --eez.enabled --datadir "$DATADIR" --eez.l1.chainid="$l1_chain" \
	--eez.registry="$registry" --eez.registry.deployblock="$deploy_block" --eez.rollupid="$rollup_id" \
	--eez.l1.blocktime="$((l1_ms / 1000))s" --eez.l2.blocktime="$((l2_ms / 1000))s" \
	"$OUT/deployments/l2-genesis.json" > "$OUT/init.log" 2>&1 || { tail -20 "$OUT/init.log"; fail "erigon init"; }

start_erigon
[ "$(block "$ERIGON_RPC" 0x0)" = "$(block "$EEZ_DEVNET_L2_RPC" 0x0)" ] || fail "genesis differs"
log "follower up"
sleep 60

wave mixed

# T-NET-6: stop Erigon during a wave; its safe block stays canonical and it
# resumes from its checkpoint.
wave mixed-pure &
wave_pid=$!
sleep 90
read -r safe_num safe_hash < <(block "$ERIGON_RPC" safe)
log "restart at safe block $safe_num"
stop_erigon
start_erigon
wait "$wave_pid"
grep -q "resuming from checkpoint" "$OUT/erigon.log" || fail "no resume from the checkpoint"
[ "$(block "$ERIGON_RPC" "$(hex "$safe_num")")" = "$safe_num $safe_hash" ] || fail "pre-restart safe block $safe_num replaced"

wave mixed-pure
wave inbound

log "waiting for finality"
for _ in $(seq 120); do
	read -r ef _ < <(block "$ERIGON_RPC" finalized)
	read -r rf _ < <(block "$EEZ_DEVNET_L2_RPC" finalized)
	[ "$ef" -gt 0 ] && [ "$rf" -gt 0 ] && break
	sleep 10
done
sleep 30

# T-NET-8: latest follows the sequencer within one block.
read -r ref_latest _ < <(block "$EEZ_DEVNET_L2_RPC" latest)
read -r erigon_latest _ < <(block "$ERIGON_RPC" latest)
[ $((ref_latest - erigon_latest)) -le 1 ] && [ $((erigon_latest - ref_latest)) -le 1 ] \
	|| fail "latest $erigon_latest, reference $ref_latest"

# Safe and finalized are blocks of the reference chain.
for tag in safe finalized; do
	read -r n h < <(block "$ERIGON_RPC" "$tag")
	[ "$n" -gt 0 ] || fail "no $tag block"
	[ "$(block "$EEZ_DEVNET_L2_RPC" "$(hex "$n")")" = "$n $h" ] || fail "$tag block $n differs"
	log "$tag $n $h"
done

# T-NET-2 up to safe.
read -r safe _ < <(block "$ERIGON_RPC" safe)
"$EEZ" diff --a "$EEZ_DEVNET_L2_RPC" --b "$ERIGON_RPC" --from 0 --to "$safe" > "$OUT/diff.txt" 2>&1 || true
grep -qx "no difference" "$OUT/diff.txt" || { cat "$OUT/diff.txt"; fail "blocks 0..$safe differ"; }

system_txs=0
for n in $(seq 0 "$safe"); do
	k=$(rpc "$ERIGON_RPC" eth_getBlockByNumber "[\"$(hex "$n")\",true]" | jq '[.result.transactions[] | select(.type == "0x76")] | length')
	system_txs=$((system_txs + k))
done
[ "$system_txs" -gt 0 ] || fail "no 0x76 transaction up to safe"
log "pass: blocks 0..$safe identical, $system_txs 0x76 transactions"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
	echo "Erigon equals eez-rollup0 over L2 blocks 0..$safe ($system_txs type-0x76 transactions)." >> "$GITHUB_STEP_SUMMARY"
fi

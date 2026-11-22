#!/usr/bin/env bash
# T-CMP-6, interim: Erigon sequences and composes a fresh eez-rollup0 devnet
# chain whose manager requires two ECDSA proof systems, each attested by a
# reference eez-proof-signer. Pure-L2, inbound and outbound transactions must
# settle with both proofs; an attester outage must stall settlement until it
# ends; the reference follower must derive Erigon's chain up to safe.
#
#   ROLLUP0           eez-rollup0 checkout, for its devnet args
#   ERIGON, EEZ       erigon and eez binaries under test
#   OUT               directory for the logs and the report
#   KURTOSIS_ENCLAVE  enclave name, default eez-quorum
#   KEEP_ENCLAVE      1 keeps the enclave after the run; else it is removed,
#                     reused or not
#   REUSE_ENCLAVE     1 runs against the enclave a kept run left
#
# Requires kurtosis, docker, cast, jq, curl and openssl, and the images
# eez-deploy:ci, eez-proof-signer:ci and eez-node:ci.
set -euo pipefail

: "${ROLLUP0:?}" "${ERIGON:?}" "${EEZ:?}" "${OUT:?}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-quorum}"
DATADIR="$OUT/erigon"
ERIGON_RPC=http://127.0.0.1:18545
L1_FRONT=http://127.0.0.1:18999
L2_FRONT=http://127.0.0.1:18998
mkdir -p "$OUT"

log() { echo "==> $(date -u +%T) $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
lower() { tr '[:upper:]' '[:lower:]'; }

rpc() {
	curl -sf -X POST -H 'content-type: application/json' \
		--data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":$3}" "$1"
}

# block prints "<number> <hash>" of a block tag or hex number, "0 null" when
# there is none or the node does not answer.
block() {
	local r
	r=$(rpc "$1" eth_getBlockByNumber "[\"$2\",false]" \
		| jq -r 'if .result == null then "0x0 null" else "\(.result.number) \(.result.hash)" end') || r="0x0 null"
	printf '%d %s\n' "${r%% *}" "${r##* }"
}

# url prints an endpoint kurtosis reports as an http URL.
url() {
	local v
	v=$(kurtosis port print "$ENCLAVE" "$1" "$2") || fail "no port $2 of $1"
	case $v in http://* | https://*) echo "$v" ;; *) echo "http://$v" ;; esac
}

# until_true runs its arguments every 2 s until they succeed, for at most
# $1 seconds; each try must return.
until_true() {
	local deadline=$((SECONDS + $1))
	shift
	until "$@"; do
		[ $SECONDS -lt $deadline ] || return 1
		sleep 2
	done
}

PORTS=(--authrpc.port 18551 --private.api.addr 127.0.0.1:19090 --port 30399 --torrent.port 42099
	--ws.port 18546 --mcp.port 18553 --sentinel.port 17777 --beacon.api.port 15555
	--caplin.discovery.port 14000 --caplin.discovery.tcpport 14001 --caplin.discovery.quicport 14002)

ERIGON_PID=
PAUSED=
stop_erigon() {
	[ -n "$ERIGON_PID" ] || return 0
	kill "$ERIGON_PID" 2> /dev/null || true
	wait "$ERIGON_PID" 2> /dev/null || true
	ERIGON_PID=
}
cleanup() {
	[ -z "$PAUSED" ] || docker unpause "$PAUSED" > /dev/null 2>&1 || true
	stop_erigon
	rm -rf "$OUT/enclave"
	kurtosis enclave dump "$ENCLAVE" "$OUT/enclave" > /dev/null 2>&1 || true
	[ "${KEEP_ENCLAVE:-0}" = 1 ] || kurtosis enclave rm -f "$ENCLAVE" > /dev/null 2>&1 || true
}
trap cleanup EXIT

sed -E 's/^([[:space:]]*enable_explorers:[[:space:]]*)true/\1false/' "$ROLLUP0/testing/kurtosis/ci-args.yaml" > "$OUT/args.yaml"
arg() { sed -nE "s/^[[:space:]]*$1:[[:space:]]*\"?([0-9a-fA-Fx]+)\"?.*/\1/p" "$OUT/args.yaml" | head -1; }
l1_ms=$(arg l1_block_time_ms)
l2_ms=$(arg l2_block_time_ms)
proof_ms=$(arg proof_time_ms)
slack_ms=$(arg submission_slack_ms)
l1_chain=$(arg network_id)
poster_key=$(arg poster_key)
[ -n "$l1_ms" ] && [ -n "$l2_ms" ] && [ -n "$proof_ms" ] && [ -n "$slack_ms" ] && [ -n "$l1_chain" ] && [ -n "$poster_key" ] \
	|| fail "args.yaml lacks the block times, proof time, slack, network id or poster key"
l1_s=$((l1_ms / 1000))
# Hardhat development account 2, funded on both chains.
user_key=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a

if [ "${REUSE_ENCLAVE:-0}" = 1 ]; then
	log "reusing the enclave"
else
	log "starting the enclave"
	kurtosis enclave rm -f "$ENCLAVE" > /dev/null 2>&1 || true
	kurtosis run --enclave "$ENCLAVE" "$HERE/quorum" --args-file "$OUT/args.yaml" > "$OUT/start.log" 2>&1 \
		|| { tail -50 "$OUT/start.log"; fail "kurtosis run"; }
fi
rm -rf "$OUT/deployments" "$OUT/quorum"
kurtosis files download "$ENCLAVE" eez-deployments "$OUT/deployments" > /dev/null
kurtosis files download "$ENCLAVE" eez-quorum "$OUT/quorum" > /dev/null
env_value() { grep -E "^$2=" "$1" | head -1 | cut -d= -f2- | tr -d '"'; }
deployments=$OUT/deployments/deployments.env
registry=$(env_value "$deployments" EEZ_REGISTRY_ADDRESS)
deploy_block=$(env_value "$deployments" EEZ_REGISTRY_DEPLOY_BLOCK)
rollup_id=$(env_value "$deployments" EEZ_ROLLUP_ID)
manager=$(env_value "$deployments" EEZ_ROLLUP_MANAGER_ADDRESS)
bridge=$(env_value "$deployments" EEZ_L1_BRIDGE_SENDER)
receiver=$(env_value "$deployments" EEZ_L2_BRIDGE_RECEIVER)
eezl2=$(env_value "$deployments" EEZL2_ADDRESS)
ps1=$(env_value "$deployments" EEZ_PROOF_SYSTEM)
att1=$(env_value "$deployments" EEZ_ATTESTER_ADDRESS)
ps2=$(env_value "$OUT/quorum/quorum.env" EEZ_PROOF_SYSTEM)
att2=$(env_value "$OUT/quorum/quorum.env" EEZ_ATTESTER_ADDRESS)

L1=$(url el-1-reth-lighthouse rpc)
services=$(kurtosis enclave inspect --full-uuids "$ENCLAVE")
builder_service=$(awk '$2 ~ /^el-[0-9]+-.*builder/ {print $2}' <<< "$services" | head -1)
[ -n "$builder_service" ] || fail "no builder execution client"
BUILDER=$(url "$builder_service" rbuilder-rpc)
SIGNER1=$(url eez-proof-signer-1 grpc)
SIGNER2=$(url eez-proof-signer-2 grpc)
FOLLOWER=$(url eez-follower l2-rpc)
# Kurtosis names a service's container after the service and its UUID.
signer2_uuid=$(awk '$2 == "eez-proof-signer-2" {print $1}' <<< "$services")
[[ $signer2_uuid =~ ^[0-9a-f]{32}$ ]] || fail "no UUID of eez-proof-signer-2"

threshold=$(cast call --rpc-url "$L1" "$manager" 'threshold()(uint256)')
[ "$threshold" = 2 ] || fail "manager threshold $threshold"
for pair in "$ps1 $att1" "$ps2 $att2"; do
	read -r ps att <<< "$pair"
	signer=$(cast call --rpc-url "$L1" "$ps" 'signer()(address)')
	[ "$(lower <<< "$signer")" = "$(lower <<< "$att")" ] || fail "proof system $ps signer $signer, not $att"
done
log "manager $manager, threshold 2: $ps1 by $att1, $ps2 by $att2"

# signed counts the windows eez-proof-signer-$1 signed since it started.
signed() {
	kurtosis service logs -a "$ENCLAVE" "eez-proof-signer-$1" 2> /dev/null | grep -c 'eez.proof_signer.window_signed' || true
}
signed_before=("$(signed 1)" "$(signed 2)")

log "erigon init: registry $registry from L1 block $deploy_block, rollup $rollup_id"
rm -rf "$DATADIR"
"$ERIGON" init --eez.enabled --datadir "$DATADIR" --eez.l1.chainid="$l1_chain" \
	--eez.registry="$registry" --eez.registry.deployblock="$deploy_block" --eez.rollupid="$rollup_id" \
	--eez.l1.blocktime="${l1_ms}ms" --eez.l2.blocktime="${l2_ms}ms" \
	"$OUT/deployments/l2-genesis.json" > "$OUT/init.log" 2>&1 || { tail -20 "$OUT/init.log"; fail "erigon init"; }
printf '%s' "${poster_key#0x}" > "$OUT/poster.key"
chmod 600 "$OUT/poster.key"

start_erigon() {
	! rpc "$ERIGON_RPC" eth_blockNumber '[]' > /dev/null 2>&1 || fail "another node answers on $ERIGON_RPC"
	"$ERIGON" --datadir "$DATADIR" --eez.enabled --eez.l1.rpc "$L1" --eez.l1.poll=1s --eez.sync.l1only=true \
		"--http.api=eth,debug,eez" --http.port 18545 --nodiscover "${PORTS[@]}" \
		--eez.sequencer --eez.proof.time="${proof_ms}ms" --eez.submission.slack="${slack_ms}ms" \
		--eez.composer --eez.poster.keyfile "$OUT/poster.key" --eez.l1.builder "$BUILDER" \
		--eez.attesters "$SIGNER1=$att1=$ps1,$SIGNER2=$att2=$ps2" \
		--eez.composer.l1front 127.0.0.1:18999 --eez.composer.l2front 127.0.0.1:18998 \
		--eez.composer.l2rpc "$ERIGON_RPC" \
		--log.console.verbosity=info > "$OUT/erigon.log" 2>&1 &
	ERIGON_PID=$!
	for _ in $(seq 120); do
		kill -0 "$ERIGON_PID" 2> /dev/null || { tail -30 "$OUT/erigon.log"; fail "erigon exited"; }
		rpc "$ERIGON_RPC" eth_blockNumber '[]' > /dev/null 2>&1 && return
		sleep 2
	done
	fail "erigon did not start"
}
start_erigon
[ "$(block "$ERIGON_RPC" 0x0)" = "$(block "$FOLLOWER" 0x0)" ] || fail "genesis differs from the reference follower's"
log "erigon up"

# commitment prints the L2 block hash L1 settled the rollup at.
commitment() { cast call --rpc-url "$L1" "$registry" 'rollups(uint64)(address,bytes32,uint256)' "$rollup_id" | sed -n 2p | lower; }
# settled_number prints the number of the L2 block L1 settled the rollup at.
settled_number() {
	local n
	n=$(rpc "$ERIGON_RPC" eth_getBlockByHash "[\"$(commitment)\",false]" | jq -r '.result.number // "0x0"') || n=0x0
	printf '%d\n' "$n"
}
# settled_past succeeds once Erigon's safe block is at least $1 and is the
# block L1 settled the rollup at.
settled_past() {
	local n h
	read -r n h < <(block "$ERIGON_RPC" safe)
	[ "$n" -ge "$1" ] && [ "$(lower <<< "$h")" = "$(commitment)" ]
}
head_number() { block "$ERIGON_RPC" latest | cut -d' ' -f1; }
balance() { cast balance --rpc-url "$1" "$2"; }
receipt_ok() { [ "$(rpc "$1" eth_getTransactionReceipt "[\"$2\"]" | jq -r '.result.status // empty')" = 0x1 ]; }
balance_is() { [ "$(balance "$1" "$2" 2> /dev/null)" = "$3" ]; }
reached() { [ "$(block "$1" latest | cut -d' ' -f1)" -ge "$2" ]; }
steady() { grep -q 'event=eez.composer.sync_slot.invoked .*mode=steady' "$OUT/erigon.log"; }

log "pure L2: three transfers"
for _ in 1 2 3; do
	cast send --rpc-url "$ERIGON_RPC" --private-key "$user_key" "0x$(openssl rand -hex 20)" --value 1000 > /dev/null
done
# Held transactions compose only in steady slots, once the sequencer has
# produced the blocks it missed since genesis; the relay takes the builder's
# blocks only some epochs after genesis.
until_true 1800 steady || fail "the sequencer never reached a steady slot"
target=$(head_number)
until_true 600 settled_past "$target" || fail "pure-L2 blocks up to $target not settled"
log "settled past $target: $(block "$ERIGON_RPC" safe)"

log "inbound: bridge deposit through the L1 front"
deposit=10000000000000000
before=$(balance "$ERIGON_RPC" "$receiver")
raw=$(cast mktx --rpc-url "$L1" --private-key "$user_key" --gas-limit 600000 --value "$deposit" "$bridge" 'bridge()')
in_hash=$(rpc "$L1_FRONT" eth_sendRawTransaction "[\"$raw\"]" | jq -r '.result // empty') || in_hash=
[ -n "$in_hash" ] || fail "L1 front refused the deposit"
until_true 300 receipt_ok "$L1" "$in_hash" || fail "deposit $in_hash not executed on L1"
after=$(balance "$ERIGON_RPC" "$receiver")
[ "$after" = "$((before + deposit))" ] || fail "receiver balance $after, expected $((before + deposit))"

log "outbound: withdrawal through the L2 front"
recipient=0x$(openssl rand -hex 20)
proxy=$(cast call --rpc-url "$ERIGON_RPC" "$eezl2" 'computeCrossChainProxyAddress(address,uint64)(address)' "$recipient" 0)
cast send --rpc-url "$ERIGON_RPC" --private-key "$user_key" --gas-limit 1500000 \
	"$eezl2" 'createCrossChainProxy(address,uint64)' "$recipient" 0 > /dev/null
withdrawal=1000000000000000
raw=$(cast mktx --rpc-url "$ERIGON_RPC" --private-key "$user_key" --gas-limit 600000 --value "$withdrawal" "$proxy")
out_hash=$(rpc "$L2_FRONT" eth_sendRawTransaction "[\"$raw\"]" | jq -r '.result // empty') || out_hash=
[ -n "$out_hash" ] || fail "L2 front refused the withdrawal"
until_true 300 balance_is "$L1" "$recipient" "$withdrawal" || fail "withdrawal $out_hash not paid on L1"
receipt_ok "$ERIGON_RPC" "$out_hash" || fail "withdrawal $out_hash failed on L2"
target=$(head_number)
until_true 300 settled_past "$target" || fail "effects up to $target not settled"
log "effects settled past $target"

# Paused, the signer keeps its host port: Erigon dials the same URL after.
# A batch proven before the pause may still settle, up to the head at the
# pause; none past it can.
log "attester outage: pausing eez-proof-signer-2"
signer2=$(docker ps -q --filter "name=^eez-proof-signer-2--$signer2_uuid\$")
[ "$(wc -l <<< "$signer2")" -eq 1 ] && [ -n "$signer2" ] || fail "no single container of eez-proof-signer-2"
mark=$(wc -l < "$OUT/erigon.log")
paused_at=$(head_number)
docker pause "$signer2" > /dev/null
PAUSED=$signer2
for _ in $(seq $((2 * l1_s))); do
	settled=$(settled_number)
	[ "$settled" -le "$paused_at" ] || fail "L1 settled block $settled, past $paused_at, with one attester of a threshold-2 manager"
	sleep 2
done
tail -n +$((mark + 1)) "$OUT/erigon.log" | grep -q "quorum of 2 not reached: .*$(lower <<< "${ps2#0x}"):" \
	|| fail "no quorum failure naming $ps2 logged during the outage"
docker unpause "$signer2" > /dev/null
PAUSED=
target=$(head_number)
until_true 300 settled_past "$target" || fail "settlement did not resume after the outage"
log "settlement resumed past $target"

log "reference follower"
read -r safe _ < <(block "$ERIGON_RPC" safe)
until_true 300 reached "$FOLLOWER" "$safe" || fail "reference follower behind $safe"
"$EEZ" diff --a "$FOLLOWER" --b "$ERIGON_RPC" --from 0 --to "$safe" > "$OUT/diff.txt" 2>&1 || true
grep -qx "no difference" "$OUT/diff.txt" || { cat "$OUT/diff.txt"; fail "blocks 0..$safe differ from the reference follower's"; }

for i in 1 2; do
	n=$(($(signed "$i") - ${signed_before[$((i - 1))]}))
	[ "$n" -gt 0 ] || fail "eez-proof-signer-$i signed no window"
	log "eez-proof-signer-$i signed $n windows"
done
failed=$(grep -c 'event=eez.composer.bundle.observed .*verdict=failed' "$OUT/erigon.log" || true)
posted=$(grep -c 'event=eez.composer.bundle.observed' "$OUT/erigon.log" || true)
log "pass: blocks 0..$safe settled under a threshold-2 manager and equal the reference follower's; $failed of $posted bundles failed"

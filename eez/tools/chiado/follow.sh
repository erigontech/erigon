#!/usr/bin/env bash
# Follow eez-chiado (eez.dev) from its genesis with Erigon for FOLLOW minutes
# of derivation and require the blocks it derived to be the reference L2's.
#
#   ERIGON     erigon binary under test
#   OUT        directory for the datadir and the logs
#   MODE       remote (default): the follower reads L1 over L1_RPC; all-in-one:
#              Erigon syncs Chiado itself, archive, with the EEZ chain beside
#              it, which needs about an hour and 40 GB before deriving
#   L1_RPC     Chiado JSON-RPC, remote mode; default https://eez.dev/rpc/l1,
#              since rpc.chiadochain.net rate-limits a follower catching up
#   L1_BEACON  beacon API serving Chiado blobs; default https://rpc-gbc.chiadochain.net
#   L2_RPC     reference L2 JSON-RPC; default https://eez.dev/rpc/l2
#   FOLLOW     minutes of derivation; default 20
#   START      minutes until the first derived block: the boot scans the L1
#              history from the registry's deploy block first, and an
#              all-in-one node syncs its L1 before; default 90, all-in-one 240
#
# Requires jq and curl.
set -euo pipefail

: "${ERIGON:?}" "${OUT:?}"
MODE="${MODE:-remote}"
L1_RPC="${L1_RPC:-https://eez.dev/rpc/l1}"
L1_BEACON="${L1_BEACON:-https://rpc-gbc.chiadochain.net}"
L2_RPC="${L2_RPC:-https://eez.dev/rpc/l2}"
FOLLOW="${FOLLOW:-20}"
DATADIR="$OUT/erigon"
ERIGON_RPC=http://127.0.0.1:28545
# Ports apart from the defaults, so the run does not clash with a node on
# the same host.
PORTS=(--authrpc.port 28551 --private.api.addr 127.0.0.1:29090 --port 30398 --torrent.port 42098
	--ws.port 28546 --mcp.port 28553 --sentinel.port 27777 --beacon.api.port 25555
	--caplin.discovery.port 24000 --caplin.discovery.tcpport 24001 --caplin.discovery.quicport 24002)
mkdir -p "$OUT"

log() { echo "==> $(date -u +%T) $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

rpc() {
	curl -sf -X POST -H 'content-type: application/json' \
		--data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":$3}" "$1"
}

# block prints "<number> <hash>" of a block tag or hex number, "0 null" when
# there is none or the endpoint does not answer.
block() {
	local r
	r=$(rpc "$1" eth_getBlockByNumber "[\"$2\",false]" \
		| jq -r 'if .result == null then "0x0 null" else "\(.result.number) \(.result.hash)" end') || true
	[ -n "$r" ] || r="0x0 null"
	printf '%d %s\n' "${r%% *}" "${r##* }"
}

# check requires Erigon's block n to be the reference's, asking the public
# reference up to three times.
check() {
	local ours ref
	ours=$(block "$ERIGON_RPC" "$(hex "$1")")
	for _ in 1 2 3; do
		ref=$(block "$L2_RPC" "$(hex "$1")")
		[ "$ref" != "0 null" ] && break
		sleep 10
	done
	[ "$ours" = "$ref" ] || fail "block $1: erigon $ours, $L2_RPC $ref"
}

hex() { printf '0x%x' "$1"; }

case $MODE in
remote)
	args=(--chain=eez-chiado --eez.enabled --eez.l1.rpc "$L1_RPC" --http.api=eth,eez --http.port 28545)
	START="${START:-90}"
	;;
all-in-one)
	args=(--chain=chiado --prune.mode=archive --caplin.blobs-immediate-backfill
		--http.port 28645 --eez.enabled --eez.chain=eez-chiado --eez.http.port 28545 --eez.port 30498)
	START="${START:-240}"
	;;
*) fail "MODE $MODE: remote or all-in-one" ;;
esac

log "starting erigon ($MODE)"
"$ERIGON" --datadir "$DATADIR" "${args[@]}" "${PORTS[@]}" --eez.l1.beacon "$L1_BEACON" \
	--log.console.verbosity=info >> "$OUT/erigon.log" 2>&1 &
pid=$!
trap 'kill "$pid" 2> /dev/null || true; wait "$pid" 2> /dev/null || true' EXIT

# The follower derives once it has booted; an all-in-one node boots it after
# its L1 reaches the tip.
deadline=$((SECONDS + START * 60))
until read -r safe _ < <(block "$ERIGON_RPC" safe 2> /dev/null) && [ "$safe" -gt 0 ]; do
	kill -0 "$pid" 2> /dev/null || { tail -30 "$OUT/erigon.log"; fail "erigon exited"; }
	[ "$SECONDS" -lt "$deadline" ] || fail "no derived block after $START minutes"
	sleep 30
done
log "deriving: safe block $safe after $((SECONDS / 60)) minutes"
sleep $((FOLLOW * 60))
kill -0 "$pid" 2> /dev/null || { tail -30 "$OUT/erigon.log"; fail "erigon exited"; }
! grep -q '\[CRIT\]' "$OUT/erigon.log" || { grep '\[CRIT\]' "$OUT/erigon.log" | tail -5; fail "critical log line"; }

read -r safe hash < <(block "$ERIGON_RPC" safe)
[ "$safe" -gt 0 ] || fail "no safe block"
for n in 1 $((safe / 2)) "$safe"; do
	check "$n"
done
log "pass: safe block $safe $hash equals the reference"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
	echo "Erigon ($MODE) derived eez-chiado to safe block $safe, equal to $L2_RPC." >> "$GITHUB_STEP_SUMMARY"
fi

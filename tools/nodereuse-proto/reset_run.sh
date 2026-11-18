#!/bin/bash
# Per-test reset of the shared erigon container: kill the previous node, wipe
# the datadir, map the test's genesis (same jq pipeline as erigon.sh), init,
# import the blocks, boot the node, wait for the RPC endpoint.
# $1 = /proto/<test dir> containing genesis-input.json and blocks/
set -e
test_dir=$1

# Kill the previous test's node and let the port free.
pkill -f '/usr/local/bin/erigon' 2>/dev/null || true
sleep 0.3

rm -rf /erigon-hive-datadir
cd "$test_dir"

# Same genesis mapping as hive's erigon.sh: dump the input, marker, mapped genesis.
jq -r -f <(cat /mapper.jq; echo '| (.alloc), "#hive-genesis#", .') "$test_dir/genesis-input.json" \
	| awk 'w { print > "/genesis.json"; next } $0 == "#hive-genesis#" { w = 1; next } { print }'
[ "${PIPESTATUS[0]}" -eq 0 ]

set +e
erigon $FLAGS init /genesis.json >> /proto/reset.log 2>&1
(cd "$test_dir/blocks" && erigon $FLAGS import $(ls | sort -n)) >> /proto/reset.log 2>&1
set -e

echo -n "0x7365637265747365637265747365637265747365637265747365637265747365" > /jwt.secret
erigon $FLAGS >> /proto/erigon.log 2>&1 &

for i in $(seq 1 1200); do
	if curl -s -X POST -H 'Content-Type: application/json' \
		--data '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' \
		http://127.0.0.1:8545 | grep -q result; then
		exit 0
	fi
	sleep 0.05
done
echo "node did not come up" >&2
exit 1

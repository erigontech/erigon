#!/usr/bin/env bash
# Node-reuse prototype for the hive consume-rlp simulator.
# Runs the same 500-test selection through two client architectures:
#
#   before: one docker container per test, exactly like the current
#           single_test_client path (create -> start -> RPC checks -> stop).
#   after:  one long-lived container; per test a docker exec wipes the
#           datadir, re-inits genesis, imports the blocks and boots the node.
#
# Per-test segment times land in /tmp/rlp-proto/results-{arm}.jsonl, a
# summary per arm in /tmp/rlp-proto/summary-{arm}.json.
set -euo pipefail
cd "$(dirname "$0")"

IMAGE=${IMAGE:-hive-erigon-proto}
COUNT=${COUNT:-500}

echo "== building client image (pulls erigontech/erigon:main-latest) =="
if ! docker image inspect "$IMAGE" > /dev/null 2>&1; then
	rm -rf hive-ctx && mkdir -p hive-ctx/clients/erigon
	base=https://raw.githubusercontent.com/ethereum/hive/master/clients/erigon
	for f in Dockerfile.local genesis.json mapper.jq erigon.sh enode.sh; do
		curl -sL -o "hive-ctx/clients/erigon/$f" "$base/$f"
	done
	docker build -t "$IMAGE" hive-ctx/clients/erigon/ -f hive-ctx/clients/erigon/Dockerfile.local \
		> /tmp/rlp-proto-image.log 2>&1
fi

echo "== selecting fixtures =="
[[ -f manifest.json ]] || {
	python3 select500.py /tmp/fixtures/.meta/index.json /tmp/eest_stable.tar.gz "$COUNT"
}
[[ -f rulesets.json ]] || {
	python3 gen_ruleset.py manifest.json > rulesets.json
}

for arm in before after; do
	echo "== arm: $arm =="
	docker rm -f rlp-proto-runner > /dev/null 2>&1 || true
	python3 proto_run.py "$arm" manifest.json rulesets.json "$IMAGE" \
		| tee "results-$arm.log"
	echo "summary written to summary-$arm.json"
done

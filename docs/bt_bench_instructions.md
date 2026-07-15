# efi+vi → bt A/B bench (commitment-history Sepolia server)

Branch `rpc_bt` implements efi+vi→bt gated behind env `AGG_II_BT=true`.
A = recsplit (gate off), D = bt (gate on). Datadir must have commitment history
(synced with `--prune.include-commitment-history`), so the gate also converts
commitment history `.v`→bt.

Let SRC = the synced datadir on that server (read-only source of truth).

## 1. Build
    git fetch && git checkout rpc_bt && git pull
    make erigon rpcdaemon integration
    go build -o build/bin/rpctest ./cmd/rpctest

## 2. Mirror SRC → two arms (hardlinks snapshots, copies chaindata; cheap)
    ./cmd/scripts/mirror-datadir.sh <SRC> /data/hashmap_sepolia   # arm A
    ./cmd/scripts/mirror-datadir.sh <SRC> /data/bt_sepolia        # arm D
(pick any paths with a few tens of GB free)

## 3. Regen bt accessors on arm D (builds .bt/.kvei for IIs, .vbt/.vanchor for paged history)
    AGG_II_BT=true ./build/bin/erigon seg index --datadir=/data/bt_sepolia
(env warns "use ERIGON_ prefix" — harmless, bare name still applies. Takes ~15 min for full Sepolia.)
NOTE: if you regen'd previously on this datadir, delete stale accessors first
(the .vbt/.vanchor anchor-key format changed for variable-length keys):
    rm -f /data/bt_sepolia/snapshots/accessor/*.vbt /data/bt_sepolia/snapshots/accessor/*.vanchor
A fresh mirror from SRC (step 2) has none, so this only matters on a re-run.

## 4. VERIFY commitment history got bt (this is the whole point of this server)
    ACC=/data/bt_sepolia/snapshots/accessor
    ls "$ACC" | grep -c '\.bt$'                       # II bt (should be ~100+)
    ls "$ACC" | grep 'commitment' | grep -E '\.vbt$|\.vanchor$'   # commitment history bt — MUST be non-empty
    ls "$ACC" | grep -E '\.vbt$' | sed -E 's/.*-([a-z]+)\..*/\1/' | sort -u  # which domains got vi→bt
If NO commitment .vbt appears: the regen didn't see commitment history as enabled.
Check `./build/bin/integration ...` or confirm the DB flag; commitment history must be
active during `seg index` (it reads the DB flag). If needed, ensure the datadir's
erigondb/commitment-history flag is set before regen.

## 5. Launch both rpcdaemons (offset ports; standalone --datadir mode)
    ./build/bin/rpcdaemon --datadir=/data/hashmap_sepolia --http.port=8645 \
        --private.api.addr=127.0.0.1:9191 --http.api=eth,debug,trace,erigon &
    AGG_II_BT=true ./build/bin/rpcdaemon --datadir=/data/bt_sepolia --http.port=8745 \
        --private.api.addr=127.0.0.1:9192 --http.api=eth,debug,trace,erigon &
Wait for "[rpc] endpoint opened" on each.

## 6. CORRECTNESS first — bt must equal recsplit (pick a historical block, e.g. 5,000,000 = 0x4C4B40)
    A=http://127.0.0.1:8645; D=http://127.0.0.1:8745
    # eth_getProof at a historical block (reads COMMITMENT history via bt) — the key new query
    REQ='{"jsonrpc":"2.0","id":1,"method":"eth_getProof","params":["0x<addr>",["0x0"],"0x4C4B40"]}'
    diff <(curl -s -XPOST -H content-type:application/json --data "$REQ" $A) \
         <(curl -s -XPOST -H content-type:application/json --data "$REQ" $D) && echo "PROOF MATCH"
    # also sanity: getBalance / getLogs (as on the other server)
Any mismatch = bug; stop and report.

## 7. PERF A/B — run THAT SERVER'S OWN bench suite against each arm
The only bt-specific requirements:
  - arm D's datadir was regen'd with `AGG_II_BT=true ./erigon seg index` (step 3), AND
  - arm D's rpcdaemon is launched with `AGG_II_BT=true` in its environment (step 5).
  - arm A is plain (no env, recsplit).
Then point the existing bench harness at the two endpoints and compare A vs D.

If the harness targets a fixed port/datadir (e.g. 8545), run one arm at a time:
  a) start arm A rpcdaemon (plain) → run bench suite → save results → stop it
  b) start arm D rpcdaemon (AGG_II_BT=true) → run same bench suite → save → stop
Per the cold protocol: `sync; echo 3 | sudo tee /proc/sys/vm/drop_caches` right BEFORE
starting each arm's daemon, so it starts cold. (rpctest RandomAccount/RandomBlock, if used,
ignores SIGTERM → stop with `timeout -s KILL`.)

Make sure the suite includes an **eth_getProof** (historical block) workload — that's the query
that exercises commitment history vi→bt, which is the unique thing this server can test.

## 8. Cleanup
    kill both rpcdaemons; rm -rf /data/hashmap_sepolia /data/bt_sepolia (disposable mirrors)

## Notes / gotchas (from the first server)
- Realistic result there (125G RAM): getBalance bt ~15% lower latency + ~4× tighter tail; getLogs single-block a wash; bt accessors ~6× smaller on disk (.bt vs .efi).
- Memory cap (systemd-run --property=MemoryHigh=<N>G) only matters if <N> < working set. 32G didn't bind for getBalance (~13G working set). Commitment-history/getProof working set may be larger — worth trying a realistic cap (e.g. node's real limit) if RAM >> data.
- rpctest RandomAccount/RandomBlock loop forever printing p50/p90/p99/RPS every 1s; use `timeout -s KILL` (rpctest ignores SIGTERM).

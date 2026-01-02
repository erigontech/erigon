# Giulio2002 PRs to erigontech/erigon

## Open PRs

| PR # | Title | URL |
|------|-------|-----|
| 18527 | Caplin: reduced amount of map assigned for head state | https://github.com/erigontech/erigon/pull/18527 |
| 18526 | TxPool: map reduce and batch load | https://github.com/erigontech/erigon/pull/18526 |
| 18525 | Caplin: Global Active Validator Caches | https://github.com/erigontech/erigon/pull/18525 |
| 18524 | Caplin: More unnecessary validator work removed | https://github.com/erigontech/erigon/pull/18524 |
| 18514 | Performance: perform receipts root and bloom check concurrently when on chain tip | https://github.com/erigontech/erigon/pull/18514 |
| 18488 | WIP: Linearized EVM | https://github.com/erigontech/erigon/pull/18488 |
| 18473 | Performance: Add warmup cache | https://github.com/erigontech/erigon/pull/18473 |
| 18326 | Caplin: support backfilling blobs through a remote beacon API (Draft) | https://github.com/erigontech/erigon/pull/18326 |
| 17343 | NOT MERGE | https://github.com/erigontech/erigon/pull/17343 |
| 17089 | DO NOT MERGE - Docker image | https://github.com/erigontech/erigon/pull/17089 |
| 16791 | WIP: History dedup optimization (Draft) | https://github.com/erigontech/erigon/pull/16791 |

## Merged PRs

| PR # | Title | URL |
|------|-------|-----|
| 18522 | Erigon+Caplin: Decrease GC pressure | https://github.com/erigontech/erigon/pull/18522 |
| 18521 | Caplin: reduce amount of work done when we don't have validators | https://github.com/erigontech/erigon/pull/18521 |
| 18515 | Revert "Revert "Performance: do not rehash the trie storage that is memoized"" | https://github.com/erigontech/erigon/pull/18515 |
| 18503 | Performance: use memclear instead of the shit we were doing before | https://github.com/erigontech/erigon/pull/18503 |
| 18502 | Performance: switch to a global jumpdest cache | https://github.com/erigontech/erigon/pull/18502 |
| 18485 | Optimized the Huffman decoding hot paths in the decompression routines | https://github.com/erigontech/erigon/pull/18485 |
| 18479 | Fixed false positive data race | https://github.com/erigontech/erigon/pull/18479 |
| 18462 | Revert "half block exec fix in receipts" | https://github.com/erigontech/erigon/pull/18462 |
| 18460 | Revert "Performance: do not rehash the trie storage that is memoized" | https://github.com/erigontech/erigon/pull/18460 |
| 18455 | Erigon: fixed sender recovery race | https://github.com/erigontech/erigon/pull/18455 |
| 18422 | Performance: do not rehash the trie storage that is memoized | https://github.com/erigontech/erigon/pull/18422 |
| 18393 | More repressentitive logging of actual performance | https://github.com/erigontech/erigon/pull/18393 |
| 18390 | Better streamlining of sender recovery | https://github.com/erigontech/erigon/pull/18390 |
| 18354 | Performance: Commitment asyncronous warmup | https://github.com/erigontech/erigon/pull/18354 |
| 18320 | Caplin: Persistent historical download | https://github.com/erigontech/erigon/pull/18320 |
| 18314 | Cherry-pick: Refactor: Extract BlobHistoryDownloader into dedicated struct (#18269) | https://github.com/erigontech/erigon/pull/18314 |
| 18269 | Refactor: Extract BlobHistoryDownloader into dedicated struct | https://github.com/erigontech/erigon/pull/18269 |
| 18123 | Caplin: Simplified peer refreshing | https://github.com/erigontech/erigon/pull/18123 |
| 18006 | Erigon: remove experimental tag for historical commitments trie | https://github.com/erigontech/erigon/pull/18006 |
| 17829 | Caplin: add get blobs support (fusaka) | https://github.com/erigontech/erigon/pull/17829 |
| 17705 | Caplin: downscore peer sending attestations not in range | https://github.com/erigontech/erigon/pull/17705 |
| 17611 | Cherry-Pick: Add cli tool to fetch and recover blobs from a remote beacon api | https://github.com/erigontech/erigon/pull/17611 |
| 17450 | Add cli tool to fetch and recover blobs from a remote beacon api | https://github.com/erigontech/erigon/pull/17450 |
| 17392 | Caplin: faster DA recovery | https://github.com/erigontech/erigon/pull/17392 |
| 17326 | Revert "Caplin: Improved block downloader". potential regression | https://github.com/erigontech/erigon/pull/17326 |
| 17219 | Caplin: better waiting huristic for snapshot downloader (#17204) | https://github.com/erigontech/erigon/pull/17219 |
| 17215 | Cherry-Pick: fix bad snapshots name wait handling | https://github.com/erigontech/erigon/pull/17215 |
| 17204 | Caplin: better waiting huristic for snapshot downloader | https://github.com/erigontech/erigon/pull/17204 |
| 17128 | Cherry-Pick: Caplin: prioritize head event (#17122) | https://github.com/erigontech/erigon/pull/17128 |
| 17127 | Caplin: correct seconds-per-eth1-block for hoodi | https://github.com/erigontech/erigon/pull/17127 |
| 17122 | Caplin: prioritize head event | https://github.com/erigontech/erigon/pull/17122 |
| 17110 | Charry-Pick: more unittests and spectests for fulu (#16623) | https://github.com/erigontech/erigon/pull/17110 |
| 17016 | Cherry-pick: pick Caplin fixes for Fusaka and block downloader | https://github.com/erigontech/erigon/pull/17016 |
| 16996 | Caplin: disable queue based peer selection | https://github.com/erigontech/erigon/pull/16996 |
| 16995 | Caplin: improvement and simplification on peer selection | https://github.com/erigontech/erigon/pull/16995 |
| 16915 | Caplin: better startSlot heuristic for non-finality | https://github.com/erigontech/erigon/pull/16915 |
| 16745 | Cherry-pick: Adjust Beacon API (#16651) | https://github.com/erigontech/erigon/pull/16745 |
| 16719 | Caplin: add stricter timeouts to p2p handling | https://github.com/erigontech/erigon/pull/16719 |
| 16710 | Cherry-pick: download log indicies alongside rcache | https://github.com/erigontech/erigon/pull/16710 |
| 16648 | Erigon 3: Download `logaddr` and `logtopic` indicies | https://github.com/erigontech/erigon/pull/16648 |
| 16510 | Update readme.md | https://github.com/erigontech/erigon/pull/16510 |
| 16509 | Cherry-Pick: revert receipts persistence | https://github.com/erigontech/erigon/pull/16509 |
| 16508 | Cherry-Pick: Caplin: fixed running lighthouse vc alongside it | https://github.com/erigontech/erigon/pull/16508 |
| 16499 | Revert "enable `--persist.receipts` by default" | https://github.com/erigontech/erigon/pull/16499 |
| 16495 | Caplin: fixed running lighthouse vc alongside it | https://github.com/erigontech/erigon/pull/16495 |
| 16492 | Cherry-Pick: Fulu devnet-3 in Caplin | https://github.com/erigontech/erigon/pull/16492 |
| 16428 | Caplin: simplified column store and `peerDAS` object | https://github.com/erigontech/erigon/pull/16428 |
| 16420 | Caplin: implemented mev-boost for Fulu | https://github.com/erigontech/erigon/pull/16420 |
| 16367 | Cherry-pick: `1e971694fcd8e29a5a98b9a8e263a86fb4fa28a7` | https://github.com/erigontech/erigon/pull/16367 |
| 16296 | Cherry-pick: fix `eth_getProof` for historical values | https://github.com/erigontech/erigon/pull/16296 |
| 16295 | Erigon v3: do not terminate `exec3` after pruning unless batch is full | https://github.com/erigontech/erigon/pull/16295 |
| 16236 | Cherry-pick: removed leftover `fmt.Println` | https://github.com/erigontech/erigon/pull/16236 |
| 16233 | Cherry-Pick: Added pending enpoints | https://github.com/erigontech/erigon/pull/16233 |
| 16232 | Cherry-picked disabling peerdas code pre-peerDAS | https://github.com/erigontech/erigon/pull/16232 |
| 16231 | Caplin: cherry-pick changes in 3.1 to main | https://github.com/erigontech/erigon/pull/16231 |
| 16221 | Caplin: fixed `CaplinTypeString` parsing for v3.1 | https://github.com/erigontech/erigon/pull/16221 |
| 16219 | Caplin: added new queue endpoints for deposit lists | https://github.com/erigontech/erigon/pull/16219 |
| 16192 | removed leftover datadir | https://github.com/erigontech/erigon/pull/16192 |
| 16169 | Restore flag's default value | https://github.com/erigontech/erigon/pull/16169 |

## Closed PRs (not merged)

| PR # | Title | URL |
|------|-------|-----|
| 18520 | reduce GC pressure and allocs | https://github.com/erigontech/erigon/pull/18520 |
| 18486 | Performance: recursion optimization | https://github.com/erigontech/erigon/pull/18486 |
| 18407 | i just need to check these fucking branches | https://github.com/erigontech/erigon/pull/18407 |
| 16868 | Caplin: some tweaks towards non-finality in peerDAS | https://github.com/erigontech/erigon/pull/16868 |
| 16344 | Fixed caplin blob archive on devnet-3 tentaitively | https://github.com/erigontech/erigon/pull/16344 |

# PBin M1b smoke run — `--chain=dev` on the binary commitment trie

Record of the M1b gate: a local dev chain booted from genesis on the EIP-8297 binary
trie, produced blocks, deployed and called contracts, and resumed after a restart.

Binary: `awskii/pbin-patricia`, erigon `v3.7.0-dev`, darwin/arm64.
Hash: Keccak-256 (BLAKE3 is test-only). Roots below agree with no other client.

## Command line

```bash
make erigon
./build/bin/erigon \
  --chain=dev \
  --datadir=/tmp/pbin-m1b/gate \
  --experimental.bin-commitment \
  --beacon.api=beacon,validator,node,config \
  --dev.slot-time=2 \
  --http.api=eth,erigon,web3,net,debug,trace,txpool \
  --http.port=8545 --beacon.api.port=5555 --private.api.addr=127.0.0.1:9090
```

The header state-root check is on (its default), so every block below had its executed
root compared against the header the builder produced.

Restart uses the same line **without** `--experimental.bin-commitment`: the variant is
persisted in `snapshots/erigondb.toml` (`trie_variant = 'bin'`) and re-adopted, logged as
`datadir uses the bin commitment trie; enabling it for this process`.

## Genesis

| | root | block hash |
|---|---|---|
| bin | `0xa314dd2e35d820afa60105d356faeae5beb379796fe3bf691a39df6e7bc9a331` | `0xa6d15d434deb7f19f5ac9655b7bf4918c056ecaacc25769bf5f6b3c242a9f538` |
| hex | `0xeed1da9777066ae75039e23f5d0ccc4ae5efae81b9314afcc87af0e714179b4c` | `0x3aa9a433bdbbf19493a237861e62e6c4a66ad676da6d1978dd8039228f64e2c0` |

The dev beacon takes `Eth1Data` from the EL genesis hash, accepted it, and produced from
slot 1 on. The alloc's deposit contract is 6358 bytes = 206 code chunks (128 header + 78
CODE_ZONE overflow), so block 0 already exercises Task 13.

## Blocks, contracts

Deployed from the dev signer `0x78eF752367584ee389aCB8824Ceec734456402b6`
(key = `sha256("signer:devnet")`).

- **A** `0x55d8f9693a57f932cde89739f93d4a271d56a156` — init `0x600680600b6000396000f3600035600055`,
  runtime stores calldata word 0 into slot 0.
- **B** `0x02dcc6fdd01d75a5bda67e4e7c074cfddc204111` — 4983-byte runtime (151 × `PUSH32`),
  161 chunks, so 33 land in CODE_ZONE overflow at runtime rather than at genesis.

| block | event | root |
|---|---|---|
| 0 | genesis | `0xa314dd2e35d820afa60105d356faeae5beb379796fe3bf691a39df6e7bc9a331` |
| 15 | deploy A | `0x4cf9eb8a276c1dc5f7debf3d70f50228de7d5b28bf21e8d3b34d88df47426aef` |
| 16 | call A, slot 0 := `0x2a` | `0xa399537a22b085b5df15ffb5fc855870d67225817e0d18c74583db1009b2182a` |
| 17 | deploy B | `0xd513691489314ab5c754b18e3e51db092918725decbc87d475d1151174a3f773` |

`eth_getStorageAt(A, 0)` = `0x…2a`, `eth_getCode(B)` = 4983 bytes.

## Restart

Stopped at head 21 (SIGTERM), restarted flagless on the same datadir. Roots at blocks
0/15/16/17 identical, head preserved, zero `Wrong trie root`.

A longer run reached head 241 and repeated the restart over a datadir that had already
collated and merged state files (`v2.2-commitment.0-4.kv`, `4-6`, `6-7`): roots at blocks
0/15/16/17/100/200 identical across the restart. Before it, an earlier run resumed and
produced ~90 further blocks (79 → 171) with the root check on.

Block *production* does not always resume after a restart: Caplin's forward sync stalls
("could not find sync committee for epoch"). Reproduced identically on hex, so it is a
dev-mode CL limitation, not a trie one. The EL side always resumed.

## Collation and merge

With `step_size = 64` and `MAX_REORG_DEPTH=8` (defaults never freeze on a chain this
short) the chain built and merged pbin commitment files while running, with no root
mismatch.

## `integration commitment rebuild`

Runs to completion under bin: adopts the persisted variant, rebuilds all three shards from
the pbin state files (497 / 256 / 160 keys, blocks 126 / 190 / 222).

It does **not** confirm the chain's roots here. The per-shard roots the tool prints are
partial — each shard folds only its own key range — and the documented follow-up
(`integration stage_exec --reset`) cannot run on this datadir at all: `readGenesis` has no
`dev` entry and panics with `unknown chain spec with name dev`. Without it, DB remnants
past the rebuilt range make the first post-rebuild block report a wrong root — **on hex
exactly as on bin**, so the check as available is not variant-discriminating.

The real forward-run-vs-rebuild oracle for pbin is the M1a gate
(`execution/commitment/backtester/pbin_m1a_test.go`), which does that comparison over a
real MDBX datadir with real `.kv` files.

## Limitations hit during the run

- **Parallel execution is off under bin.** The parallel executor's normalized write set
  produces a different bin root than the same block executed serially (block 0:
  `e557bca8…` vs the genesis root `a314dd2e…`; hex agrees on both executors). Rather than
  leave a wrong-root path reachable, `executeInParallel` keeps bin on the serial executor.
  Unresolved — the divergence itself still needs a root cause.
- Dev-mode CL cannot reliably resume block production after a restart (above).

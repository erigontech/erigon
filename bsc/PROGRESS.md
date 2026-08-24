# BSC (Chapel) bring-up on Erigon — progress log

Branch: `bsc_support`. Goal: sync BSC testnet (Chapel, chainID 97) over devp2p with
no CL, no execution yet — just get blocks and print number+hash — then snapshots.

## Status

| Phase | State | Checkpoint |
|-------|-------|-----------|
| 0. References | ✅ | `node-real/bsc-erigon` @v1.4.3 + `bnb-chain/reth-bsc` cloned to `/home/erigon/bsc_refs/` |
| 1. `--chain=chapel` boots | ✅ | genesis `0x6d3c66c5…fbe34` commits, chainID 97, no panic |
| 2. Permissive Parlia stub engine | ✅ | registered via `RegisterL2Engine("parlia", …)`; node instantiates engine |
| 3. Connect to Chapel peers + handshake | ✅ | full eth/68 + UpgradeStatus handshake; 2+ stable peers |
| 4. Blocks over p2p + logging | ✅ | live Chapel tip streaming from `--chain=chapel` alone, no extra flags |
| 5. Snapshots | ⏳ | needs active download + DB persist first, then `erigon seg retire --chain=chapel` |

## Rebased onto main after the Bor removal

Upstream deleted Polygon/Bor — `polygon/` is zero files on `origin/main`. Every rebase
conflict came from BSC lines sitting next to Bor lines that no longer exist; all Bor arms
were dropped and only the Parlia ones kept.

The block printer had been built on `polygon/p2p.Service`, which went with it. That was a
single ~120-line facade over `execution/p2p` primitives, and those all survive
(`NewMessageListener`, `NewPeerTracker`, `NewFetcher`, `NewPublisher`,
`NewBackwardBlockDownloader`, the observer registrations). It is now vendored as
**`bsc/p2p/service.go`**, package `bscp2p` — same facade, no `polygon/` dependency. It keeps
the fetch and publish surface, which phase 5 needs.

Also: `StatusPacket.TD` is `*uint256.Int` **upstream** now, so phase-3 item 4 below is no
longer ours to carry. `UpgradeStatusMsg` is still ours alone.

## Phase 4 — blocks over devp2p, printed

`node/eth/backend.go` sync dispatch (`func Start`) started sync only for `IsChainPoS` or
`chainConfig.Bor != nil` — a BSC chain started nothing. A `chainConfig.Parlia != nil` arm
runs `bsc/sync.RunBlockPrinter` over a `bsc/p2p.Service` (constructed in `NewEthereum`,
reusing the chain-agnostic fetcher / message-listener / peer-tracker). The printer registers
`NewBlockHashes`/`NewBlock` observers and logs number+hash — no verification, no execution,
no persistence.

Caveat: this is **tip-following via announcements**, not historical download. Phase 5 needs a
driver that actively `FetchHeaders`+`FetchBodies` from genesis, does a minimal root/hash
verify, and `InsertBlocks` into the DB — then `erigon seg retire`. `polygon/sync` is gone, so
the reference is now `execution/p2p/bbd.go` (backward block downloader) plus the exec module;
`bsc/p2p.Service` already exposes `FetchHeaders`, `FetchBodies` and `FetchBlocksBackwards`.

## What was built (Phases 1–2)

- `execution/chain/networkname`: `Chapel = "chapel"`; `execution/chain/spec/network_id.go`: `ChapelChainID = 97`.
- `execution/chain/rules.go`: `ParliaRules = "parlia"` added to `ValidRulesNames`.
- `execution/chain/chain_config.go`: BSC fork fields added as plain `*uint64` so forkid's
  reflection gathers them and they unmarshal from the chainspec.
- `execution/chain`: Parlia is a first-class L1 consensus engine — `ParliaConfig` + a
  `Parlia` field on `chain.Config` (peer to `Aura`), set from the chainspec `"parlia"` key;
  `getEngine()` reports `parlia`.
- New `bsc/chain/` package: `chapel.json` chainspec (trimmed — no `parlia.blockAlloc`, not
  needed without execution), `allocs/chapel.json` (verbatim, for genesis hash), genesis, and
  the static peer enodes.
- New `bsc/parlia/` package: permissive `rules.Engine` stub (accepts any header, no execution).
- Engine wired into the rules-engine factories as `case *chain.ParliaConfig` (not
  merge-wrapped): `CreateRulesEngine`, `CreateRulesEngineBareBones`, backend's `rulesConfig`
  switch, and the integration + rpcdaemon engine switches. Chains registered via blank
  imports at the usual sites.

## Phase 3 — the wire-level fixes to reach a stable BSC peer

All diagnosed from raw bytes / trace logs against the live Chapel network. Items 1–3 are now
`--chain=chapel` defaults in `cmd/utils/flags.go`, each behind `ctx.IsSet` so an explicit
flag still wins.

1. **eth/68 not advertised.** Default is `[eth/69,70,71]`. BSC advertises `[eth/70, eth/68]`
   and prefers 70, but its eth/70 `StatusPacket` keeps `TD` *and* takes the eth/69 block-range
   fields (`EarliestBlock`, `LatestBlock`, `LatestBlockHash`), which does not match our
   `StatusPacket69` (no TD). So eth/68 is the only version we can decode today, and pinning to
   it is a limitation on our side, not a gap in BSC. → **now a default.** Supporting BSC's
   eth/70 needs a BSC-specific status variant; worth doing, since 70 is their primary.
2. **Discovery was v5-only.** BSC enodes are discv4, and BSC publishes no DNS node list, so
   discv5 has nothing to resolve. → **now a default** (`v4=true`, `v5=false`).
3. **BSC bootstraps via static peers, not bootnodes.** bsc-geth ships Chapel's peers as
   `StaticNodes` in `testnet.zip` with no `BootstrapNodes` key at all (mainnet is the mirror
   image: 6 bootnodes, `StaticNodes = []`). → **now a default**, via a new `Spec.StaticPeers`
   field that `StaticPeerURLsOfChain` resolves from the chain registry.
4. **`execution/rlp` can't decode `StatusPacket.TD` (`*big.Int`).** `makeDecoder` has a
   `uint256.Int` case but none for `big.Int`, so a `*big.Int` field falls through to the
   struct decoder → "expected input list for big.Int". Dormant upstream because eth/69+
   `StatusPacket69` has no TD. → **fixed upstream**; `TD` is `*uint256.Int` on main.
5. **Fork schedule was stale.** bsc-erigon v1.4.3's chapel.json stops at `fermi`; current
   Chapel tips have activated Mendel/Osaka (`1774319400`) and Pasteur (`1784601000`). Missing
   forks → forkid checksum mismatch → EIP-2124 filter rejected the peer ("local incompatible
   or needs update"). → added the two timestamps.
6. **BSC `UpgradeStatusMsg` (0x0b) handshake extension.** After Status, BSC networks
   (56/97/714) exchange an `UpgradeStatusMsg`; without it the peer drops us after ~5s
   (`read timeout`), and our sentry logged "Unknown message code 11". → ported
   `UpgradeStatusMsg`/`UpgradeStatusPacket`/`UpgradeStatusExtension` and the post-Status
   exchange into `handShake` (`p2p/protocols/eth/protocol.go`, `p2p/sentry/eth_handshake.go`).
   Not specified in any BEP — the only sources are bsc-geth's source and
   [PR #412](https://github.com/bnb-chain/bsc/pull/412). eth/70 does not exchange it.
7. **Three of four Chapel enodes had stale node IDs.** They came from the archived
   `node-real/bsc-erigon`; only `665cf77c…` still matched what bsc-geth ships. A wrong node ID
   fails the RLPx identity check before any protocol handshake, which is why only a subset of
   peers accepted us. → refreshed from the current `testnet.zip` asset. The two peers now
   answering (`db1e2c76…`, `e5c4320e…`) are both ones that were wrong before.

## Repro

```
./build/bin/erigon --datadir=<dir> --chain=chapel --http=false --nat=none
```

No p2p flags needed — protocol version, discovery and static peers all come from the chain
default. Expect `[bsc] new block hash number=… hash=…` within a second or two.

The listen port is left at Erigon's default. bsc-geth compiles in `:30311` and its mainnet
`config.toml` sets it, but the listen port has no bearing on dialling peers at their `:30311`
— reth-bsc listens on reth's default `:30303` and works. Pass `--port` if inbound
reachability on the conventional port matters.

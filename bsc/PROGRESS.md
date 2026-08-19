# BSC (Chapel) bring-up on Erigon — progress log

Branch: `bsc_support`. Goal: sync BSC testnet (Chapel, chainID 97) over devp2p with
no CL, no execution yet — just get blocks and print number+hash — then snapshots.

## Status

| Phase | State | Checkpoint |
|-------|-------|-----------|
| 0. References | ✅ | `node-real/bsc-erigon` @v1.4.3 + `bnb-chain/reth-bsc` cloned to `/home/erigon/bsc_refs/` |
| 1. `--chain=chapel` boots | ✅ | genesis `0x6d3c66c5…fbe34` commits, chainID 97, no panic |
| 2. Permissive Parlia stub engine | ✅ | registered via `RegisterL2Engine("parlia", …)`; node instantiates engine |
| 3. Connect to Chapel peers + handshake | ✅ | `[p2p] GoodPeers eth68=1` — stable BSC peer (Geth v1.7.6), full eth/68 + UpgradeStatus handshake |
| 4. Blocks over p2p + logging | ✅ | live Chapel tip streaming: `[bsc] new block hash number=125969034 hash=0x…` at sub-second cadence |
| 5. Snapshots | ⏳ | needs active download + DB persist first, then `erigon seg retire --chain=chapel` |

## Phase 4 — blocks over devp2p, printed

`node/eth/backend.go` sync dispatch (`func Start`) previously started sync only for
`IsChainPoS` or `chainConfig.Bor != nil` — a BSC chain started nothing. Added a
`chainConfig.Parlia != nil` arm that runs `bsc/sync.RunBlockPrinter` over a
`polygon/p2p.Service` (constructed in `NewEthereum`, reusing the chain-agnostic fetcher /
message-listener / peer-tracker). The printer registers `NewBlockHashes`/`NewBlock`
observers and logs number+hash — no verification, no execution, no persistence.

Result: with a stable BSC peer, we log the live Chapel tip continuously, e.g.
`[bsc] new block hash number=125969038 hash=0x90a0…` (~0.45s apart).

Caveat: this is **tip-following via announcements**, not historical download. Phase 5
(snapshots) needs a driver that actively `FetchHeaders`+`FetchBodies` from genesis, does a
minimal root/hash verify, and `InsertBlocks` into the DB — then `erigon seg retire`.
Also, public Chapel peers are flaky/saturated; only a subset accept us. The bring-up flags
(`--p2p.protocol=68 --discovery.v4=true --discovery.v5=false --staticpeers=…`) should become
`--chain=chapel` defaults.

## What was built (Phases 1–2)

- `execution/chain/networkname`: `Chapel = "chapel"`; `execution/chain/spec/network_id.go`: `ChapelChainID = 97`.
- `execution/chain/rules.go`: `ParliaRules = "parlia"` added to `ValidRulesNames`.
- `execution/chain/chain_config.go`: BSC fork fields added as plain `*uint64` so forkid's
  reflection gathers them and they unmarshal from the chainspec.
- `execution/chain`: Parlia is a first-class L1 consensus engine — `ParliaConfig` + a
  `Parlia` field on `chain.Config` (peer to `Aura`/`Bor`), set from the chainspec `"parlia"`
  key; `getEngine()` reports `parlia`.
- New `bsc/chain/` package (mirrors `polygon/chain/`): `chapel.json` chainspec (trimmed —
  no `parlia.blockAlloc`, not needed without execution), `allocs/chapel.json` (verbatim, for
  genesis hash), genesis, and static bootstrap enodes.
- New `bsc/parlia/` package: permissive `rules.Engine` stub (accepts any header, no execution).
- Engine wired into the rules-engine factories as `case *chain.ParliaConfig` (alongside
  Aura/Bor, not merge-wrapped): `CreateRulesEngine`, `CreateRulesEngineBareBones`, backend's
  `rulesConfig` switch, and the integration + rpcdaemon engine switches. Chains registered via
  blank imports at the usual sites.

## Phase 3 — the six wire-level fixes to reach a stable BSC peer

All diagnosed from raw bytes / trace logs against the live Chapel network:

1. **eth/68 not advertised.** Default is `[eth/69,70,71]`; BSC speaks eth/68. → run with
   `--p2p.protocol=68` (should become a BSC default).
2. **Discovery was v5-only.** BSC bootstrap enodes are discv4. → `--discovery.v4=true`.
3. **BSC bootstraps via static peers, not bootnodes** (bsc-erigon's `chapelBootnodes` is empty;
   it ships `chapelStaticPeers`). → `--staticpeers=<enodes>` (should become a BSC default).
4. **`execution/rlp` can't decode `StatusPacket.TD` (`*big.Int`).** `makeDecoder` has a
   `uint256.Int` case but none for `big.Int`, so a `*big.Int` field falls through to the struct
   decoder → "expected input list for big.Int". Dormant upstream because eth/69+ `StatusPacket69`
   has no TD. → changed `StatusPacket.TD` to `*uint256.Int` (wire-identical for non-negative).
   `p2p/protocols/eth/protocol.go`, `p2p/sentry/eth_handshake.go`.
5. **Fork schedule was stale.** bsc-erigon v1.4.3's chapel.json stops at `fermi`; current Chapel
   tips have activated Mendel/Osaka (`1774319400`) and Pasteur (`1784601000`). Missing forks →
   forkid checksum mismatch → EIP-2124 filter rejected the peer ("local incompatible or needs
   update"). → added the two timestamps.
6. **BSC `UpgradeStatusMsg` (0x0b) handshake extension.** After Status, BSC networks
   (56/97/714) exchange an `UpgradeStatusMsg`; without it the peer drops us after ~5s
   (`read timeout`), and our sentry logged "Unknown message code 11". → ported
   `UpgradeStatusMsg`/`UpgradeStatusPacket`/`UpgradeStatusExtension` and the post-Status
   exchange into `handShake` (`p2p/protocols/eth/protocol.go`, `p2p/sentry/eth_handshake.go`).

Result: `[p2p] GoodPeers eth68=1` — a stable BSC Chapel peer.

## Repro (current, flags — to be turned into `--chain=chapel` defaults later)

```
./build/bin/erigon --datadir=<dir> --chain=chapel --http=false \
  --discovery.v4=true --discovery.v5=false --p2p.protocol=68 \
  --staticpeers=<chapel enodes> --nat=none
```
Chapel static enodes: see `bsc/chain/bootnodes.go` / bsc-erigon `chapelStaticPeers`.

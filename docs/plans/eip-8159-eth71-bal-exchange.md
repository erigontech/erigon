# EIP-8159 — eth/71 Block Access List Exchange

**Status:** Plan. No code changes yet on this branch.
**Branch:** `feat/eip-8159-eth71-bal-exchange` off `main`.
**Spec:** https://eips.ethereum.org/EIPS/eip-8159 (Draft, Networking / Standards Track).
**Depends on:** EIP-7928 (BAL generation — already partially in-tree).
**Fork gate:** Amsterdam (`chain.Config.AmsterdamTime`).

## What the EIP adds

1. **Block header field** (already present in the code): `BlockAccessListHash` — `keccak256(rlp.encode(bal))`, sits after `RequestsHash`.
2. **eth wire protocol bump**: `eth/70 → eth/71`.
3. **Two new wire messages**:
   - `GetBlockAccessLists (0x12)`  — request BALs by block hash list
   - `BlockAccessLists    (0x13)`  — response, BAL bytes per requested hash (empty RLP list `0xc0` for unavailable)
4. **Validation**: peers verify received BAL by `keccak256(rlp.encode(bal)) == header.BlockAccessListHash`.

No execution-layer / opcode / gas / consensus changes. Purely p2p distribution.

## What's already in the tree (credit to EIP-7928 prep)

Minimising surprise, these pieces are reusable as-is:

| Piece | Location | Notes |
|---|---|---|
| Header field `BlockAccessListHash *common.Hash` | [execution/types/block.go:110](execution/types/block.go#L110) | Already RLP-encoded conditionally after `RequestsHash` ([:164–170, :316–321](execution/types/block.go#L164)) |
| BAL type + `Hash()` method computing `keccak256(rlp.encode(bal))` | [execution/types/block_access_list.go:827](execution/types/block_access_list.go#L827) | Matches EIP-8159 hash definition exactly |
| BAL rawdb sidecar storage | [db/rawdb/accessors_chain.go:597–612](db/rawdb/accessors_chain.go#L597) — `ReadBlockAccessListBytes` / `WriteBlockAccessListBytes`, table `kv.BlockAccessList` | Already RLP bytes on disk — exactly what we send on the wire |
| Amsterdam fork gate | [execution/chain/chain_config.go:401–403](execution/chain/chain_config.go#L401) — `IsAmsterdam(time)` | No new fork constant needed |
| Known-empty BAL hash constant | [common/empty/empty_hashes.go:49–50](common/empty/empty_hashes.go#L49) | Reusable for validation of empty-list BALs |
| BAL creation on execute | [execution/stagedsync/bal_create.go](execution/stagedsync/bal_create.go) — `CreateBAL`, `ProcessBAL` | Attaches hash validation against `header.BlockAccessListHash` post-Amsterdam |
| Protocol plumbing precedent | `eth/68 → eth/69 → eth/70` upgrades (see below) | Same pattern to replicate |

The networking additions are the only new code.

## Implementation plan — 5 phases, each a landable commit

### Phase 1 — Wire protocol constants + protobuf enum

- [p2p/protocols/eth/protocol.go:37](p2p/protocols/eth/protocol.go#L37): add `ETH71` to `ProtocolToString`, set `ProtocolLengths[ETH71] = 20` (17 → 18 → 18 → 20 to accommodate the 2 new codes).
- [node/direct/sentry_client.go:35–37](node/direct/sentry_client.go#L35): add `ETH71 = 71`.
- [p2p/protocols/eth/protocol.go:53–71](p2p/protocols/eth/protocol.go#L53): add message-code constants:
  ```go
  GetBlockAccessListsMsg = 0x12
  BlockAccessListsMsg    = 0x13
  ```
- [node/interfaces/p2psentry/sentry.proto:73](node/interfaces/p2psentry/sentry.proto#L73): add `GET_BLOCK_ACCESS_LISTS_71 = 42; BLOCK_ACCESS_LISTS_71 = 43;`. Regenerate bindings (`make gen`).
- Unit: lint + build. No functional change yet.

### Phase 2 — Packet types + RLP

Add in `p2p/protocols/eth/protocol.go` alongside `GetBlockBodiesPacket` / `BlockBodiesPacket` (the closest-shaped pair — list of hashes in, list of RLP payloads out):

```go
// GetBlockAccessListsPacket66 requests BALs for the given block hashes.
type GetBlockAccessListsPacket66 struct {
    RequestId uint64
    BlockHashes []common.Hash
}

// BlockAccessListsPacket66 replies with RLP-encoded BALs, positionally aligned
// with the request. An empty RLP list (0xc0) indicates the peer does not have
// that BAL.
type BlockAccessListsPacket66 struct {
    RequestId uint64
    BALs      []rlp.RawValue
}
```

Matches the eth/66 request-id-wrapped envelope used for BlockBodies. We don't wrap the inner BAL bytes again — the on-disk `ReadBlockAccessListBytes` output is already RLP, we hand it through as `RawValue`.

Add `ToProto` / `FromProto` entries for `ETH71` in the same file (big switch tables at :73 and :122) covering all existing eth/70 codes plus the two new ones.

Round-trip serialization test in [p2p/protocols/eth/protocol_test.go](p2p/protocols/eth/protocol_test.go) following the `TestGetBlockHeadersDataEncodeDecode` pattern.

### Phase 3 — Answer handler (server side)

In `p2p/protocols/eth/handlers.go` add:

```go
// AnswerGetBlockAccessListsQuery looks up BAL RLP bytes for each requested
// hash from rawdb. Returns empty RLP list for any hash not in the local store.
// Caller enforces the 2 MiB response-size cap recommended by EIP-8159.
func AnswerGetBlockAccessListsQuery(
    db kv.Getter,
    blockReader services.FullBlockReader,
    query GetBlockAccessListsPacket66,
    softResponseLimit int,
) []rlp.RawValue { … }
```

- Iterate `query.BlockHashes`, resolve block number via `blockReader.HeaderNumber(hash)`.
- Call `rawdb.ReadBlockAccessListBytes(db, hash, num)`.
- If nil → append empty RLP list `[]byte{0xc0}`.
- If size would exceed `softResponseLimit` (default 2 MiB), truncate and stop.

Unit tests in `handlers_test.go`: seed rawdb with a BAL for one hash, leave another unseeded; assert response ordering + empty for the missing one.

### Phase 4 — Sentry dispatch + subscriber plumbing

- [p2p/sentry/sentry_grpc_server.go:450–540](p2p/sentry/sentry_grpc_server.go#L450): extend the inbound switch with:
  ```go
  case eth.GetBlockAccessListsMsg:
      send(eth.ToProto[protocolVersion][msg.Code], peerID, msgBytes)
  case eth.BlockAccessListsMsg:
      send(eth.ToProto[protocolVersion][msg.Code], peerID, msgBytes)
  ```
  Same fan-out pattern as `GetBlockBodiesMsg` / `BlockBodiesMsg`.
- [p2p/sentry/libsentry/protocol.go:25–94](p2p/sentry/libsentry/protocol.go#L25): add `ETH71` to `ethProtocolsByVersion`, extend the per-protocol `ProtoIds` whitelist with the two new MessageIds.
- Negotiation: `MinProtocol(GET_BLOCK_ACCESS_LISTS_71)` must return `ETH71` so only eth/71 peers are queried.

### Phase 5 — Consumer side: BAL fetcher for sync

This is the substantial, net-new piece. The goal: during sync, fill the `kv.BlockAccessList` table for blocks whose `header.BlockAccessListHash != nil` and that we don't already have a BAL for.

- New `p2p/download/bal_fetcher.go` (mirroring the bodies fetcher). Consumes from the sentry Messages stream subscribed to `BLOCK_ACCESS_LISTS_71`, validates `keccak256(rlp.encode(payload)) == header.BlockAccessListHash`, writes via `rawdb.WriteBlockAccessListBytes`.
- Stage hook: post-headers, post-bodies, a `[N/M DownloadBALs]` stage (or attach to the existing bodies stage) that queues missing BAL hashes and waits on the fetcher.
- Eviction: deferred — EIP-7928 defines BAL pruning separately; out of scope here.

Decision point: whether to make BAL fetching a first-class blocking stage or an opportunistic background fetch. Recommend **background**: stage_exec already validates BAL locally via `ProcessBAL`, so missing-peer BALs are self-healing via local regeneration. p2p BAL is an optimisation for nodes that want to skip the recompute cost.

**Validation lives in the p2p layer (not a higher layer callback).** Follow the existing pattern where BlockBodies / Receipts validation runs inside the fetcher against header data it already has in scope. The fetcher pre-loads `header.BlockAccessListHash` for each pending request hash, and on each inbound `BlockAccessLists` response validates `keccak256(payload) == expected_hash` inline — **before** dispatching to the rawdb writer. Non-matching payloads never cross the p2p boundary.

**Empty-RLP ambiguity** (`0xc0` has two meanings on the wire):

- `0xc0` means *"peer does not have this BAL"* when `expected_hash != empty.BlockAccessListHash`.
- `0xc0` means *"block genuinely has an empty BAL"* when `expected_hash == empty.BlockAccessListHash` (the keccak256 of the empty RLP list, `0x1dcc4de8...`; already exported at [common/empty/empty_hashes.go:49](common/empty/empty_hashes.go#L49)).

The fetcher always disambiguates against the expected hash, never by inspecting the payload alone. Accept `0xc0` only when the hashes match.

**Bad-peer management** — two distinct penalty tracks layered on top of the per-peer request-rate limits:

1. **Garbage / wrong hash.** Any non-`0xc0` payload whose `keccak256` does not equal the expected hash → immediate disconnect via `Sentry.PenalizePeer`. No ambiguity: the peer is corrupt or malicious.
2. **Silent withholding.** A peer that consistently returns `0xc0` for BAL hashes we know to be non-empty is DoS-ing the stream without sending garbage. Maintain a per-peer score that decrements on each confirmed-withholding reply (expected ≠ empty-list hash but peer returned `0xc0`) and increments on each valid answer; drop and temporarily ban below a threshold (N withholdings in a rolling window, or an absolute ratio — tune against the existing body-fetcher thresholds for parity). Same `Sentry.PenalizePeer` path so the usual peer telemetry and reconnection logic applies.

Unit tests in the fetcher cover: valid full BAL accepted, valid empty BAL accepted (expected hash = empty-list hash), mismatched-hash → disconnect, and repeated-`0xc0`-for-non-empty-expected → ban. Fetcher should emit metrics for each case so the behaviour is observable in production.

### Phase 6 — Hive / integration tests

- Hive EEST: verify eth/71 handshake negotiation.
- Add a fixture block with a non-trivial BAL, exercise round-trip: peer A writes BAL, peer B sends `GetBlockAccessLists`, validates hash match.
- CI entry under `.github/workflows/test-hive.yml` if the hive fixtures for eth/71 are added upstream; otherwise defer until they land.

## Test plan per phase

Before merging each phase:
- `make lint`
- `make erigon`
- `go test -short ./p2p/... ./execution/types/... ./db/rawdb/...`
- For phase 4+: run `mainnet-rpc-integ-tests` and `race-tests / tests-linux` on the PR.
- Final: bal-devnet-2 integration (the existing skill `launch-bal-devnet-2`) with eth/71 enabled on both endpoints — verify BALs propagate over the wire rather than being generated locally only.

## Risks & open questions

1. **Amsterdam timing**: the Amsterdam activation timestamp on mainnet is not yet set (field is `*uint64`, nil on mainnet today). eth/71 must negotiate regardless of fork activation (peers can request BALs for historic blocks after the fork activates). No fork-gate on the wire protocol version itself — only on the header field's required-ness.
2. **Empty-list semantics**: EIP spec says empty RLP list = "not available". Our implementation also produces `0xc0` when `ReadBlockAccessListBytes` returns nil. Need to distinguish "peer genuinely has no BAL for this hash" from "BAL is empty because the block had zero state accesses" — the latter is a valid hash (`empty.BlockAccessListHash`). Callers must compare against the header's expected hash.
3. **Ordering**: spec says response array aligns positionally with request. Handler must preserve request order even when some entries are missing. Phase 3 enforces this.
4. **Response limit**: EIP recommends 2 MiB per message. If a single BAL exceeds 2 MiB we return empty for it (peer will fall back to regenerate). Phase 3 handles this.
5. **DoS**: large `GetBlockAccessLists` requests. Enforce per-peer request-rate and max-hashes-per-message limits (follow `BlockBodiesMsg` precedent — check its rate limit code path).
6. **Pectra / Fusaka coexistence**: eth/71 must be backwards-compatible with older peers. Protocol negotiation already supports this in `libsentry/protocol.go` — new code paths must be gated on negotiated protocol version, not on header presence.

## Out of scope for this feature branch

- EIP-7928 improvements (BAL generation changes) — already tracked elsewhere.
- BAL pruning / retention policy — separate EIP/work.
- Consensus-layer changes in Caplin — EIP-8159 is EL-only.
- RPC-level `debug_getBlockAccessList` (if wanted, separate endpoint addition).

## Suggested PR stacking

1. **PR 1** — Phases 1 + 2 (constants, packet types, serialization tests). Small, easily reviewed.
2. **PR 2** — Phases 3 + 4 (handler + sentry wiring + libsentry negotiation). Mid-sized.
3. **PR 3** — Phase 5 (consumer-side fetcher + stage integration). Largest, most review-worthy; depends on PR 2.
4. **PR 4** — Phase 6 (hive tests, devnet verification). Independent; can land any time after PR 3.

Each PR independently revertible. Total estimated diff ~500 LOC code + ~300 LOC tests.

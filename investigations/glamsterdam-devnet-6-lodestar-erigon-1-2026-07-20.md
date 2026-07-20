# Investigation: `lodestar-erigon-1` on `glamsterdam-devnet-6`

Date: 2026-07-20

Times: UTC unless stated otherwise

Method: Panda `investigate` remote-devnet runbook

## Summary

`lodestar-erigon-1` is not offline. Its Lodestar consensus client is trapped in
range sync by a malformed Gloas execution-payload envelope whose
`block_access_list` is empty (`0x`) even though the canonical block has a
non-empty EIP-7928 block access list.

The incident began immediately after Watchtower restarted only the Lodestar
beacon and validator containers on 2026-07-08. Lodestar resumed from a database
state about 900 slots behind the wall clock and entered range sync. At beacon
slot 96096 it sent Erigon `engine_newPayloadV5` with an empty BAL for canonical
execution block 72995. Erigon reconstructed a different header hash and rejected
the payload. The hash difference is exactly reproducible by replacing the
canonical BAL hash with Erigon's empty-list BAL hash.

The deployed Lodestar branch retains an unverified bad payload envelope in its
seen cache after validation fails. Each retry therefore reuses the same bad
bytes. Later Lodestar restarts cleared the in-memory poison and let the node
advance in steps, first to slot 98912 and then to slot 124320, where it is
stuck again on the same failure class. A later Lodestar commit explicitly adds
the missing behavior: detach invalid unverified envelopes so another peer can
supply fresh bytes. That commit is not in the deployed
`glamsterdam-devnet-6` branch.

The immediate failure and persistent retry loop are proven. The peer or storage
path that originally supplied each empty BAL is not identified by the deployed
build's logs, so deeper origin attribution remains open.

## Target identity

The inventory has no `erigon-lodestart-1`. The exact pair is
`lodestar-erigon-1`:

```bash
curl -fsSL https://config.glamsterdam-devnet-6.ethpandaops.io/api/v1/nodes/inventory |
  jq '.ethereum_pairs["lodestar-erigon-1"]'
```

```text
consensus.client = lodestar
consensus.image  = ethpandaops/lodestar:glamsterdam-devnet-6
execution.client = erigon
execution.image  = ethpandaops/erigon:glamsterdam-devnet-6
```

The image tags are mutable. Runtime log commits, not the tag names, were used
for source matching.

## Health baseline

At the final sample, the network was synced and finalizing:

```bash
panda dora overview glamsterdam-devnet-6 -o json
```

```text
current_slot=180423 current_epoch=5638
finalized_epoch=5636 epochs_since_finality=2 finalizing=true is_synced=true
```

Dora warned that its participation aggregation was incomplete, so participation
was not used to judge health. Checkpoint progression was used instead.

The target was about 56,100 slots behind:

```bash
panda ethnode syncing glamsterdam-devnet-6 lodestar-erigon-1 -o json
panda ethnode finality glamsterdam-devnet-6 lodestar-erigon-1 -o json
panda ethnode block-number glamsterdam-devnet-6 lodestar-erigon-1 -o json
```

```text
head_slot=124320 is_syncing=true sync_distance=56103 el_offline=false
finalized_epoch=3883 current_justified_epoch=3884
execution_block=96111 (0x1776f)
```

The target's consensus peer count fluctuated between zero and three during the
investigation. Erigon itself was alive and well-peered:

```sql
SELECT Timestamp, Body
FROM external.otel_logs
WHERE Timestamp >= now64(9) - INTERVAL 1 HOUR
  AND ResourceAttributes['network'] = 'glamsterdam-devnet-6'
  AND ResourceAttributes['host.name'] = 'lodestar-erigon-1'
  AND ResourceAttributes['service.name'] = 'execution'
  AND positionCaseInsensitive(Body, 'GoodPeers') > 0
ORDER BY Timestamp DESC
LIMIT 1
```

```text
[p2p] GoodPeers eth69=1 eth71=34
```

Same-client controls were healthy:

```bash
panda ethnode syncing glamsterdam-devnet-6 lodestar-geth-1 -o json
panda ethnode syncing glamsterdam-devnet-6 lighthouse-erigon-1 -o json
```

```text
lodestar-geth-1:    head_slot=180455 is_syncing=false sync_distance=0
lighthouse-erigon-1: head_slot=180455 is_syncing=false sync_distance=0
```

This excludes a network-wide outage, a general Lodestar outage, and a general
Erigon outage.

## Active protocol model

The live configuration is Gloas from epoch 30:

```bash
panda ethnode beacon-get glamsterdam-devnet-6 lodestar-geth-1 \
  /eth/v1/config/spec -o json |
  jq '.data | {SECONDS_PER_SLOT, SLOTS_PER_EPOCH, GLOAS_FORK_EPOCH, GLOAS_FORK_VERSION}'
```

```json
{
  "SECONDS_PER_SLOT": "12",
  "SLOTS_PER_EPOCH": "32",
  "GLOAS_FORK_EPOCH": "30",
  "GLOAS_FORK_VERSION": "0x80435048"
}
```

Both blocked slots, 96096 (epoch 3003) and 124320 (epoch 3885), are far beyond
Gloas activation. In this branch, EIP-7928 places
`block_access_list_hash = keccak256(rlp(block_access_list))` in the execution
header and transmits the full BAL in the Gloas `ExecutionPayload` sent through
`engine_newPayloadV5`.

## Matched symptom branch

Runbook branch: **one node stuck / syncing while the network remains healthy**.

The failure localizes to the CL-to-EL Engine API boundary:

```text
range-sync payload envelope
  -> Lodestar cached ExecutionPayload.blockAccessList
  -> engine_newPayloadV5
  -> Erigon EIP-7928 decode/hash validation
  -> rejection
  -> Lodestar cannot import the payload or advance range sync
```

## Timeline

Immediately before the update, Lodestar was near the network head:

```text
2026-07-08 21:46:06
Synced - slot: 96680 - head: (slot -25) ... finalized: ...:3018 - peers: 30
```

Watchtower then replaced only the two Lodestar containers:

```sql
SELECT Timestamp, Body, ResourceAttributes['service.name'] AS service
FROM external.otel_logs
WHERE Timestamp >= toDateTime64('2026-07-08 21:45:50', 9, 'UTC')
  AND Timestamp <  toDateTime64('2026-07-08 21:46:30', 9, 'UTC')
  AND ResourceAttributes['network'] = 'glamsterdam-devnet-6'
  AND ResourceAttributes['host.name'] = 'lodestar-erigon-1'
  AND (
    positionCaseInsensitive(Body, 'Started new container') > 0 OR
    positionCaseInsensitive(Body, 'Update session completed') > 0 OR
    positionCaseInsensitive(Body, 'Lodestar network=') > 0 OR
    positionCaseInsensitive(Body, 'Initializing beacon') > 0
  )
ORDER BY Timestamp
```

```text
21:46:18 Started new container container=validator image=ethpandaops/lodestar:glamsterdam-devnet-6
21:46:18 Started new container container=beacon    image=ethpandaops/lodestar:glamsterdam-devnet-6
21:46:18 Update session completed failed=0 scanned=4 updated=2
21:46:21 Lodestar version=v1.43.0/30770cd commit=30770cd658ae36bcb59c64ebf7b352fbed39d172
21:46:22 Initializing beacon from a valid db state slot=95776 epoch=2993
```

The Erigon container was not replaced. Its incident build was:

```text
git_commit=ab552a3882bb41d2f8f84ee26ecb67a8d62b7739
```

Lodestar made rapid range-sync progress until the first hard failure:

```text
21:47:30 Syncing ... head: (slot -624) ... exec-block: valid(72970) ... peers: 2
21:47:32 [NewPayload] invalid block hash
  stated=0x8eb5ff9ea81cff9b2e5b980019dfdf3d3140b749071ccd072027e204225e53d1
  actual=0xc3b1774ca2118c3eed6c8ce1070d22c49e5baed3aab202280adcf6559d0f85c7
21:47:32 Execution client is syncing oldState=SYNCED, newState=SYNCING
21:47:36 Node is syncing ... headSlot=96096
```

The target produced 5,425 such invalid-block-hash errors between the first
failure and 2026-07-15. Other hosts produced at most 21 in the same interval:

```sql
SELECT ResourceAttributes['host.name'] AS host, count() AS errors,
       min(Timestamp) AS first_seen, max(Timestamp) AS last_seen
FROM external.otel_logs
WHERE Timestamp >= toDateTime64('2026-07-08 00:00:00', 9, 'UTC')
  AND Timestamp <  toDateTime64('2026-07-16 00:00:00', 9, 'UTC')
  AND ResourceAttributes['network'] = 'glamsterdam-devnet-6'
  AND positionCaseInsensitive(Body, '[NewPayload] invalid block hash') > 0
GROUP BY host
ORDER BY errors DESC
```

```text
lodestar-erigon-1  5425
prysm-erigon-1       21
grandine-erigon-1    15
lighthouse-erigon-1   3
nimbus-erigon-1       3
```

The daily head plateaus correlate with later Lodestar restarts:

```sql
SELECT toDate(Timestamp) AS day,
       max(toUInt64OrZero(extract(Body, 'headSlot=([0-9]+)'))) AS max_head_slot
FROM external.otel_logs
WHERE Timestamp >= toDateTime64('2026-07-08 00:00:00', 9, 'UTC')
  AND Timestamp <  toDateTime64('2026-07-17 00:00:00', 9, 'UTC')
  AND ResourceAttributes['network'] = 'glamsterdam-devnet-6'
  AND ResourceAttributes['host.name'] = 'lodestar-erigon-1'
  AND position(Body, 'headSlot=') > 0
GROUP BY day
ORDER BY day
```

```text
2026-07-08..12  96096
2026-07-13      98912
2026-07-14..16 124320
```

Lodestar restarted on July 13 and three times on July 14. Clearing process
memory explains why each restart could discard one poisoned cache entry, advance,
and then become stuck on another malformed envelope.

## Exact artifact proof

### First failure: slot 96096 / execution block 72995

The canonical block is
`0x8eb5ff9ea81cff9b2e5b980019dfdf3d3140b749071ccd072027e204225e53d1`.
Its raw header contains the non-empty BAL hash
`0xe54755a58e4e1aa9b695a28f3960836039c6c0d6568ab9bcac92b3b52503d7d0`.

The incident Erigon code at
`ab552a3882bb41d2f8f84ee26ecb67a8d62b7739` treats a zero-length
`blockAccessList` as the empty BAL and inserts
`0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347`
into the reconstructed header.

Reproduction:

```bash
panda ethnode exec-rpc glamsterdam-devnet-6 lodestar-erigon-1 \
  debug_getRawHeader '["0x11d23"]' -o json |
  jq -r '.result' |
  go run /private/tmp/recompute_bal_header_hash.go
```

```text
0xc3b1774ca2118c3eed6c8ce1070d22c49e5baed3aab202280adcf6559d0f85c7
```

That exactly equals Erigon's logged `actual` hash. This is direct proof that
the incident request used the empty BAL hash rather than the canonical BAL
hash. It is not evidence of a bad canonical block.

### Current failure: slot 124320 / execution block 96112

The target currently exposes the malformed envelope directly:

```bash
panda ethnode beacon-get glamsterdam-devnet-6 lodestar-erigon-1 \
  /eth/v1/beacon/execution_payload_envelopes/124320 -o json |
  jq '{block_hash:.data.message.payload.block_hash,
       bal_chars:(.data.message.payload.block_access_list|length),
       signature:.data.signature,
       finalized}'
```

```text
block_hash = 0xb7ee09c3a97a6f480e101b6b6ce747d199dfd5dac52393c6ccc9f1ff5730c36b
bal_chars  = 2
BAL        = 0x
signature  = 0xb5eb0c...4134dcc
finalized  = false
```

A healthy Lodestar serves the same block hash and signature with a full BAL:

```bash
panda ethnode beacon-get glamsterdam-devnet-6 lodestar-geth-1 \
  /eth/v1/beacon/execution_payload_envelopes/124320 -o json |
  jq '{block_hash:.data.message.payload.block_hash,
       bal_chars:(.data.message.payload.block_access_list|length),
       signature:.data.signature,
       finalized}'
```

```text
block_hash = 0xb7ee09c3a97a6f480e101b6b6ce747d199dfd5dac52393c6ccc9f1ff5730c36b
bal_chars  = 63472
signature  = 0xb5eb0c...4134dcc
finalized  = true
```

Lighthouse, Nimbus, Prysm, and four other Lodestar controls also served 63,472
hex characters for this envelope. The target is the outlier.

The healthy canonical EL reports BAL hash
`0x9d4154619daf5870f3ae89ab6f678e1a29a34261b6f2206f3f05c924f44e19f4`
for execution block 96112. The target Erigon has only block 96111 and returns
`null` for block 96112.

Newer Erigon build
`2d6f811e3a8075964c8d9091eb11ab8b79b91ba6` fully decodes the supplied
BAL before hashing it. The same empty input now fails earlier:

```text
engine_newPayloadV5 err="undecodable blockAccessList: EOF"
```

Only `lodestar-erigon-1` produced this error: 3,913 occurrences from
2026-07-15 13:54 through the final sample. The changed error text reflects
stricter Erigon validation; it is the same malformed-input class.

## Exact source trace

Runtime commits:

```text
incident Lodestar: 30770cd658ae36bcb59c64ebf7b352fbed39d172
current Lodestar:  00d85c1d98ec93cf6d56af831868f8666a3add69
incident Erigon:   ab552a3882bb41d2f8f84ee26ecb67a8d62b7739
current Erigon:    2d6f811e3a8075964c8d9091eb11ab8b79b91ba6
```

At the exact Lodestar incident commit:

1. `requestByRange` obtains `ExecutionPayloadEnvelopesByRange` from a peer.
2. `validateEnvelopesByRangeResponse` checks that the envelope's
   `beaconBlockRoot` matches the batch block. It does not validate the BAL
   against the execution block hash.
3. `cacheByRangeResponses` attaches the peer envelope to
   `seenPayloadEnvelopeInputCache` only when the cache entry has no envelope.
4. `importExecutionPayload` passes `envelope.payload` to
   `notifyNewPayload`.
5. `serializeExecutionPayload` copies `blockAccessList` verbatim and sends
   `engine_newPayloadV5`.
6. On failure, the runtime code throws but does not detach the bad envelope.
   The `if (!payloadInput.hasPayloadEnvelope())` guard then prevents a later
   download from replacing it.

Re-derive the path:

```bash
git -C /private/tmp/lodestar-source show \
  30770cd658ae36bcb59c64ebf7b352fbed39d172:packages/beacon-node/src/sync/utils/downloadByRange.ts
git -C /private/tmp/lodestar-source show \
  30770cd658ae36bcb59c64ebf7b352fbed39d172:packages/beacon-node/src/chain/blocks/importExecutionPayload.ts
git -C /private/tmp/lodestar-source show \
  30770cd658ae36bcb59c64ebf7b352fbed39d172:packages/beacon-node/src/execution/engine/types.ts
git -C /private/tmp/lodestar-source show \
  30770cd658ae36bcb59c64ebf7b352fbed39d172:packages/beacon-node/src/execution/engine/http.ts
```

Lodestar commit
`16a59730954c9f238f1dbea17ee19e4949afe367`, authored on July 15, adds
exactly the missing recovery path:

```text
On any verification failure, detach the envelope so a retry can attach fresh
bytes from a different peer instead of re-verifying the same cached bad bytes
forever.
```

Its implementation calls:

```text
seenPayloadEnvelopeInputCache.removeUnverifiedPayloadEnvelope(blockRootHex)
```

The commit is not an ancestor of deployed Lodestar `00d85c1`; it exists only
on `origin/te/unstable_evict_payload` in the inspected repository.

The July 8 `30770cd` update itself only changes client-info graffiti. It
triggered the problem by restarting a node into range sync; it did not
introduce the BAL/cache logic.

## Reachability verdict

```yaml
trace:
  summary: >
    A Lodestar-only restart put the target into Gloas range sync. The target
    cached an envelope with an empty BAL, serialized it verbatim into
    engine_newPayloadV5, and Erigon rejected it. The deployed Lodestar code
    retained that unverified envelope, so every retry reused the poisoned
    bytes. The immediate path is fully reachable; the upstream producer of
    the empty envelope is not identified.
  verdict: partially-reachable
  scope:
    client_specific: true
    pair_specific: false
    fork_specific: true
    topology_dependent: true
    network_wide: false
  paths:
    - candidate: "bad unverified Lodestar payload envelope retained in seen cache"
      chain:
        - "Watchtower Lodestar-only restart"
        - "Lodestar Gloas range sync"
        - "empty-BAL envelope attached to seen cache"
        - "Lodestar engine_newPayloadV5"
        - "Erigon BAL/hash rejection"
        - "range-sync head plateau"
      edge_evidence:
        - direct
        - direct
        - direct
        - direct
        - direct
      reachability: reachable
    - candidate: "specific upstream peer supplied the empty BAL"
      edge_evidence:
        - topology-only
      reachability: partially-reachable
  roles:
    trigger:
      - "Watchtower restart"
    carrier:
      - "Lodestar range-sync and seen-envelope cache"
    validator:
      - "Erigon EIP-7928/newPayload validation"
    victim:
      - "lodestar-erigon-1 sync progress"
  confidence:
    immediate_failure: high
    persistent_retry_loop: high
    upstream_empty_envelope_source: medium
```

The top-level verdict is capped at `partially-reachable` because the deeper
origin of the empty envelope has one missing edge.

## Likely scope, layer, and actor

- **Observed scope:** one node instance. Same-Lodestar and same-Erigon controls
  are healthy.
- **Layer:** Gloas CL payload-envelope acquisition/cache and the CL-to-EL Engine
  API boundary.
- **Primary failing behavior:** Lodestar retains and retries an invalid
  unverified envelope.
- **Correct rejecting actor:** Erigon detects that the supplied BAL cannot
  produce the stated canonical block.
- **Trigger:** a Lodestar-only automated restart into stale-state range sync.
- **Unknown actor:** the peer or persistence path that first supplied each
  empty BAL.

## Affected components

- Lodestar range sync
- Lodestar `SeenPayloadEnvelopeInput` cache
- Gloas `ExecutionPayloadEnvelopesByRange`
- Engine API `engine_newPayloadV5`
- Erigon EIP-7928 BAL validation
- The target validator process, as downstream fallout from a syncing beacon
  node

Erigon's transient `Too deep reorg` responses immediately after the stale
restart were secondary. Lodestar continued advancing hundreds of slots after
those responses and stopped precisely at the first empty-BAL block.

The post-update Lodestar peer-discovery `TypeError: ... includes` was also
secondary: it appeared on multiple Lodestar nodes, and the current
`00d85c1` build contains the peer-discovery initialization fix. Healthy
Lodestar controls recovered; the BAL/cache poison remained unique to this
target.

## Rejected alternatives

- **Network finality incident:** rejected by live finality and healthy control
  heads.
- **Erigon process or EL-network outage:** rejected by `el_offline=false`,
  execution height 96111, and 34 `eth71` peers.
- **Bad canonical execution block:** rejected by healthy clients agreeing on
  the canonical block and full BAL, plus the exact empty-hash reconstruction.
- **General Erigon incompatibility:** rejected by healthy
  `lighthouse-erigon-1`.
- **General Lodestar incompatibility:** rejected by healthy
  `lodestar-geth-1` and other Lodestar controls serving the full envelope.
- **The `30770cd` graffiti commit introduced the bug:** rejected by its source
  diff; the restart exposed existing logic.

## Source disagreements and observability limits

- The requested name `erigon-lodestart-1` disagreed with inventory; the exact
  inventory actor `lodestar-erigon-1` was used.
- Panda's public-network context derived Gloas wall time from
  `MIN_GENESIS_TIME`, 60 seconds before the live genesis. Live beacon genesis
  and Dora agree on Gloas activation time `1782398520`; this does not affect
  the fork/slot diagnosis.
- Dora's participation aggregate was incomplete and was excluded.
- Tracoor returned zero captured bad blocks/blobs in the pinned first-failure
  window. This is treated as a capture gap, not proof of absence, because both
  clients directly logged the request/rejection.
- The deployed Lodestar build does not expose the cached envelope's original
  `sourceMeta.peerIdStr`. Its live sync debug state currently lists
  `prysm-erigon-1` as a failed download peer, but that does not prove Prysm
  supplied the original poisoned cache entry.

## Confidence

- **High:** node health and scope.
- **High:** exact empty-BAL cause of the first block-hash mismatch.
- **High:** current target envelope is empty while canonical controls are full.
- **High:** missing Lodestar cache eviction explains persistent retries and
  restart-driven stepwise progress.
- **Medium:** the original upstream source of each empty envelope.

## Next distinguishing query

Capture one `ExecutionPayloadEnvelopesByRange` response before it is attached
to the target cache, logging:

```text
peer_id, beacon_block_root, slot_number, block_hash, len(block_access_list),
envelope_signature
```

Then compare the response against the canonical 63,472-character BAL for slot
124320. Alternatively, run a diagnostic Lodestar build containing
`16a59730954c9f238f1dbea17ee19e4949afe367`; its
`removeUnverifiedPayloadEnvelope` verbose log records the original source and
peer when it detaches the bad entry. Either observation would name or reject
the upstream peer without changing the already-established immediate cause.

## Operational implication

A plain Lodestar restart can clear the current in-memory poison and may let the
node advance, as happened on July 13 and July 14, but it is not a durable fix.
The node can become stuck again on the next malformed envelope until invalid
unverified envelopes are evicted and replaced with fresh peer data.

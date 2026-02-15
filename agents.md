# Implementing the "gloas" hard fork in Erigon's Caplin (CL)

You are implementing the "gloas" Ethereum consensus hard fork. The previous fork is "fulu".
Consensus specs (read-only reference): /tmp/consensus-specs/specs

## Spec files to read (if present for this fork)

Check `/tmp/consensus-specs/specs/gloas/` for ALL of these — each maps to Erigon code:

| Spec file | What it changes in Erigon |
|-----------|--------------------------|
| `beacon-chain.md` | Types, state, block/epoch processing (`cl/cltypes/`, `cl/phase1/core/state/`, `cl/transition/`) |
| `fork.md` | Upgrade function (`cl/phase1/core/state/upgrade.go`) |
| `p2p-interface.md` | Gossip topics, message types, networking (`cl/sentinel/`, `cl/phase1/network/`) |
| `validator.md` | Validator duties, attestation, block proposal (`cl/validator/`, `cl/beacon/handler/`) |
| `builder.md` | Builder API, MEV-Boost changes (`cl/beacon/handler/builder.go`, `cl/beacon/builder/`) |

Do NOT skip p2p-interface.md, validator.md, or builder.md — they contain critical fork-specific changes.

## Erigon CL architecture

| Area | Key paths |
|------|-----------|
| Version constants | `cl/clparams/version.go` (Phase0Version=0..GloasVersion=7), `cl/clparams/config.go` |
| Types | `cl/cltypes/` (BeaconBlock, BeaconBody, etc.), `cl/cltypes/solid/` (optimized SSZ) |
| State & upgrades | `cl/phase1/core/state/` (getters/setters), `cl/phase1/core/state/upgrade.go` |
| Transition | `cl/transition/impl/eth2/operations.go` (block ops), `cl/transition/impl/eth2/statechange/` (epoch) |
| Block production | `cl/beacon/handler/block_production.go` (version gates), `cl/beacon/handler/builder.go` |
| Antiquary | `cl/antiquary/state_antiquary.go`, `cl/antiquary/beacon_states_collector.go` |
| Persistence | `cl/persistence/state/slot_data.go`, `cl/persistence/state/epoch_data.go` |
| State reconstruction | `cl/persistence/state/historical_states_reader/` |
| Snapshots | `db/snapshotsync/caplin_state_snapshots.go` |
| P2P / networking | `cl/sentinel/` (gossip, message types), `cl/phase1/network/` |
| Validator | `cl/validator/` (attestation producer, duties) |
| Builder / MEV | `cl/beacon/handler/builder.go`, `cl/beacon/builder/` |
| Spec tests | `cl/spectest/` (Makefile, spectest/suite.go, consensus_tests/) |

## Spec test enablement — 6 layers (ALL required)

1. **Makefile** (`cl/spectest/Makefile`): Remove `rm -rf tests/mainnet/gloas` line
2. **suite.go** (`cl/spectest/spectest/suite.go:~43`): Remove from runtime skip list if present
3. **forks.go** (`cl/spectest/consensus_tests/forks.go`): Add upgrade switch case
4. **transition.go** (`cl/spectest/consensus_tests/transition.go`): Add fork epoch switch case
5. **appendix.go**: Register new SSZ types with `runAfterVersion(clparams.GloasVersion)`
6. **appendix.go**: Register new operation/epoch processing handlers

## Antiquary — 7-step pattern for new fork fields

1. State upgrade in `upgrade.go` — initialize new fields
2. Collector init in `beacon_states_collector.go` — `etl.NewCollector` for new DB table
3. Version-gated collection in `state_antiquary.go` — `if version >= clparams.GloasVersion`
4. Collection method in `beacon_states_collector.go` — `collectNewField()`
5. Flush in `beacon_states_collector.go:flush()` — `Load()` call
6. Schema in `slot_data.go` / `epoch_data.go` — version-gated `getSchema()` field
7. Reconstruction in `historical_states_reader/` — version-gated read

## Hard rules

- **Backward compatible**: NEVER modify behavior for forks before gloas. ALL new logic behind version guards.
- **finality + random** spec test categories are the most complex — work on them LAST, after everything else passes.
- **`TestStateAntiquaryGloas`** MUST exist and PASS (in `cl/antiquary/state_antiquary_test.go`).
- **`caplin_state_snapshots.go`** must compile with the new fork.
- SSZ field order MUST match the spec exactly.
- Follow existing code patterns (study "fulu" as template).
- Use `spectest.UnimplementedHandler` for test categories that aren't implementable yet.

## Test commands

```bash
# Spec tests — fast first, finality+random last
cd cl/spectest && CGO_CFLAGS=-D__BLST_PORTABLE__ go test -tags=spectest -run=/mainnet/gloas/ -skip=/(finality|random)/ -v --timeout 30m
cd cl/spectest && CGO_CFLAGS=-D__BLST_PORTABLE__ go test -tags=spectest -run=/mainnet/gloas/(finality|random)/ -v --timeout 30m

# Antiquary/persistence tests
CGO_CFLAGS=-D__BLST_PORTABLE__ go test -v ./cl/antiquary/... ./cl/persistence/state/... ./cl/persistence/base_encoding/... ./cl/persistence/format/snapshot_format/... ./cl/phase1/core/state/... --timeout 10m

# Compile check
go build ./cl/...

# Full backward compat check (all forks)
cd cl/spectest && CGO_CFLAGS=-D__BLST_PORTABLE__ go test -tags=spectest -run=/mainnet/ -skip=/(finality|random)/ -v --timeout 30m
```

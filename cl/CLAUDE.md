# Caplin Spec Conformance Review Rules

This directory contains consensus-critical Caplin code. Review changes against
the upstream Ethereum consensus specifications, not only against local tests.

Primary references:

- Consensus specs repository: https://github.com/ethereum/consensus-specs
- Phase0 fork choice: https://ethereum.github.io/consensus-specs/specs/phase0/fork-choice/
- Bellatrix fork choice: https://ethereum.github.io/consensus-specs/specs/bellatrix/fork-choice/
- Deneb fork choice: https://ethereum.github.io/consensus-specs/specs/deneb/fork-choice/
- Electra fork choice changes are inherited through Bellatrix/Deneb `on_block`
  execution-request plumbing and beacon-chain state transition rules.
- Fulu fork choice: https://ethereum.github.io/consensus-specs/specs/fulu/fork-choice/
- Gloas fork choice: https://ethereum.github.io/consensus-specs/specs/gloas/fork-choice/

## Forkchoice Spec Map

Use this map when reviewing `cl/phase1/forkchoice/`. Function names below are
verified against the Go code in this repository.

| Go file/function | Spec section |
| --- | --- |
| `forkchoice.go`: `NewForkChoiceStore` | Phase0 `get_forkchoice_store`; Gloas `get_forkchoice_store` and extended `Store` fields |
| `forkchoice.go`: `GetRecentExecutionPayloadStatus`, `IsBlobDataAvailable`, `HasEnvelope`, `ReadEnvelopeFromDisk`, pending EL payload helpers | Gloas `Store`, `on_execution_payload_envelope`; Gloas `Store.payload_states`, `Store.payloads`; Gloas payload-attestation API support; Fulu data-column availability support |
| `types.go`: `LatestMessage`, `ForkChoiceNode` | Phase0 `LatestMessage`; Gloas modified `LatestMessage`, `ForkChoiceNode` |
| `get_head.go`: `accountWeights`, `computeVotes`, `getHead`, `GetHead` | Phase0 `get_attestation_score`, `get_weight`, `get_head`; Gloas dispatch boundary for modified `get_head` |
| `get_head.go`: `getHeadGloas` | Gloas `get_head`, `get_node_children`, `get_weight`, `get_payload_status_tiebreaker` |
| `get_head.go`: `getFilteredBlockTree`, `getFilterBlockTree` | Phase0 `get_filtered_block_tree`, `filter_block_tree`, `get_voting_source`; Gloas inherits filtered-tree viability |
| `weight_store.go`, `weight_store_indexed.go`: `GetWeight`, `GetAttestationScore`, `GetProposerScore`, `ShouldApplyProposerBoost`, `IndexVote`, `RemoveVote` | Phase0 `get_weight`, `get_attestation_score`, `get_proposer_score`; Gloas modified `get_weight`, `get_attestation_score`, `should_apply_proposer_boost`, `LatestMessage` indexing |
| `payload_vote.go`: `notifyPtcMessages`, `applyPayloadAttestationVote` | Gloas `notify_ptc_messages`, `on_payload_attestation_message` vote application |
| `payload_vote.go`: `isPayloadTimely`, `isPayloadDataAvailable` | Gloas `is_payload_timely`, `is_payload_data_available`; requires local envelope availability plus independent PTC majorities |
| `payload_vote.go`: `getParentPayloadStatus`, `isParentNodeFull`, `isSupportingVote` | Gloas `get_parent_payload_status`, `is_parent_node_full`, `is_supporting_vote` |
| `payload_vote.go`: `ShouldExtendPayload`, `getPayloadStatusTiebreaker`, `getNodeChildren`, `validateParentPayloadPath` | Gloas `should_extend_payload`, `get_payload_status_tiebreaker`, `get_node_children`, modified `on_block` parent payload path checks |
| `timing.go`: `getAttestationDueMs`, `getAggregateDueMs`, `getSyncMessageDueMs`, `getContributionDueMs`, `getPayloadAttestationDueMs` | Phase0 timing helpers; Gloas modified timing helpers and `get_payload_attestation_due_ms` |
| `timing.go`: `recordBlockTimeliness`, `updateProposerBoostRoot`, `shouldApplyProposerBoostGloas` | Phase0 `record_block_timeliness`, `update_proposer_boost_root`; Gloas modified `record_block_timeliness`, `update_proposer_boost_root`, `should_apply_proposer_boost` |
| `timing.go`: `isHeadLate`, `isHeadWeak`, `isParentStrong` | Phase0 proposer reorg helpers; Gloas modified `is_head_late`, `is_head_weak`, `is_parent_strong` |
| `on_tick.go`: `OnTick`, `onTickPerSlot` | Phase0 `on_tick`, `on_tick_per_slot`; Gloas inherits proposer-boost reset and unrealized checkpoint promotion |
| `on_block.go`: `OnBlock` | Phase0 `on_block`; Bellatrix execution payload validation; Deneb blob availability; Electra execution requests; Gloas deferred payload/envelope processing; Fulu modified `on_block` data availability call |
| `on_block.go`: `verifyKzgCommitmentsAgainstTransactions` | Deneb execution payload blob versioned-hash checks; Electra/Fulu maximum blob count plumbing |
| `on_block.go`: `isDataAvailable` | Deneb `is_data_available` for blob sidecars; pre-Gloas local blob storage path |
| `on_block.go`: PeerDAS `IsDataAvailable` and `SyncColumnDataLater` branch inside `OnBlock` | Fulu modified `is_data_available`: data availability is checked by block root through data column sidecars, without passing `blob_kzg_commitments`; modified Fulu `on_block` calls it as `is_data_available(hash_tree_root(block))` |
| `on_execution_payload.go`: `OnExecutionPayload`, `applyEnvelope`, `applyEnvelopeLocked`, `ApplyLocalSelfBuildEnvelope`, `StoreAnchorEnvelope` | Gloas `on_execution_payload_envelope`, `Store.payloads`, `Store.payload_timeliness_vote`, `Store.payload_data_availability_vote` |
| `on_execution_payload.go`: `validateEnvelopeAgainstBlock`, `verifyEnvelopeBuilderSignature`, `checkDataAvailability`, `validatePayloadWithEL` | Gloas `on_execution_payload_envelope`; Gloas/Fulu data availability for committed bid blob data; Bellatrix `ExecutionEngine.notify_forkchoice_updated`/payload validation context |
| `on_payload_attestation_message.go`: `OnPayloadAttestationMessage` | Gloas `on_payload_attestation_message`, PTC membership/signature/current-slot checks |
| `on_attestation.go`: `OnAttestation`, `ProcessAttestingIndicies`, `ValidateOnAttestation`, `validateTargetEpochAgainstCurrentTime` | Phase0 `on_attestation`, `validate_on_attestation`, `validate_target_epoch_against_current_time`; Gloas modified `validate_on_attestation` |
| `on_attestation.go`: `updateLatestMessages`, `updateLatestMessagesPreGloas`, `updateLatestMessagesGloas`, latest-message accessors | Phase0 `update_latest_messages`; Gloas modified `update_latest_messages` with slot ordering and payload-present tracking |
| `on_attester_slashing.go`: `OnAttesterSlashing`, `onProcessAttesterSlashing`, `getIndexedAttestationPublicKeys` | Phase0 `on_attester_slashing`, `is_valid_indexed_attestation` |
| `checkpoint_state.go`: `getAttestingIndicies`, `getActiveIndicies`, `committeeCount`, `getDomain`, `isValidIndexedAttestation`, `epochAtSlot` | Phase0 attestation helpers used by `on_attestation`; beacon-chain `get_attesting_indices`, `get_active_validator_indices`, `get_committee_count_per_slot`, `get_domain`, `is_valid_indexed_attestation`, `compute_epoch_at_slot`; Electra committee-index/aggregation changes |
| `utils.go`: `updateCheckpoints`, `updateUnrealizedCheckpoints`, `Ancestor`, `getCheckpointState`, slot/epoch helpers | Phase0 `update_checkpoints`, `update_unrealized_checkpoints`, `get_ancestor`, `store_target_checkpoint_state`, slot/epoch helpers; Gloas modified `get_ancestor` returning payload status |
| `blob_sidecars.go`: `AddPreverifiedBlobSidecar` | Deneb data availability support for `is_data_available`; pre-Fulu blob sidecar inventory |
| `latest_messages_store.go` | Implementation storage for Phase0/Gloas `Store.latest_messages` |
| `interface.go` | Local interface surface for the fork-choice handlers and readers above |
| `fork_graph/interface.go`, `fork_graph/fork_graph_disk.go`, `fork_graph/fork_graph_disk_fs.go`, `fork_graph/participation_indicies_store.go` | Implementation storage for Phase0/Gloas `Store.blocks`, `Store.block_states`, `Store.checkpoint_states`, payload envelope persistence, and participation data |
| `optimistic/optimistic.go`, `optimistic/optimistic_impl.go` | Optimistic sync support for execution payload validity tracking; review with Bellatrix execution payload validity and fork-choice update semantics |
| `public_keys_registry/interface.go`, `public_keys_registry/in_memory_public_keys_registry.go`, `public_keys_registry/db_public_keys_registry.go` | Signature verification support for `is_valid_indexed_attestation` and payload attestation validation |

## Fulu Review Notes

- Fulu changes fork-choice data availability from Deneb blob-sidecar retrieval by
  commitments to PeerDAS data-column retrieval by beacon block root. In this
  codebase, review the `OnBlock` Fulu branch and any `peerDas.IsDataAvailable`
  calls as the implementation of Fulu `is_data_available(beacon_block_root)`.
- Fulu `on_block` only changes the data-availability assertion shape: it checks
  `is_data_available(hash_tree_root(block))` before `state_transition` and store
  mutation. Do not reintroduce a Fulu dependency on `block.body.blob_kzg_commitments`
  in fork choice.
- Deneb and Fulu data availability paths are not interchangeable. Deneb
  `isDataAvailable(ctx, slot, blockRoot, blobKzgCommitments)` verifies blob
  sidecars against commitments; Fulu uses PeerDAS/data-column availability and
  may schedule deferred column-data sync.
- Gloas separates beacon block processing from execution payload envelope
  processing. For post-Gloas/Fulu blocks, check data availability in both the
  `OnBlock` PeerDAS path and `OnExecutionPayload.checkDataAvailability` path
  according to the caller's `checkDataAvaiability`/`checkBlobData` flags.

## Review Checklist

- Conditional vs fallback: if the spec says `if condition: return X; return Y`,
  preserve that exact fallback behavior. Be suspicious of local shortcuts that
  return `false`, zero roots, empty payload status, or `nil` when the spec would
  continue through a later disjunct.
- Missing disjuncts: compare every `or` in `filter_block_tree`,
  `should_extend_payload`, `should_apply_proposer_boost`,
  `validate_on_attestation`, `get_proposer_head`-related helpers, and payload
  status tiebreaking. A missing permissive disjunct can reject a viable branch;
  a missing restrictive disjunct can accept an invalid block or vote.
- Epoch boundaries: explicitly test genesis epoch, first slot of an epoch,
  current vs previous epoch attestations, and slots where
  `compute_slots_since_epoch_start(slot) == 0`. These paths affect
  `filter_block_tree`, `on_tick_per_slot`, pulled-up unrealized checkpoints, and
  proposer reorg helper behavior.
- Timeliness boundaries: check `<` vs `<=` against the spec for attestation,
  aggregate, contribution, sync-message, PTC, and proposer reorg cutoffs. Tests
  should cover exactly-at-threshold arrival.
- Gloas payload status: when reviewing `PENDING`, `EMPTY`, and `FULL` handling,
  trace both the beacon block root and the parent execution block hash. Confirm
  `get_parent_payload_status`, `get_node_children`,
  `get_payload_status_tiebreaker`, and `is_supporting_vote` agree on the same
  status transition.
- PTC votes: payload timeliness and blob-data availability are separate vote
  vectors. A payload-present vote must not imply blob-data availability, and
  local payload/envelope availability must still gate both spec helpers.
- Equivocations: `equivocating_indices` must be excluded from latest-message
  weight, but Gloas `is_head_weak` also adds back the specified committee
  weight for equivocating validators. Review both sides together.
- Store mutation safety: spec handlers that fail assertions must not mutate the
  store. In Go, validate all reject/ignore conditions before writes to latest
  messages, block trees, payload states, timeliness maps, and checkpoint fields.
- Cache invalidation: any change to latest messages, payload status, block
  timeliness, proposer boost root, checkpoints, or fork graph contents can make
  cached head/weight data stale. Confirm invalidation happens on every mutating
  path.
- Pre-fork vs post-fork behavior: preserve Phase0/Bellatrix/Deneb behavior when
  the current state version is pre-Gloas. Gloas-only logic should be gated by
  the same fork-version or epoch boundary that the surrounding code uses.
- Electra execution requests: for Electra and later, `OnBlock`/envelope EL
  validation must pass execution requests consistently with the beacon block or
  envelope source. Empty request lists and nil request lists are not equivalent
  unless the surrounding code intentionally normalizes them.
- Data availability: Deneb checks blob sidecars before accepting a block; Fulu
  checks data column sidecars through PeerDAS by block root; Gloas checks
  committed bid blob data during envelope processing. Review fork-version
  branches so the check is neither skipped nor duplicated for the active fork.
- Tests: add or update focused tests for any spec branch that changes. Prefer
  table tests that name the spec condition, especially for conditional/fallback,
  missing-disjunct, data-availability, and epoch-boundary cases.

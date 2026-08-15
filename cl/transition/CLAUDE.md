# Caplin State Transition Spec Map

This directory contains the beacon state transition pipeline and the eth2
operation implementations. Review changes against the upstream consensus specs,
especially when fork-version branches change.

Primary references:

- Beacon chain state transition: https://ethereum.github.io/consensus-specs/specs/phase0/beacon-chain/
- Altair beacon chain: https://ethereum.github.io/consensus-specs/specs/altair/beacon-chain/
- Bellatrix beacon chain: https://ethereum.github.io/consensus-specs/specs/bellatrix/beacon-chain/
- Capella beacon chain: https://ethereum.github.io/consensus-specs/specs/capella/beacon-chain/
- Deneb beacon chain: https://ethereum.github.io/consensus-specs/specs/deneb/beacon-chain/
- Electra beacon chain: https://ethereum.github.io/consensus-specs/specs/electra/beacon-chain/
- Fulu beacon chain: https://ethereum.github.io/consensus-specs/specs/fulu/beacon-chain/
- Gloas beacon chain: https://ethereum.github.io/consensus-specs/specs/gloas/beacon-chain/

## Transition Pipeline Map

Use this map when reviewing `cl/transition/machine/`.

| Go file/function | Spec section |
| --- | --- |
| `machine/transition.go`: `TransitionState` | Phase0 `state_transition`: `process_slots`, block signature verification, `process_block`, final state-root assertion |
| `machine/block.go`: `ProcessBlock` | Phase0 `process_block`; Bellatrix/Capella execution payload and withdrawals; Gloas modified `process_block` ordering with `process_parent_execution_payload`, `process_withdrawals`, and `process_execution_payload_bid` |
| `machine/block.go`: `ProcessOperations` | Phase0 `process_operations`; Capella `process_bls_to_execution_change`; Electra execution requests; Gloas payload attestations |
| `machine/block.go`: `processRandao` | Phase0 `process_randao`; batched signature collection for block-level validation |
| `machine/block.go`: `processProposerSlashings` | Phase0 `process_proposer_slashing`; signature domain/signing-root validation |
| `machine/block.go`: `processVoluntaryExits` | Phase0 `process_voluntary_exit`; Capella/Deneb voluntary-exit domain rule; Gloas builder-index exit signature source |
| `machine/block.go`: `processBlsToExecutionChanges` | Capella `process_bls_to_execution_change`; fork-agnostic BLS-to-execution-change domain |
| `machine/block.go`: `forEachProcess` | Local dispatcher for homogeneous SSZ operation lists; review with the operation-specific spec function |
| `machine/helpers.go`: `executionEnabled` | Bellatrix `is_execution_enabled` merge-transition gate |
| `impl/eth2/validation.go`: `VerifyTransition`, `VerifyBlockSignature` | Phase0 state-root assertion after `process_block`; `verify_block_signature` |
| `impl/eth2/utils.go`: `transitionSlot`, `computeSigningRootEpoch` | Phase0 `process_slot` (state/block root caching, Gloas payload availability reset); signing-root helper |
| `compat.go`: `TransitionState` | Top-level entry point wiring `DefaultMachine` and `ValidatingMachine` |
| `machine/machine.go`: `Interface`, `BlockProcessor`, `BlockValidator`, `SlotProcessor`, `BlockHeaderProcessor`, `BlockOperationProcessor` | Local interface hierarchy for the spec functions implemented in `impl/eth2/` |

## Block Operation Spec Map

Use this map when reviewing `cl/transition/impl/eth2/operations.go`.

| Go file/function | Spec section |
| --- | --- |
| `operations.go`: `FullValidate` | Local validation-mode switch; affects signature and Merkle proof assertions around spec processing |
| `operations.go`: `ProcessProposerSlashing` | Phase0 `process_proposer_slashing`; Gloas builder pending-payment removal for slashed proposer window |
| `operations.go`: `ProcessAttesterSlashing` | Phase0 `process_attester_slashing`; `is_slashable_attestation_data`; `is_valid_indexed_attestation` |
| `operations.go`: `ProcessDeposit` | Phase0 `process_deposit`; Electra modified deposit queuing through `pending_deposits` |
| `operations.go`: `getPendingBalanceToWithdraw` | Electra helper for `process_withdrawal_request`, `process_consolidation_request`, voluntary exits, and pending partial withdrawals |
| `operations.go`: `IsVoluntaryExitApplicable`, `ProcessVoluntaryExit` | Phase0 `process_voluntary_exit`; Electra pending-partial-withdrawal guard; Gloas builder voluntary exits |
| `operations.go`: `ProcessWithdrawals`, `processWithdrawalsPreGloas`, `processWithdrawalsGloas` | Capella `process_withdrawals`; Electra pending partial withdrawals; Gloas modified `process_withdrawals` with parent-empty early return and payload expected withdrawals |
| `operations.go`: `applyWithdrawals` | Capella balance decreases; Gloas builder balance withdrawals |
| `operations.go`: `updateNextWithdrawalIndex`, `updatePendingPartialWithdrawals`, `updateNextWithdrawalValidatorIndex`, `updatePayloadExpectedWithdrawals`, `updateBuilderPendingWithdrawals`, `updateNextWithdrawalBuilderIndex` | Capella/Electra/Gloas withdrawal cursor and queue updates |
| `operations.go`: `ProcessExecutionPayloadBid`, `verifyExecutionPayloadBidSignature` | Gloas `process_execution_payload_bid`; builder activity, bid coverage, bid signature, blob limit, bid slot/parent/randao checks |
| `operations.go`: `ApplyParentExecutionPayload`, `ProcessParentExecutionPayload` | Gloas `apply_parent_execution_payload`; `process_parent_execution_payload`; parent empty/full branching; execution request root commitment |
| `operations.go`: `ProcessExecutionPayloadEnvelope` | Gloas `process_execution_payload_envelope`; committed bid consistency, execution requests root, timestamp, withdrawals root |
| `operations_utils.go`: `verifyExecutionPayloadEnvelopeSignature` | Gloas envelope builder signature verification |
| `operations.go`: `ProcessExecutionPayload` | Bellatrix `process_execution_payload`; Deneb/Fulu blob commitment limits; latest execution payload header update |
| `operations.go`: `ProcessSyncAggregate`, `processSyncAggregate` | Altair `process_sync_aggregate`; sync committee participant/proposer rewards and signature check |
| `operations.go`: `ProcessBlsToExecutionChange` | Capella `process_bls_to_execution_change` |
| `operations.go`: `ProcessAttestations`, `processAttestation`, `IsAttestationApplicable` | Phase0 `process_attestation`; Altair participation flags; Deneb/Electra modified inclusion and committee-index checks |
| `operations.go`: `processAttestationPhase0` | Phase0 `process_attestation`, pending attestation queues, matching source/target/head helpers |
| `operations.go`: `processAttestationPostAltair` | Altair modified `process_attestation`; Electra committee bits; Gloas same-slot builder pending-payment weight accumulation |
| `operations.go`: `verifyAttestations`, `batchVerifyAttestations` | Phase0 `is_valid_indexed_attestation`; local batched aggregate signature verification |
| `operations.go`: `ProcessBlockHeader` | Phase0 `process_block_header` |
| `operations.go`: `ProcessRandao` | Phase0 `process_randao` |
| `operations.go`: `ProcessEth1Data` | Phase0 `process_eth1_data` |
| `operations.go`: `ProcessSlots` | Phase0 `process_slots`, `process_slot`, `process_epoch`; local fork upgrades at epoch boundaries |
| `operations.go`: `ProcessDepositRequest` | Electra `process_deposit_request`; Gloas builder deposit routing and deposit-request start-index behavior |
| `operations.go`: `ProcessWithdrawalRequest`, `processBuilderWithdrawalRequest` | Electra `process_withdrawal_request`; Gloas builder withdrawal/full-exit request handling |
| `operations.go`: `ProcessConsolidationRequest`, `isValidSwitchToCompoundingRequest`, `switchToCompoundingValidator`, `computeConsolidationEpochAndUpdateChurn` | Electra `process_consolidation_request`; `is_valid_switch_to_compounding_request`; `switch_to_compounding_validator`; `compute_consolidation_epoch_and_update_churn` |
| `operations.go`: `ProcessPayloadAttestation` | Gloas `process_payload_attestation`; parent block root, previous slot, indexed payload attestation signature |

## Epoch Processing Spec Map

Use this map when reviewing `cl/transition/impl/eth2/statechange/`.

| Go file/function | Spec section |
| --- | --- |
| `process_epoch.go`: `ProcessEpoch`, `GetUnslashedIndiciesSet` | Phase0/Altair `process_epoch`; `get_unslashed_participating_indices`; fork-version ordered epoch sub-transitions |
| `finalization_and_justification.go`: `ProcessJustificationBitsAndFinality`, `weighJustificationAndFinalization`, `computePreviousAndCurrentTargetBalancePostAltair` | Phase0/Altair `process_justification_and_finalization`; target balance thresholds and justification bits |
| `process_rewards_and_penalties.go`: `ProcessRewardsAndPenalties`, `processRewardsAndPenaltiesPhase0`, `processRewardsAndPenaltiesPostAltair`, `getFlagsTotalBalances` | Phase0/Altair `process_rewards_and_penalties`; base rewards, participation flags, inactivity penalties |
| `process_registry_updates.go`: `ProcessRegistryUpdates`, `computeActivationExitEpoch` | Phase0 `process_registry_updates`; `compute_activation_exit_epoch`; Electra churn-aware activation/exit rules |
| `process_effective_balance_update.go`: `ProcessEffectiveBalanceUpdates` | Phase0 `process_effective_balance_updates`; hysteresis thresholds; Electra max effective balance |
| `process_slashings.go`: `ProcessSlashings`, `processSlashingsElectra` | Phase0/Electra `process_slashings`; proportional slashing penalties |
| `process_historical_roots.go`: `ProcessParticipationRecordUpdates` | Phase0 `process_participation_record_updates` |
| `process_historical_roots_update.go`: `ProcessHistoricalRootsUpdate` | Phase0 `process_historical_roots_update` |
| `process_inactivity_scores.go`: `ProcessInactivityScores` | Altair `process_inactivity_updates` / `process_inactivity_scores` |
| `process_sync_committee_update.go`: `ProcessSyncCommitteeUpdate` | Altair `process_sync_committee_updates` |
| `resets.go`: `ProcessEth1DataReset`, `ProcessSlashingsReset`, `ProcessRandaoMixesReset`, `ProcessParticipationFlagUpdates` | Phase0/Altair epoch resets: eth1 data votes, slashings, randao mixes, participation flags |
| `process_pending_deposits.go`: `ProcessPendingDeposits`, `applyPendingDeposit` | Electra `process_pending_deposits`; finalized deposit gating; churn consumption; postponed deposits; deposit signature/registry application |
| `process_pending_consolidations.go`: `ProcessPendingConsolidations` | Electra `process_pending_consolidations` |
| `process_builder_pending_payments.go`: `ProcessBuilderPendingPayments` | Gloas builder pending-payment epoch rotation and payment finalization |
| `process_proposer_lookahead.go`: `ProcessProposerLookahead` | Gloas proposer lookahead update |
| `process_ptc_window.go`: `ProcessPtcWindow` | Gloas PTC window update |
| `utils.go`: `IsValidDepositSignature`, `AddValidatorToRegistry` | Phase0/Electra deposit helpers: deposit proof-of-possession and validator registry append |

## Review Checklist

- Preserve the spec order in `ProcessSlots`, `ProcessBlock`, and `ProcessEpoch`.
  Gloas intentionally moves parent execution payload handling before block
  header processing and moves execution-payload effects across block boundaries.
- For every operation, separate reject/assert failures from ignore/no-op cases.
  Execution requests often ignore invalid EL-triggered items, while block
  operations such as slashings, attestations, and headers generally reject.
- Validate before mutating state when the spec assertion can fail. Pay special
  attention to deposits, attestations, execution payload bids/envelopes,
  withdrawal cursors, and builder payment queues.
- Review fork gates explicitly. Phase0, Altair, Bellatrix, Capella, Deneb,
  Electra, Fulu, and Gloas branches must preserve pre-fork behavior and use the
  same epoch/version boundary as the surrounding transition code.
- Check partial-list queue updates with the local pattern from pending deposits:
  iterate until the first unprocessed item, `ShallowCopy`, `Cut(processedCount)`,
  append postponed items in order, then write the list back. Do not mutate a
  shared list or drop postponed entries. Watch for code that processes only a
  prefix and silently drops the tail, or that substitutes the full list where the
  spec intentionally uses the partial accumulator.
- Withdrawal processing has multiple independent cursors and queues:
  `next_withdrawal_index`, `next_withdrawal_validator_index`,
  `pending_partial_withdrawals`, Gloas `builder_pending_withdrawals`, and
  `next_withdrawal_builder_index`. Verify each cursor advances from the same
  processed count used by the spec helper.
- In Gloas, distinguish `latest_block_hash`, `latest_execution_payload_bid`,
  parent execution requests, payload expected withdrawals, and builder pending
  payments. Empty parent blocks must not apply full-parent payload effects.
- For attestations, test current vs previous epoch, exactly
  `MIN_ATTESTATION_INCLUSION_DELAY`, Deneb's relaxed upper bound, Electra
  committee bits, and Gloas same-slot builder-payment weight.
- Execution payload validation changed across forks: Bellatrix checks payload
  header consistency, Deneb/Fulu add blob commitment limits, Electra adds
  execution requests, and Gloas verifies envelopes against committed bids.
- Epoch processing tests should cover genesis/early epochs, finality delay,
  inactivity leak entry/exit, slashed validators, churn exhaustion, postponed
  deposits, and Gloas builder/PTC window rotations.

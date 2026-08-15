# Caplin Beacon State Helper Spec Map

This directory contains consensus-critical beacon state helper logic used by
fork choice and state transition. Review helpers as direct implementations of
consensus spec accessors/mutators, even when the call site lives outside this
directory.

Primary references:

- Phase0 beacon chain helpers: https://ethereum.github.io/consensus-specs/specs/phase0/beacon-chain/
- Altair beacon chain helpers: https://ethereum.github.io/consensus-specs/specs/altair/beacon-chain/
- Capella withdrawals: https://ethereum.github.io/consensus-specs/specs/capella/beacon-chain/
- Electra beacon chain helpers: https://ethereum.github.io/consensus-specs/specs/electra/beacon-chain/
- Fulu beacon chain helpers: https://ethereum.github.io/consensus-specs/specs/fulu/beacon-chain/
- Gloas beacon chain helpers: https://ethereum.github.io/consensus-specs/specs/gloas/beacon-chain/

## Accessor Spec Map

Use this map when reviewing `accessors.go`, `cache_accessors.go`, `util.go`,
and Gloas helper extensions in `epbs.go`.

| Go file/function | Spec section |
| --- | --- |
| `accessors.go`: `GetEpochAtSlot`, `Epoch`, `PreviousEpoch` | Phase0 `compute_epoch_at_slot`, `get_current_epoch`, `get_previous_epoch` |
| `accessors.go`: `IsAggregator` | Phase0 `is_aggregator` |
| `accessors.go`: `GetTotalBalance` | Phase0 `get_total_balance` |
| `accessors.go`: `GetTotalSlashingAmount` | Phase0 `get_total_slashing_amount` |
| `accessors.go`: `GetBlockRoot` | Phase0 `get_block_root` |
| `accessors.go`: `FinalityDelay`, `InactivityLeaking` | Phase0/Altair `get_finality_delay`, `is_in_inactivity_leak` |
| `accessors.go`: `IsUnslashedParticipatingIndex`, `EligibleValidatorsIndicies` | Phase0/Altair `get_unslashed_participating_indices`, `get_eligible_validator_indices` |
| `accessors.go`: `IsValidIndexedAttestation` | Phase0 `is_valid_indexed_attestation` |
| `accessors.go`: `IsValidatorEligibleForActivationQueue` | Phase0 `is_eligible_for_activation_queue`; Electra modified `is_eligible_for_activation_queue` |
| `accessors.go`: `IsMergeTransitionComplete` | Bellatrix `is_merge_transition_complete` |
| `accessors.go`: `ComputeTimestampAtSlot` | Bellatrix `compute_timestamp_at_slot` |
| `accessors.go`: `GetExpectedWithdrawals` | Capella `get_expected_withdrawals`; Electra pending partial withdrawals; Gloas builder withdrawals and builder sweep withdrawals |
| `accessors.go`: `GetPendingPartialWithdrawals`, `getBalanceAfterWithdrawals` | Electra `get_expected_withdrawals` pending partial withdrawal pass |
| `accessors.go`: `GetValidatorsSweepWithdrawals` | Capella/Electra validator sweep in `get_expected_withdrawals` |
| `accessors.go`: `GetBuilderWithdrawals`, `GetBuildersSweepWithdrawals` | Gloas builder withdrawal passes in `get_expected_withdrawals` |
| `accessors.go`: `GetNextSyncCommitteeIndices` | Altair `get_next_sync_committee_indices`; Gloas modified balance-weighted selection |
| `util.go`: `GetIndexedAttestation` | Phase0 `get_indexed_attestation` |
| `util.go`: `GetValidatorFromDeposit` | Phase0 deposit validator construction inside `process_deposit` |
| `util.go`: `HasEth1WithdrawalCredential`, `HasCompoundingWithdrawalCredential`, `HasExecutionWithdrawalCredential` | Capella/Electra withdrawal credential helpers |
| `util.go`: `ComputeActivationExitEpoch` | Phase0 `compute_activation_exit_epoch` |
| `util.go`: `GetActivationExitChurnLimit`, `GetBalanceChurnLimit`, `GetConsolidationChurnLimit`, `QueueExcessActiveBalance` | Electra activation/exit balance churn and consolidation helpers |
| `util.go`: `GetValidatorsCustodyRequirement` | Fulu data-column custody helper |
| `util.go`: `IsAttestationSameSlot` | Gloas same-slot attestation helper for builder pending-payment weights |
| `epbs.go`: `IsBuilderIndex`, `ConvertBuilderIndexToValidatorIndex`, `ConvertValidatorIndexToBuilderIndex` | Gloas builder-index encoding helpers |
| `epbs.go`: `IsActiveBuilder`, `IsBuilderWithdrawalCredential` | Gloas builder registry and withdrawal-credential helpers |
| `epbs.go`: `IsValidIndexedPayloadAttestation` | Gloas `is_valid_indexed_payload_attestation` |
| `epbs.go`: `CanBuilderCoverBid`, `GetPendingBalanceToWithdrawForBuilder` | Gloas builder bid coverage and pending-withdrawal helpers |
| `epbs.go`: `IsPendingValidator`, `IsBuilderPubkey` | Gloas deposit request routing helpers |
| `util.go`: `GetMaxEffectiveBalanceByVersion` | Electra `get_max_effective_balance`; version-gated effective balance cap |
| `cache_accessors.go`: `GetActiveValidatorsIndices` | Phase0 `get_active_validator_indices` |
| `cache_accessors.go`: `GetTotalActiveBalance` | Phase0 `get_total_active_balance` |
| `cache_accessors.go`: `ComputeCommittee` | Phase0 `compute_committee` |
| `cache_accessors.go`: `GetBeaconProposerIndex`, `GetBeaconProposerIndices`, `GetBeaconProposerIndexForSlot` | Phase0 `get_beacon_proposer_index` |
| `cache_accessors.go`: `BaseRewardPerIncrement`, `BaseReward` | Altair `get_base_reward_per_increment`, `get_base_reward` |
| `cache_accessors.go`: `SyncRewards` | Altair sync committee reward computation |
| `cache_accessors.go`: `CommitteeCount` | Phase0 `get_committee_count_per_slot` |
| `cache_accessors.go`: `GetAttestationParticipationFlagIndicies` | Altair `get_attestation_participation_flag_indices` |
| `cache_accessors.go`: `GetBeaconCommitee` | Phase0 `get_beacon_committee` |
| `cache_accessors.go`: `ComputeNextSyncCommittee` | Altair `get_next_sync_committee` |
| `cache_accessors.go`: `GetAttestingIndicies` | Phase0 `get_attesting_indices` |
| `cache_accessors.go`: `GetValidatorChurnLimit` | Phase0 `get_validator_churn_limit` |
| `cache_accessors.go`: `GetValidatorActivationChurnLimit` | Electra `get_validator_activation_churn_limit` |
| `cache_accessors.go`: `GetPTC`, `GetPTCFromWindow`, `ComputePTC`, `InitializePtcWindow` | Gloas payload timeliness committee helpers |
| `cache_accessors.go`: `GetIndexedPayloadAttestation` | Gloas `get_indexed_payload_attestation` |
| `cache_accessors.go`: `GetBuilderPaymentQuorumThreshold` | Gloas builder payment quorum |
| `epbs.go`: `IsValidDepositSignature` | Phase0/Gloas deposit proof-of-possession helper |
| `epbs.go`: `GetIndexForNewBuilder`, `AddBuilderToRegistry`, `ApplyDepositForBuilder` | Gloas builder registry deposit helpers |

## Mutator Spec Map

Use this map when reviewing `mutators.go`, `cache_mutators.go`, and mutating
helper paths in `epbs.go`.

| Go file/function | Spec section |
| --- | --- |
| `mutators.go`: `IncreaseBalance` | Phase0 `increase_balance` |
| `mutators.go`: `DecreaseBalance` | Phase0 `decrease_balance`; saturates at zero |
| `cache_mutators.go`: `SlashValidator` | Phase0 `slash_validator`; whistleblower/proposer reward split |
| `cache_mutators.go`: `InitiateValidatorExit` | Phase0 `initiate_validator_exit` |
| `cache_mutators.go`: `ComputeExitEpochAndUpdateChurn` | Electra `compute_exit_epoch_and_update_churn` |
| `cache_mutators.go`: `InitiateBuilderExit` | Gloas builder exit |
| `epbs.go`: `AddBuilderToRegistry` | Gloas builder registry append/reuse during builder deposits |
| `epbs.go`: `ApplyDepositForBuilder` | Gloas builder deposit application: new builder proof-of-possession or existing builder balance increase |

## Review Checklist

- Treat helpers as consensus code, not convenience wrappers. A small change here
  can alter fork choice, block processing, epoch processing, or withdrawal
  roots.
- Preserve spec minimums and saturation behavior: `get_total_balance` must
  return at least `EFFECTIVE_BALANCE_INCREMENT`, and `decrease_balance` must not
  underflow.
- Withdrawal helpers must keep ordering and limits stable across Capella,
  Electra, and Gloas passes. Check `MAX_WITHDRAWALS_PER_PAYLOAD - 1` builder
  limits separately from validator sweep limits.
- When helpers consume a partial list or queue, use copy-on-write patterns if
  the underlying SSZ list can share pointers across state copies.
- Distinguish "pending" validators (have a pending deposit but not yet activated)
  from "active" validators (past activation epoch, before exit epoch). These are
  separate predicates; using one where the spec requires the other causes
  deposit-routing and balance-accounting bugs.
- Builder helpers must keep encoded builder indices distinct from validator
  indices and must not accidentally look up a builder index in the validator
  registry without conversion.
- Signature helpers must use the exact spec domain and fork version. Deposit and
  BLS-to-execution-change signatures are fork agnostic; attestations and payload
  attestations are epoch/fork dependent.
- Fork gates matter: Electra changes activation/withdrawal churn and pending
  deposits; Fulu changes custody requirements; Gloas adds builders, PTC payload
  attestations, and balance-weighted sync committee selection.

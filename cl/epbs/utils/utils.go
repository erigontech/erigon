package epbs

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common/log/v3"
)

// IsBuilderIndex returns true if the given validator index is actually a builder index.
// Builder indices have the most significant bit (bit 63) set.
func IsBuilderIndex(validatorIndex uint64) bool {
	return (validatorIndex & clparams.BuilderIndexFlag) != 0
}

// ConvertBuilderIndexToValidatorIndex converts a builder index to a validator index
// by setting the BUILDER_INDEX_FLAG bit.
func ConvertBuilderIndexToValidatorIndex(builderIndex cltypes.BuilderIndex) uint64 {
	return uint64(builderIndex) | clparams.BuilderIndexFlag
}

// ConvertValidatorIndexToBuilderIndex converts a validator index (with builder flag set)
// to a builder index by clearing the BUILDER_INDEX_FLAG bit.
func ConvertValidatorIndexToBuilderIndex(validatorIndex uint64) cltypes.BuilderIndex {
	return cltypes.BuilderIndex(validatorIndex &^ clparams.BuilderIndexFlag)
}

// IsActiveBuilder checks if the builder at builderIndex is active for the given state.
// A builder is considered active if:
// - Its deposit epoch is finalized (deposit_epoch < finalized_checkpoint.epoch)
// - It has not initiated an exit (withdrawable_epoch == FAR_FUTURE_EPOCH)
func IsActiveBuilder(state abstract.BeaconState, builderIndex cltypes.BuilderIndex) bool {
	builders := state.GetBuilders()
	if builders == nil {
		log.Warn("builders is nil")
		return false
	}
	builder := builders.Get(int(builderIndex))
	if builder == nil {
		log.Warn("builder is nil", "builderIndex", builderIndex)
		return false
	}
	return builder.DepositEpoch < state.FinalizedCheckpoint().Epoch &&
		builder.WithdrawableEpoch == state.BeaconConfig().FarFutureEpoch
}

// IsBuilderWithdrawalCredential checks if the withdrawal credentials belong to a builder.
// Builder withdrawal credentials have the BUILDER_WITHDRAWAL_PREFIX (0x03) as the first byte.
func IsBuilderWithdrawalCredential(withdrawalCredentials [32]byte, beaconConfig *clparams.BeaconChainConfig) bool {
	return withdrawalCredentials[0] == byte(beaconConfig.BuilderWithdrawalPrefix)
}

// IsValidIndexedPayloadAttestation checks if the indexed payload attestation is valid.
// It verifies that:
// - Indices are non-empty and sorted
// - The aggregate signature is valid
func IsValidIndexedPayloadAttestation(s abstract.BeaconState, attestation *cltypes.IndexedPayloadAttestation) (bool, error) {
	indices := attestation.AttestingIndices
	if indices.Length() == 0 || !solid.IsUint64SortedSet(indices) {
		return false, errors.New("isValidIndexedPayloadAttestation: attesting indices are empty or not sorted")
	}

	// Collect public keys from validators
	pks := make([][]byte, 0, indices.Length())
	indices.Range(func(_ int, idx uint64, _ int) bool {
		val, err := s.ValidatorForValidatorIndex(int(idx))
		if err != nil {
			return false
		}
		pk := val.PublicKeyBytes()
		pks = append(pks, pk)
		return true
	})
	if len(pks) != indices.Length() {
		return false, errors.New("isValidIndexedPayloadAttestation: failed to get all validator public keys")
	}

	// Get domain for PTC attester
	epoch := state.GetEpochAtSlot(s.BeaconConfig(), attestation.Data.Slot)
	domain, err := s.GetDomain(s.BeaconConfig().DomainPtcAttester, epoch)
	if err != nil {
		return false, fmt.Errorf("unable to get the domain: %v", err)
	}

	// Compute signing root
	signingRoot, err := fork.ComputeSigningRoot(attestation.Data, domain)
	if err != nil {
		return false, fmt.Errorf("unable to get signing root: %v", err)
	}

	// Verify aggregate signature
	valid, err := bls.VerifyAggregate(attestation.Signature[:], signingRoot[:], pks)
	if err != nil {
		return false, fmt.Errorf("error while validating signature: %v", err)
	}
	if !valid {
		return false, errors.New("invalid aggregate signature")
	}

	return true, nil
}

// IsParentBlockFull returns true if the last committed payload bid was fulfilled with a payload,
// which can only happen when both beacon block and payload were present.
// Note: This function must be called on a beacon state before processing the execution payload bid in the block.
func IsParentBlockFull(s abstract.BeaconState) bool {
	bid := s.GetLatestExecutionPayloadBid()
	if bid == nil {
		return false
	}
	return bid.BlockHash == s.GetLatestBlockHash()
}

// CanBuilderCoverBid returns true if the builder has enough balance to cover the bid amount
// after accounting for the minimum deposit and pending withdrawals.
func CanBuilderCoverBid(s abstract.BeaconState, builderIndex cltypes.BuilderIndex, bidAmount uint64) bool {
	builders := s.GetBuilders()
	if builders == nil {
		log.Warn("builders is nil")
		return false
	}
	builder := builders.Get(int(builderIndex))
	if builder == nil {
		log.Warn("builder is nil", "builderIndex", builderIndex)
		return false
	}

	builderBalance := builder.Balance
	pendingWithdrawalsAmount := GetPendingBalanceToWithdrawForBuilder(s, builderIndex)
	minBalance := s.BeaconConfig().MinDepositAmount + pendingWithdrawalsAmount
	if builderBalance < minBalance {
		return false
	}
	return builderBalance-minBalance >= bidAmount
}

// GetPendingBalanceToWithdrawForBuilder returns the total pending balance to withdraw for a builder.
// This includes:
// - Amounts from builder_pending_withdrawals (direct withdrawal requests)
// - Amounts from builder_pending_payments (payments that include withdrawals)
func GetPendingBalanceToWithdrawForBuilder(s abstract.BeaconState, builderIndex cltypes.BuilderIndex) uint64 {
	var total uint64

	// Sum from builder_pending_withdrawals
	pendingWithdrawals := s.GetBuilderPendingWithdrawals()
	if pendingWithdrawals != nil {
		pendingWithdrawals.Range(func(_ int, withdrawal *cltypes.BuilderPendingWithdrawal, _ int) bool {
			if withdrawal != nil && withdrawal.BuilderIndex == builderIndex {
				total += withdrawal.Amount
			}
			return true
		})
	} else {
		log.Warn("builder_pending_withdrawals is nil")
	}

	// Sum from builder_pending_payments
	pendingPayments := s.GetBuilderPendingPayments()
	if pendingPayments != nil {
		pendingPayments.Range(func(_ int, payment *cltypes.BuilderPendingPayment, _ int) bool {
			if payment != nil && payment.Withdrawal != nil && payment.Withdrawal.BuilderIndex == builderIndex {
				total += payment.Withdrawal.Amount
			}
			return true
		})
	} else {
		log.Warn("builder_pending_payments is nil")
	}

	return total
}

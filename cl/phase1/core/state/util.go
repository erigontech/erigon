package state

import (
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	"sort"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func copyLRU[K comparable, V any](dst *lru.Cache[K, V], src *lru.Cache[K, V]) *lru.Cache[K, V] {
	dst.Purge()
	for _, key := range src.Keys() {
		val, has := src.Get(key)
		if !has {
			continue
		}
		dst.Add(key, val)
	}
	return dst
}

func GetIndexedAttestation(attestation *cltypes.Attestation, attestingIndicies []uint64) *cltypes.IndexedAttestation {
	// Sort the the attestation indicies.
	sort.Slice(attestingIndicies, func(i, j int) bool {
		return attestingIndicies[i] < attestingIndicies[j]
	})
	return &cltypes.IndexedAttestation{
		AttestingIndices: attestingIndicies,
		Data:             attestation.Data,
		Signature:        attestation.Signature,
	}
}

func ValidatorFromDeposit(conf *clparams.BeaconChainConfig, deposit *cltypes.Deposit) *cltypes.Validator {
	amount := deposit.Data.Amount
	effectiveBalance := utils.Min64(amount-amount%conf.EffectiveBalanceIncrement, conf.MaxEffectiveBalance)

	validator := &cltypes.Validator{}
	validator.SetPublicKey(deposit.Data.PubKey)
	validator.SetWithdrawalCredentials(deposit.Data.WithdrawalCredentials)
	validator.SetActivationEligibilityEpoch(conf.FarFutureEpoch)
	validator.SetActivationEpoch(conf.FarFutureEpoch)
	validator.SetExitEpoch(conf.FarFutureEpoch)
	validator.SetWithdrawableEpoch(conf.FarFutureEpoch)
	validator.SetEffectiveBalance(effectiveBalance)
	return validator
}

// Check whether a validator is fully withdrawable at the given epoch.
func isFullyWithdrawableValidator(conf *clparams.BeaconChainConfig, validator *cltypes.Validator, balance uint64, epoch uint64) bool {
	return validator.WithdrawalCredentials()[0] == conf.ETH1AddressWithdrawalPrefixByte &&
		validator.WithdrawableEpoch() <= epoch && balance > 0
}

// Check whether a validator is partially withdrawable.
func isPartiallyWithdrawableValidator(conf *clparams.BeaconChainConfig, validator *cltypes.Validator, balance uint64) bool {
	return validator.WithdrawalCredentials()[0] == conf.ETH1AddressWithdrawalPrefixByte &&
		validator.EffectiveBalance() == conf.MaxEffectiveBalance && balance > conf.MaxEffectiveBalance
}

func ComputeActivationExitEpoch(config *clparams.BeaconChainConfig, epoch uint64) uint64 {
	return epoch + 1 + config.MaxSeedLookahead
}

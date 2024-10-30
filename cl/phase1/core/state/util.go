// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"sort"

	"github.com/erigontech/erigon/v3/cl/clparams"
	"github.com/erigontech/erigon/v3/cl/cltypes"
	"github.com/erigontech/erigon/v3/cl/cltypes/solid"
	"github.com/erigontech/erigon/v3/cl/phase1/core/state/lru"
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

func GetIndexedAttestation(attestation *solid.Attestation, attestingIndicies []uint64) *cltypes.IndexedAttestation {
	// Sort the attestation indicies.
	sort.Slice(attestingIndicies, func(i, j int) bool {
		return attestingIndicies[i] < attestingIndicies[j]
	})
	return &cltypes.IndexedAttestation{
		AttestingIndices: solid.NewRawUint64List(2048, attestingIndicies),
		Data:             attestation.Data,
		Signature:        attestation.Signature,
	}
}

func ValidatorFromDeposit(conf *clparams.BeaconChainConfig, deposit *cltypes.Deposit) solid.Validator {
	amount := deposit.Data.Amount
	effectiveBalance := min(amount-amount%conf.EffectiveBalanceIncrement, conf.MaxEffectiveBalance)

	validator := solid.NewValidator()
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
func isFullyWithdrawableValidator(conf *clparams.BeaconChainConfig, validator solid.Validator, balance uint64, epoch uint64) bool {
	withdrawalCredentials := validator.WithdrawalCredentials()
	return withdrawalCredentials[0] == byte(conf.ETH1AddressWithdrawalPrefixByte) &&
		validator.WithdrawableEpoch() <= epoch && balance > 0
}

// Check whether a validator is partially withdrawable.
func isPartiallyWithdrawableValidator(conf *clparams.BeaconChainConfig, validator solid.Validator, balance uint64) bool {
	withdrawalCredentials := validator.WithdrawalCredentials()
	return withdrawalCredentials[0] == byte(conf.ETH1AddressWithdrawalPrefixByte) &&
		validator.EffectiveBalance() == conf.MaxEffectiveBalance && balance > conf.MaxEffectiveBalance
}

func ComputeActivationExitEpoch(config *clparams.BeaconChainConfig, epoch uint64) uint64 {
	return epoch + 1 + config.MaxSeedLookahead
}

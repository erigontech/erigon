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
	"slices"

	"github.com/erigontech/erigon/cl/utils/bls"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
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
	slices.Sort(attestingIndicies)
	return &cltypes.IndexedAttestation{
		AttestingIndices: solid.NewRawUint64List(2048*64, attestingIndicies),
		Data:             attestation.Data,
		Signature:        attestation.Signature,
	}
}

func GetValidatorFromDeposit(s abstract.BeaconState, pubkey [48]byte, withdrawalCredentials common.Hash, amount uint64) solid.Validator {
	conf := s.BeaconConfig()

	validator := solid.NewValidator()
	validator.SetPublicKey(pubkey)
	validator.SetWithdrawalCredentials(withdrawalCredentials)
	validator.SetActivationEligibilityEpoch(conf.FarFutureEpoch)
	validator.SetActivationEpoch(conf.FarFutureEpoch)
	validator.SetExitEpoch(conf.FarFutureEpoch)
	validator.SetWithdrawableEpoch(conf.FarFutureEpoch)

	// maxEffectiveBalance differs based on the version
	maxEffectiveBalance := GetMaxEffectiveBalanceByVersion(validator, conf, s.Version())
	effectiveBalance := min(amount-amount%conf.EffectiveBalanceIncrement, maxEffectiveBalance)
	validator.SetEffectiveBalance(effectiveBalance)
	return validator
}

func HasEth1WithdrawalCredential(validator solid.Validator, conf *clparams.BeaconChainConfig) bool {
	withdrawalCredentials := validator.WithdrawalCredentials()
	return withdrawalCredentials[0] == byte(conf.ETH1AddressWithdrawalPrefixByte)
}

func HasCompoundingWithdrawalCredential(validator solid.Validator, conf *clparams.BeaconChainConfig) bool {
	withdrawalCredentials := validator.WithdrawalCredentials()
	return withdrawalCredentials[0] == byte(conf.CompoundingWithdrawalPrefix)
}

func HasExecutionWithdrawalCredential(validator solid.Validator, conf *clparams.BeaconChainConfig) bool {
	return HasCompoundingWithdrawalCredential(validator, conf) || HasEth1WithdrawalCredential(validator, conf)
}

// Check whether a validator is fully withdrawable at the given epoch.
func isFullyWithdrawableValidator(b abstract.BeaconState, validator solid.Validator, balance uint64, epoch uint64) bool {
	conf := b.BeaconConfig()
	if b.Version().BeforeOrEqual(clparams.DenebVersion) {
		return HasEth1WithdrawalCredential(validator, conf) &&
			validator.WithdrawableEpoch() <= epoch &&
			balance > 0
	}
	// electra and after
	return HasExecutionWithdrawalCredential(validator, conf) &&
		validator.WithdrawableEpoch() <= epoch &&
		balance > 0
}

// getMaxEffectiveBalanceElectra is new in electra
func getMaxEffectiveBalanceElectra(v solid.Validator, conf *clparams.BeaconChainConfig) uint64 {
	if HasCompoundingWithdrawalCredential(v, conf) {
		return conf.MaxEffectiveBalanceElectra
	}
	return conf.MinActivationBalance
}

// GetMaxEffectiveBalanceByVersion is a helper function to get the max effective balance based on the state version.
// In Electra, the max effective balance is different based on the withdrawal credential.
func GetMaxEffectiveBalanceByVersion(v solid.Validator, conf *clparams.BeaconChainConfig, version clparams.StateVersion) uint64 {
	if version.BeforeOrEqual(clparams.DenebVersion) {
		return conf.MaxEffectiveBalance
	}
	return getMaxEffectiveBalanceElectra(v, conf)
}

// Check whether a validator is partially withdrawable.
func isPartiallyWithdrawableValidator(b abstract.BeaconState, validator solid.Validator, balance uint64) bool {
	conf := b.BeaconConfig()
	withdrawalCredentials := validator.WithdrawalCredentials()
	if b.Version().BeforeOrEqual(clparams.DenebVersion) {
		return withdrawalCredentials[0] == byte(conf.ETH1AddressWithdrawalPrefixByte) &&
			validator.EffectiveBalance() == conf.MaxEffectiveBalance &&
			balance > conf.MaxEffectiveBalance
	}
	// electra and after
	maxEffectiveBalance := getMaxEffectiveBalanceElectra(validator, conf)
	return HasExecutionWithdrawalCredential(validator, conf) &&
		validator.EffectiveBalance() == maxEffectiveBalance &&
		balance > maxEffectiveBalance
}

func ComputeActivationExitEpoch(config *clparams.BeaconChainConfig, epoch uint64) uint64 {
	return epoch + 1 + config.MaxSeedLookahead
}

func GetActivationExitChurnLimit(s abstract.BeaconState) uint64 {
	return min(
		s.BeaconConfig().MaxPerEpochActivationExitChurnLimit,
		GetBalanceChurnLimit(s),
	)
}

func GetBalanceChurnLimit(s abstract.BeaconState) uint64 {
	churn := max(
		s.BeaconConfig().MinPerEpochChurnLimitElectra,
		s.GetTotalActiveBalance()/s.BeaconConfig().ChurnLimitQuotient,
	)
	return churn - churn%s.BeaconConfig().EffectiveBalanceIncrement
}

func GetConsolidationChurnLimit(s abstract.BeaconState) uint64 {
	return GetBalanceChurnLimit(s) - GetActivationExitChurnLimit(s)
}

func QueueExcessActiveBalance(s abstract.BeaconState, vindex uint64, validator *solid.Validator) error {
	balance, err := s.ValidatorBalance(int(vindex))
	if err != nil {
		return err
	}
	if balance > s.BeaconConfig().MinActivationBalance {
		excessBalance := balance - s.BeaconConfig().MinActivationBalance
		if err := s.SetValidatorBalance(int(vindex), s.BeaconConfig().MinActivationBalance); err != nil {
			return err
		}
		// Use bls.G2_POINT_AT_INFINITY as a signature field placeholder
		// and GENESIS_SLOT to distinguish from a pending deposit request
		s.AppendPendingDeposit(&solid.PendingDeposit{
			PubKey:                validator.PublicKey(),
			WithdrawalCredentials: validator.WithdrawalCredentials(),
			Amount:                excessBalance,
			Signature:             bls.InfiniteSignature,
			Slot:                  s.BeaconConfig().GenesisSlot,
		})
	}
	return nil
}

func GetValidatorsCustodyRequirement(s abstract.BeaconState, validatorIndices []uint64) uint64 {
	totalNodeBalance := uint64(0)
	for _, index := range validatorIndices {
		effectiveBalance, err := s.ValidatorEffectiveBalance(int(index))
		if err != nil {
			continue
		}
		totalNodeBalance += effectiveBalance
	}

	count := totalNodeBalance / s.BeaconConfig().BalancePerAdditionalCustodyGroup
	return min(
		max(count, s.BeaconConfig().ValidatorCustodyRequirement),
		s.BeaconConfig().NumberOfCustodyGroups,
	)
}

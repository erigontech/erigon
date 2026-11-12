// Copyright 2026 The Erigon Authors
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
	"encoding/binary"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestNewStateSetValidatorsBuildsPubkeyIndex(t *testing.T) {
	beaconState := New(&clparams.MainnetBeaconConfig)
	validators := validatorSet(40, 41, 42)

	beaconState.SetValidators(validators)

	for index := range validators.Length() {
		got, ok := beaconState.ValidatorIndexByPubkey(validators.Get(index).PublicKey())
		require.True(t, ok)
		require.Equal(t, uint64(index), got)
	}
	_, ok := beaconState.ValidatorIndexByPubkey(activeValidatorWithIndex(100).PublicKey())
	require.False(t, ok)
}

func TestPubkeyIndexSurvivesFuluToGloasUpgrade(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	beaconState := New(&cfg)
	beaconState.SetVersion(clparams.FuluVersion)
	for i := uint64(1); i <= cfg.SlotsPerEpoch; i++ {
		require.NoError(t, beaconState.AddValidator(activeValidatorWithIndex(i), 32_000_000_000))
	}
	require.NoError(t, beaconState.InitBeaconState())

	require.NoError(t, beaconState.UpgradeToGloas())

	assertPubkeyIndexMatchesValidators(t, beaconState)
}

func TestGloasBuilderOnboardingUsesBaseAndDeltaPubkeyIndex(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	beaconState := New(&cfg)
	beaconState.SetVersion(clparams.GloasVersion)
	require.NoError(t, beaconState.AddValidator(activeValidatorWithIndex(1), 32_000_000_000))
	require.NoError(t, beaconState.InitBeaconState())
	require.NoError(t, beaconState.AddValidator(activeValidatorWithIndex(2), 32_000_000_000))

	credentials := common.Hash{byte(cfg.BuilderWithdrawalPrefix)}
	pendingDeposits := solid.NewPendingDepositList(&cfg)
	for _, index := range []uint64{1, 2, 3} {
		pendingDeposits.Append(&solid.PendingDeposit{
			PubKey:                activeValidatorWithIndex(index).PublicKey(),
			WithdrawalCredentials: credentials,
			Amount:                1,
		})
	}
	beaconState.SetPendingDeposits(pendingDeposits)

	require.NoError(t, beaconState.onboardBuildersFromPendingDeposits())

	require.Equal(t, 2, beaconState.GetPendingDeposits().Len())
	require.Equal(t, common.Bytes48(activeValidatorWithIndex(1).PublicKey()), beaconState.GetPendingDeposits().Get(0).PubKey)
	require.Equal(t, common.Bytes48(activeValidatorWithIndex(2).PublicKey()), beaconState.GetPendingDeposits().Get(1).PubKey)
}

func TestPubkeyIndexRandomOperations(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	random := rand.New(rand.NewSource(1))
	universe := make(map[[48]byte]struct{})
	nextValidator := uint64(1)
	newValidator := func() solid.Validator {
		validator := activeValidatorWithIndex(nextValidator)
		universe[validator.PublicKey()] = struct{}{}
		nextValidator++
		return validator
	}
	newValidatorSet := func(count int) *solid.ValidatorSet {
		validators := solid.NewValidatorSet(int(cfg.ValidatorRegistryLimit))
		for range count {
			validators.Append(newValidator())
		}
		return validators
	}
	newState := func(count int) *CachingBeaconState {
		beaconState := New(&cfg)
		beaconState.SetVersion(clparams.AltairVersion)
		for range count {
			require.NoError(t, beaconState.AddValidator(newValidator(), 32_000_000_000))
		}
		require.NoError(t, beaconState.InitBeaconState())
		return beaconState
	}
	states := []*CachingBeaconState{newState(2), newState(2), newState(0)}

	for range 500 {
		target := random.Intn(len(states))
		switch random.Intn(10) {
		case 0:
			require.NoError(t, states[target].AddValidator(newValidator(), 32_000_000_000))
		case 1:
			states[target].AppendValidator(newValidator())
		case 2:
			source := random.Intn(len(states))
			if source == target {
				source = (source + 1) % len(states)
			}
			require.NoError(t, states[source].CopyInto(states[target]))
		case 3:
			states[target].SetValidators(newValidatorSet(2))
		case 4:
			states[target].SetValidatorSet(newValidatorSet(3))
		case 5:
			if states[target].ValidatorLength() > 0 {
				states[target].SetValidatorAtIndex(random.Intn(states[target].ValidatorLength()), newValidator())
			}
		case 6:
			require.NoError(t, states[target].InitBeaconState())
		case 7:
			source := (target + 1) % len(states)
			encoded, err := states[source].EncodeSSZ(nil)
			require.NoError(t, err)
			require.NoError(t, states[target].DecodeSSZ(encoded, int(states[source].Version())))
		case 8:
			require.Error(t, states[target].DecodeSSZ([]byte{1}, int(states[target].Version())))
			assertNoPubkeyIndexLookups(t, states[target], universe)
			source := (target + 1) % len(states)
			require.NoError(t, states[source].CopyInto(states[target]))
		case 9:
			source := (target + 1) % len(states)
			middle := (target + 2) % len(states)
			require.NoError(t, states[source].CopyInto(states[middle]))
			require.NoError(t, states[middle].CopyInto(states[target]))
		}
		for _, beaconState := range states {
			assertPubkeyIndexMatchesUniverse(t, beaconState, universe)
		}
	}
}

func TestPubkeyIndexSharedBaseConcurrentMutation(t *testing.T) {
	source := stateWithValidators(t, 1, 2, 3, 4)
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	readers := make([]*CachingBeaconState, 8)
	for i := range readers {
		readers[i] = New(source.BeaconConfig())
		require.NoError(t, source.CopyInto(readers[i]))
	}

	var waitGroup sync.WaitGroup
	for _, reader := range readers {
		waitGroup.Go(func() {
			for range 1_000 {
				for index := range uint64(4) {
					got, ok := reader.ValidatorIndexByPubkey(activeValidatorWithIndex(index + 1).PublicKey())
					if !ok || got != index {
						t.Errorf("lookup %d returned (%d, %v)", index+1, got, ok)
						return
					}
				}
			}
		})
	}
	for offset := range uint64(90) {
		require.NoError(t, source.AddValidator(activeValidatorWithIndex(offset+10), 32_000_000_000))
	}
	require.NoError(t, source.InitBeaconState())
	require.NoError(t, source.DecodeSSZ(encoded, int(source.Version())))
	waitGroup.Wait()
}

func validatorWithIndex(index uint64) solid.Validator {
	validator := solid.NewValidator()
	var publicKey [48]byte
	binary.LittleEndian.PutUint64(publicKey[:8], index+1)
	validator.SetPublicKey(publicKey)
	return validator
}

func activeValidatorWithIndex(index uint64) solid.Validator {
	validator := validatorWithIndex(index)
	validator.SetEffectiveBalance(32_000_000_000)
	validator.SetActivationEpoch(0)
	validator.SetExitEpoch(math.MaxUint64)
	validator.SetWithdrawableEpoch(math.MaxUint64)
	return validator
}

func validatorSet(indices ...uint64) *solid.ValidatorSet {
	validators := solid.NewValidatorSet(int(clparams.MainnetBeaconConfig.ValidatorRegistryLimit))
	for _, index := range indices {
		validators.Append(activeValidatorWithIndex(index))
	}
	return validators
}

func stateWithValidators(t *testing.T, indices ...uint64) *CachingBeaconState {
	t.Helper()
	beaconState := New(&clparams.MainnetBeaconConfig)
	beaconState.SetVersion(clparams.AltairVersion)
	for _, index := range indices {
		require.NoError(t, beaconState.AddValidator(activeValidatorWithIndex(index), 32_000_000_000))
	}
	require.NoError(t, beaconState.InitBeaconState())
	return beaconState
}

func assertPubkeyIndexMatchesValidators(t *testing.T, beaconState *CachingBeaconState) {
	t.Helper()
	want := pubkeyIndexFromValidators(beaconState)
	for key, index := range want {
		got, ok := beaconState.ValidatorIndexByPubkey(key)
		require.True(t, ok)
		require.Equal(t, index, got)
	}
	_, ok := beaconState.ValidatorIndexByPubkey(activeValidatorWithIndex(math.MaxUint64).PublicKey())
	require.False(t, ok)
}

func assertPubkeyIndexMatchesUniverse(t *testing.T, beaconState *CachingBeaconState, universe map[[48]byte]struct{}) {
	t.Helper()
	want := pubkeyIndexFromValidators(beaconState)
	for key := range universe {
		got, ok := beaconState.ValidatorIndexByPubkey(key)
		wantIndex, wantOK := want[key]
		require.Equal(t, wantOK, ok)
		if wantOK {
			require.Equal(t, wantIndex, got)
		}
	}
}

func pubkeyIndexFromValidators(beaconState *CachingBeaconState) map[[48]byte]uint64 {
	want := make(map[[48]byte]uint64, beaconState.ValidatorLength())
	beaconState.ForEachValidator(func(validator solid.Validator, index, _ int) bool {
		want[validator.PublicKey()] = uint64(index)
		return true
	})
	return want
}

func assertNoPubkeyIndexLookups(t *testing.T, beaconState *CachingBeaconState, universe map[[48]byte]struct{}) {
	t.Helper()
	for key := range universe {
		_, ok := beaconState.ValidatorIndexByPubkey(key)
		require.False(t, ok)
	}
}

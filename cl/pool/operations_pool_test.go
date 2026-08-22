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

package pool

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func bytes96(i int) common.Bytes96 {
	var k common.Bytes96
	binary.LittleEndian.PutUint64(k[:], uint64(i))
	return k
}

// Capacity is a retention window, not a per-block bound: filling a pool past its
// capacity must evict, and the surviving count is what a proposer gets to pack from.
func TestOperationsPoolCapacity(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	pools := NewOperationsPool(&cfg)

	t.Run("attestations hold the retention window", func(t *testing.T) {
		want := attestationRetentionSlots * int(cfg.MaxCommitteesPerSlot*cfg.TargetAggregatorsPerCommittee)
		for i := 0; i <= want; i++ {
			pools.AttestationsPool.Insert(bytes96(i), &solid.Attestation{})
		}
		require.Equal(t, want, len(pools.AttestationsPool.Raw()))
	})

	t.Run("attester slashings", func(t *testing.T) {
		for i := 0; i <= attesterSlashingsCapacity; i++ {
			pools.AttesterSlashingsPool.Insert(bytes96(i), &cltypes.AttesterSlashing{})
		}
		require.Equal(t, attesterSlashingsCapacity, len(pools.AttesterSlashingsPool.Raw()))
	})

	t.Run("proposer slashings", func(t *testing.T) {
		for i := 0; i <= proposerSlashingsCapacity; i++ {
			pools.ProposerSlashingsPool.Insert(bytes96(i), &cltypes.ProposerSlashing{})
		}
		require.Equal(t, proposerSlashingsCapacity, len(pools.ProposerSlashingsPool.Raw()))
	})

	t.Run("bls to execution changes", func(t *testing.T) {
		for i := 0; i <= blsToExecutionChangesCapacity; i++ {
			pools.BLSToExecutionChangesPool.Insert(bytes96(i), &cltypes.SignedBLSToExecutionChange{})
		}
		require.Equal(t, blsToExecutionChangesCapacity, len(pools.BLSToExecutionChangesPool.Raw()))
	})

	t.Run("voluntary exits", func(t *testing.T) {
		for i := 0; i <= voluntaryExitsCapacity; i++ {
			pools.VoluntaryExitsPool.Insert(uint64(i), &cltypes.SignedVoluntaryExit{})
		}
		require.Equal(t, voluntaryExitsCapacity, len(pools.VoluntaryExitsPool.Raw()))
	})
}

// The attestation pool tracks the committee layout of the network it runs on, so a
// smaller preset must not reserve the mainnet window.
func TestAttestationsPoolCapacityFollowsPreset(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	pools := NewOperationsPool(&cfg)

	want := attestationRetentionSlots * int(cfg.MaxCommitteesPerSlot*cfg.TargetAggregatorsPerCommittee)
	require.Less(t, want, attestationsCapacity(&clparams.MainnetBeaconConfig))

	for i := 0; i <= want; i++ {
		pools.AttestationsPool.Insert(bytes96(i), &solid.Attestation{})
	}
	require.Equal(t, want, len(pools.AttestationsPool.Raw()))
}

func TestOperationsPool(t *testing.T) {
	pools := NewOperationsPool(&clparams.MainnetBeaconConfig)

	// AttestationsPool
	pools.AttestationsPool.Insert([96]byte{}, &solid.Attestation{})
	pools.AttestationsPool.Insert([96]byte{1}, &solid.Attestation{})
	require.Len(t, pools.AttestationsPool.Raw(), 2)
	require.True(t, pools.AttestationsPool.DeleteIfExist([96]byte{}))
	require.Len(t, pools.AttestationsPool.Raw(), 1)
	// ProposerSlashingsPool
	slashing1 := &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Signature: [96]byte{1},
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Signature: [96]byte{2},
		},
	}
	slashing2 := &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Signature: [96]byte{3},
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Signature: [96]byte{4},
		},
	}
	pools.ProposerSlashingsPool.Insert(ComputeKeyForProposerSlashing(slashing1), slashing1)
	pools.ProposerSlashingsPool.Insert(ComputeKeyForProposerSlashing(slashing2), slashing2)
	require.True(t, pools.ProposerSlashingsPool.DeleteIfExist(ComputeKeyForProposerSlashing(slashing2)))
	// AttesterSlashingsPool
	attesterSlashing1 := &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			Signature: [96]byte{1},
		},
		Attestation_2: &cltypes.IndexedAttestation{
			Signature: [96]byte{2},
		},
	}
	attesterSlashing2 := &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			Signature: [96]byte{3},
		},
		Attestation_2: &cltypes.IndexedAttestation{
			Signature: [96]byte{4},
		},
	}
	pools.AttesterSlashingsPool.Insert(ComputeKeyForAttesterSlashing(attesterSlashing1), attesterSlashing1)
	pools.AttesterSlashingsPool.Insert(ComputeKeyForAttesterSlashing(attesterSlashing2), attesterSlashing2)
	require.True(t, pools.AttesterSlashingsPool.DeleteIfExist(ComputeKeyForAttesterSlashing(attesterSlashing2)))
	require.Len(t, pools.AttesterSlashingsPool.Raw(), 1)

	// BLSToExecutionChangesPool
	pools.BLSToExecutionChangesPool.Insert([96]byte{}, &cltypes.SignedBLSToExecutionChange{})
	pools.BLSToExecutionChangesPool.Insert([96]byte{1}, &cltypes.SignedBLSToExecutionChange{})
	require.Len(t, pools.BLSToExecutionChangesPool.Raw(), 2)
	require.True(t, pools.BLSToExecutionChangesPool.DeleteIfExist([96]byte{}))
	require.Len(t, pools.BLSToExecutionChangesPool.Raw(), 1)

	require.Len(t, pools.ProposerSlashingsPool.Raw(), 1)
}

func TestOperationsPoolPruneFinalized(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	finalizedState := state.New(&cfg)
	finalizedState.SetSlot(3 * cfg.SlotsPerEpoch)
	for i := range 3 {
		finalizedState.AddValidator(solid.NewValidatorFromParameters(
			common.Bytes48{byte(i)},
			common.Hash{},
			cfg.MaxEffectiveBalance,
			false,
			0,
			0,
			cfg.FarFutureEpoch,
			cfg.FarFutureEpoch,
		), cfg.MaxEffectiveBalance)
	}

	t.Run("proposer slashings", func(t *testing.T) {
		pools := NewOperationsPool(&cfg)
		keep := proposerSlashing(0, 1)
		removeSlashed := proposerSlashing(1, 3)
		removeWithdrawable := proposerSlashing(2, 5)
		missing := proposerSlashing(10, 7)
		pools.ProposerSlashingsPool.Insert(ComputeKeyForProposerSlashing(keep), keep)
		pools.ProposerSlashingsPool.Insert(ComputeKeyForProposerSlashing(removeSlashed), removeSlashed)
		pools.ProposerSlashingsPool.Insert(ComputeKeyForProposerSlashing(removeWithdrawable), removeWithdrawable)
		pools.ProposerSlashingsPool.Insert(ComputeKeyForProposerSlashing(missing), missing)
		require.NoError(t, finalizedState.SetValidatorSlashed(1, true))
		require.NoError(t, finalizedState.SetWithdrawableEpochForValidatorAtIndex(2, 3))

		pools.PruneFinalized(finalizedState, 3)

		require.True(t, pools.ProposerSlashingsPool.Has(ComputeKeyForProposerSlashing(keep)))
		require.False(t, pools.ProposerSlashingsPool.Has(ComputeKeyForProposerSlashing(removeSlashed)))
		require.False(t, pools.ProposerSlashingsPool.Has(ComputeKeyForProposerSlashing(removeWithdrawable)))
		require.True(t, pools.ProposerSlashingsPool.Has(ComputeKeyForProposerSlashing(missing)))
	})

	t.Run("attester slashings retain a slashable intersection member", func(t *testing.T) {
		pools := NewOperationsPool(&cfg)
		partial := attesterSlashing([]uint64{0, 1}, []uint64{0, 1}, 4)
		terminal := attesterSlashing([]uint64{1}, []uint64{1}, 5)
		pools.AttesterSlashingsPool.Insert(ComputeKeyForAttesterSlashing(partial), partial)
		pools.AttesterSlashingsPool.Insert(ComputeKeyForAttesterSlashing(terminal), terminal)

		pools.PruneFinalized(finalizedState, 3)

		require.True(t, pools.AttesterSlashingsPool.Has(ComputeKeyForAttesterSlashing(partial)))
		require.False(t, pools.AttesterSlashingsPool.Has(ComputeKeyForAttesterSlashing(terminal)))
	})

	t.Run("voluntary exits", func(t *testing.T) {
		pools := NewOperationsPool(&cfg)
		pools.VoluntaryExitsPool.Insert(0, voluntaryExit(0))
		pools.VoluntaryExitsPool.Insert(2, voluntaryExit(2))
		pools.VoluntaryExitsPool.Insert(10, voluntaryExit(10))
		finalizedState.SetExitEpochForValidatorAtIndex(2, 4)

		pools.PruneFinalized(finalizedState, 3)

		require.True(t, pools.VoluntaryExitsPool.Has(0))
		require.False(t, pools.VoluntaryExitsPool.Has(2))
		require.True(t, pools.VoluntaryExitsPool.Has(10))
	})

	t.Run("bls to execution changes", func(t *testing.T) {
		pools := NewOperationsPool(&cfg)
		keep := blsChange(0, 6)
		remove := blsChange(2, 7)
		missing := blsChange(10, 8)
		pools.BLSToExecutionChangesPool.Insert(keep.Signature, keep)
		pools.BLSToExecutionChangesPool.Insert(remove.Signature, remove)
		pools.BLSToExecutionChangesPool.Insert(missing.Signature, missing)
		credentials := common.Hash{byte(cfg.ETH1AddressWithdrawalPrefixByte)}
		finalizedState.SetWithdrawalCredentialForValidatorAtIndex(2, credentials)

		pools.PruneFinalized(finalizedState, 3)

		require.True(t, pools.BLSToExecutionChangesPool.Has(keep.Signature))
		require.False(t, pools.BLSToExecutionChangesPool.Has(remove.Signature))
		require.True(t, pools.BLSToExecutionChangesPool.Has(missing.Signature))
	})

	t.Run("gloas builder exits are retained across reusable indices", func(t *testing.T) {
		gloasState := state.New(&cfg)
		gloasState.SetVersion(clparams.GloasVersion)
		builders := solid.NewStaticListSSZ[*cltypes.Builder](
			int(cfg.BuilderRegistryLimit),
			new(cltypes.Builder).EncodingSizeSSZ(),
		)
		builders.Append(&cltypes.Builder{WithdrawableEpoch: cfg.FarFutureEpoch})
		builders.Append(&cltypes.Builder{WithdrawableEpoch: 4})
		gloasState.SetBuilders(builders)
		keepIndex := state.ConvertBuilderIndexToValidatorIndex(0)
		removeIndex := state.ConvertBuilderIndexToValidatorIndex(1)
		pools := NewOperationsPool(&cfg)
		pools.VoluntaryExitsPool.Insert(keepIndex, voluntaryExit(keepIndex))
		pools.VoluntaryExitsPool.Insert(removeIndex, voluntaryExit(removeIndex))

		pools.PruneFinalized(gloasState, 0)

		require.True(t, pools.VoluntaryExitsPool.Has(keepIndex))
		require.True(t, pools.VoluntaryExitsPool.Has(removeIndex))
	})

	t.Run("attestations are not finalized-state pruned", func(t *testing.T) {
		pools := NewOperationsPool(&cfg)
		key := common.Bytes96{1}
		pools.AttestationsPool.Insert(key, &solid.Attestation{})

		pools.PruneFinalized(finalizedState, 3)

		require.True(t, pools.AttestationsPool.Has(key))
	})
}

func TestOperationsPoolPruneFinalizedIgnoresIncompleteEntries(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	finalizedState := state.New(&cfg)
	validator := solid.NewValidator()
	validator.SetSlashed(true)
	finalizedState.AddValidator(validator, 0)
	pools := NewOperationsPool(&cfg)
	pools.ProposerSlashingsPool.Insert(common.Bytes96{1}, nil)
	pools.ProposerSlashingsPool.Insert(common.Bytes96{4}, &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{ProposerIndex: 0},
		},
	})
	pools.AttesterSlashingsPool.Insert(common.Bytes96{2}, nil)
	pools.VoluntaryExitsPool.Insert(0, nil)
	pools.BLSToExecutionChangesPool.Insert(common.Bytes96{3}, nil)

	require.NotPanics(t, func() {
		pools.PruneFinalized(finalizedState, 0)
	})
	require.Len(t, pools.ProposerSlashingsPool.Raw(), 2)
	require.Len(t, pools.AttesterSlashingsPool.Raw(), 1)
	require.Len(t, pools.VoluntaryExitsPool.Raw(), 1)
	require.Len(t, pools.BLSToExecutionChangesPool.Raw(), 1)
}

func TestOperationsPoolPruneFinalizedUsesCheckpointEpoch(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	finalizedState := state.New(&cfg)
	finalizedState.SetSlot(2 * cfg.SlotsPerEpoch)
	validator := solid.NewValidatorFromParameters(
		common.Bytes48{},
		common.Hash{},
		cfg.MaxEffectiveBalance,
		false,
		0,
		0,
		cfg.FarFutureEpoch,
		3,
	)
	finalizedState.AddValidator(validator, cfg.MaxEffectiveBalance)
	slashing := proposerSlashing(0, 1)

	t.Run("before boundary", func(t *testing.T) {
		pools := NewOperationsPool(&cfg)
		pools.ProposerSlashingsPool.Insert(ComputeKeyForProposerSlashing(slashing), slashing)

		pools.PruneFinalized(finalizedState, 2)

		require.True(t, pools.ProposerSlashingsPool.Has(ComputeKeyForProposerSlashing(slashing)))
	})

	t.Run("at skipped-slot checkpoint boundary", func(t *testing.T) {
		pools := NewOperationsPool(&cfg)
		pools.ProposerSlashingsPool.Insert(ComputeKeyForProposerSlashing(slashing), slashing)

		pools.PruneFinalized(finalizedState, 3)

		require.False(t, pools.ProposerSlashingsPool.Has(ComputeKeyForProposerSlashing(slashing)))
	})
}

func TestOperationsPoolPruneFinalizedConcurrentInsert(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	finalizedState := state.New(&cfg)
	validator := solid.NewValidator()
	validator.SetExitEpoch(1)
	finalizedState.AddValidator(validator, 0)
	pools := NewOperationsPool(&cfg)
	pools.VoluntaryExitsPool.Insert(0, voluntaryExit(0))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 100 {
			pools.PruneFinalized(finalizedState, 1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= 100; i++ {
			pools.VoluntaryExitsPool.Insert(i, voluntaryExit(i))
		}
	}()
	wg.Wait()

	require.False(t, pools.VoluntaryExitsPool.Has(0))
}

func proposerSlashing(validatorIndex uint64, signatureByte byte) *cltypes.ProposerSlashing {
	return &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Header:    &cltypes.BeaconBlockHeader{ProposerIndex: validatorIndex},
			Signature: common.Bytes96{signatureByte},
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Header:    &cltypes.BeaconBlockHeader{ProposerIndex: validatorIndex},
			Signature: common.Bytes96{signatureByte + 1},
		},
	}
}

func attesterSlashing(one, two []uint64, signatureByte byte) *cltypes.AttesterSlashing {
	return &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, one),
			Signature:        common.Bytes96{signatureByte},
		},
		Attestation_2: &cltypes.IndexedAttestation{
			AttestingIndices: solid.NewRawUint64List(2048, two),
			Signature:        common.Bytes96{signatureByte + 1},
		},
	}
}

func voluntaryExit(validatorIndex uint64) *cltypes.SignedVoluntaryExit {
	return &cltypes.SignedVoluntaryExit{
		VoluntaryExit: &cltypes.VoluntaryExit{ValidatorIndex: validatorIndex},
	}
}

func blsChange(validatorIndex uint64, signatureByte byte) *cltypes.SignedBLSToExecutionChange {
	return &cltypes.SignedBLSToExecutionChange{
		Message:   &cltypes.BLSToExecutionChange{ValidatorIndex: validatorIndex},
		Signature: common.Bytes96{signatureByte},
	}
}

func TestEpbsPoolGetPreferenceExactLookup(t *testing.T) {
	p := NewEpbsPool()
	slot := uint64(10)
	root := common.Hash{0x01}
	otherRoot := common.Hash{0x02}
	want := &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{ProposalSlot: slot, DependentRoot: root}}
	p.ProposerPreferences.Add(ProposerPreferencesKey{Slot: slot, DependentRoot: root}, want)

	got, ok := p.GetPreference(slot, root)
	require.True(t, ok)
	require.Equal(t, want, got)

	_, ok = p.GetPreference(slot, otherRoot)
	require.False(t, ok)
}

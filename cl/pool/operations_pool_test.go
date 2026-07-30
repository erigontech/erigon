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
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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

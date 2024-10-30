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
	"testing"

	"github.com/erigontech/erigon/v3/cl/clparams"
	"github.com/erigontech/erigon/v3/cl/cltypes"
	"github.com/erigontech/erigon/v3/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestOperationsPool(t *testing.T) {
	pools := NewOperationsPool(&clparams.MainnetBeaconConfig)

	// AttestationsPool
	pools.AttestationsPool.Insert([96]byte{}, &solid.Attestation{})
	pools.AttestationsPool.Insert([96]byte{1}, &solid.Attestation{})
	require.Equal(t, 2, len(pools.AttestationsPool.Raw()))
	require.True(t, pools.AttestationsPool.DeleteIfExist([96]byte{}))
	require.Equal(t, 1, len(pools.AttestationsPool.Raw()))
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
	require.Equal(t, 1, len(pools.AttesterSlashingsPool.Raw()))

	// BLSToExecutionChangesPool
	pools.BLSToExecutionChangesPool.Insert([96]byte{}, &cltypes.SignedBLSToExecutionChange{})
	pools.BLSToExecutionChangesPool.Insert([96]byte{1}, &cltypes.SignedBLSToExecutionChange{})
	require.Equal(t, 2, len(pools.BLSToExecutionChangesPool.Raw()))
	require.True(t, pools.BLSToExecutionChangesPool.DeleteIfExist([96]byte{}))
	require.Equal(t, 1, len(pools.BLSToExecutionChangesPool.Raw()))

	require.Equal(t, 1, len(pools.ProposerSlashingsPool.Raw()))
}

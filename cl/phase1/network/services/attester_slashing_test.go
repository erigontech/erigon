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

package services

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/common"
)

func TestAttesterSlashingServiceIgnoresDisjointIndices(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.Phase0Version, clparams.ElectraVersion} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			anchor := state.New(&cfg)
			anchor.SetVersion(version)
			synced := synced_data.NewSyncedDataManager(&cfg, true)
			opPool := pool.NewOperationsPool(&cfg)
			store, err := forkchoice.NewForkChoiceStore(nil, anchor, nil, opPool, nil, nil, synced, nil, public_keys_registry.NewInMemoryPublicKeysRegistry(), nil, false, nil)
			require.NoError(t, err)
			root, err := anchor.BlockRoot()
			require.NoError(t, err)
			require.NoError(t, synced.OnHeadStateWithBlockRoot(anchor, root))
			slashing := cltypes.NewAttesterSlashing(version)
			slashing.Attestation_1.AttestingIndices.Append(1)
			slashing.Attestation_2.AttestingIndices.Append(2)
			slashing.Attestation_1.Data.BeaconBlockRoot = common.Hash{1}
			slashing.Attestation_2.Data.BeaconBlockRoot = common.Hash{2}

			service := NewAttesterSlashingService(store)
			err = service.ProcessMessage(t.Context(), nil, slashing)

			require.ErrorIs(t, err, ErrIgnore)
			require.Empty(t, opPool.AttesterSlashingsPool.Raw())
		})
	}
}

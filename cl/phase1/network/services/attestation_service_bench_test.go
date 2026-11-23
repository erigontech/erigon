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

package services

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
)

func BenchmarkBeaconCommitteeFromHeadState(b *testing.B) {
	_, st, _ := tests.GetBellatrixRandom()
	syncedData := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(b, syncedData.OnHeadState(st))
	slot := st.Slot()
	target := st.CurrentJustifiedCheckpoint()
	target.Epoch = state.Epoch(st)
	fc := &mock_services.ForkChoiceStorageMock{}
	committee, _, err := beaconCommittee(fc, syncedData, target, st.LatestBlockHeader().Root, slot, 0, st.BeaconConfig().SlotsPerEpoch, nil, nil)
	require.NoError(b, err)
	require.NotEmpty(b, committee)

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := beaconCommittee(fc, syncedData, target, st.LatestBlockHeader().Root, slot, 0, st.BeaconConfig().SlotsPerEpoch, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

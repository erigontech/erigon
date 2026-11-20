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

package fork_graph

import (
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// BenchmarkGetLightClientUpdateRangeDuringNonFinality looks up every period from 0 to the head's in one request, after
// a week without finality: one unfinalized block with a candidate per slot.
func BenchmarkGetLightClientUpdateRangeDuringNonFinality(b *testing.B) {
	cfg := &clparams.MainnetBeaconConfig
	const blocks = 50_400
	firstSlot := 1700 * cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	f := &forkGraphDisk{beaconCfg: cfg}
	var parent, head common.Hash
	for i := range uint64(blocks) {
		slot := firstSlot + i
		binary.BigEndian.PutUint64(head[:], slot)
		f.headers.Store(head, &cltypes.BeaconBlockHeader{Slot: slot, ParentRoot: parent})
		f.recordLightClientUpdateLocked(head, lightClientTestUpdate(cfg, 400, slot-1))
		parent = head
	}
	headPeriod := cfg.SyncCommitteePeriod(firstSlot + blocks - 1)

	b.ResetTimer()
	for range b.N {
		f.GetLightClientUpdates(0, headPeriod+1, head)
	}
}

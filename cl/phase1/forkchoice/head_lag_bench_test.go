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

package forkchoice

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// BenchmarkHasImportedDescendantAfter runs the head lag check over a chain of n blocks on the FULL
// variant of the served head. Served on EMPTY with an available payload, the chain is excluded;
// served on FULL, it is counted, and the check walks to the first block past the tolerance.
func BenchmarkHasImportedDescendantAfter(b *testing.B) {
	for _, n := range []int{128, 7200} {
		for _, served := range []struct {
			name   string
			status cltypes.PayloadStatus
		}{{"EMPTY", cltypes.PayloadStatusEmpty}, {"FULL", cltypes.PayloadStatusFull}} {
			b.Run(fmt.Sprintf("descendants=%d/served=%s", n, served.name), func(b *testing.B) {
				v := newViabilityTree(b)
				head := common.Hash{0xbb}
				v.add(head, v.justified.Root, 64, true, true)
				parent := head
				for i := range n {
					root := common.BytesToHash([]byte(fmt.Sprintf("block-%d", i)))
					v.add(root, parent, uint64(65+i), true, true)
					parent = root
				}
				v.store.timeMs.Store((uint64(64+n+1) + v.store.beaconCfg.SlotsPerEpoch) * v.store.beaconCfg.SecondsPerSlot * 1000)
				require.True(b, v.store.HasImportedDescendantAfter(head, 64) || served.status == cltypes.PayloadStatusEmpty)
				v.store.selectedHead.Store(&ForkChoiceNode{Root: head, PayloadStatus: served.status})
				b.ReportAllocs()
				for b.Loop() {
					v.store.HasImportedDescendantAfter(head, uint64(64+n))
				}
			})
		}
	}
}

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
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func BenchmarkAddPendingDepositsAndHash(b *testing.B) {
	blockRoot := common.Hash{0x01}
	parentRoot := common.Hash{0x02}
	store := newPendingListsTestStore(b, map[common.Hash]*cltypes.BeaconBlockHeader{
		blockRoot: {ParentRoot: parentRoot},
	})
	parent := solid.NewPendingDepositList(store.beaconCfg)
	if _, err := parent.HashSSZ(); err != nil {
		b.Fatal(err)
	}
	store.pendingDeposits.Add(parentRoot, parent)
	source := solid.NewPendingDepositList(store.beaconCfg)
	for i := range 16_532 {
		source.Append(&solid.PendingDeposit{Amount: uint64(i), Slot: uint64(i)})
	}
	if _, err := source.HashSSZ(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := store.addPendingDeposits(blockRoot, source); err != nil {
			b.Fatal(err)
		}
		stored, ok := store.GetPendingDeposits(blockRoot)
		if !ok {
			b.Fatal("pending deposits were not stored")
		}
		if _, err := stored.HashSSZ(); err != nil {
			b.Fatal(err)
		}
	}
}

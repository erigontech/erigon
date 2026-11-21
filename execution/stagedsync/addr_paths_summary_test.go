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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestSummaryMarkReadersDirtyMatchesHeaderWalk(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0xa1"))
	other := accounts.InternAddress(common.HexToAddress("0xa2"))
	slot1 := accounts.InternKey(common.HexToHash("0x01"))
	slot2 := accounts.InternKey(common.HexToHash("0x02"))
	writer := state.Version{TxIndex: 1}
	one := *uint256.NewInt(1)
	writes := map[string]*state.WriteSet{
		"storage":   newWS().stor(addr, slot1, writer, one).build(),
		"other":     newWS().stor(other, slot1, writer, one).build(),
		"balance":   newWS().bal(addr, writer, one).stor(addr, slot2, writer, one).build(),
		"lifecycle": newWS().selfDestruct(addr, writer, true).build(),
		"code":      newWS().code(addr, writer, accounts.Code{}).build(),
	}
	for name, ws := range writes {
		t.Run(name, func(t *testing.T) {
			walk := newDetExecutor(t, 4)
			sum := newDetExecutor(t, 4)
			for _, be := range []*blockExecutor{walk, sum} {
				for _, tx := range []int{2, 3} {
					rs := state.ReadSet{}
					rs.SetHeader(addr, state.StoragePath, slot1, state.ReadHeader{Source: state.StorageRead})
					if tx == 3 {
						rs.SetHeader(addr, state.BalancePath, accounts.StorageKey{}, state.ReadHeader{Source: state.StorageRead})
					}
					be.blockIO.RecordReads(state.Version{TxIndex: tx}, rs)
					be.indexReads(tx, rs)
				}
			}
			walk.markReadersDirty(1, ws, nil)
			sum.markReadersDirty(1, ws, ws.AddrPaths())
			require.Equal(t, walk.revalidate, sum.revalidate)
		})
	}
}

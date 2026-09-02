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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func BenchmarkCalcStateFlushWithAccumulatedAccounts(b *testing.B) {
	const perBlock = 32
	for _, accumulated := range []int{1_000, 100_000, 300_000} {
		b.Run(fmt.Sprintf("accumulated=%d", accumulated), func(b *testing.B) {
			cs := newTestCalcState()
			addrs := make([]accounts.Address, accumulated)
			for i := range addrs {
				var raw common.Address
				binary.BigEndian.PutUint32(raw[16:], uint32(i))
				addrs[i] = accounts.InternAddress(raw)
				cs.ApplyWrites(newWS().bal(addrs[i], state.Version{}, *uint256.NewInt(uint64(i) + 1)).build(), false)
			}
			cs.ResetBlockFlags()
			updates := newTestUpdates()
			defer updates.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w := newWS()
				for j := range perBlock {
					w.nonce(addrs[(i*perBlock+j)%accumulated], state.Version{}, uint64(i))
				}
				cs.ApplyWrites(w.build(), false)
				cs.FlushToUpdates(updates)
				updates.Reset()
				cs.ResetBlockFlags()
			}
		})
	}
}

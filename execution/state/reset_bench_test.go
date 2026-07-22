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

package state

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// BenchmarkIntraBlockStateReset measures the touch-then-Reset cycle the executor
// runs per tx, at a few realistic touched-account counts. The touch phase stays
// inside the timer on purpose: reusing a map's buckets saves the regrow cost on
// the next fill, which timing Reset alone would hide.
func BenchmarkIntraBlockStateReset(b *testing.B) {
	for _, n := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("accounts=%d", n), func(b *testing.B) {
			addrs := make([]accounts.Address, n)
			for i := range addrs {
				addrs[i] = accounts.InternAddress(common.BytesToAddress([]byte{byte(i), byte(i >> 8), 0x5a}))
			}
			ibs := New(nil)
			var one uint256.Int
			one.SetUint64(1)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for _, a := range addrs {
					ibs.nilAccounts[a] = struct{}{}
					ibs.stateObjectsDirty[a] = struct{}{}
					ibs.balanceInc[a] = nil
					ibs.transientStorage.Set(a, accounts.NilKey, one)
				}
				ibs.Reset()
			}
		})
	}
}

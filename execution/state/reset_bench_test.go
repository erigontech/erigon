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

// BenchmarkIntraBlockStateReset measures the per-tx Reset the executor runs
// between transactions, at a few realistic touched-account counts.
func BenchmarkIntraBlockStateReset(b *testing.B) {
	for _, n := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("accounts=%d", n), func(b *testing.B) {
			addrs := make([]accounts.Address, n)
			for i := range addrs {
				addrs[i] = accounts.InternAddress(common.BytesToAddress([]byte{byte(i), byte(i >> 8), 0x5a}))
			}
			ibs := New(nil)
			b.ReportAllocs()
			for b.Loop() {
				for _, a := range addrs {
					ibs.nilAccounts[a] = struct{}{}
					ibs.stateObjectsDirty[a] = struct{}{}
					ibs.balanceInc[a] = &BalanceIncrease{}
					ibs.transientStorage.Set(a, accounts.NilKey, *uint256.NewInt(1))
				}
				ibs.Reset()
			}
		})
	}
}

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

package exec

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// syntheticBAL has one storage change per two slots, the rest are storage reads.
func syntheticBAL(nAccounts, slotsPerAccount int) types.BlockAccessList {
	bal := make(types.BlockAccessList, nAccounts)
	for i := range bal {
		binary.BigEndian.PutUint32(bal[i].Address[16:], uint32(i))
		for j := range slotsPerAccount {
			var h common.Hash
			binary.BigEndian.PutUint32(h[24:], uint32(i))
			binary.BigEndian.PutUint32(h[28:], uint32(j))
			if j%2 == 0 {
				bal[i].StorageChanges = append(bal[i].StorageChanges, types.SlotChanges{Slot: accounts.InternKey(h)})
			} else {
				bal[i].StorageReads = append(bal[i].StorageReads, h)
			}
		}
	}
	return bal
}

func BenchmarkBALPrefetchKeys(b *testing.B) {
	for _, c := range []struct{ accounts, slotsPerAccount int }{{1000, 3}, {10000, 20}} {
		bal := syntheticBAL(c.accounts, c.slotsPerAccount)
		b.Run(fmt.Sprintf("accounts=%d/slots=%d", c.accounts, c.accounts*c.slotsPerAccount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				balPrefetchKeys(bal)
			}
		})
	}
}

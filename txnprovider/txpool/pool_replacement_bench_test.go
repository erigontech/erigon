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

package txpool

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// Each batch replaces every transaction of a sender whose balance covers all of them.
func BenchmarkReplacementBatch(b *testing.B) {
	for _, n := range []uint64{100, 400, 1600} {
		b.Run(fmt.Sprintf("txns=%d", n), func(b *testing.B) {
			ctx, pool, _, _ := newTestPool(b)
			sender := common.Address{1}
			require.NoError(b, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{}, 0, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
			add := func(value uint64) {
				var txns TxnSlots
				for nonce := range n {
					txn := newTestTxnSlot(nonce, 0, 0, 1, 21_000)
					txn.Txn.GetValue().SetUint64(value)
					binary.BigEndian.PutUint64(txn.IDHash[:], value)
					binary.BigEndian.PutUint64(txn.IDHash[8:], nonce)
					txns.Append(txn, sender[:], true)
				}
				_, err := pool.AddLocalTxns(ctx, txns)
				require.NoError(b, err)
			}
			add(0)
			for value := uint64(1); b.Loop(); value++ {
				add(value)
			}
			require.Equal(b, int(n), pool.pending.Len())
		})
	}
}

func BenchmarkFrameReplacementFee(b *testing.B) {
	maximum := *new(uint256.Int).SetAllOne()
	for _, tc := range []struct {
		name        string
		previous    uint256.Int
		replacement uint256.Int
	}{
		{"normal", uint256.Int{1_000_000_000}, uint256.Int{1_100_000_000}},
		{"uint256", uint256.Int{0, 0, 0, 1 << 63}, maximum},
		{"overflow", *new(uint256.Int).SubUint64(&maximum, 1), maximum},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				frameReplacementFee(&tc.previous, &tc.replacement, 10)
			}
		})
	}
}

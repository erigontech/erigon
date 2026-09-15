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

package receipts

import (
	"context"
	"testing"

	"github.com/c2h5oh/datasize"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// BenchmarkGetReceiptsHotBlock: every caller asks for the same cached block, so only the dedup step differs.
func BenchmarkGetReceiptsHotBlock(b *testing.B) {
	txs := make([]types.Transaction, 200)
	for i := range txs {
		txs[i] = types.NewTransaction(uint64(i), common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(1), nil)
	}
	block := types.NewBlock(&types.Header{Number: *uint256.NewInt(1)}, txs, nil, nil, nil, nil)
	receipts := make(types.Receipts, len(txs))
	defer func(dedup string, precheck bool) { receiptsDedup, receiptsPrecheck = dedup, precheck }(receiptsDedup, receiptsPrecheck)

	for _, v := range []struct {
		name, dedup string
		precheck    bool
	}{
		{"mutex", "mutex", false},
		{"mutex-precheck", "mutex", true},
		{"singleflight", "singleflight", false},
		{"singleflight-shared", "singleflight-shared", false},
		{"singleflight-retry", "singleflight-retry", false},
		{"singleflight-chan", "singleflight-chan", false},
	} {
		b.Run(v.name, func(b *testing.B) {
			receiptsDedup, receiptsPrecheck = v.dedup, v.precheck
			cache, err := lru.New[common.Hash, types.Receipts](16)
			if err != nil {
				b.Fatal(err)
			}
			g := &Generator{receiptsCache: cache, receiptCache: newReceiptCache(datasize.MB), blockExecMutex: &loaderMutex[common.Hash]{}}
			g.receiptsCache.Add(block.Hash(), receipts)
			ctx := context.Background()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := g.GetReceipts(ctx, nil, nil, block, eth.ReceiptsOpts{}); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

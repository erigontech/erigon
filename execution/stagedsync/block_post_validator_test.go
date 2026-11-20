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
	"context"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func genReceipts(n int, seed uint64) types.Receipts {
	rng := rand.New(rand.NewPCG(seed, seed))
	receipts := make(types.Receipts, n)
	var cumGas uint64
	for i := range receipts {
		cumGas += 21000 + rng.Uint64N(100000)
		logs := make([]*types.Log, rng.IntN(4))
		for j := range logs {
			var addr common.Address
			var topic common.Hash
			addr[0], topic[0] = byte(rng.Uint32()), byte(rng.Uint32())
			logs[j] = &types.Log{Address: addr, Topics: []common.Hash{topic}, Data: []byte{byte(j)}}
		}
		receipts[i] = &types.Receipt{Type: types.DynamicFeeTxType, Status: types.ReceiptStatusSuccessful, CumulativeGasUsed: cumGas, Logs: logs}
	}
	return receipts
}

func TestReceiptPipelineMatchesBlockEndHashing(t *testing.T) {
	t.Parallel()
	for _, n := range []int{0, 1, 127, 128, 129, 300} {
		fed, want := genReceipts(n, uint64(n)), genReceipts(n, uint64(n))
		p := newReceiptPipeline(context.Background(), n)
		for _, r := range fed {
			p.feed(r)
		}
		got := p.join()
		require.NotNil(t, got, "n=%d", n)

		var wantBloom types.Bloom
		for _, r := range want {
			r.Bloom = types.CreateBloom(types.Receipts{r})
			wantBloom.Or(&r.Bloom)
		}
		require.Equal(t, types.DeriveSha(want), got.Root, "n=%d", n)
		require.Equal(t, wantBloom, got.Bloom, "n=%d", n)
		for i := range fed {
			require.Equal(t, want[i].Bloom, fed[i].Bloom, "n=%d receipt %d", n, i)
		}
	}
}

func TestReceiptPipelineCancelledGivesNoDigest(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	p := newReceiptPipeline(ctx, 2)
	cancel()
	p.feed(genReceipts(1, 1)[0])
	require.Nil(t, p.join())
}

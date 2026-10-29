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

package derivertest

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/execution/types"
)

// ReplayBlobs serves the blobs of the replay's beacon slots.
type ReplayBlobs [][]byte

func (r ReplayBlobs) Blobs(_ context.Context, _ *l1.Header, hashes []common.Hash) ([][]byte, error) {
	return l1.MatchBlobs(r, hashes)
}

// ReplayL1 returns the L1 of the replay: the genesis's L1 reference, then
// its batches' blocks with their posting transactions and logs, joined by
// headers with made-up hashes, finalized at the last.
func ReplayL1(t testing.TB, r *testfixtures.EEZDevReplay, ref *config.L1Reference) *recording.MemoryChain {
	t.Helper()
	from := ref.Number
	recorded := map[uint64]*recording.Block{
		from: {Header: l1.Header{Number: from, Hash: ref.Hash, Time: ref.Timestamp, MixDigest: ref.PrevRandao}},
	}
	var last uint64
	for i := range r.Batches {
		b := &r.Batches[i]
		tx := &types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: uint64(i), To: &r.Registry, Data: b.Transaction.Input},
				ChainID:  *uint256.NewInt(10200),
			},
			BlobVersionedHashes: b.Transaction.BlobVersionedHashes,
		}
		n := uint64(b.L1Block.Number)
		blk := &recording.Block{
			Header: l1.Header{Number: n, Hash: b.L1Block.Hash, ParentHash: b.L1Block.ParentHash, Time: uint64(b.L1Block.Timestamp), MixDigest: b.L1Block.MixHash},
			Txs:    make([]types.Transaction, int(b.Transaction.Index)+1),
		}
		blk.Txs[b.Transaction.Index] = tx
		for _, l := range b.Logs {
			blk.Logs = append(blk.Logs, &types.Log{Address: r.Registry, Topics: l.Topics, Data: l.Data, TxIndex: hexutil.Uint(l.TransactionIndex)})
			if int(l.TransactionIndex) >= len(blk.Txs) {
				blk.Txs = append(blk.Txs, make([]types.Transaction, int(l.TransactionIndex)+1-len(blk.Txs))...)
			}
		}
		recorded[n] = blk
		last = max(last, n)
	}
	hashOf := func(n uint64) common.Hash {
		if b, ok := recorded[n]; ok {
			return b.Header.Hash
		}
		if b, ok := recorded[n+1]; ok {
			return b.Header.ParentHash
		}
		return crypto.Keccak256Hash(append([]byte("replay"), uint256.NewInt(n).Bytes()...))
	}
	block := func(n uint64) *recording.Block {
		if b, ok := recorded[n]; ok {
			return b
		}
		return &recording.Block{Header: l1.Header{Number: n, Hash: hashOf(n), ParentHash: hashOf(n - 1)}}
	}
	c, err := recording.NewMemoryChain(10200, block(from))
	require.NoError(t, err)
	for n := from + 1; n <= last; n++ {
		require.NoError(t, c.Append(block(n)))
	}
	require.NoError(t, c.Finalize(last))
	return c
}

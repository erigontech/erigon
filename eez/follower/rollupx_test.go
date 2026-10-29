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

package follower_test

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/execution/types"
)

// replayBlobs serves the blobs of the replay's beacon slots.
type replayBlobs [][]byte

func (r replayBlobs) Blobs(_ context.Context, _ *l1.Header, hashes []common.Hash) ([][]byte, error) {
	return l1.MatchBlobs(r, hashes)
}

// replayL1 returns the L1 of the replay: the genesis's L1 reference, then
// its batches' blocks with their posting transactions and logs, joined by
// headers with made-up hashes, finalized at the last.
func replayL1(t *testing.T, r *testfixtures.EEZDevReplay, ref *config.L1Reference) *recording.MemoryChain {
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

// TestEEZDevReplay derives eez.dev, the rollupX network on Chiado, from the
// L1 data of its first fourteen settlements, and requires the hash the
// network served for each of its blocks 0 to 2250.
func TestEEZDevReplay(t *testing.T) {
	r, err := testfixtures.LoadEEZDevReplay()
	require.NoError(t, err)
	spec := networks.Chiado699069
	_, chain := derivertest.NewL2Of(t, spec)
	var blobs replayBlobs
	for _, b := range r.Batches {
		blobs = append(blobs, b.Blobs()...)
	}
	cfg := testConfig
	cfg.Blobs = blobs
	f, err := follower.New(replayL1(t, r, &spec.Config.L2.(*config.L2Config).RollupX.L1Reference), chain, spec.Config, cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	for n, want := range r.L2Hashes {
		h, ok, err := chain.CanonicalHash(t.Context(), uint64(n))
		require.NoError(t, err)
		require.True(t, ok, "block %d", n)
		require.Equal(t, want, h, "block %d", n)
	}
	require.Equal(t, uint64(len(r.L2Hashes)-1), f.Deriver().Safe())

	// Block 1950 delivers the deposit, 1968 and 1969 carry the funded
	// account's transactions; every other block is empty.
	txs := map[uint64]common.Hash{
		1950: common.HexToHash("0x4fb4890fd715062e52da029a1c3398fda4c97f47b9b0a8d95178f853cfa02678"),
		1968: common.HexToHash("0x949e74bea9a8452534a946a75a5365c4565d61f8d021644cc4e72a741a076426"),
		1969: common.HexToHash("0xef63849e09d417b44cd5f45872f52646f45a81b5e42d4fa0fc026a353cc00ceb"),
	}
	for n := range uint64(len(r.L2Hashes)) {
		b, err := chain.BlockByNumber(t.Context(), n)
		require.NoError(t, err)
		if want, ok := txs[n]; ok {
			require.Len(t, b.Transactions(), 1, "block %d", n)
			require.Equal(t, want, b.Transactions()[0].Hash(), "block %d", n)
		} else {
			require.Empty(t, b.Transactions(), "block %d", n)
		}
	}
	// The live settlement of 1950 reseeds prevRandao from block 1951 on.
	for n, seed := range map[uint64]string{
		1901: "0xd883bd2bb2e24364d3060325dc2c11d922d5a90ab7c1658ed069def8b3981488",
		1950: "0xd883bd2bb2e24364d3060325dc2c11d922d5a90ab7c1658ed069def8b3981488",
		1951: "0xcc5dc91bf261272d2f0fc10361fd26397e7528957584336558a1c842c38dfc12",
		2100: "0xcc5dc91bf261272d2f0fc10361fd26397e7528957584336558a1c842c38dfc12",
		1:    "0x1af463d6327b8c6b5a5bb581238f0b136d2b93bce04a35b9672734f0383f44ee",
	} {
		h, err := chain.HeaderByNumber(t.Context(), n)
		require.NoError(t, err)
		require.Equal(t, common.HexToHash(seed), h.MixDigest, "block %d", n)
	}
}

func TestL1Reference(t *testing.T) {
	r, err := testfixtures.LoadEEZDevReplay()
	require.NoError(t, err)
	spec := networks.Chiado699069
	ref := spec.Config.L2.(*config.L2Config).RollupX.L1Reference
	_, chain := derivertest.NewL2Of(t, spec)
	other := ref
	other.PrevRandao = common.Hash{1}
	f, err := follower.New(replayL1(t, r, &other), chain, spec.Config, testConfig, log.New())
	require.NoError(t, err)
	require.ErrorIs(t, f.Boot(t.Context()), follower.ErrL1Reference)
}

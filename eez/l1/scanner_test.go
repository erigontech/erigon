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
package l1_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// TestScanChiado reads the live Chiado settlement through the scanner.
func TestScanChiado(t *testing.T) {
	s, err := testfixtures.LoadChiadoSettlement()
	require.NoError(t, err)
	enc, err := testfixtures.LoadChiadoHeader()
	require.NoError(t, err)
	var h types.Header
	require.NoError(t, rlp.DecodeBytes(enc, &h))
	require.Equal(t, s.L1BlockHash, h.Hash())
	tx, err := s.Tx()
	require.NoError(t, err)

	index := uint64(s.Transaction.TransactionIndex)
	block := &recording.Block{
		Header:   l1.Header{Number: h.Number.Uint64(), Hash: h.Hash(), ParentHash: h.ParentHash, Time: h.Time},
		Txs:      make([]types.Transaction, index+1),
		Recorded: true,
	}
	block.Txs[index] = tx
	for _, l := range s.Logs {
		block.Logs = append(block.Logs, &types.Log{
			Address: l.Address, Topics: l.Topics, Data: l.Data,
			BlockNumber: hexutil.Uint64(s.L1BlockNumber), BlockHash: s.L1BlockHash,
			TxHash: s.Transaction.Hash, TxIndex: hexutil.Uint(index), Index: hexutil.Uint(l.LogIndex),
		})
	}
	chain, err := recording.NewMemoryChain(s.L1ChainID, block)
	require.NoError(t, err)

	batches, reached, err := l1.NewScanner(chain, s.Registry, 1).Scan(context.Background(), s.L1BlockNumber, s.L1BlockNumber)
	require.NoError(t, err)
	require.Equal(t, s.L1BlockNumber, reached)
	require.Len(t, batches, 1)
	b := batches[0]
	require.Equal(t, s.Transaction.Hash, b.TxHash)
	require.Equal(t, s.L1BlockHash, b.L1BlockHash)
	require.Equal(t, []protocol.AppliedEntry{{Index: 0, Role: protocol.RoleAnchor}}, b.Settlement.Applied)
	require.Equal(t, s.StateAtBlock.RollupRoot, b.Settlement.FinalState)
	require.Equal(t, b.Batch.Entries[0].RollupUpdates[0].CurrentRoot, b.Settlement.EntryState)
}

var testRegistry = common.HexToAddress("0x5fbdb2315678afecb367f032d93f642f64180aa3")

type scanChain struct {
	*recording.MemoryChain
	postBatch []byte
	settled   common.Hash
}

// newScanChain returns blocks 0..10 where blocks 3 and 8 settle the window-84
// batch and block 5 holds a foreign posting no decoder reads.
func newScanChain(t *testing.T) *scanChain {
	t.Helper()
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	post, err := w.OracleHash("window_post_block_hash")
	require.NoError(t, err)
	sc := &scanChain{MemoryChain: linear(t, 0), postBatch: w.PostBatch, settled: post}
	for n := uint64(1); n <= 10; n++ {
		b := &recording.Block{Header: l1.Header{Number: n, Hash: hashOf(n, 0), ParentHash: hashOf(n-1, 0)}}
		switch n {
		case 3, 8:
			sc.post(t, b, w.PostBatch, true)
		case 5:
			sc.post(t, b, []byte{0xde, 0xad, 0xbe, 0xef}, false)
		}
		require.NoError(t, sc.Append(b))
	}
	return sc
}

// post adds a posting transaction, its settlement root when settles, and its
// BatchPosted log.
func (sc *scanChain) post(t *testing.T, b *recording.Block, input []byte, settles bool) {
	t.Helper()
	index := hexutil.Uint(len(b.Txs))
	b.Txs = append(b.Txs, types.NewTransaction(b.Header.Number, testRegistry, new(uint256.Int), 1_000_000, new(uint256.Int), input))
	if settles {
		root, err := protocol.L2ExecutionPerformedLog(testRegistry, 1, sc.settled, new(big.Int))
		require.NoError(t, err)
		root.TxIndex = index
		b.Logs = append(b.Logs, root)
	}
	posted, err := protocol.BatchPostedLog(testRegistry, common.Hash{}, []uint64{1})
	require.NoError(t, err)
	posted.TxIndex = index
	b.Logs = append(b.Logs, posted)
}

func scanAll(t *testing.T, s *l1.Scanner, from, to uint64) ([]l1.ScannedBatch, int) {
	t.Helper()
	var all []l1.ScannedBatch
	calls := 0
	for from <= to {
		batches, reached, err := s.Scan(context.Background(), from, to)
		require.NoError(t, err)
		require.GreaterOrEqual(t, reached, from)
		all = append(all, batches...)
		from = reached + 1
		calls++
	}
	return all, calls
}

func TestScanRange(t *testing.T) {
	sc := newScanChain(t)
	batches, calls := scanAll(t, l1.NewScanner(sc, testRegistry, 1), 0, 10)
	require.Equal(t, 1, calls)
	require.Len(t, batches, 2)
	for i, n := range []uint64{3, 8} {
		require.Equal(t, n, batches[i].L1Block)
		require.Equal(t, sc.settled, batches[i].Settlement.FinalState)
		require.Equal(t, []protocol.AppliedEntry{{Index: 0, Role: protocol.RoleAnchor}}, batches[i].Settlement.Applied)
		require.Equal(t, sc.postBatch[:4], protocol.PostAndVerifyBatchSelector[:])
	}

	// A source refusing wide ranges is read in narrower ones, to the same result.
	sc.SetMaxLogRange(3)
	narrowed, calls := scanAll(t, l1.NewScanner(sc, testRegistry, 1), 0, 10)
	require.Greater(t, calls, 3)
	require.Equal(t, batches, narrowed)

	// Another rollup's scanner finds nothing settled for it.
	other, _ := scanAll(t, l1.NewScanner(sc, testRegistry, 2), 0, 10)
	require.Empty(t, other)
}

func TestScanUndecodableWinner(t *testing.T) {
	sc := newScanChain(t)
	b := &recording.Block{Header: l1.Header{Number: 11, Hash: hashOf(11, 0), ParentHash: hashOf(10, 0)}}
	sc.post(t, b, []byte{0xde, 0xad, 0xbe, 0xef}, true)
	require.NoError(t, sc.Append(b))
	_, _, err := l1.NewScanner(sc, testRegistry, 1).Scan(context.Background(), 0, 11)
	require.ErrorIs(t, err, l1.ErrUndecodable)
}

// lyingReader answers some calls inconsistently.
type lyingReader struct {
	*recording.MemoryChain
	forkLog bool
	swapTx  bool
}

func (r lyingReader) Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	logs, err := r.MemoryChain.Logs(ctx, from, to, address, topics)
	if err == nil && r.forkLog && len(logs) > 1 {
		logs[1].BlockHash = common.Hash{0x0f}
	}
	return logs, err
}

func (r lyingReader) TransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index uint64) (types.Transaction, error) {
	tx, err := r.MemoryChain.TransactionByBlockHashAndIndex(ctx, blockHash, index)
	if err == nil && r.swapTx {
		return types.NewTransaction(99, testRegistry, new(uint256.Int), 1, new(uint256.Int), tx.GetData()), nil
	}
	return tx, err
}

func TestScanRejectsInconsistentSources(t *testing.T) {
	sc := newScanChain(t)
	_, _, err := l1.NewScanner(lyingReader{MemoryChain: sc.MemoryChain, forkLog: true}, testRegistry, 1).Scan(context.Background(), 0, 10)
	require.ErrorIs(t, err, l1.ErrSourceIncomplete)
	_, _, err = l1.NewScanner(lyingReader{MemoryChain: sc.MemoryChain, swapTx: true}, testRegistry, 1).Scan(context.Background(), 0, 10)
	require.ErrorIs(t, err, l1.ErrInconsistent)
}

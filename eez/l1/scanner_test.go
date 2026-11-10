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
	"errors"
	"fmt"
	"math/big"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// chiadoChain returns the live Chiado settlement and an L1 chain holding its
// block, header, posting transaction and logs.
func chiadoChain(t *testing.T) (*testfixtures.ChiadoSettlement, *recording.MemoryChain) {
	t.Helper()
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
	return s, chain
}

// TestScanChiado reads the live Chiado settlement through the scanner.
func TestScanChiado(t *testing.T) {
	s, chain := chiadoChain(t)
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
// batch and block 5 holds another rollup's posting no decoder reads.
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
			sc.postTx(t, b, unsigned(n, testRegistry, []byte{0xde, 0xad, 0xbe, 0xef}), common.Hash{}, []uint64{3}, false)
		}
		require.NoError(t, sc.Append(b))
	}
	return sc
}

func unsigned(nonce uint64, to common.Address, input []byte) types.Transaction {
	return types.NewTransaction(nonce, to, new(uint256.Int), 1_000_000, new(uint256.Int), input)
}

// post adds a posting transaction to the registry, its settlement root when
// settles, and its BatchPosted log listing rollup 1.
func (sc *scanChain) post(t *testing.T, b *recording.Block, input []byte, settles bool) {
	t.Helper()
	sc.postTx(t, b, unsigned(b.Header.Number, testRegistry, input), common.Hash{}, []uint64{1}, settles)
}

// postTx adds tx, rollup 1's settlement root when settles, and a BatchPosted
// log reporting spi for rollups.
func (sc *scanChain) postTx(t *testing.T, b *recording.Block, tx types.Transaction, spi common.Hash, rollups []uint64, settles bool) {
	t.Helper()
	index := hexutil.Uint(len(b.Txs))
	b.Txs = append(b.Txs, tx)
	if settles {
		root, err := protocol.L2ExecutionPerformedLog(testRegistry, 1, sc.settled, new(big.Int))
		require.NoError(t, err)
		root.TxIndex = index
		b.Logs = append(b.Logs, root)
	}
	posted, err := protocol.BatchPostedLog(testRegistry, spi, rollups)
	require.NoError(t, err)
	posted.TxIndex = index
	b.Logs = append(b.Logs, posted)
}

// block11 returns block 11 on sc's chain, with a timestamp and parent a live
// post binds.
func block11() *recording.Block {
	return &recording.Block{Header: l1.Header{Number: 11, Hash: hashOf(11, 0), ParentHash: hashOf(10, 0), Time: 1_700_000_000}}
}

// window84 returns the window-84 batch with edit applied, and its input.
func window84(t *testing.T, edit func(*protocol.ProofSystemBatch)) (*protocol.ProofSystemBatch, []byte) {
	t.Helper()
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	b, err := protocol.DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	edit(b)
	input, err := protocol.EncodePostAndVerifyBatch(b)
	require.NoError(t, err)
	return b, input
}

// liveInput returns the sharedPublicInput of the live batch b posted in
// block11 by sender, every listed rollup binding customData[i].
func liveInput(t *testing.T, b *protocol.ProofSystemBatch, customData [][]byte, sender common.Address) common.Hash {
	t.Helper()
	p, err := protocol.NewPublicInputs(b, nil, customData, make([][]common.Hash, len(customData)), sender)
	require.NoError(t, err)
	return p.SharedHash()
}

// block11Context is the custom data a live post in block11 binds.
func block11Context(t *testing.T) []byte {
	t.Helper()
	h := block11().Header
	data, err := protocol.SettlementCustomData(protocol.LiveBlockNumber, h.Time, h.ParentHash)
	require.NoError(t, err)
	return data
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

// refusing counts the log queries its source refused as too wide.
type refusing struct {
	l1.ChainReader
	refused int
}

func (r *refusing) Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	logs, err := r.ChainReader.Logs(ctx, from, to, address, topics)
	if errors.Is(err, l1.ErrRangeTooWide) {
		r.refused++
	}
	return logs, err
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

	// A source refusing wide ranges is read in narrower ones, to the same
	// result; only the first scan probes for the width.
	sc.SetMaxLogRange(3)
	src := &refusing{ChainReader: sc}
	narrowed, calls := scanAll(t, l1.NewScanner(src, testRegistry, 1), 0, 10)
	require.Greater(t, calls, 3)
	require.Equal(t, batches, narrowed)
	require.Equal(t, 2, src.refused)

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

// contextReader serves headers and logs altered by its hooks.
type contextReader struct {
	*recording.MemoryChain
	header func(*l1.Header)
	posted func(data []byte)
	// unsettled drops every log but BatchPosted.
	unsettled bool
}

func (r contextReader) HeaderByHash(ctx context.Context, hash common.Hash) (*l1.Header, error) {
	h, err := r.MemoryChain.HeaderByHash(ctx, hash)
	if err == nil && r.header != nil {
		r.header(h)
	}
	return h, err
}

func (r contextReader) Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	logs, err := r.MemoryChain.Logs(ctx, from, to, address, topics)
	var out []*types.Log
	for _, l := range logs {
		posted := l.Topics[0] == protocol.BatchPostedTopic
		if r.unsettled && !posted {
			continue
		}
		if r.posted != nil && posted {
			l.Data = slices.Clone(l.Data)
			r.posted(l.Data)
		}
		out = append(out, l)
	}
	return out, err
}

// requireClass requires err to carry want and no other scan error class.
func requireClass(t *testing.T, err, want error) {
	t.Helper()
	require.ErrorIs(t, err, want)
	for _, class := range []error{l1.ErrSourceIncomplete, l1.ErrUndecodable, l1.ErrUnsupported} {
		if !errors.Is(class, want) {
			require.NotErrorIs(t, err, class)
		}
	}
}

// TestScanLiveContext is T-DRV-3 on a live post: the Chiado settlement's
// sharedPublicInput binds the timestamp and parent of the L1 block that
// includes it, which the scanner reads from that block's header, whether or
// not the post settled anything. A header served for another block is
// retried; one that matches the logs yet gives another input is permanent.
func TestScanLiveContext(t *testing.T) {
	s, chain := chiadoChain(t)
	scan := func(r l1.ChainReader) ([]l1.ScannedBatch, error) {
		batches, _, err := l1.NewScanner(r, s.Registry, 1).Scan(t.Context(), s.L1BlockNumber, s.L1BlockNumber)
		return batches, err
	}
	want, err := scan(chain)
	require.NoError(t, err)
	require.Len(t, want, 1)
	require.Equal(t, uint64(protocol.LiveBlockNumber), want[0].Batch.BlockNumber)
	got, err := scan(contextReader{MemoryChain: chain, header: func(*l1.Header) {}})
	require.NoError(t, err)
	require.Equal(t, want, got)
	got, err = scan(contextReader{MemoryChain: chain, unsettled: true})
	require.NoError(t, err)
	require.Empty(t, got)

	timestamp := func(h *l1.Header) { h.Time++ }
	for name, c := range map[string]struct {
		r    contextReader
		want error
	}{
		"parent":              {contextReader{header: func(h *l1.Header) { h.ParentHash[31] ^= 1 }}, l1.ErrUndecodable},
		"timestamp":           {contextReader{header: timestamp}, l1.ErrUndecodable},
		"sharedPublicInput":   {contextReader{posted: func(data []byte) { data[31] ^= 1 }}, l1.ErrUndecodable},
		"unsettled timestamp": {contextReader{header: timestamp, unsettled: true}, l1.ErrUndecodable},
		"zero timestamp":      {contextReader{header: func(h *l1.Header) { h.Time = 0 }}, l1.ErrUnsupported},
		"zero parent":         {contextReader{header: func(h *l1.Header) { h.ParentHash = common.Hash{} }}, l1.ErrUnsupported},
		"number":              {contextReader{header: func(h *l1.Header) { h.Number++ }}, l1.ErrSourceIncomplete},
		"hash":                {contextReader{header: func(h *l1.Header) { h.Hash[31] ^= 1 }}, l1.ErrSourceIncomplete},
	} {
		t.Run(name, func(t *testing.T) {
			c.r.MemoryChain = chain
			_, err := scan(c.r)
			requireClass(t, err, c.want)
		})
	}
}

// TestScanLiveRollups is T-DRV-3 on a live post listing two rollups: each
// binds the inclusion context. A mismatch there may come from the other
// rollup's manager, which the scanner cannot verify.
func TestScanLiveRollups(t *testing.T) {
	b, input := window84(t, func(b *protocol.ProofSystemBatch) {
		b.BlockNumber = protocol.LiveBlockNumber
		b.RollupIDsWithProofSystems = []protocol.RollupIDWithProofSystems{{RollupID: 1, ProofSystemIndexes: []uint64{0}}, {RollupID: 2, ProofSystemIndexes: []uint64{0}}}
	})
	data := block11Context(t)
	for name, c := range map[string]struct {
		spi  common.Hash
		want error
	}{
		"both bind the context":     {liveInput(t, b, [][]byte{data, data}, common.Address{}), nil},
		"the other binds nothing":   {liveInput(t, b, [][]byte{data, nil}, common.Address{}), l1.ErrUnsupported},
		"the other binds its own":   {liveInput(t, b, [][]byte{data, {1}}, common.Address{}), l1.ErrUnsupported},
		"neither binds the context": {liveInput(t, b, [][]byte{nil, nil}, common.Address{}), l1.ErrUnsupported},
	} {
		t.Run(name, func(t *testing.T) {
			sc := newScanChain(t)
			block := block11()
			sc.postTx(t, block, unsigned(11, testRegistry, input), c.spi, []uint64{1, 2}, true)
			require.NoError(t, sc.Append(block))
			batches, _, err := l1.NewScanner(sc, testRegistry, 1).Scan(t.Context(), 11, 11)
			if c.want != nil {
				requireClass(t, err, c.want)
				return
			}
			require.NoError(t, err)
			require.Len(t, batches, 1)
			require.Equal(t, uint64(protocol.LiveBlockNumber), batches[0].Batch.BlockNumber)
		})
	}
}

// TestScanLiveSender is T-DRV-3 on a live post binding its sender: the
// registry's msg.sender is the signer of the posting transaction.
func TestScanLiveSender(t *testing.T) {
	key, err := crypto.HexToECDSA("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
	require.NoError(t, err)
	b, input := window84(t, func(b *protocol.ProofSystemBatch) {
		b.BlockNumber = protocol.LiveBlockNumber
		b.BindMsgSenderInPublicInput = true
	})
	tx, err := types.SignTx(unsigned(0, testRegistry, input), *types.LatestSignerForChainID(uint256.NewInt(7331)), key)
	require.NoError(t, err)
	data := block11Context(t)
	for name, c := range map[string]struct {
		sender common.Address
		want   error
	}{
		"signer":  {crypto.PubkeyToAddress(key.PublicKey), nil},
		"another": {common.HexToAddress("0xbeef"), l1.ErrUndecodable},
		"none":    {common.Address{}, l1.ErrUndecodable},
	} {
		t.Run(name, func(t *testing.T) {
			sc := newScanChain(t)
			block := block11()
			sc.postTx(t, block, tx, liveInput(t, b, [][]byte{data}, c.sender), []uint64{1}, true)
			require.NoError(t, sc.Append(block))
			batches, _, err := l1.NewScanner(sc, testRegistry, 1).Scan(t.Context(), 11, 11)
			if c.want != nil {
				requireClass(t, err, c.want)
				return
			}
			require.NoError(t, err)
			require.Len(t, batches, 1)
		})
	}
}

// TestScanBlockNumber is T-DRV-3 on the batch's blockNumber: a post listing
// the rollup is timeless (zero) or live (the maximum); any other value binds
// L1 state the scanner cannot read, whether or not the post settled
// anything. A post that does not list the rollup is not checked, and the
// rollupX scanner reads blockNumber as an anchor instead.
func TestScanBlockNumber(t *testing.T) {
	for _, n := range []uint64{1, 5, protocol.LiveBlockNumber - 1} {
		for _, settles := range []bool{true, false} {
			t.Run(fmt.Sprintf("%d settles %v", n, settles), func(t *testing.T) {
				_, input := window84(t, func(b *protocol.ProofSystemBatch) { b.BlockNumber = n })
				sc := newScanChain(t)
				block := block11()
				sc.post(t, block, input, settles)
				require.NoError(t, sc.Append(block))

				_, _, err := l1.NewScanner(sc, testRegistry, 1).Scan(t.Context(), 0, 11)
				requireClass(t, err, l1.ErrUnsupported)
				other, _ := scanAll(t, l1.NewScanner(sc, testRegistry, 2), 0, 11)
				require.Empty(t, other)
				x := l1.NewScanner(sc, testRegistry, 1)
				x.UseRollupX()
				batches, _ := scanAll(t, x, 11, 11)
				want := 0
				if settles {
					want = 1
				}
				require.Len(t, batches, want)
			})
		}
	}
}

// TestScanPostRouting: BatchPosted, which lists the rollups the registry
// verified, decides whose post it is. A post it does not list for the rollup
// is skipped whatever its transaction's input says. One it lists must be a
// direct call whose input verifies the same rollups: a post made through a
// contract cannot be read from its transaction.
func TestScanPostRouting(t *testing.T) {
	forwarder := common.HexToAddress("0x0f0f")
	for _, n := range []uint64{0, 5, protocol.LiveBlockNumber} {
		t.Run(fmt.Sprintf("blockNumber %d", n), func(t *testing.T) {
			_, input := window84(t, func(b *protocol.ProofSystemBatch) { b.BlockNumber = n })
			sc := newScanChain(t)
			block := block11()
			sc.postTx(t, block, unsigned(11, forwarder, input), common.Hash{0xaa}, []uint64{2}, false)
			require.NoError(t, sc.Append(block))

			batches, _ := scanAll(t, l1.NewScanner(sc, testRegistry, 1), 0, 11)
			require.Len(t, batches, 2)
			_, _, err := l1.NewScanner(sc, testRegistry, 2).Scan(t.Context(), 0, 11)
			requireClass(t, err, l1.ErrUnsupported)
		})
	}

	sc := newScanChain(t)
	block := block11()
	sc.postTx(t, block, unsigned(11, testRegistry, sc.postBatch), common.Hash{}, []uint64{1, 2}, true)
	require.NoError(t, sc.Append(block))
	_, _, err := l1.NewScanner(sc, testRegistry, 1).Scan(t.Context(), 11, 11)
	requireClass(t, err, l1.ErrUndecodable)
}

// unserved is a source that does not serve the logs of blocks below logsFrom,
// nor the transactions of block txGone, nor the header of block headerGone;
// with lagging, it serves logs one block at a time only.
type unserved struct {
	*recording.MemoryChain
	logsFrom   uint64
	txGone     common.Hash
	headerGone common.Hash
	lagging    bool
}

func (r unserved) HeaderByHash(ctx context.Context, hash common.Hash) (*l1.Header, error) {
	if hash == r.headerGone {
		return nil, fmt.Errorf("%w: block %x", l1.ErrNotFound, hash)
	}
	return r.MemoryChain.HeaderByHash(ctx, hash)
}

func (r unserved) Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	if from < r.logsFrom || (r.lagging && to > from) {
		return nil, fmt.Errorf("%w: logs of [%d, %d]", l1.ErrNotFound, from, to)
	}
	return r.MemoryChain.Logs(ctx, from, to, address, topics)
}

func (r unserved) TransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index uint64) (types.Transaction, error) {
	if blockHash == r.txGone {
		return nil, fmt.Errorf("%w: transactions of block %x", l1.ErrNotFound, blockHash)
	}
	return r.MemoryChain.TransactionByBlockHashAndIndex(ctx, blockHash, index)
}

// TestScanUnserved: a scan reports the first block it needs that the source
// does not serve, whether its logs, a posting transaction or the header of a
// block holding a settlement; a range refused while its first block is
// served names none.
func TestScanUnserved(t *testing.T) {
	sc := newScanChain(t)
	var u *l1.UnservedError
	_, _, err := l1.NewScanner(unserved{MemoryChain: sc.MemoryChain, logsFrom: 5}, testRegistry, 1).Scan(t.Context(), 2, 10)
	require.ErrorAs(t, err, &u)
	require.Equal(t, uint64(2), u.Block)
	require.ErrorIs(t, err, l1.ErrNotFound)

	h, err := sc.HeaderByNumber(t.Context(), 8)
	require.NoError(t, err)
	_, _, err = l1.NewScanner(unserved{MemoryChain: sc.MemoryChain, txGone: h.Hash}, testRegistry, 1).Scan(t.Context(), 0, 10)
	require.ErrorAs(t, err, &u)
	require.Equal(t, uint64(8), u.Block)
	_, _, err = l1.NewScanner(unserved{MemoryChain: sc.MemoryChain, headerGone: h.Hash}, testRegistry, 1).Scan(t.Context(), 0, 10)
	require.ErrorAs(t, err, &u)
	require.Equal(t, uint64(8), u.Block)

	_, _, err = l1.NewScanner(unserved{MemoryChain: sc.MemoryChain, lagging: true}, testRegistry, 1).Scan(t.Context(), 0, 10)
	require.ErrorIs(t, err, l1.ErrNotFound)
	require.NotErrorAs(t, err, &u, "names no block")
}

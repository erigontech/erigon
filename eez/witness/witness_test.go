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

package witness_test

import (
	"context"
	"encoding/json"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/eez/witness"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/txnprovider"
)

// pool yields its transactions once each.
type pool struct {
	mu  sync.Mutex
	txs []types.Transaction
}

func (p *pool) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	txs := p.txs
	p.txs = nil
	return txs, nil
}

// user signs the devnet user's transactions.
type user struct {
	t     *testing.T
	nonce uint64
}

func (u *user) tx(to *common.Address, data []byte) types.Transaction {
	cc := derivertest.Config()
	signer := types.LatestSignerForChainID(cc.ChainID)
	tx, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: u.nonce, GasLimit: 200_000, To: to, Data: data},
		ChainID:  *cc.ChainID,
		TipCap:   *uint256.NewInt(1),
		FeeCap:   *uint256.NewInt(10 * common.GWei),
	}, *signer, derivertest.UserKey)
	require.NoError(u.t, err)
	sender, err := signer.Sender(tx)
	require.NoError(u.t, err)
	tx.SetSender(sender)
	u.nonce++
	return tx
}

// store returns a call setting slot to value: the contract stores the
// second calldata word at the slot the first names.
func store(slot, value uint64) []byte {
	data := make([]byte, 64)
	uint256.NewInt(slot).PutUint256(data[:32])
	uint256.NewInt(value).PutUint256(data[32:])
	return data
}

// TestCapture is T-SEQ-4 on Erigon's side: every produced block's witness,
// with slots set, cleared then refilled, and cleared beside a new one,
// replays to the header root and to a state root after each transaction on
// the stateless executor; the store serves it by block and prunes what L1
// finality covers.
func TestCapture(t *testing.T) {
	c := derivertest.NewComposer(t)
	m, chain := derivertest.NewL2(t)
	f, err := follower.New(c.L1, chain, derivertest.Config(), follower.DefaultConfig, log.New())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = f.Run(ctx) }()

	st, err := witness.Open(t.Context(), t.TempDir(), log.New())
	require.NoError(t, err)
	defer st.Close()
	var finalized uint64
	capture := witness.NewCapture(jsonrpc.NewHeadWitnesses(m.DB, m.BlockReader, m.Engine), st, func() uint64 { return finalized })
	p := &pool{}
	timing := sequencer.Timing{L1: 12 * time.Second, L2: 2 * time.Second, Proof: 4 * time.Second, Slack: 1500 * time.Millisecond}
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: timing, Witnesses: capture}, f, p, log.New())
	require.NoError(t, err)

	u := &user{t: t}
	// A contract storing calldata word 1 at the slot calldata word 0 names.
	deploy := common.FromHex("0x67602035600035550060005260086018f3")
	contract := types.CreateAddress(crypto.PubkeyToAddress(derivertest.UserKey.PublicKey), 0)
	// With two slots, clearing one collapses the storage trie onto the other
	// after that transaction, though the block refills or replaces it.
	blocks := [][]types.Transaction{
		{u.tx(nil, deploy)},
		{u.tx(&contract, store(1, 1)), u.tx(&contract, store(2, 2))},
		{u.tx(&contract, store(1, 0)), u.tx(&contract, store(1, 7))},
		{u.tx(&contract, store(2, 0)), u.tx(&contract, store(3, 3))},
	}
	for i, txs := range blocks {
		p.mu.Lock()
		p.txs = txs
		p.mu.Unlock()
		require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 1_000}))
		head, err := f.Head(t.Context())
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), head.Number.Uint64())
	}

	slot := func(n uint64) common.Hash {
		v, err := chain.Storage(t.Context(), uint64(len(blocks)), contract, common.BigToHash(new(big.Int).SetUint64(n)))
		require.NoError(t, err)
		return v
	}
	require.Equal(t, []common.Hash{common.BigToHash(big.NewInt(7)), {}, common.BigToHash(big.NewInt(3))}, []common.Hash{slot(1), slot(2), slot(3)})
	for n := uint64(1); n <= uint64(len(blocks)); n++ {
		block, err := chain.BlockByNumber(t.Context(), n)
		require.NoError(t, err)
		require.Len(t, block.Transactions(), len(blocks[n-1]), "block %d", n)
		receipts, err := chain.Receipts(t.Context(), block)
		require.NoError(t, err)
		for _, r := range receipts {
			require.Equal(t, types.ReceiptStatusSuccessful, r.Status, "block %d", n)
		}
		replay(t, st, m.Engine, block)

		other, err := st.Get(t.Context(), n, common.Hash{1})
		require.NoError(t, err)
		require.Nil(t, other, "the hash is checked")
	}

	// The next block's record prunes what finality covers.
	finalized = 3
	require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 1_000}))
	for n := uint64(1); n <= uint64(len(blocks))+1; n++ {
		block, err := chain.BlockByNumber(t.Context(), n)
		require.NoError(t, err)
		raw, err := st.Get(t.Context(), n, block.Hash())
		require.NoError(t, err)
		require.Equal(t, n >= 3, raw != nil, "block %d", n)
	}
}

// replay checks that st holds the witness of block and that it replays to
// the header root and to a state root after each transaction.
func replay(t *testing.T, st *witness.Store, engine rules.Engine, block *types.Block) {
	t.Helper()
	raw, err := st.Get(t.Context(), block.NumberU64(), block.Hash())
	require.NoError(t, err)
	require.NotNil(t, raw, "block %d has a witness", block.NumberU64())
	var w struct{ State, Codes, Headers []hexutil.Bytes }
	require.NoError(t, json.Unmarshal(raw, &w))
	sw := &stateless.Witness{}
	for _, b := range w.State {
		sw.State = append(sw.State, b)
	}
	for _, b := range w.Codes {
		sw.Codes = append(sw.Codes, b)
	}
	for _, b := range w.Headers {
		sw.Headers = append(sw.Headers, b)
	}
	res, err := stateless.Verify(derivertest.Config(), engine, block, sw, stateless.WithTxStateRoots())
	require.NoError(t, err, "block %d", block.NumberU64())
	require.Len(t, res.TxStateRoots, len(block.Transactions()))
}

// lateGenesis executes genesis once the first pin is taken, as the node's
// first stage loop can while the sequencer waits on the execution module.
type lateGenesis struct {
	*witness.Capture
	execute func(context.Context) error
	once    sync.Once
	err     error
}

func (w *lateGenesis) Pin(ctx context.Context) (interface{ Close() }, error) {
	pin, err := w.Capture.Pin(ctx)
	w.once.Do(func() { w.err = w.execute(ctx) })
	return pin, err
}

// TestCaptureUnexecutedGenesis produces block 1 on a node that wrote genesis
// without executing it and executes it only after the sequencer's first pin:
// the follower's boot executes genesis, so that pin holds its state and block
// 1 has a witness.
func TestCaptureUnexecutedGenesis(t *testing.T) {
	c := derivertest.NewComposer(t)
	m, chain := derivertest.NewL2Of(t, networks.Devnet, execmoduletester.WithUnexecutedGenesis())
	cfg := follower.DefaultConfig
	cfg.BootAttempts = 1
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	// The sequencer's first trigger follows an L1 head, which Run hands over
	// once booted.
	require.NoError(t, f.Boot(t.Context()))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = f.Run(ctx) }()

	st, err := witness.Open(t.Context(), t.TempDir(), log.New())
	require.NoError(t, err)
	defer st.Close()
	genesis, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	g := genesis.Hash()
	capture := &lateGenesis{
		Capture: witness.NewCapture(jsonrpc.NewHeadWitnesses(m.DB, m.BlockReader, m.Engine), st, func() uint64 { return 0 }),
		execute: func(ctx context.Context) error { return chain.ForkChoice(ctx, g, g, g) },
	}
	p := &pool{}
	timing := sequencer.Timing{L1: 12 * time.Second, L2: 2 * time.Second, Proof: 4 * time.Second, Slack: 1500 * time.Millisecond}
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: timing, Witnesses: capture}, f, p, log.New())
	require.NoError(t, err)

	u := &user{t: t}
	p.txs = []types.Transaction{u.tx(&common.Address{1}, nil)}
	require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 1_000}))
	require.NoError(t, capture.err)
	block, err := chain.BlockByNumber(t.Context(), 1)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.Len(t, block.Transactions(), 1)
	replay(t, st, m.Engine, block)
}

// stopping ends the sequencer's context at its first request, as a node
// stopping while the sequencer builds does.
type stopping struct {
	stop context.CancelFunc
	txs  []types.Transaction
}

func (s *stopping) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	s.stop()
	txs := s.txs
	s.txs = nil
	return txs, nil
}

// TestCaptureWhileStopping: a block that commits while the sequencer stops
// keeps its witness; nothing can build it later.
func TestCaptureWhileStopping(t *testing.T) {
	c := derivertest.NewComposer(t)
	m, chain := derivertest.NewL2(t)
	f, err := follower.New(c.L1, chain, derivertest.Config(), follower.DefaultConfig, log.New())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = f.Run(ctx) }()

	st, err := witness.Open(t.Context(), t.TempDir(), log.New())
	require.NoError(t, err)
	defer st.Close()
	capture := witness.NewCapture(jsonrpc.NewHeadWitnesses(m.DB, m.BlockReader, m.Engine), st, func() uint64 { return 0 })
	seqCtx, stop := context.WithCancel(t.Context())
	u := &user{t: t}
	p := &stopping{stop: stop, txs: []types.Transaction{u.tx(&common.Address{1}, nil)}}
	timing := sequencer.Timing{L1: 12 * time.Second, L2: 2 * time.Second, Proof: 4 * time.Second, Slack: 1500 * time.Millisecond}
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: timing, Witnesses: capture}, f, p, log.New())
	require.NoError(t, err)

	require.NoError(t, s.Handle(seqCtx, sequencer.LiveTick{SyncHeight: 1_000}))
	block, err := chain.BlockByNumber(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 1)
	replay(t, st, m.Engine, block)
}

// replaced has each produced block replaced as soon as the command that
// committed it ends, as derivation can replace it.
type replaced struct {
	*follower.Follower
}

func (r replaced) Produce(ctx context.Context, in deriver.HeaderInputs, txs txnprovider.TxnProvider, hook follower.CommitHook) (*types.BlockWithReceipts, error) {
	built, err := r.Follower.Produce(ctx, in, txs, hook)
	if err != nil {
		return nil, err
	}
	return built, r.HeadTo(ctx, in.ParentHash)
}

// TestCaptureBeforeDerivation: a produced block's witness is kept before
// derivation can replace the block, after which nothing builds it.
func TestCaptureBeforeDerivation(t *testing.T) {
	c := derivertest.NewComposer(t)
	m, chain := derivertest.NewL2(t)
	f, err := follower.New(c.L1, chain, derivertest.Config(), follower.DefaultConfig, log.New())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = f.Run(ctx) }()

	st, err := witness.Open(t.Context(), t.TempDir(), log.New())
	require.NoError(t, err)
	defer st.Close()
	capture := witness.NewCapture(jsonrpc.NewHeadWitnesses(m.DB, m.BlockReader, m.Engine), st, func() uint64 { return 0 })
	u := &user{t: t}
	p := &pool{txs: []types.Transaction{u.tx(&common.Address{1}, nil)}}
	timing := sequencer.Timing{L1: 12 * time.Second, L2: 2 * time.Second, Proof: 4 * time.Second, Slack: 1500 * time.Millisecond}
	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: timing, Witnesses: capture}, replaced{f}, p, log.New())
	require.NoError(t, err)

	require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 1_000}))
	produced := s.Status().Last
	require.NotNil(t, produced)
	head, err := f.Head(t.Context())
	require.NoError(t, err)
	require.NotEqual(t, produced.Hash(), head.Hash(), "the produced block was replaced")
	raw, err := st.Get(t.Context(), produced.Number.Uint64(), produced.Hash())
	require.NoError(t, err)
	require.NotNil(t, raw, "the replaced block has no witness")
}

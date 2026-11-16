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

package composer

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/l1/post"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

// memL1 is an L1 whose every block has state m.
type memL1 struct {
	m      *memState
	failed error
}

func (l *memL1) ViewState(_ context.Context, hash common.Hash, read func(*types.Header, state.StateReader) error) error {
	if l.failed != nil {
		return l.failed
	}
	h := &types.Header{Number: *uint256.NewInt(100), Time: 1_800_000_000, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1), Extra: []byte{}}
	return read(h, l.m)
}

var crossProxy = common.HexToAddress("0xaaaa0000000000000000000000000000000000ff")

// crossFixture composes on window 468's Sync block parent, with an L1 on
// which the window's recorded inbound call is one held transaction away.
type crossFixture struct {
	w      *syncWindow
	call   *Call
	output []byte
	l1     *memL1
	key    *ecdsa.PrivateKey
	pool   *HeldPool
	c      *Composer
}

func newCrossFixture(t *testing.T) *crossFixture {
	t.Helper()
	w := loadSyncWindow(t)
	_, call, output := recordedDelivery(t, w)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	m := newMemState()
	m.fund(crypto.PubkeyToAddress(key.PublicKey), 1e18)
	m.deploy(manager, []byte{0x00})
	m.set(manager, protocol.ProxyKey(crossProxy), proxyWord(call.Target, 1))
	m.deploy(call.Source, forward(crossProxy, false))
	m.deploy(unproxied, append(callTo(nobody), 0x00))
	f := &crossFixture{w: w, call: call, output: output, l1: &memL1{m: m}, key: key, pool: NewHeldPool()}
	f.c = New(Config{}, nil, nil, nil, nil, nil, nil, nil, nil, log.New())
	t.Cleanup(f.c.Close)
	f.c.CrossChain(&CrossChain{
		Pool: f.pool, L1: f.l1, L1Config: chain.TestChainOsakaConfig, Manager: manager,
		L1Hash: func(_ context.Context, n uint64) (common.Hash, error) { return common.Hash{0x1, byte(n)}, nil },
		L2:     func(_ context.Context, _ *types.Header, read func(PreState) error) error { return read(w.pre) },
		Header: func(*types.Header, rules.ChainHeaderReader) (*types.Header, error) {
			return types.CopyHeader(w.sync.Header()), nil
		},
		L2Config: w.config, Engine: w.engine, Sync: w.ctx(),
	})
	return f
}

// hold holds the L1 transaction of nonce calling to with data.
func (f *crossFixture) hold(t *testing.T, nonce uint64, to common.Address, data []byte) *Held {
	t.Helper()
	tx := &types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 1_000_000, To: &to, Data: data}, ChainID: *uint256.MustFromBig(chain.TestChainOsakaConfig.ChainID.ToBig())}
	tx.FeeCap.SetUint64(10)
	tx.TipCap.SetUint64(1)
	signed, err := types.SignTx(tx, *types.LatestSignerForChainID(&tx.ChainID), f.key)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, signed.MarshalBinary(&buf))
	h := &Held{Raw: buf.Bytes(), Hash: signed.Hash(), Sender: crypto.PubkeyToAddress(f.key.PublicKey), Nonce: nonce, Direction: Inbound}
	h.MaxFee.SetUint64(10)
	h.Tip.SetUint64(1)
	require.NoError(t, f.pool.Push(h, 0))
	return h
}

func TestComposeDeliversAHeldCall(t *testing.T) {
	f := newCrossFixture(t)
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	fx := f.c.compose(t.Context(), f.w.sync.Header(), 101)
	require.NotNil(t, fx)
	require.Equal(t, []*Held{trigger}, fx.held)
	require.Equal(t, common.Hash{0x1, 100}, fx.anchor)
	require.Len(t, fx.deliveries, 1)
	require.Len(t, fx.txs, 1)
	d := fx.deliveries[0]
	require.Equal(t, f.output, d.Sidecar.ReturnData)
	require.Equal(t, d.Tx.Hash(), fx.txs[0].Hash())
	require.Equal(t, f.call.Source, d.Call.SourceAddress)
	require.Equal(t, f.call.Target, d.Call.TargetAddress)
	require.Zero(t, f.pool.Len())
}

func TestComposeRefuses(t *testing.T) {
	t.Run("a call to no proxy is evicted with its chain", func(t *testing.T) {
		f := newCrossFixture(t)
		f.hold(t, 0, unproxied, nil)
		f.hold(t, 1, f.call.Source, f.call.Data)
		require.Nil(t, f.c.compose(t.Context(), f.w.sync.Header(), 101))
		require.Zero(t, f.pool.Len())
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		require.Empty(t, f.pool.inFlight)
	})
	t.Run("an L1 failure waits for the next slot", func(t *testing.T) {
		f := newCrossFixture(t)
		trigger := f.hold(t, 0, f.call.Source, f.call.Data)
		f.l1.failed = errors.New("L1 node down")
		require.Nil(t, f.c.compose(t.Context(), f.w.sync.Header(), 101))
		require.Equal(t, []*Held{trigger}, f.pool.PopN(math.MaxInt))
	})
}

// windowChain serves recorded blocks by number.
type windowChain map[uint64]*types.Block

func (c windowChain) BlockByNumber(_ context.Context, n uint64) (*types.Block, error) {
	if b := c[n]; b != nil {
		return b, nil
	}
	return nil, errors.New("no such block")
}

func (c windowChain) HeaderByNumber(ctx context.Context, n uint64) (*types.Header, error) {
	b, err := c.BlockByNumber(ctx, n)
	if err != nil {
		return nil, err
	}
	return b.Header(), nil
}

// syncWitness serves the window's Sync block witness for every block.
type syncWitness struct{ w *stateless.Witness }

func (s syncWitness) Get(context.Context, uint64, common.Hash) (json.RawMessage, error) {
	hexes := func(in [][]byte) []hexutil.Bytes {
		out := make([]hexutil.Bytes, len(in))
		for i, b := range in {
			out[i] = b
		}
		return out
	}
	return json.Marshal(map[string][]hexutil.Bytes{"state": hexes(s.w.State), "codes": hexes(s.w.Codes), "keys": nil, "headers": hexes(s.w.Headers)})
}

// sealed returns the sibling of window 468's Sync block that carries txs.
func (f *crossFixture) sealed(t *testing.T, txs []types.Transaction) *types.Block {
	t.Helper()
	d, err := NewDraft(f.w.config, f.w.engine, f.w.sync.Header(), f.w.pre)
	require.NoError(t, err)
	for _, tx := range txs {
		_, err := d.Apply(tx, nil)
		require.NoError(t, err)
	}
	h := types.CopyHeader(f.w.sync.Header())
	h.TxHash = types.DeriveSha(types.Transactions(txs))
	h.ReceiptHash = types.DeriveSha(d.Receipts())
	h.Bloom = types.CreateBloom(d.Receipts())
	h.GasUsed = d.GasUsed()
	res, err := stateless.Execute(f.w.config, f.w.engine, types.NewBlockFromStorage(h.Hash(), h, txs, nil, f.w.sync.Withdrawals(), nil), f.w.witness)
	require.NoError(t, err)
	sealed := types.CopyHeader(h)
	sealed.Root = res.StateRoot
	return types.NewBlockFromStorage(sealed.Hash(), sealed, txs, nil, f.w.sync.Withdrawals(), nil)
}

// poster returns a composer of the window's chain, cursor at its first
// block's parent, with sync as its Sync block when set.
func (f *crossFixture) poster(t *testing.T, sync *types.Block) (*Composer, *testProver, *testPoster, windowChain) {
	t.Helper()
	fw, err := testfixtures.LoadWindow("captured-devnet-window-468")
	require.NoError(t, err)
	chain := windowChain{}
	for _, b := range fw.Blocks[:len(fw.Blocks)-1] {
		block := new(types.Block)
		require.NoError(t, rlp.DecodeBytes(b.RLP, block))
		chain[block.NumberU64()] = block
	}
	// The cursor's header is in the first block's witness.
	for _, enc := range fw.Blocks[0].Witness.Headers {
		h := new(types.Header)
		require.NoError(t, rlp.DecodeBytes(enc, h))
		if first := chain[fw.Blocks[0].Number]; h.Hash() == first.ParentHash() {
			chain[h.Number.Uint64()] = types.NewBlockWithHeader(h, nil)
		}
	}
	require.Contains(t, chain, fw.Blocks[0].Number-1)
	if sync != nil {
		chain[sync.NumberU64()] = sync
	}
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	prover, poster := &testProver{}, &testPoster{key: key}
	cfg := Config{Batcher: devnetBatcher(f.w.batch.ProofSystems[0]), K: 6, MaxBlocks: 300, MaxGas: MaxPostBatchGas, Proof: time.Second, Slack: 100 * time.Millisecond}
	obs := &Observer{Node: quiet{}, RollupID: 1, EEZ: eezL1, Every: time.Millisecond, Logger: log.New()}
	c := New(cfg, chain, syncWitness{f.w.witness}, prover, poster, obs, func() uint64 { return 462 }, func() uint64 { return 0 }, nil, log.New())
	t.Cleanup(c.Close)
	c.CrossChain(f.c.x)
	return c, prover, poster, chain
}

func TestComposedSyncBlockPosts(t *testing.T) {
	f := newCrossFixture(t)
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	c, prover, poster, chain := f.poster(t, nil)
	ts := f.w.sync.Time()
	composed := c.ComposeSyncSlot(t.Context(), chain[467].Header(), ts, 101, sequencer.ModeSteady)
	require.Len(t, composed.Txs, 1)
	fx := c.pending.fx
	sync := f.sealed(t, composed.Txs)
	chain[sync.NumberU64()] = sync
	target := post.Target{Block: 101, Time: ts}
	c.SyncCommitted(t.Context(), sync.Header())

	require.Len(t, prover.proven, 1)
	p := prover.proven[0]
	require.Equal(t, [2]uint64{463, 468}, [2]uint64{p.from, p.to})
	require.Equal(t, uint64(protocol.LiveBlockNumber), p.batch.BlockNumber)
	require.Equal(t, common.Hash{0x1, 100}, p.l1Parent, "the anchor the triggers ran on")
	require.Equal(t, sync.Time(), p.time)
	require.Len(t, p.batch.Entries, 2)
	require.Equal(t, sync.Hash(), p.batch.Entries[1].RollupUpdates[0].NewRoot)
	require.Equal(t, f.output, p.batch.Entries[1].ReturnData)
	require.Equal(t, fx.deliveries[0].Sidecar.ProxyEntryHash, p.batch.Entries[1].ProxyEntryHash)
	require.Equal(t, uint64(1), p.batch.ImmediateEntryCount.Uint64())

	bundles := func() [][]common.Hash {
		poster.mu.Lock()
		defer poster.mu.Unlock()
		return poster.bundles
	}
	require.Eventually(t, func() bool { return len(bundles()) == 1 }, 5*time.Second, time.Millisecond)
	require.Equal(t, []common.Hash{poster.txs[0].Hash(), trigger.Hash}, bundles()[0])
	require.Equal(t, target, poster.sent[0])
	c.ledger.mu.Lock()
	e := c.ledger.entries[sync.NumberU64()]
	c.ledger.mu.Unlock()
	require.NotNil(t, e)
	require.Equal(t, []*Held{trigger}, e.Txs)
}

func TestComposedSyncBlockMismatch(t *testing.T) {
	f := newCrossFixture(t)
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	fx := f.c.compose(t.Context(), f.w.sync.Header(), 101)
	require.NotNil(t, fx)
	c, prover, _, _ := f.poster(t, f.w.sync)
	c.pending = emission{kind: emitEffects, cursor: 462, sync: 468, target: post.Target{Block: 101, Time: f.w.sync.Time()}, fx: fx}
	c.SyncCommitted(t.Context(), f.w.sync.Header())
	require.Empty(t, prover.proven)
	failed, ok := c.ledger.TakeFailed(0)
	require.True(t, ok, "the batch fails, for recovery to replace the block")
	require.Equal(t, []*Held{trigger}, failed.Txs)
	require.True(t, failed.SlotSkipped, "not the transactions' fault")
}

// TestComposedSyncBlockOfOtherTransactions: a committed block of the
// composition's shape but other transactions is not posted.
func TestComposedSyncBlockOfOtherTransactions(t *testing.T) {
	f := newCrossFixture(t)
	f.hold(t, 0, f.call.Source, f.call.Data)
	c, prover, _, chain := f.poster(t, nil)
	composed := c.ComposeSyncSlot(t.Context(), chain[467].Header(), f.w.sync.Time(), 101, sequencer.ModeSteady)
	require.Len(t, composed.Txs, 1)
	sync := f.sealed(t, composed.Txs)
	chain[sync.NumberU64()] = sync
	c.pending.fx.txs = []types.Transaction{f.w.sync.Transactions()[0]}
	c.SyncCommitted(t.Context(), sync.Header())
	require.Empty(t, prover.proven)
	_, ok := c.ledger.TakeFailed(0)
	require.True(t, ok)
}

// TestComposeOnlySteadySlots: a catch-up slot carries no effects.
func TestComposeOnlySteadySlots(t *testing.T) {
	f := newCrossFixture(t)
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	c, _, _, chain := f.poster(t, nil)
	composed := c.ComposeSyncSlot(t.Context(), chain[467].Header(), f.w.sync.Time(), 101, sequencer.ModeCatchup)
	require.Equal(t, sequencer.Composed{Empty: true}, composed)
	require.Equal(t, []*Held{trigger}, f.pool.PopN(math.MaxInt))
}

// TestComposeOutlivesAHungL1: a composition whose L1 reads go unanswered
// ends when the slot's proof must start, its held transactions back in the
// pool, and the slot answers an empty Sync block.
func TestComposeOutlivesAHungL1(t *testing.T) {
	f := newCrossFixture(t)
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	c, _, _, chain := f.poster(t, nil)
	c.x.L1Hash = func(ctx context.Context, _ uint64) (common.Hash, error) {
		<-ctx.Done()
		return common.Hash{}, ctx.Err()
	}
	ts := uint64(time.Now().Add(c.cfg.Slack + c.cfg.Proof + time.Second).Unix())
	done := make(chan sequencer.Composed, 1)
	go func() { done <- c.ComposeSyncSlot(t.Context(), chain[467].Header(), ts, 101, sequencer.ModeSteady) }()
	select {
	case composed := <-done:
		require.Equal(t, sequencer.Composed{Empty: true}, composed)
	case <-time.After(5 * time.Second):
		t.Fatal("the Sync slot waits on L1")
	}
	require.Equal(t, []*Held{trigger}, f.pool.PopN(math.MaxInt))
}

// TestComposedSyncBlockWithAFailedDelivery: a Sync block whose delivery
// failed is not posted.
func TestComposedSyncBlockWithAFailedDelivery(t *testing.T) {
	f := newCrossFixture(t)
	f.hold(t, 0, f.call.Source, f.call.Data)
	c, prover, _, chain := f.poster(t, nil)
	composed := c.ComposeSyncSlot(t.Context(), chain[467].Header(), f.w.sync.Time(), 101, sequencer.ModeSteady)
	require.Len(t, composed.Txs, 1)
	// The same delivery claiming another output reverts.
	d := c.pending.fx.deliveries[0]
	wrong := *d.Call
	wrong.ReturnData = []byte{1}
	ds, err := NewDeliveries(f.w.ctx(), mustDraft(t, f))
	require.NoError(t, err)
	bad, err := ds.delivery(&wrong)
	require.NoError(t, err)
	sync := f.sealed(t, []types.Transaction{bad.Tx})
	chain[sync.NumberU64()] = sync
	c.pending.fx.txs = []types.Transaction{bad.Tx}
	c.SyncCommitted(t.Context(), sync.Header())
	require.Empty(t, prover.proven)
	_, ok := c.ledger.TakeFailed(0)
	require.True(t, ok)
}

func mustDraft(t *testing.T, f *crossFixture) *Draft {
	t.Helper()
	d, err := NewDraft(f.w.config, f.w.engine, f.w.sync.Header(), f.w.pre)
	require.NoError(t, err)
	return d
}

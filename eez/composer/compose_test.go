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
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/erigontech/erigon/eez/prove"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	"github.com/erigontech/erigon/execution/vm"
)

// memL1 is an L1 whose every block has state m, and gas limit gasLimit,
// 30,000,000 when zero.
type memL1 struct {
	m        *memState
	failed   error
	gasLimit uint64
	// gasUsed is every block's gas used.
	gasUsed uint64
}

func (l *memL1) ViewState(_ context.Context, hash common.Hash, read func(*types.Header, state.StateReader) error) error {
	if l.failed != nil {
		return l.failed
	}
	gas := l.gasLimit
	if gas == 0 {
		gas = 30_000_000
	}
	h := &types.Header{Number: *uint256.NewInt(100), Time: 1_800_000_000, GasLimit: gas, GasUsed: l.gasUsed, BaseFee: uint256.NewInt(1), Extra: []byte{}}
	return read(h, l.m)
}

var crossProxy = common.HexToAddress("0xaaaa0000000000000000000000000000000000ff")

// targetTime is the time of block 101, the one after every memL1 block.
const targetTime = 1_800_000_012

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
	// witnesses, when set, replaces the window's store; rewound records
	// the heads the composer moves back to.
	witnesses Witnesses
	rewound   []common.Hash
	// gas is the gas limit of the transactions held, 1,000,000 when zero.
	gas uint64
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
	f.c = New(Config{MaxGas: MaxPostBatchGas, Batcher: devnetBatcher(w.batch.ProofSystems[0])}, nil, nil, nil, nil, nil, nil, nil, nil, log.New())
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

// compose composes window 468's Sync block on a cursor at its parent: the
// window holds the Sync block alone.
func (f *crossFixture) compose(t *testing.T) *effects {
	t.Helper()
	sync := f.w.sync.Header()
	return f.c.compose(t.Context(), sync.Number.Uint64(), sync, 101, targetTime)
}

// windowGas is the posting's projection at the standard rate of window 468's
// anchor-only batch over blocks, the cursor's child to an empty Sync block.
func (f *crossFixture) windowGas(t *testing.T, blocks ...*types.Block) uint64 {
	t.Helper()
	b, err := windowBudget(f.c.cfg.Batcher, MaxPostBatchGas, 0, append(blocks, types.NewBlockWithHeader(f.w.sync.Header(), nil)))
	require.NoError(t, err)
	return b.projected
}

// deliveryCost is what window 468's inbound delivery adds to a posting: an
// entry, its deferred form and its sidecar.
func (f *crossFixture) deliveryCost(t *testing.T) uint64 {
	t.Helper()
	sidecar, err := protocol.NewInboundSidecar(&protocol.IncomingCall{
		SourceAddress: f.call.Source, SourceRollupID: f.call.SourceRollup, TargetAddress: f.call.Target, RollupID: f.call.RollupID,
		Value: f.call.Value.ToBig(), Data: f.call.Data, ReturnData: f.output,
	})
	require.NoError(t, err)
	encSidecar, err := protocol.EncodeExecutionEntry(sidecar)
	require.NoError(t, err)
	encDeferred, err := protocol.EncodeExecutionEntry(&protocol.ExecutionEntry{ProxyEntryHash: sidecar.ProxyEntryHash, ReturnData: f.output, Success: true})
	require.NoError(t, err)
	return postBatchEntryGas + calldataGas(encSidecar) + calldataGas(encDeferred)
}

// hold holds the L1 transaction of nonce calling to with data.
func (f *crossFixture) hold(t *testing.T, nonce uint64, to common.Address, data []byte) *Held {
	t.Helper()
	return f.holdOn(t, nonce, 0, to, data)
}

// holdOn holds the L1 transaction of nonce calling to with data, admitted on
// a pending L1 nonce onChain.
func (f *crossFixture) holdOn(t *testing.T, nonce, onChain uint64, to common.Address, data []byte) *Held {
	t.Helper()
	gas := f.gas
	if gas == 0 {
		gas = 1_000_000
	}
	tx := &types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gas, To: &to, Data: data}, ChainID: *uint256.MustFromBig(chain.TestChainOsakaConfig.ChainID.ToBig())}
	tx.FeeCap.SetUint64(10)
	tx.TipCap.SetUint64(1)
	signed, err := types.SignTx(tx, *types.LatestSignerForChainID(&tx.ChainID), f.key)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, signed.MarshalBinary(&buf))
	h := &Held{Raw: buf.Bytes(), Hash: signed.Hash(), Sender: crypto.PubkeyToAddress(f.key.PublicKey), Nonce: nonce, Direction: Inbound}
	h.MaxFee.SetUint64(10)
	h.Tip.SetUint64(1)
	require.NoError(t, f.pool.Push(h, onChain))
	return h
}

func TestComposeDeliversAHeldCall(t *testing.T) {
	f := newCrossFixture(t)
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	fx := f.compose(t)
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
		require.Nil(t, f.compose(t))
		require.Zero(t, f.pool.Len())
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		require.Empty(t, f.pool.inFlight)
	})
	t.Run("a spent nonce is dropped, not its chain", func(t *testing.T) {
		f := newCrossFixture(t)
		f.hold(t, 0, f.call.Source, f.call.Data)
		next := f.hold(t, 1, f.call.Source, f.call.Data)
		f.l1.m.accounts[crypto.PubkeyToAddress(f.key.PublicKey)].Nonce = 1
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{next}, fx.held)
		require.Zero(t, f.pool.Len())
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		require.Len(t, f.pool.inFlight, 1)
		require.Contains(t, f.pool.inFlight, next.Hash)
	})
	t.Run("an L1 failure waits for the next slot", func(t *testing.T) {
		f := newCrossFixture(t)
		trigger := f.hold(t, 0, f.call.Source, f.call.Data)
		f.l1.failed = errors.New("L1 node down")
		require.Nil(t, f.compose(t))
		require.Equal(t, []*Held{trigger}, f.pool.PopN(math.MaxInt))
	})
}

// TestComposeWaitsOnANonceGap: a held transaction whose nonce is ahead of
// its sender's on L1, admitted on a pending nonce, waits with the rest of
// its chain for the predecessor sent elsewhere, without taking the slot's
// drain from others; a gap that does not close evicts the chain.
func TestComposeWaitsOnANonceGap(t *testing.T) {
	sender := func(f *crossFixture) common.Address { return crypto.PubkeyToAddress(f.key.PublicKey) }
	t.Run("waits, then composes", func(t *testing.T) {
		f := newCrossFixture(t)
		first := f.holdOn(t, 1, 1, f.call.Source, f.call.Data)
		second := f.hold(t, 2, f.call.Source, f.call.Data)
		require.Nil(t, f.compose(t))
		require.Equal(t, 2, f.pool.Len(), "not evicted")
		require.Equal(t, uint32(1), first.Deferred)
		f.l1.m.accounts[sender(f)].Nonce = 1
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{first, second}, fx.held)
		require.Zero(t, first.Deferred, "composed, its wait is over")
	})
	t.Run("others compose behind a full drain of it", func(t *testing.T) {
		f := newCrossFixture(t)
		f.c.x.MaxTxs = 2
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		f.l1.m.fund(crypto.PubkeyToAddress(key.PublicKey), 1e18)
		gapped := []*Held{f.holdOn(t, 1, 1, f.call.Source, f.call.Data), f.hold(t, 2, f.call.Source, f.call.Data)}
		other := *f
		other.key = key
		ready := other.hold(t, 0, f.call.Source, f.call.Data)
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{ready}, fx.held)
		require.Equal(t, gapped, f.pool.PopN(math.MaxInt), "waiting, in order")
	})
	t.Run("a gap that does not close", func(t *testing.T) {
		f := newCrossFixture(t)
		first := f.holdOn(t, 1, 1, f.call.Source, f.call.Data)
		f.hold(t, 2, f.call.Source, f.call.Data)
		first.Deferred = maxDeferrals - 1
		require.Nil(t, f.compose(t))
		require.Zero(t, f.pool.Len())
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		require.Empty(t, f.pool.inFlight)
	})
}

// TestComposeStopsAtItsDeadline: a composition whose context has ended
// takes nothing and leaves the pool as it was.
func TestComposeStopsAtItsDeadline(t *testing.T) {
	f := newCrossFixture(t)
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	sync := f.w.sync.Header()
	require.Nil(t, f.c.compose(ctx, sync.Number.Uint64(), sync, 101, targetTime))
	require.Equal(t, []*Held{trigger}, f.pool.PopN(math.MaxInt))
	require.Zero(t, trigger.Attempts)
	require.Zero(t, trigger.Deferred)
}

// TestComposeRunsInTheTargetBlock: a held transaction runs on the anchor's
// state in the context of the block its bundle aims at: that block's number,
// time and base fee, not the anchor's.
func TestComposeRunsInTheTargetBlock(t *testing.T) {
	f := newCrossFixture(t)
	// A full anchor at base fee 1 has its child at base fee 2.
	f.l1.gasUsed = 30_000_000
	// Calls the proxy only at block 101, time targetTime and base fee 2.
	guard := []byte{
		byte(vm.NUMBER), byte(vm.PUSH1), 101, byte(vm.EQ), byte(vm.PUSH1), 8, byte(vm.JUMPI), byte(vm.STOP),
		byte(vm.JUMPDEST), byte(vm.TIMESTAMP), byte(vm.PUSH4), 0, 0, 0, 0, byte(vm.EQ), byte(vm.PUSH1), 20, byte(vm.JUMPI), byte(vm.STOP),
		byte(vm.JUMPDEST), byte(vm.BASEFEE), byte(vm.PUSH1), 2, byte(vm.EQ), byte(vm.PUSH1), 29, byte(vm.JUMPI), byte(vm.STOP),
		byte(vm.JUMPDEST),
	}
	binary.BigEndian.PutUint32(guard[11:15], targetTime)
	f.l1.m.deploy(f.call.Source, append(guard, forward(crossProxy, false)...))
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	fx := f.compose(t)
	require.NotNil(t, fx)
	require.Equal(t, []*Held{trigger}, fx.held)
}

// TestComposeBoundsThePostingGas: held transactions compose while the
// posting's projected gas, from the anchor-only batch of the window, stays
// within the ceiling less the drain margin; the rest waits, and one over it
// in a window of the Sync block alone is evicted with its chain.
func TestComposeBoundsThePostingGas(t *testing.T) {
	t.Run("the rest waits", func(t *testing.T) {
		f := newCrossFixture(t)
		first := f.hold(t, 0, f.call.Source, f.call.Data)
		second := f.hold(t, 1, f.call.Source, f.call.Data)
		f.c.cfg.MaxGas = drainMargin + f.windowGas(t) + f.deliveryCost(t)
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{first}, fx.held)
		require.Len(t, fx.txs, 1)
		require.Equal(t, []*Held{second}, f.pool.PopN(math.MaxInt))
		require.Zero(t, second.Deferred, "behind a composed one, it only waits its turn")
	})
	t.Run("one over alone is evicted with its chain", func(t *testing.T) {
		f := newCrossFixture(t)
		f.hold(t, 0, f.call.Source, f.call.Data)
		f.hold(t, 1, f.call.Source, f.call.Data)
		f.c.cfg.MaxGas = drainMargin + f.windowGas(t) + f.deliveryCost(t) - 1
		require.Nil(t, f.compose(t))
		require.Zero(t, f.pool.Len())
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		require.Empty(t, f.pool.inFlight)
	})
}

// TestComposeWaitsForALighterWindow: a transaction the window leaves no room
// for, which a window of the Sync block alone would, waits uncharged for a
// lighter one, up to maxDeferrals compositions.
func TestComposeWaitsForALighterWindow(t *testing.T) {
	// heavy has f compose on eight blocks carrying a transaction each, and
	// returns the posting's standard projection of their window and of the
	// Sync block's alone.
	heavy := func(t *testing.T, f *crossFixture) (chain *testChain, window, alone uint64) {
		chain = newTestChain(t, 8, 1, 2, 3, 4, 5, 6, 7, 8)
		f.c.chain = chain
		f.c.cfg.Batcher = &Batcher{RollupID: 1, ProofSystem: f.c.cfg.Batcher.ProofSystem, Signer: testSigner}
		sync := types.CopyHeader(f.w.sync.Header())
		sync.ParentHash = chain.blocks[8].Hash()
		f.c.x.Header = func(*types.Header, rules.ChainHeaderReader) (*types.Header, error) {
			return types.CopyHeader(sync), nil
		}
		budget := func(blocks ...*types.Block) uint64 {
			b, err := windowBudget(f.c.cfg.Batcher, MaxPostBatchGas, 0, append(blocks, types.NewBlockWithHeader(sync, nil)))
			require.NoError(t, err)
			return b.projected
		}
		return chain, budget(chain.blocks[1:]...), budget()
	}
	setup := func(t *testing.T) (*crossFixture, *testChain, *Held) {
		f := newCrossFixture(t)
		trigger := f.hold(t, 0, f.call.Source, f.call.Data)
		chain, window, alone := heavy(t, f)
		f.c.cfg.MaxGas = drainMargin + window + f.deliveryCost(t) - 1
		require.GreaterOrEqual(t, f.c.cfg.MaxGas, drainMargin+alone+f.deliveryCost(t), "room behind the Sync block alone")
		return f, chain, trigger
	}
	t.Run("waits", func(t *testing.T) {
		f, chain, trigger := setup(t)
		require.Nil(t, f.c.compose(t.Context(), 0, chain.header(8), 101, targetTime))
		require.Equal(t, []*Held{trigger}, f.pool.PopN(math.MaxInt))
		require.Equal(t, uint32(1), trigger.Deferred)
		require.Zero(t, trigger.Attempts)
	})
	t.Run("others compose behind it", func(t *testing.T) {
		f := newCrossFixture(t)
		// 10,000,000 of gas for the first sender's triggers, 1,000,000 for
		// the other's: the L1 block holds the larger behind the Sync block
		// alone, not behind the window.
		f.gas = 10_000_000
		first := f.hold(t, 0, f.call.Source, f.call.Data)
		next := f.hold(t, 1, f.call.Source, f.call.Data)
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		f.l1.m.fund(crypto.PubkeyToAddress(key.PublicKey), 1e18)
		other := *f
		other.key, other.gas = key, 0
		small := other.hold(t, 0, f.call.Source, f.call.Data)
		chain, window, alone := heavy(t, f)
		block := alone + f.deliveryCost(t) + drainMargin + f.gas + (window-alone)/2
		f.l1.gasLimit, f.c.cfg.MaxGas = block*1024/1023, block-3
		fx := f.c.compose(t.Context(), 0, chain.header(8), 101, targetTime)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{small}, fx.held)
		require.Equal(t, []*Held{first, next}, f.pool.PopN(math.MaxInt))
		require.Equal(t, uint32(1), first.Deferred)
		require.Zero(t, next.Deferred, "it waits on the one ahead, not on L1")
	})
	t.Run("those set aside count against the drain", func(t *testing.T) {
		f := newCrossFixture(t)
		f.c.x.MaxTxs = 2
		// Two senders' triggers of 10,000,000 gas the window has no room for,
		// then one of 1,000,000 it has.
		var big []*Held
		for range 2 {
			key, err := crypto.GenerateKey()
			require.NoError(t, err)
			f.l1.m.fund(crypto.PubkeyToAddress(key.PublicKey), 1e18)
			sender := *f
			sender.key, sender.gas = key, 10_000_000
			big = append(big, sender.hold(t, 0, f.call.Source, f.call.Data))
		}
		small := f.hold(t, 0, f.call.Source, f.call.Data)
		chain, window, alone := heavy(t, f)
		block := alone + f.deliveryCost(t) + drainMargin + 10_000_000 + (window-alone)/2
		f.l1.gasLimit, f.c.cfg.MaxGas = block*1024/1023, block-3
		require.Nil(t, f.c.compose(t.Context(), 0, chain.header(8), 101, targetTime), "two simulations, the drain's")
		require.Equal(t, append(big, small), f.pool.PopN(math.MaxInt))
		require.Equal(t, []uint32{1, 1, 0}, []uint32{big[0].Deferred, big[1].Deferred, small.Deferred})
	})
	t.Run("composes behind a lighter window", func(t *testing.T) {
		f, chain, trigger := setup(t)
		require.Nil(t, f.c.compose(t.Context(), 0, chain.header(8), 101, targetTime))
		fx := f.c.compose(t.Context(), 8, chain.header(8), 101, targetTime)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{trigger}, fx.held)
		require.Zero(t, trigger.Deferred)
	})
	t.Run("a window that stays too heavy evicts it", func(t *testing.T) {
		f, chain, trigger := setup(t)
		trigger.Deferred = maxDeferrals - 1
		require.Nil(t, f.c.compose(t.Context(), 0, chain.header(8), 101, targetTime))
		require.Zero(t, f.pool.Len())
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		require.Empty(t, f.pool.inFlight)
	})
}

// TestComposeFitsTheL1Block: the bundle fits the L1 block it aims at, whose
// gas may be the anchor's less 1/1024: the posting's projection with the
// drain margin and its triggers' gas limits, 1,000,000 each. The rest waits,
// and a trigger a bundle of its own cannot fit is evicted. A block under the
// posting's gas limit, the ceiling, takes no posting: nothing composes.
func TestComposeFitsTheL1Block(t *testing.T) {
	// limit has the bundle's block take about want, within 2, and the
	// posting's gas limit fit it.
	limit := func(f *crossFixture, want uint64) {
		f.l1.gasLimit, f.c.cfg.MaxGas = want*1024/1023, want-3
	}
	t.Run("the rest waits", func(t *testing.T) {
		f := newCrossFixture(t)
		first := f.hold(t, 0, f.call.Source, f.call.Data)
		second := f.hold(t, 1, f.call.Source, f.call.Data)
		limit(f, f.windowGas(t)+f.deliveryCost(t)+drainMargin+1_500_000)
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{first}, fx.held)
		require.Equal(t, []*Held{second}, f.pool.PopN(math.MaxInt))
	})
	t.Run("a block under the posting's gas limit composes nothing", func(t *testing.T) {
		// The least anchor gas limit whose block takes MaxPostBatchGas.
		least := uint64(MaxPostBatchGas)
		for least-least/1024 < MaxPostBatchGas {
			least++
		}
		f := newCrossFixture(t)
		trigger := f.hold(t, 0, f.call.Source, f.call.Data)
		f.l1.gasLimit = least - 1
		require.Nil(t, f.compose(t))
		require.Equal(t, []*Held{trigger}, f.pool.PopN(math.MaxInt), "left in the pool")
		require.Zero(t, trigger.Deferred)
		f.pool.PushFront([]*Held{trigger})
		f.l1.gasLimit = least
		require.NotNil(t, f.compose(t), "a block of the posting's gas limit")
	})
	t.Run("one a bundle of its own cannot fit", func(t *testing.T) {
		f := newCrossFixture(t)
		f.hold(t, 0, f.call.Source, f.call.Data)
		limit(f, f.windowGas(t)+f.deliveryCost(t)+drainMargin+1_000_000-3)
		require.Nil(t, f.compose(t))
		require.Zero(t, f.pool.Len())
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		require.Empty(t, f.pool.inFlight, "evicted")
	})
}

// TestComposeFillsTheBlock: inbound deliveries compose while the Sync
// block has gas for them; the rest waits, and a transaction whose delivery
// an empty Sync block cannot hold is evicted.
func TestComposeFillsTheBlock(t *testing.T) {
	limit := func(f *crossFixture, gas uint64) {
		f.c.x.Header = func(*types.Header, rules.ChainHeaderReader) (*types.Header, error) {
			h := types.CopyHeader(f.w.sync.Header())
			h.GasLimit = gas
			return h, nil
		}
	}
	t.Run("two in one block", func(t *testing.T) {
		f := newCrossFixture(t)
		first := f.hold(t, 0, f.call.Source, f.call.Data)
		second := f.hold(t, 1, f.call.Source, f.call.Data)
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{first, second}, fx.held)
		require.Len(t, fx.deliveries, 2)
		require.Len(t, fx.txs, 2)
		require.Equal(t, fx.deliveries[1].Tx.Hash(), fx.txs[1].Hash())
	})
	t.Run("the rest waits", func(t *testing.T) {
		f := newCrossFixture(t)
		first := f.hold(t, 0, f.call.Source, f.call.Data)
		second := f.hold(t, 1, f.call.Source, f.call.Data)
		// Room for one delivery's declared gas, not two.
		limit(f, protocol.SystemTxGasLimit)
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{first}, fx.held)
		require.Equal(t, []*Held{second}, f.pool.PopN(math.MaxInt))
	})
	t.Run("one an empty block cannot hold", func(t *testing.T) {
		f := newCrossFixture(t)
		f.hold(t, 0, f.call.Source, f.call.Data)
		limit(f, protocol.SystemTxGasLimit-1)
		require.Nil(t, f.compose(t))
		require.Zero(t, f.pool.Len())
	})
}

// TestUncommittedCompositionRequeues: the held transactions of a composed
// Sync block that never committed go back to the pool, in the next slot or
// when another block commits instead.
func TestUncommittedCompositionRequeues(t *testing.T) {
	t.Run("the next slot", func(t *testing.T) {
		f := newCrossFixture(t)
		trigger := f.hold(t, 0, f.call.Source, f.call.Data)
		c, _, _, chain := f.poster(t, nil)
		ts := f.w.sync.Time()
		require.Len(t, c.ComposeSyncSlot(t.Context(), chain[467].Header(), ts, 101, sequencer.ModeSteady).Txs, 1)
		composed := c.ComposeSyncSlot(t.Context(), chain[467].Header(), ts, 101, sequencer.ModeSteady)
		require.Len(t, composed.Txs, 1, "composed again")
		require.Equal(t, []*Held{trigger}, c.pending.fx.held)
	})
	t.Run("another block", func(t *testing.T) {
		f := newCrossFixture(t)
		trigger := f.hold(t, 0, f.call.Source, f.call.Data)
		c, _, _, chain := f.poster(t, nil)
		require.Len(t, c.ComposeSyncSlot(t.Context(), chain[467].Header(), f.w.sync.Time(), 101, sequencer.ModeSteady).Txs, 1)
		c.SyncCommitted(t.Context(), chain[467].Header())
		require.Equal(t, []*Held{trigger}, f.pool.PopN(math.MaxInt))
	})
}

// TestComposeLeavesTheEvictedChains: a composition stopped after an
// eviction requeues no later nonce of the evicted chain.
func TestComposeLeavesTheEvictedChains(t *testing.T) {
	f := newCrossFixture(t)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	f.l1.m.fund(crypto.PubkeyToAddress(key.PublicKey), 1e18)
	f.hold(t, 0, unproxied, nil)
	other := *f
	other.key = key
	first := other.hold(t, 0, f.call.Source, f.call.Data)
	second := other.hold(t, 1, f.call.Source, f.call.Data)
	f.hold(t, 1, f.call.Source, f.call.Data)
	f.c.cfg.MaxGas = drainMargin + 551_000 + 2*postBatchEntryGas - 1
	fx := f.compose(t)
	require.NotNil(t, fx)
	require.Equal(t, []*Held{first}, fx.held)
	require.Equal(t, []*Held{second}, f.pool.PopN(math.MaxInt))
}

// onLog is a log handler calling f on each record.
type onLog func(r *log.Record)

func (f onLog) Log(r *log.Record) error { f(r); return nil }

func (onLog) Enabled(context.Context, log.Lvl) bool { return true }

// TestComposeTakesAChainAdmittedAfterItsEviction: a chain the front admits
// anew after an eviction, drained by the same composition, composes or
// waits whole; no reservation of it is left without its transaction.
func TestComposeTakesAChainAdmittedAfterItsEviction(t *testing.T) {
	setup := func(t *testing.T) (*crossFixture, *[]*Held) {
		f := newCrossFixture(t)
		f.hold(t, 0, unproxied, nil)
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		stale := *f
		stale.key = key
		stale.hold(t, 0, f.call.Source, f.call.Data)
		f.l1.m.fund(crypto.PubkeyToAddress(key.PublicKey), 1e18)
		f.l1.m.accounts[crypto.PubkeyToAddress(key.PublicKey)].Nonce = 1
		// Dropping the stale one comes after the eviction and before the
		// drain takes more: the front admits a new chain of the evicted
		// sender then.
		fresh := new([]*Held)
		l := log.New()
		l.SetHandler(onLog(func(r *log.Record) {
			if strings.Contains(r.Msg, "nonce is spent") && *fresh == nil {
				*fresh = []*Held{f.hold(t, 0, f.call.Source, f.call.Data), f.hold(t, 1, f.call.Source, f.call.Data)}
			}
		}))
		f.c.logger = l
		return f, fresh
	}
	t.Run("composes", func(t *testing.T) {
		f, fresh := setup(t)
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Len(t, *fresh, 2)
		require.Equal(t, *fresh, fx.held)
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		require.Len(t, f.pool.inFlight, 2, "only the composed are reserved")
	})
	t.Run("waits when the composition stops", func(t *testing.T) {
		f, fresh := setup(t)
		// A Sync block with room for one delivery.
		f.c.x.Header = func(*types.Header, rules.ChainHeaderReader) (*types.Header, error) {
			h := types.CopyHeader(f.w.sync.Header())
			h.GasLimit = protocol.SystemTxGasLimit
			return h, nil
		}
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Len(t, *fresh, 2)
		require.Equal(t, (*fresh)[:1], fx.held)
		require.Equal(t, (*fresh)[1:], f.pool.PopN(math.MaxInt), "requeued, not dropped")
		f.pool.mu.Lock()
		defer f.pool.mu.Unlock()
		require.Len(t, f.pool.inFlight, 2, "the composed one and the one drained here")
	})
}

// missingAt is a witness store without the witness of block at.
type missingAt struct {
	syncWitness
	at uint64
}

func (m missingAt) Get(ctx context.Context, n uint64, hash common.Hash) (json.RawMessage, error) {
	if n == m.at {
		return nil, nil
	}
	return m.syncWitness.Get(ctx, n, hash)
}

// TestComposedSyncBlockWithoutAWitness: a composed Sync block missing its
// witness fails uncharged and has its head moved back to rebuild it.
func TestComposedSyncBlockWithoutAWitness(t *testing.T) {
	f := newCrossFixture(t)
	f.hold(t, 0, f.call.Source, f.call.Data)
	f.witnesses = missingAt{syncWitness{f.w.witness}, 468}
	c, prover, _, chain := f.poster(t, nil)
	composed := c.ComposeSyncSlot(t.Context(), chain[467].Header(), f.w.sync.Time(), 101, sequencer.ModeSteady)
	sync := f.sealed(t, composed.Txs)
	chain[sync.NumberU64()] = sync
	c.SyncCommitted(t.Context(), sync.Header())
	require.Empty(t, prover.proven)
	require.Equal(t, []common.Hash{sync.ParentHash()}, f.rewound)
	p, ok := c.ledger.TakeFailed(0)
	require.True(t, ok)
	require.True(t, p.SlotSkipped)
}

// TestComposedSyncBlockFailureCharges: a composition the prover refuses
// fails charged; one it cannot reach fails uncharged.
func TestComposedSyncBlockFailureCharges(t *testing.T) {
	for _, c := range []struct {
		name    string
		err     error
		maxGas  uint64 // of the posting, once composed
		rollup  uint64 // of the batcher, once composed
		skipped bool
	}{
		{name: "a refused proof", err: status.Error(codes.InvalidArgument, "window refused")},
		{name: "a batch the batcher refuses", rollup: 2},
		{name: "a posting over its gas", maxGas: 900_000},
		{name: "an unreachable prover", err: status.Error(codes.Unavailable, "down"), skipped: true},
		{name: "a cancelled proof", err: status.Error(codes.Canceled, "stopping"), skipped: true},
		{name: "a malformed attestation", err: prove.ErrAttestation, skipped: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newCrossFixture(t)
			f.hold(t, 0, f.call.Source, f.call.Data)
			comp, prover, _, chain := f.poster(t, nil)
			prover.err = c.err
			composed := comp.ComposeSyncSlot(t.Context(), chain[467].Header(), f.w.sync.Time(), 101, sequencer.ModeSteady)
			if c.maxGas != 0 {
				comp.cfg.MaxGas = c.maxGas
			}
			if c.rollup != 0 {
				b := *comp.cfg.Batcher
				b.RollupID = c.rollup
				comp.cfg.Batcher = &b
			}
			sync := f.sealed(t, composed.Txs)
			chain[sync.NumberU64()] = sync
			comp.SyncCommitted(t.Context(), sync.Header())
			p, ok := comp.ledger.TakeFailed(0)
			require.True(t, ok)
			require.Equal(t, c.skipped, p.SlotSkipped)
		})
	}
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
	var witnesses Witnesses = syncWitness{f.w.witness}
	if f.witnesses != nil {
		witnesses = f.witnesses
	}
	headTo := func(_ context.Context, hash common.Hash) error {
		f.rewound = append(f.rewound, hash)
		return nil
	}
	c := New(cfg, chain, witnesses, prover, poster, obs, func() uint64 { return 462 }, func() uint64 { return 0 }, headTo, log.New())
	t.Cleanup(c.Close)
	// The window's slot is a minute away.
	c.now = func() time.Time { return time.Unix(int64(f.w.sync.Time())-60, 0) }
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
	require.Equal(t, time.Unix(int64(ts), 0).Add(-c.cfg.Slack-c.cfg.Proof), p.start, "proven in time for the slot")
	require.Equal(t, c.cfg.Proof, p.per)
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

// TestComposedSyncBlockWithinTheGasLimit: across gas limits around a window
// with one delivery, whatever the slot composes posts within the limit, and
// a trigger the window leaves no room for is never charged.
func TestComposedSyncBlockWithinTheGasLimit(t *testing.T) {
	probe := newCrossFixture(t)
	c, _, _, _ := probe.poster(t, nil)
	window, err := c.blocks(t.Context(), 462, 467)
	require.NoError(t, err)
	seed, alone, one := probe.windowGas(t, window...), probe.windowGas(t), probe.deliveryCost(t)
	composed := 0
	for extra := one - 20_000; extra <= one+20_000; extra += 5_000 {
		f := newCrossFixture(t)
		trigger := f.hold(t, 0, f.call.Source, f.call.Data)
		c, prover, _, chain := f.poster(t, nil)
		c.cfg.MaxGas = drainMargin + seed + extra
		got := c.ComposeSyncSlot(t.Context(), chain[467].Header(), f.w.sync.Time(), 101, sequencer.ModeSteady)
		if got.Empty {
			require.Less(t, extra, one, "max gas %d", c.cfg.MaxGas)
			require.Zero(t, trigger.Attempts)
			if seed+extra >= alone+one {
				require.Equal(t, []*Held{trigger}, f.pool.PopN(math.MaxInt), "max gas %d: waits for a lighter window", c.cfg.MaxGas)
			} else {
				require.Zero(t, f.pool.Len(), "max gas %d: no room behind the Sync block alone", c.cfg.MaxGas)
			}
			continue
		}
		composed++
		sync := f.sealed(t, got.Txs)
		chain[sync.NumberU64()] = sync
		c.SyncCommitted(t.Context(), sync.Header())
		require.Len(t, prover.proven, 1, "max gas %d: posted within it", c.cfg.MaxGas)
	}
	require.NotZero(t, composed)
}

func TestComposedSyncBlockMismatch(t *testing.T) {
	f := newCrossFixture(t)
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	fx := f.compose(t)
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
	c.now = time.Now
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
	p, ok := c.ledger.TakeFailed(0)
	require.True(t, ok)
	require.False(t, p.SlotSkipped, "owed to the effect")
}

func mustDraft(t *testing.T, f *crossFixture) *Draft {
	t.Helper()
	d, err := NewDraft(f.w.config, f.w.engine, f.w.sync.Header(), f.w.pre)
	require.NoError(t, err)
	return d
}

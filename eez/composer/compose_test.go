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
	"math/big"
	"slices"
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
	"github.com/erigontech/erigon/execution/types/accounts"
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

var (
	crossProxy = common.HexToAddress("0xaaaa0000000000000000000000000000000000ff")
	// The window's outbound calls go through the rollup's proxies of
	// outboundTargets, and arrive through senderProxy, l2Sender's on L1.
	outboundProxies = []common.Address{common.HexToAddress("0x1Fc99E43B3DBECFe7dFe20Ae0ACbD58dEea30cf7"), common.HexToAddress("0x7A9dc1EC60AcB39eEb90A470Ab014C5998998582")}
	outboundTargets = []common.Address{common.HexToAddress("0x2E983A1Ba5e8b38AAAeC4B440B9dDcFBf72E15d1"), common.HexToAddress("0x8438Ad1C834623CfF278AB6829a248E37C2D7E3f")}
	senderProxy     = common.HexToAddress("0x7ed95b38a527a21e90176b4b441b15f1725872b7")
)

// targetTime is the time of block 101, the one after every memL1 block.
const targetTime = 1_800_000_012

// crossFixture composes on window 468's Sync block parent, with an L1 on
// which the window's recorded calls are held transactions away: its manager
// runs EEZL2's code at EEZL2's address, where the rollup's proxies, copied,
// take its calls; the outbound sender's proxy is registered, and the
// outbound targets answer as recorded.
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
	// l2 overlays the rollup's state; l2Key and l2Key2 are rollup accounts
	// in it.
	l2            *memState
	l2Key, l2Key2 *ecdsa.PrivateKey
	escrow        *uint256.Int
	// escrowAt records the L1 block the escrow was read at.
	escrowAt common.Hash
}

func newCrossFixture(t *testing.T) *crossFixture {
	t.Helper()
	w := loadSyncWindow(t)
	_, call, output := recordedDelivery(t, w)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	m := newMemState()
	m.fund(crypto.PubkeyToAddress(key.PublicKey), 1e18)
	eezl2, err := w.pre.Reader.ReadAccountCode(accounts.InternAddress(protocol.EEZL2Address))
	require.NoError(t, err)
	proxyCode, err := w.pre.Reader.ReadAccountCode(accounts.InternAddress(outboundProxies[0]))
	require.NoError(t, err)
	m.deploy(protocol.EEZL2Address, eezl2)
	m.set(protocol.EEZL2Address, protocol.ProxyKey(crossProxy), proxyWord(call.Target, 1))
	m.deploy(senderProxy, proxyCode)
	m.set(protocol.EEZL2Address, protocol.ProxyKey(senderProxy), proxyWord(l2Sender, 1))
	m.deploy(outboundTargets[0], []byte{0x60, 0x01, 0x60, 0x00, 0x52, 0x60, 0x04, 0x35, 0x60, 0x20, 0x52, 0x60, 0x40, 0x60, 0x00, 0xf3})
	m.deploy(outboundTargets[1], []byte{0x00})
	m.deploy(call.Source, forward(crossProxy, false))
	m.deploy(unproxied, append(callTo(nobody), 0x00))
	m.deploy(revertTarget, []byte{0x61, 0xde, 0xad, 0x60, 0x00, 0x52, 0x60, 0x02, 0x60, 0x1e, 0xfd})
	m.deploy(aaTarget, slices.Concat([]byte{0x7f}, bytes.Repeat([]byte{0xaa}, 32), []byte{0x5f, 0x52, 0x60, 0x20, 0x5f, 0xf3}))
	m.deploy(bbTarget, slices.Concat([]byte{0x7f}, bytes.Repeat([]byte{0xbb}, 32), []byte{0x5f, 0x52, 0x60, 0x20, 0x5f, 0xf3}))
	m.deploy(counter, []byte{0x5f, 0x54, 0x60, 0x01, 0x01, 0x80, 0x5f, 0x55, 0x5f, 0x52, 0x60, 0x20, 0x5f, 0xf3})
	m.deploy(tcounter, tcounterCode)
	m.deploy(originTarget, originCode)
	l2Key, err := crypto.GenerateKey()
	require.NoError(t, err)
	f := &crossFixture{w: w, call: call, output: output, l1: &memL1{m: m}, key: key, pool: NewHeldPool(), l2: newMemState(), l2Key: l2Key}
	f.l2.fund(crypto.PubkeyToAddress(l2Key.PublicKey), 1e18)
	f.l2.deploy(l2Twice, slices.Concat(callTo(outboundProxies[1]), callTo(outboundProxies[1]), []byte{0x00}))
	for p, target := range map[common.Address]common.Address{otherProxy: outboundTargets[1], revertProxy: revertTarget} {
		f.l2.deploy(p, proxyCode)
		rollup := uint64(0)
		if p == otherProxy {
			rollup = 2
		}
		f.l2.set(protocol.EEZL2Address, protocol.ProxyKey(p), proxyWord(target, rollup))
	}
	f.registerL1Proxy(t, crypto.PubkeyToAddress(l2Key.PublicKey))
	f.registerL1Proxy(t, l2Twice)
	for _, c := range []struct{ proxy, target common.Address }{{aaProxy, aaTarget}, {bbProxy, bbTarget}, {counterProxy, counter}, {tcounterProxy, tcounter}, {originProxy, originTarget}} {
		f.l2.deploy(c.proxy, proxyCode)
		f.l2.set(protocol.EEZL2Address, protocol.ProxyKey(c.proxy), proxyWord(c.target, 0))
	}
	f.l2.deploy(l2CounterTwice, slices.Concat(callTo(counterProxy), callTo(counterProxy), []byte{0x00}))
	f.registerL1Proxy(t, l2CounterTwice)
	f.l2Key2, err = crypto.GenerateKey()
	require.NoError(t, err)
	f.l2.fund(crypto.PubkeyToAddress(f.l2Key2.PublicKey), 1e18)
	f.registerL1Proxy(t, crypto.PubkeyToAddress(f.l2Key2.PublicKey))
	f.l2.deploy(l2Stingy, slices.Concat([]byte{0x5f, 0x5f, 0x5f, 0x5f, 0x5f}, push20(outboundProxies[1]), []byte{0x61, 0x75, 0x30, 0xf1, 0x50, 0x00}))
	f.registerL1Proxy(t, l2Stingy)
	pre := w.pre
	pre.Reader = &overlay{StateReader: w.pre.Reader, m: f.l2}
	posterKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	poster := &testPoster{key: posterKey}
	poster.tip.SetUint64(2)
	// The posting buys its gas.
	f.l1.m.fund(poster.From(), 1e18)
	none := func() uint64 { return 0 }
	f.c = New(Config{MaxGas: MaxPostBatchGas, Batcher: devnetBatcher(w.batch.ProofSystems[0])}, nil, nil, nil, poster, nil, none, none, nil, log.New())
	t.Cleanup(f.c.Close)
	f.c.CrossChain(&CrossChain{
		Pool: f.pool, L1: f.l1, L1Config: chain.TestChainOsakaConfig, Manager: protocol.EEZL2Address,
		L1Hash: func(_ context.Context, n uint64) (common.Hash, error) { return common.Hash{0x1, byte(n)}, nil },
		L2:     func(_ context.Context, _ *types.Header, read func(PreState) error) error { return read(pre) },
		Header: func(*types.Header, rules.ChainHeaderReader) (*types.Header, error) {
			return types.CopyHeader(w.sync.Header()), nil
		},
		L2Config: w.config, Engine: w.engine, Sync: w.ctx(),
		Escrow: func(_ context.Context, block common.Hash) (*uint256.Int, error) {
			f.escrowAt = block
			if f.escrow == nil {
				return nil, errDown
			}
			return f.escrow, nil
		},
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

var (
	// l2Twice calls the second outbound proxy twice.
	l2Twice = common.HexToAddress("0xb000000000000000000000000000000000000001")
	// aaProxy, bbProxy and counterProxy stand for L1's aaTarget and
	// bbTarget, which return a word of 0xaa and 0xbb, and counter, which
	// returns its count after one more.
	aaProxy      = common.HexToAddress("0xb0000000000000000000000000000000000000f3")
	bbProxy      = common.HexToAddress("0xb0000000000000000000000000000000000000f4")
	counterProxy = common.HexToAddress("0xb0000000000000000000000000000000000000f5")
	// tcounterProxy and originProxy stand for L1's tcounter and
	// originTarget.
	tcounterProxy  = common.HexToAddress("0xb0000000000000000000000000000000000000f6")
	originProxy    = common.HexToAddress("0xb0000000000000000000000000000000000000f7")
	aaTarget       = common.HexToAddress("0xd0000000000000000000000000000000000000aa")
	bbTarget       = common.HexToAddress("0xd0000000000000000000000000000000000000bb")
	counter        = common.HexToAddress("0xd0000000000000000000000000000000000000cc")
	l2CounterTwice = common.HexToAddress("0xb000000000000000000000000000000000000003")
	// l2Stingy calls the second outbound proxy with 30,000 gas, too little
	// for EEZL2 to serve the call, and ignores the failure.
	l2Stingy = common.HexToAddress("0xb000000000000000000000000000000000000002")
	// otherProxy stands on the rollup for a contract of rollup 2;
	// revertProxy for revertTarget on L1.
	otherProxy  = common.HexToAddress("0xb0000000000000000000000000000000000000f1")
	revertProxy = common.HexToAddress("0xb0000000000000000000000000000000000000f2")
)

// registerL1Proxy deploys and registers on L1 the proxy of a, a rollup
// account: EEZL2's code, the manager's here, refuses to create it.
func (f *crossFixture) registerL1Proxy(t *testing.T, a common.Address) {
	t.Helper()
	header := &types.Header{Number: *uint256.NewInt(100), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1), Extra: []byte{}}
	src := NewSource(chain.TestChainOsakaConfig, header, f.l1.m, func(uint64) (common.Hash, error) { return common.Hash{}, nil }, protocol.EEZL2Address, 0)
	out, err := src.view(protocol.EncodeComputeCrossChainProxyAddress(a, 1))
	require.NoError(t, err)
	p, err := protocol.DecodeAddressReturn(out)
	require.NoError(t, err)
	f.l1.m.deploy(p, f.l1.m.code[senderProxy])
	f.l1.m.set(protocol.EEZL2Address, protocol.ProxyKey(p), proxyWord(a, 1))
}

// holdOut holds a rollup transaction of l2Key's, of nonce, calling to.
func (f *crossFixture) holdOut(t *testing.T, nonce uint64, to common.Address, value uint64, data []byte) *Held {
	t.Helper()
	return f.holdOutGas(t, nonce, to, value, data, 600_000)
}

func (f *crossFixture) holdOutGas(t *testing.T, nonce uint64, to common.Address, value uint64, data []byte, gas uint64) *Held {
	t.Helper()
	return f.holdOutOf(t, f.l2Key, nonce, to, value, data, gas)
}

func (f *crossFixture) holdOutOf(t *testing.T, key *ecdsa.PrivateKey, nonce uint64, to common.Address, value uint64, data []byte, gas uint64) *Held {
	t.Helper()
	sender := crypto.PubkeyToAddress(key.PublicKey)
	tx := &types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gas, To: &to, Data: data}, ChainID: *uint256.MustFromBig(f.w.config.ChainID.ToBig())}
	tx.Value.SetUint64(value)
	tx.FeeCap.Mul(f.w.sync.BaseFee(), uint256.NewInt(2))
	tx.TipCap.SetUint64(1)
	signed, err := types.SignTx(tx, *types.LatestSignerForChainID(&tx.ChainID), key)
	require.NoError(t, err)
	return f.pushHeld(t, signed, sender, Outbound, f.l2.accounts[sender].Nonce)
}

// pushHeld holds tx, sender's, crossing direction, admitted on sender's
// nonce onChain.
func (f *crossFixture) pushHeld(t *testing.T, tx types.Transaction, sender common.Address, direction Direction, onChain uint64) *Held {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))
	h := &Held{Raw: buf.Bytes(), Hash: tx.Hash(), Sender: sender, Nonce: tx.GetNonce(), Direction: direction}
	h.MaxFee.Set(tx.GetFeeCap())
	h.Tip.Set(tx.GetTipCap())
	require.NoError(t, f.pool.Push(h, onChain))
	return h
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
	return f.pushHeld(t, signed, crypto.PubkeyToAddress(f.key.PublicKey), Inbound, onChain)
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

// TestComposeDropsALateComposition: a deadline that passes after a
// transaction composed leaves nothing composed: a Sync block committed then
// could not be proven in time. Every drained transaction returns to the pool.
func TestComposeDropsALateComposition(t *testing.T) {
	f := newCrossFixture(t)
	first := f.hold(t, 0, f.call.Source, f.call.Data)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	stale := *f
	stale.key = key
	spent := stale.hold(t, 0, f.call.Source, f.call.Data)
	f.l1.m.fund(crypto.PubkeyToAddress(key.PublicKey), 1e18)
	f.l1.m.accounts[crypto.PubkeyToAddress(key.PublicKey)].Nonce = 1
	next := f.hold(t, 1, f.call.Source, f.call.Data)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// The deadline passes as the spent one is dropped, after the first composed.
	l := log.New()
	l.SetHandler(onLog(func(r *log.Record) {
		if strings.Contains(r.Msg, "nonce is spent") {
			cancel()
		}
	}))
	f.c.logger = l
	sync := f.w.sync.Header()
	require.Nil(t, f.c.compose(ctx, sync.Number.Uint64(), sync, 101, targetTime))
	require.Equal(t, []*Held{first, next}, f.pool.PopN(math.MaxInt))
	f.pool.mu.Lock()
	defer f.pool.mu.Unlock()
	require.Len(t, f.pool.inFlight, 2, "the two just drained again")
	require.NotContains(t, f.pool.inFlight, spent.Hash)
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

// rewinding is a chain whose block at is then after its first read.
type rewinding struct {
	windowChain
	at    uint64
	then  *types.Block
	reads int
}

func (c *rewinding) BlockByNumber(ctx context.Context, n uint64) (*types.Block, error) {
	if n == c.at {
		if c.reads++; c.reads > 1 {
			return c.then, nil
		}
	}
	return c.windowChain.BlockByNumber(ctx, n)
}

// emptyWitness is a witness store whose every witness is empty.
type emptyWitness struct{}

func (emptyWitness) Get(context.Context, uint64, common.Hash) (json.RawMessage, error) {
	return json.RawMessage(`{"state":[],"codes":[],"keys":[],"headers":[]}`), nil
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
		name   string
		err    error
		maxGas uint64 // of the posting, once composed
		rollup uint64 // of the batcher, once composed
		// unread has the window's anchor header unreadable once composed.
		unread bool
		// witnesses, when set, is the witness store.
		witnesses Witnesses
		// rewound has the Sync block replaced after its first read.
		rewound bool
		skipped bool
	}{
		{name: "a refused proof", err: status.Error(codes.InvalidArgument, "window refused")},
		{name: "a batch the batcher refuses", rollup: 2},
		{name: "a posting over its gas", maxGas: 900_000},
		{name: "an unreachable prover", err: status.Error(codes.Unavailable, "down"), skipped: true},
		{name: "a cancelled proof", err: status.Error(codes.Canceled, "stopping"), skipped: true},
		{name: "a malformed attestation", err: prove.ErrAttestation, skipped: true},
		{name: "an anchor header unread", unread: true, skipped: true},
		{name: "a witness that does not execute", witnesses: emptyWitness{}, skipped: true},
		{name: "a Sync block rewound while posting", rewound: true, skipped: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newCrossFixture(t)
			f.hold(t, 0, f.call.Source, f.call.Data)
			f.witnesses = c.witnesses
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
			if c.unread {
				delete(chain, 462)
			}
			sync := f.sealed(t, composed.Txs)
			chain[sync.NumberU64()] = sync
			if c.rewound {
				h := sync.Header()
				h.Extra = []byte{1}
				comp.chain = &rewinding{windowChain: chain, at: sync.NumberU64(), then: types.NewBlockWithHeader(h, nil)}
			}
			comp.SyncCommitted(t.Context(), sync.Header())
			p, ok := comp.ledger.TakeFailed(0)
			require.True(t, ok)
			require.Equal(t, c.skipped, p.SlotSkipped)
		})
	}
}

// holdOutbound holds the window's recorded outbound transaction i.
func (f *crossFixture) holdOutbound(t *testing.T, i int) *Held {
	t.Helper()
	return f.pushHeld(t, f.w.sync.Transactions()[1+2*i], l2Sender, Outbound, 8)
}

// TestComposeTheWindow: the window's held transactions, inbound first in
// the pool, compose into its Sync block: each outbound call loaded ahead
// of its transaction, then the inbound delivery.
func TestComposeTheWindow(t *testing.T) {
	f := newCrossFixture(t)
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	first, second := f.holdOutbound(t, 0), f.holdOutbound(t, 1)
	fx := f.compose(t)
	require.NotNil(t, fx)
	require.Equal(t, []*Held{first, second, trigger}, fx.held)
	want := f.w.sync.Transactions()
	require.Len(t, fx.txs, len(want))
	for i := range want {
		require.Equal(t, want[i].Hash(), fx.txs[i].Hash(), "transaction %d", i)
	}
	require.Len(t, fx.outbound, 2)
	for i, e := range fx.outbound {
		mined := f.w.batch.Entries[1+i].L2ToL1Calls[0]
		got := e.L2ToL1Calls[0]
		require.Equal(t, mined.SourceAddress, got.SourceAddress)
		require.Equal(t, mined.SourceRollupID, got.SourceRollupID)
		require.Equal(t, mined.TargetAddress, got.TargetAddress)
		require.Zero(t, mined.Value.Cmp(got.Value))
		require.Equal(t, mined.Data, got.Data)
	}
	require.Len(t, fx.deliveries, 1)
	require.Zero(t, f.pool.Len())
	// The window's anchor-only batch, every entry, its calldata, and the
	// outbound calls' gas on L1.
	base := f.windowGas(t) + 3*postBatchEntryGas
	for i, e := range fx.outbound {
		enc, err := protocol.EncodeExecutionEntry(&e)
		require.NoError(t, err)
		base += 2*calldataGas(enc) + calldataGas(fx.held[i].Raw)
	}
	sidecar := fx.deliveries[0].Sidecar
	for _, e := range []*protocol.ExecutionEntry{sidecar, {ProxyEntryHash: sidecar.ProxyEntryHash, ReturnData: sidecar.ReturnData, Success: true}} {
		enc, err := protocol.EncodeExecutionEntry(e)
		require.NoError(t, err)
		base += calldataGas(enc)
	}
	require.NotZero(t, fx.targetGas)
	require.Equal(t, base+fx.targetGas, fx.projected)
}

// TestComposedWindowPosts: the composed window settles as the mined batch,
// live, in a bundle whose only trigger is the inbound transaction.
func TestComposedWindowPosts(t *testing.T) {
	f := newCrossFixture(t)
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	first, second := f.holdOutbound(t, 0), f.holdOutbound(t, 1)
	c, prover, poster, chain := f.poster(t, nil)
	composed := c.ComposeSyncSlot(t.Context(), chain[467].Header(), f.w.sync.Time(), 101, sequencer.ModeSteady)
	require.Len(t, composed.Txs, 5)
	sync := f.sealed(t, composed.Txs)
	require.Equal(t, f.w.sync.Hash(), sync.Hash())
	chain[sync.NumberU64()] = sync
	c.SyncCommitted(t.Context(), sync.Header())

	require.Len(t, prover.proven, 1)
	got := *prover.proven[0].batch
	require.Equal(t, uint64(3), got.ImmediateEntryCount.Uint64())
	mined := *f.w.batch
	mined.BlockNumber, mined.Proofs = protocol.LiveBlockNumber, nil
	got.Proofs = nil
	wantEnc, err := protocol.EncodePostAndVerifyBatch(&mined)
	require.NoError(t, err)
	gotEnc, err := protocol.EncodePostAndVerifyBatch(&got)
	require.NoError(t, err)
	require.Equal(t, wantEnc, gotEnc)

	bundles := func() [][]common.Hash {
		poster.mu.Lock()
		defer poster.mu.Unlock()
		return poster.bundles
	}
	require.Eventually(t, func() bool { return len(bundles()) == 1 }, 5*time.Second, time.Millisecond)
	require.Equal(t, []common.Hash{poster.txs[0].Hash(), trigger.Hash}, bundles()[0])
	c.ledger.mu.Lock()
	e := c.ledger.entries[sync.NumberU64()]
	c.ledger.mu.Unlock()
	require.NotNil(t, e)
	require.Equal(t, []*Held{first, second, trigger}, e.Txs)
}

// TestComposeOutboundRefuses: an outbound transaction that cannot compose
// is evicted with its chain, its L1 frames undone; one whose nonce is spent
// is dropped alone.
func TestComposeOutboundRefuses(t *testing.T) {
	for _, c := range []struct {
		name string
		to   common.Address
		err  string
	}{
		{"several L1 calls", l2Twice, ErrMultiCall.Error() + ": it makes a second"},
		{"a call to another rollup", otherProxy, "call to rollup 2"},
		{"a failing L1 call", revertProxy, ErrCallFailed.Error()},
		{"a transaction failing with its call loaded", outboundProxies[1], "the transaction fails with its L1 call loaded"},
		{"a transaction not consuming its call", l2Stingy, "outbound entry without a matching L2 call"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newCrossFixture(t)
			gas := uint64(600_000)
			if c.to == outboundProxies[1] {
				// Enough for the simulation, where the proxy runs no code.
				gas = 30_000
			}
			f.holdOutGas(t, 0, c.to, 0, nil, gas)
			f.holdOut(t, 1, outboundProxies[1], 0, nil)
			var buf bytes.Buffer
			f.c.logger = log.New()
			f.c.logger.SetHandler(log.StreamHandler(&buf, log.TerminalFormatNoColor()))
			require.Nil(t, f.compose(t))
			require.Contains(t, buf.String(), c.err)
			require.Equal(t, 1, strings.Count(buf.String(), "eez.composer.poison"), "evicted once, with its chain")
			require.Zero(t, f.pool.Len())
			f.pool.mu.Lock()
			defer f.pool.mu.Unlock()
			require.Empty(t, f.pool.inFlight)
		})
	}
	t.Run("a spent nonce", func(t *testing.T) {
		f := newCrossFixture(t)
		f.holdOut(t, 0, outboundProxies[1], 0, nil)
		next := f.holdOut(t, 1, outboundProxies[1], 0, nil)
		f.l2.accounts[crypto.PubkeyToAddress(f.l2Key.PublicKey)].Nonce = 1
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{next}, fx.held)
		require.Len(t, fx.txs, 2)
	})
}

// TestComposeOutboundBoundsThePostingGas: an outbound entry is charged to
// the posting's gas; one over it alone is evicted.
func TestComposeOutboundBoundsThePostingGas(t *testing.T) {
	f := newCrossFixture(t)
	f.holdOutbound(t, 0)
	f.c.cfg.MaxGas = drainMargin + 551_000 + postBatchEntryGas
	require.Nil(t, f.compose(t))
	require.Zero(t, f.pool.Len())
}

// TestComposeAfterThePosting: a trigger the poster signed composes on the
// nonce after its posting's, though no outbound call opened the posting.
func TestComposeAfterThePosting(t *testing.T) {
	f := newCrossFixture(t)
	poster := *f
	poster.key = f.c.poster.(*testPoster).key
	trigger := poster.holdOn(t, 1, 1, f.call.Source, f.call.Data)
	fx := f.compose(t)
	require.NotNil(t, fx)
	require.Equal(t, []*Held{trigger}, fx.held)
}

// TestComposeOutboundPastTheDeadlineWaits: an outbound transaction whose
// load the deadline cuts short waits with its chain, not evicted.
func TestComposeOutboundPastTheDeadlineWaits(t *testing.T) {
	f := newCrossFixture(t)
	f.l1.m.accounts[protocol.EEZL2Address].Balance.SetUint64(100)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// The deadline passes between the simulation and the load.
	f.c.x.Escrow = func(context.Context, common.Hash) (*uint256.Int, error) {
		cancel()
		return uint256.NewInt(10), nil
	}
	h := f.holdOut(t, 0, outboundProxies[1], 7, nil)
	sync := f.w.sync.Header()
	require.Nil(t, f.c.compose(ctx, sync.Number.Uint64(), sync, 101, targetTime))
	require.Equal(t, []*Held{h}, f.pool.PopN(math.MaxInt), "not evicted")
	require.Zero(t, h.Attempts)
}

// TestComposeOutboundValue: an outbound call's value comes out of the
// rollup's escrow, read once at the anchor, and the manager's balance; a
// call they cannot pay waits with its chain, uncomposed, until its third
// attempt evicts it.
func TestComposeOutboundValue(t *testing.T) {
	t.Run("the escrow pays one", func(t *testing.T) {
		f := newCrossFixture(t)
		f.escrow = uint256.NewInt(10)
		f.l1.m.accounts[protocol.EEZL2Address].Balance.SetUint64(100)
		first := f.holdOut(t, 0, outboundProxies[1], 7, nil)
		second := f.holdOut(t, 1, outboundProxies[1], 7, nil)
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{first}, fx.held)
		require.Len(t, fx.outbound, 1)
		require.Equal(t, uint64(7), fx.outbound[0].L2ToL1Calls[0].Value.Uint64())
		require.Equal(t, uint64(10), f.escrow.Uint64(), "the escrow read is not spent")
		require.Equal(t, common.Hash{0x1, 100}, f.escrowAt, "read at the anchor")
		require.Equal(t, []*Held{second}, f.pool.PopN(math.MaxInt))
		require.Equal(t, uint32(1), second.Attempts)
	})
	t.Run("all of the escrow", func(t *testing.T) {
		f := newCrossFixture(t)
		f.escrow = uint256.NewInt(7)
		f.l1.m.accounts[protocol.EEZL2Address].Balance.SetUint64(100)
		h := f.holdOut(t, 0, outboundProxies[1], 7, nil)
		fx := f.compose(t)
		require.NotNil(t, fx)
		require.Equal(t, []*Held{h}, fx.held)
	})
	t.Run("the manager's balance pays none", func(t *testing.T) {
		f := newCrossFixture(t)
		f.escrow = uint256.NewInt(100)
		h := f.holdOut(t, 0, outboundProxies[1], 7, nil)
		require.Nil(t, f.compose(t))
		require.Equal(t, []*Held{h}, f.pool.PopN(math.MaxInt))
		require.Equal(t, uint32(1), h.Attempts)
	})
	t.Run("a third attempt evicts", func(t *testing.T) {
		f := newCrossFixture(t)
		f.escrow = uint256.NewInt(5)
		f.l1.m.accounts[protocol.EEZL2Address].Balance.SetUint64(100)
		f.holdOut(t, 0, outboundProxies[1], 7, nil)
		f.holdOut(t, 1, outboundProxies[1], 0, nil)
		for range 2 {
			require.Nil(t, f.compose(t))
			require.Equal(t, 2, f.pool.Len())
		}
		require.Nil(t, f.compose(t))
		require.Zero(t, f.pool.Len())
	})
	t.Run("an unread escrow waits uncharged", func(t *testing.T) {
		f := newCrossFixture(t)
		f.l1.m.accounts[protocol.EEZL2Address].Balance.SetUint64(100)
		h := f.holdOut(t, 0, outboundProxies[1], 7, nil)
		require.Nil(t, f.compose(t))
		require.Equal(t, []*Held{h}, f.pool.PopN(math.MaxInt))
		require.Zero(t, h.Attempts)
	})
}

// TestComposeKeepsEachReturn: outbound calls in one Sync block keep their
// own targets' outputs, in their entries and in their loads.
func TestComposeKeepsEachReturn(t *testing.T) {
	f := newCrossFixture(t)
	f.holdOut(t, 0, aaProxy, 0, nil)
	f.holdOut(t, 1, bbProxy, 0, nil)
	fx := f.compose(t)
	require.NotNil(t, fx)
	require.Len(t, fx.outbound, 2)
	for i, b := range []byte{0xaa, 0xbb} {
		want := bytes.Repeat([]byte{b}, 32)
		require.Equal(t, want, fx.outbound[i].ReturnData, "entry %d", i)
		var buf bytes.Buffer
		require.NoError(t, fx.txs[2*i].MarshalBinary(&buf))
		sys, err := protocol.DecodeSystemTx(buf.Bytes())
		require.NoError(t, err)
		table, err := protocol.DecodeLoadExecutionTable(sys.Input)
		require.NoError(t, err)
		require.Equal(t, want, table.Entries[0].ReturnData, "load %d", i)
	}
}

// TestComposeOutboundSharesThePosting: a slot's outbound calls run on L1 as
// the posting runs them, in one transaction from the poster at its price:
// transient storage carries from one call to the next.
func TestComposeOutboundSharesThePosting(t *testing.T) {
	f := newCrossFixture(t)
	f.holdOut(t, 0, tcounterProxy, 0, nil)
	f.holdOut(t, 1, tcounterProxy, 0, nil)
	f.holdOut(t, 2, originProxy, 0, nil)
	fx := f.compose(t)
	require.NotNil(t, fx)
	require.Len(t, fx.outbound, 3)
	for i := range 2 {
		require.Equal(t, num(uint64(i)), fx.outbound[i].ReturnData, "count %d", i)
	}
	poster := f.c.poster.(*testPoster)
	// The anchor's child has a base fee of 1.
	require.Equal(t, slices.Concat(word(poster.From()), num(3)), fx.outbound[2].ReturnData)
}

// TestComposeUndoesARefusedTransactionOnL1: what a refused outbound
// transaction's calls did on L1 is undone before the next composes.
func TestComposeUndoesARefusedTransactionOnL1(t *testing.T) {
	f := newCrossFixture(t)
	f.holdOut(t, 0, l2CounterTwice, 0, nil)
	h := f.holdOutOf(t, f.l2Key2, 0, counterProxy, 0, nil, 600_000)
	fx := f.compose(t)
	require.NotNil(t, fx)
	require.Equal(t, []*Held{h}, fx.held)
	one := common.BigToHash(big.NewInt(1))
	require.Equal(t, one[:], fx.outbound[0].ReturnData, "the first count")
}

// TestComposeEvictsAnUndecodableTransaction: a held transaction that does
// not decode is evicted.
func TestComposeEvictsAnUndecodableTransaction(t *testing.T) {
	f := newCrossFixture(t)
	h := &Held{Raw: []byte{0x02, 0xc0}, Hash: common.Hash{9}, Sender: crypto.PubkeyToAddress(f.l2Key.PublicKey), Direction: Outbound}
	require.NoError(t, f.pool.Push(h, 0))
	require.Nil(t, f.compose(t))
	require.Zero(t, f.pool.Len())
	require.False(t, f.pool.Holds(h.Hash), "never in a Sync block")
}

// TestComposedWindowCountsTargetGas: a posting whose outbound calls take it
// over its gas limit on L1 fails, charged.
func TestComposedWindowCountsTargetGas(t *testing.T) {
	mined := *loadSyncWindow(t).batch
	mined.BlockNumber = protocol.LiveBlockNumber
	for _, over := range []bool{false, true} {
		f := newCrossFixture(t)
		f.hold(t, 0, f.call.Source, f.call.Data)
		f.holdOutbound(t, 0)
		f.holdOutbound(t, 1)
		c, prover, _, chain := f.poster(t, nil)
		composed := c.ComposeSyncSlot(t.Context(), chain[467].Header(), f.w.sync.Time(), 101, sequencer.ModeSteady)
		require.NotZero(t, c.pending.fx.targetGas)
		posting, err := postingGas(&mined, c.pending.fx.targetGas)
		require.NoError(t, err)
		c.cfg.MaxGas = posting
		if over {
			c.cfg.MaxGas--
		}
		sync := f.sealed(t, composed.Txs)
		chain[sync.NumberU64()] = sync
		c.SyncCommitted(t.Context(), sync.Header())
		p, failed := c.ledger.TakeFailed(0)
		require.Equal(t, over, failed)
		require.Equal(t, over, len(prover.proven) == 0)
		if over {
			require.False(t, p.SlotSkipped)
		}
	}
}

// TestSyncCommittedKeepsItsOutbound: the outbound transactions of a
// committed Sync block stay the composer's once evicted: a replacement of
// the block returns them to the rollup's pool. Those of a Sync block not
// committed do not.
func TestSyncCommittedKeepsItsOutbound(t *testing.T) {
	for _, committed := range []bool{false, true} {
		f := newCrossFixture(t)
		out := f.holdOutbound(t, 0)
		c, _, _, chain := f.poster(t, nil)
		composed := c.ComposeSyncSlot(t.Context(), chain[467].Header(), f.w.sync.Time(), 101, sequencer.ModeSteady)
		require.Len(t, composed.Txs, 2)
		if committed {
			sync := f.sealed(t, composed.Txs)
			chain[sync.NumberU64()] = sync
			c.SyncCommitted(t.Context(), sync.Header())
		}
		f.pool.Evict(out.Sender, Outbound, out.Nonce)
		require.Equal(t, committed, f.pool.Holds(out.Hash), "committed %v", committed)
	}
}

// TestComposedOutboundRecoveredStaysHeld: the outbound transaction of a
// committed Sync block whose batch failed, requeued, stays the composer's
// once a replacement takes its place: the Sync block's replacement returns
// it to the rollup's pool.
func TestComposedOutboundRecoveredStaysHeld(t *testing.T) {
	f := newCrossFixture(t)
	out := f.holdOutbound(t, 0)
	comp, prover, _, chain := f.poster(t, nil)
	comp.Recover(f.pool, func(context.Context, *types.Header) (*types.Header, error) { return nil, nil })
	prover.err = status.Error(codes.InvalidArgument, "window refused")
	composed := comp.ComposeSyncSlot(t.Context(), chain[467].Header(), f.w.sync.Time(), 101, sequencer.ModeSteady)
	require.Len(t, composed.Txs, 2)
	sync := f.sealed(t, composed.Txs)
	chain[sync.NumberU64()] = sync
	comp.SyncCommitted(t.Context(), sync.Header())
	comp.RecoverFailed(t.Context())
	require.Equal(t, 1, f.pool.Len(), "requeued")
	replacement := &Held{Raw: []byte{1}, Hash: common.Hash{0xee}, Sender: out.Sender, Nonce: out.Nonce, Direction: Outbound}
	replacement.MaxFee.Mul(&out.MaxFee, uint256.NewInt(2))
	replacement.Tip.Mul(&out.Tip, uint256.NewInt(2))
	require.NoError(t, f.pool.Push(replacement, out.Nonce))
	require.Equal(t, []*Held{replacement}, f.pool.PopN(math.MaxInt))
	require.True(t, f.pool.Holds(out.Hash))
}

// TestComposePrunesSpentDiscards: a composition lets go of the discarded
// outbound transactions whose nonces the rollup spent past a final Sync
// block.
func TestComposePrunesSpentDiscards(t *testing.T) {
	f := newCrossFixture(t)
	f.c.finalized = func() uint64 { return 460 }
	spent, kept := held(addr(0xa), 0, Outbound, 1), held(addr(0xb), 0, Outbound, 2)
	for _, h := range []*Held{spent, kept} {
		push(t, f.pool, h, 0)
	}
	f.pool.Composed(f.pool.PopN(2), 460)
	for _, h := range []*Held{spent, kept} {
		f.pool.Evict(h.Sender, Outbound, h.Nonce)
	}
	f.l2.fund(spent.Sender, 0)
	f.l2.accounts[spent.Sender].Nonce = 1
	f.hold(t, 0, f.call.Source, f.call.Data)
	require.NotNil(t, f.compose(t))
	require.False(t, f.pool.Holds(spent.Hash))
	require.True(t, f.pool.Holds(kept.Hash))
}

// TestComposeOutboundFillsTheBlock: an outbound pair the Sync block cannot
// hold stops the composition; the rest, inbound included, waits in the
// drain's order, outbound first.
func TestComposeOutboundFillsTheBlock(t *testing.T) {
	f := newCrossFixture(t)
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	first, second := f.holdOutbound(t, 0), f.holdOutbound(t, 1)
	// After the first pair uses its gas, no room for the second's load.
	f.c.x.Header = func(*types.Header, rules.ChainHeaderReader) (*types.Header, error) {
		h := types.CopyHeader(f.w.sync.Header())
		h.GasLimit = 127_241 + 82_521 + protocol.SystemTxGasLimit - 1
		return h, nil
	}
	fx := f.compose(t)
	require.NotNil(t, fx)
	require.Equal(t, []*Held{first}, fx.held)
	require.Equal(t, []*Held{second, trigger}, f.pool.PopN(math.MaxInt))
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
	f.l1.m.fund(poster.From(), 1e18)
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

// TestComposeOutboundBehindInboundWaits: an outbound transaction the drain
// pops after an inbound one ran, composed or evicted, waits for the next
// slot, uncharged: the posting, which runs a Sync block's outbound calls,
// precedes its triggers.
func TestComposeOutboundBehindInboundWaits(t *testing.T) {
	for name, composes := range map[string]bool{"composed": true, "evicted": false} {
		t.Run(name, func(t *testing.T) {
			f := newCrossFixture(t)
			f.c.x.MaxTxs = 2
			to, data := f.call.Source, f.call.Data
			if !composes {
				// It makes no cross-chain call.
				to, data = unproxied, nil
			}
			trigger := f.hold(t, 0, to, data)
			// Waiting on a nonce gap, out of the drain's count, it has the
			// drain pop the outbound transaction once the trigger ran.
			key, err := crypto.GenerateKey()
			require.NoError(t, err)
			f.l1.m.fund(crypto.PubkeyToAddress(key.PublicKey), 1e18)
			other := *f
			other.key = key
			gapped := other.holdOn(t, 1, 1, f.call.Source, f.call.Data)
			out := f.holdOutbound(t, 0)
			fx := f.compose(t)
			if composes {
				require.NotNil(t, fx)
				require.Equal(t, []*Held{trigger}, fx.held)
			} else {
				require.Nil(t, fx)
				require.False(t, f.pool.Holds(trigger.Hash), "evicted")
			}
			require.Equal(t, []*Held{gapped, out}, f.pool.PopN(math.MaxInt))
			require.Zero(t, out.Attempts)
			require.Zero(t, out.Deferred)
		})
	}
}

// TestComposeOutboundBehindInboundCounts: an outbound transaction waiting
// behind a trigger counts against the drain, which pops no more for it.
func TestComposeOutboundBehindInboundCounts(t *testing.T) {
	f := newCrossFixture(t)
	f.c.x.MaxTxs = 2
	trigger := f.hold(t, 0, f.call.Source, f.call.Data)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	f.l1.m.fund(crypto.PubkeyToAddress(key.PublicKey), 1e18)
	other := *f
	other.key = key
	gapped := other.holdOn(t, 1, 1, f.call.Source, f.call.Data)
	out := f.holdOutbound(t, 0)
	next := f.holdOutbound(t, 1)
	var buf bytes.Buffer
	f.c.logger = log.New()
	f.c.logger.SetHandler(log.StreamHandler(&buf, log.TerminalFormatNoColor()))
	fx := f.compose(t)
	require.NotNil(t, fx)
	require.Equal(t, []*Held{trigger}, fx.held)
	require.Contains(t, buf.String(), "waiting=2", "the second outbound transaction stays queued")
	require.Equal(t, []*Held{gapped, out, next}, f.pool.PopN(math.MaxInt))
}

// TestComposeOutboundWaitsOnANonceGap: an outbound transaction whose nonce
// is ahead of its sender's on the rollup waits, as an inbound one does.
func TestComposeOutboundWaitsOnANonceGap(t *testing.T) {
	f := newCrossFixture(t)
	sender := crypto.PubkeyToAddress(f.l2Key.PublicKey)
	// Admitted on a pending nonce.
	f.l2.accounts[sender].Nonce = 1
	h := f.holdOut(t, 1, outboundProxies[1], 0, nil)
	f.l2.accounts[sender].Nonce = 0
	require.Nil(t, f.compose(t))
	require.Equal(t, uint32(1), h.Deferred)
	require.Equal(t, []*Held{h}, f.pool.PopN(math.MaxInt), "not evicted")
}

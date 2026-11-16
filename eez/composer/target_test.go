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
	"errors"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	// valueTarget returns 1 and its first argument.
	valueTarget = common.HexToAddress("0xd000000000000000000000000000000000000001")
	// callerTarget returns its caller.
	callerTarget = common.HexToAddress("0xd000000000000000000000000000000000000002")
	// revertTarget reverts with 0xdead.
	revertTarget = common.HexToAddress("0xd000000000000000000000000000000000000003")
	// reentrant calls l1Proxy, a registered proxy.
	reentrant = common.HexToAddress("0xd000000000000000000000000000000000000004")
	// payee takes any value.
	payee = common.HexToAddress("0xd000000000000000000000000000000000000005")
	// flagger returns its transient flag, then sets it.
	flagger = common.HexToAddress("0xd000000000000000000000000000000000000006")
	l1Proxy = common.HexToAddress("0xd0000000000000000000000000000000000000ff")
	// l2Sender is a rollup account calling L1.
	l2Sender = common.HexToAddress("0x277aF7Ba0CFBD9455Aa0401528f4BcEC683EA27E")
)

// l2Rollup is the rollup of l2Sender: not 1, whose proxies EEZL2's code,
// standing in for the manager, refuses to create as its own rollup's.
const l2Rollup = 2

// targetFixture is an L1 whose manager runs EEZL2's code at EEZL2's
// address, so it creates a proxy where EEZL2 did on window 468's rollup.
type targetFixture struct {
	m   *memState
	src *Source
}

func newTargetFixture(t *testing.T) *targetFixture {
	t.Helper()
	w := loadSyncWindow(t)
	code, err := w.pre.Reader.ReadAccountCode(accounts.InternAddress(protocol.EEZL2Address))
	require.NoError(t, err)
	require.NotEmpty(t, code)
	m := newMemState()
	m.deploy(protocol.EEZL2Address, code)
	m.accounts[protocol.EEZL2Address].Balance.SetUint64(1000)
	m.set(protocol.EEZL2Address, protocol.ProxyKey(l1Proxy), proxyWord(valueTarget, 1))
	m.deploy(valueTarget, []byte{0x60, 0x01, 0x60, 0x00, 0x52, 0x60, 0x04, 0x35, 0x60, 0x20, 0x52, 0x60, 0x40, 0x60, 0x00, 0xf3})
	m.deploy(callerTarget, []byte{0x33, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3})
	m.deploy(revertTarget, []byte{0x61, 0xde, 0xad, 0x60, 0x00, 0x52, 0x60, 0x02, 0x60, 0x1e, 0xfd})
	m.deploy(reentrant, append(callTo(l1Proxy), 0x00))
	m.deploy(payee, []byte{0x00})
	m.deploy(flagger, []byte{0x5f, 0x5c, 0x5f, 0x52, 0x60, 0x01, 0x5f, 0x5d, 0x60, 0x20, 0x5f, 0xf3})
	header := &types.Header{Number: *uint256.NewInt(100), Time: 1_800_000_000, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1), Extra: []byte{}}
	src := NewSource(chain.TestChainOsakaConfig, header, m, func(uint64) (common.Hash, error) { return common.Hash{}, nil }, protocol.EEZL2Address, protocol.MainnetRollupID)
	return &targetFixture{m: m, src: src}
}

func (f *targetFixture) proxyOf(t *testing.T, p common.Address) (protocol.Proxy, bool) {
	t.Helper()
	word, err := f.src.ibs.GetState(accounts.InternAddress(protocol.EEZL2Address), accounts.InternKey(protocol.ProxyKey(p)))
	require.NoError(t, err)
	return protocol.DecodeProxy(word.Bytes32())
}

func (f *targetFixture) balance(t *testing.T, a common.Address) uint64 {
	t.Helper()
	b, err := f.src.ibs.GetBalance(accounts.InternAddress(a))
	require.NoError(t, err)
	return b.Uint64()
}

func (f *targetFixture) nonce(t *testing.T, a common.Address) uint64 {
	t.Helper()
	n, err := f.src.ibs.GetNonce(accounts.InternAddress(a))
	require.NoError(t, err)
	return n
}

// word is a left-padded to 32 bytes.
func word(a common.Address) []byte {
	h := common.BytesToHash(a[:])
	return h[:]
}

// execute runs c through Execute in a transaction of its own.
func (f *targetFixture) execute(c *Call) ([]byte, error) {
	var out []byte
	err := f.src.Atomic(func() error {
		var err error
		out, err = f.src.Execute(c)
		return err
	})
	return out, err
}

// TestExecuteCreatesTheProxy: the target sees the proxy of the call's
// source, created where EEZL2 created it on the rollup, and registered.
func TestExecuteCreatesTheProxy(t *testing.T) {
	f := newTargetFixture(t)
	// EEZL2 created 0x1Fc9… for 0x2E98… of rollup 0.
	recorded := common.HexToAddress("0x1Fc99E43B3DBECFe7dFe20Ae0ACbD58dEea30cf7")
	source := common.HexToAddress("0x2E983A1Ba5e8b38AAAeC4B440B9dDcFBf72E15d1")
	_, ok := f.proxyOf(t, recorded)
	require.False(t, ok)
	nonce := f.nonce(t, protocol.EEZL2Address)
	c := &Call{Target: callerTarget, Source: source, SourceRollup: 0}
	out, err := f.execute(c)
	require.NoError(t, err)
	require.Equal(t, word(recorded), out, "the target's caller")
	p, ok := f.proxyOf(t, recorded)
	require.True(t, ok)
	require.Equal(t, protocol.Proxy{Address: source, RollupID: 0}, p)
	require.Equal(t, nonce+1, f.nonce(t, protocol.EEZL2Address), "one creation")
	require.NotZero(t, c.Gas)

	// The proxy exists: no second creation.
	out, err = f.execute(&Call{Target: callerTarget, Source: source, SourceRollup: 0})
	require.NoError(t, err)
	require.Equal(t, word(recorded), out)
	require.Equal(t, nonce+1, f.nonce(t, protocol.EEZL2Address))
}

func TestExecuteReturnsTheTargetsOutput(t *testing.T) {
	f := newTargetFixture(t)
	c := &Call{Target: valueTarget, Source: l2Sender, SourceRollup: l2Rollup, Data: append([]byte{0x55, 0x24, 0x10, 0x77}, func() []byte { h := common.BigToHash(big.NewInt(403)); return h[:] }()...)}
	out, err := f.execute(c)
	require.NoError(t, err)
	require.Equal(t, common.FromHex("0x00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000193"), out)
	c = &Call{Target: payee, Source: l2Sender, SourceRollup: l2Rollup}
	out, err = f.execute(c)
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestExecuteMovesTheValue(t *testing.T) {
	f := newTargetFixture(t)
	c := &Call{Target: payee, Source: l2Sender, SourceRollup: l2Rollup}
	c.Value.SetUint64(7)
	_, err := f.execute(c)
	require.NoError(t, err)
	require.Equal(t, uint64(7), f.balance(t, payee))
	require.Equal(t, uint64(993), f.balance(t, protocol.EEZL2Address))
}

func TestExecuteRefuses(t *testing.T) {
	for _, c := range []struct {
		name   string
		call   Call
		value  uint64
		err    error
		poison bool
	}{
		{"a value the manager cannot pay", Call{Target: payee}, 1001, ErrFunding, false},
		{"a call to the manager", Call{Target: protocol.EEZL2Address}, 0, ErrToManager, true},
		{"a reverting target", Call{Target: revertTarget}, 0, ErrCallFailed, true},
		{"a target calling a proxy", Call{Target: reentrant}, 0, ErrReentry, true},
		{"a target that is a proxy", Call{Target: l1Proxy}, 0, ErrReentry, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newTargetFixture(t)
			call := c.call
			call.Source, call.SourceRollup = l2Sender, l2Rollup
			call.Value.SetUint64(c.value)
			nonce := f.nonce(t, protocol.EEZL2Address)
			_, err := f.execute(&call)
			require.ErrorIs(t, err, c.err)
			require.Equal(t, c.poison, IsPoison(err), err)
			require.Equal(t, nonce, f.nonce(t, protocol.EEZL2Address), "no proxy kept")
			require.Equal(t, uint64(1000), f.balance(t, protocol.EEZL2Address))
		})
	}
	t.Run("a proxy the manager does not create", func(t *testing.T) {
		f := newTargetFixture(t)
		// EEZL2's code refuses a proxy of its own rollup.
		_, err := f.execute(&Call{Target: payee, Source: l2Sender, SourceRollup: 1})
		require.True(t, IsPoison(err), err)
		require.ErrorContains(t, err, "create the proxy")
	})
	for name, manager := range map[string]common.Address{"failing": revertTarget, "answering nothing": payee} {
		t.Run("a manager's proxy address view "+name, func(t *testing.T) {
			f := newTargetFixture(t)
			f.src = NewSource(chain.TestChainOsakaConfig, f.src.header, f.m, f.src.blockHash, manager, protocol.MainnetRollupID)
			_, err := f.execute(&Call{Target: valueTarget, Source: l2Sender, SourceRollup: l2Rollup})
			require.True(t, IsPoison(err), err)
		})
	}
	t.Run("a failed state read waits", func(t *testing.T) {
		f := newTargetFixture(t)
		r := &failing{StateReader: f.m, addr: payee, armed: true}
		f.src = NewSource(chain.TestChainOsakaConfig, f.src.header, r, f.src.blockHash, protocol.EEZL2Address, protocol.MainnetRollupID)
		_, err := f.execute(&Call{Target: payee, Source: l2Sender, SourceRollup: l2Rollup})
		require.ErrorIs(t, err, ErrStateRead)
		require.False(t, IsPoison(err))
	})
}

// TestAtomicUndoes: writes made in a refused transaction are undone, and
// kept in an accepted one.
func TestAtomicUndoes(t *testing.T) {
	f := newTargetFixture(t)
	nonce := f.nonce(t, protocol.EEZL2Address)
	refused := errors.New("refused")
	err := f.src.Atomic(func() error {
		c := &Call{Target: payee, Source: l2Sender, SourceRollup: l2Rollup}
		c.Value.SetUint64(7)
		_, err := f.src.Execute(c)
		require.NoError(t, err)
		return refused
	})
	require.ErrorIs(t, err, refused)
	require.Equal(t, nonce, f.nonce(t, protocol.EEZL2Address))
	require.Zero(t, f.balance(t, payee))
	c := &Call{Target: payee, Source: l2Sender, SourceRollup: l2Rollup}
	c.Value.SetUint64(7)
	_, err = f.execute(c)
	require.NoError(t, err)
	require.Equal(t, nonce+1, f.nonce(t, protocol.EEZL2Address))
	require.Equal(t, uint64(7), f.balance(t, payee))
}

// TestAtomicStartsATransaction: transient storage does not outlive the
// transaction that set it (EIP-1153).
func TestAtomicStartsATransaction(t *testing.T) {
	f := newTargetFixture(t)
	for range 2 {
		out, err := f.execute(&Call{Target: flagger, Source: l2Sender, SourceRollup: l2Rollup})
		require.NoError(t, err)
		require.Equal(t, make([]byte, 32), out)
	}
}

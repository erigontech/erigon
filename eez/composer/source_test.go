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
	"crypto/ecdsa"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// memState is a state reader over maps.
type memState struct {
	accounts map[common.Address]*accounts.Account
	code     map[common.Address][]byte
	storage  map[[2]common.Hash]uint256.Int
}

func newMemState() *memState {
	return &memState{accounts: map[common.Address]*accounts.Account{}, code: map[common.Address][]byte{}, storage: map[[2]common.Hash]uint256.Int{}}
}

func (m *memState) fund(a common.Address, wei uint64) {
	acc := accounts.NewAccount()
	acc.Balance.SetUint64(wei)
	m.accounts[a] = &acc
}

func (m *memState) deploy(a common.Address, code []byte) {
	acc := accounts.NewAccount()
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	acc.Incarnation = 1
	m.accounts[a] = &acc
	m.code[a] = code
}

func (m *memState) set(a common.Address, key, value common.Hash) {
	var v uint256.Int
	v.SetBytes32(value[:])
	m.storage[[2]common.Hash{common.BytesToHash(a[:]), key}] = v
}

func (m *memState) ReadAccountData(a accounts.Address) (*accounts.Account, error) {
	return m.ReadAccountByPlainAddr(a.Value())
}

func (m *memState) ReadAccountByPlainAddr(a common.Address) (*accounts.Account, error) {
	if acc := m.accounts[a]; acc != nil {
		c := *acc
		return &c, nil
	}
	return nil, nil
}

func (m *memState) HasAccount(a common.Address) (bool, error) { return m.accounts[a] != nil, nil }

func (m *memState) ReadAccountDataForDebug(a accounts.Address) (*accounts.Account, error) {
	return m.ReadAccountData(a)
}

func (m *memState) ReadAccountStorage(a accounts.Address, k accounts.StorageKey) (uint256.Int, bool, error) {
	addr := a.Value()
	v, ok := m.storage[[2]common.Hash{common.BytesToHash(addr[:]), k.Value()}]
	return v, ok, nil
}

func (m *memState) ReadAccountCode(a accounts.Address) ([]byte, error) { return m.code[a.Value()], nil }

func (m *memState) ReadAccountCodeSize(a accounts.Address) (int, error) {
	return len(m.code[a.Value()]), nil
}

func (m *memState) ReadAccountIncarnation(a accounts.Address) (uint64, error) {
	if acc := m.accounts[a.Value()]; acc != nil {
		return acc.Incarnation, nil
	}
	return 0, nil
}

func (m *memState) SetTrace(bool, string) {}
func (m *memState) Trace() bool           { return false }
func (m *memState) TracePrefix() string   { return "" }

// proxyWord is authorizedProxies' word of a proxy for target on rollup.
func proxyWord(target common.Address, rollup uint64) common.Hash {
	var w common.Hash
	binary.BigEndian.PutUint64(w[3:11], rollup)
	copy(w[11:31], target[:])
	w[31] = 1
	return w
}

func push20(a common.Address) []byte { return append([]byte{0x73}, a[:]...) }

// callTo calls a with no data or value, dropping the outcome.
func callTo(a common.Address) []byte {
	return slices.Concat([]byte{0x5f, 0x5f, 0x5f, 0x5f, 0x5f}, push20(a), []byte{0x5a, 0xf1, 0x50})
}

// forward calls a with the calldata and value it got, then returns 32 bytes
// of the output; with revert it reverts instead.
func forward(a common.Address, revert bool) []byte {
	code := slices.Concat([]byte{0x36, 0x5f, 0x5f, 0x37, 0x60, 0x20, 0x5f, 0x36, 0x5f, 0x34}, push20(a), []byte{0x5a, 0xf1, 0x50})
	if revert {
		return append(code, 0x5f, 0x5f, 0xfd)
	}
	return append(code, 0x60, 0x20, 0x5f, 0xf3)
}

var (
	manager    = common.HexToAddress("0xeeee000000000000000000000000000000000001")
	proxy      = common.HexToAddress("0xaaaa000000000000000000000000000000000001")
	late       = common.HexToAddress("0xaaaa000000000000000000000000000000000002")
	target     = common.HexToAddress("0x7777000000000000000000000000000000000001")
	lateTarget = common.HexToAddress("0x7777000000000000000000000000000000000002")
	forwarder  = common.HexToAddress("0xc000000000000000000000000000000000000001")
	reverter   = common.HexToAddress("0xc000000000000000000000000000000000000002")
	catcher    = common.HexToAddress("0xc000000000000000000000000000000000000003")
	twice      = common.HexToAddress("0xc000000000000000000000000000000000000004")
	static     = common.HexToAddress("0xc000000000000000000000000000000000000005")
	unproxied  = common.HexToAddress("0xc000000000000000000000000000000000000006")
	registrar  = common.HexToAddress("0xc000000000000000000000000000000000000007")
	nobody     = common.HexToAddress("0xc000000000000000000000000000000000000008")
)

type sourceFixture struct {
	key    *ecdsa.PrivateKey
	sender common.Address
	src    *Source
	state  *memState
}

func newSourceFixture(t *testing.T) *sourceFixture {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	f := &sourceFixture{key: key, sender: crypto.PubkeyToAddress(key.PublicKey), state: newMemState()}
	m := f.state
	m.fund(f.sender, 1e18)
	m.deploy(manager, slices.Concat([]byte{0x7f}, func() []byte { w := proxyWord(lateTarget, 1); return w[:] }(),
		[]byte{0x7f}, func() []byte { k := protocol.ProxyKey(late); return k[:] }(), []byte{0x55, 0x00}))
	m.set(manager, protocol.ProxyKey(proxy), proxyWord(target, 1))
	m.deploy(forwarder, forward(proxy, false))
	m.deploy(reverter, forward(proxy, true))
	m.deploy(catcher, append(callTo(reverter), 0x00))
	m.deploy(twice, slices.Concat(callTo(proxy), callTo(proxy), []byte{0x00}))
	m.deploy(static, slices.Concat([]byte{0x5f, 0x5f, 0x5f, 0x5f}, push20(proxy), []byte{0x5a, 0xfa, 0x50, 0x00}))
	m.deploy(unproxied, append(callTo(nobody), 0x00))
	m.deploy(registrar, slices.Concat(callTo(manager), callTo(late), []byte{0x00}))
	header := &types.Header{Number: *uint256.NewInt(100), Time: 1_800_000_000, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1), Extra: []byte{}}
	f.src = NewSource(chain.TestChainOsakaConfig, header, m, func(uint64) (common.Hash, error) { return common.Hash{}, nil }, manager, 0)
	return f
}

func (f *sourceFixture) tx(t *testing.T, nonce uint64, to common.Address, value uint64, data []byte) types.Transaction {
	t.Helper()
	tx := &types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 1_000_000, To: &to, Data: data}, ChainID: *uint256.MustFromBig(chain.TestChainOsakaConfig.ChainID.ToBig())}
	tx.Value.SetUint64(value)
	tx.FeeCap.SetUint64(10)
	tx.TipCap.SetUint64(1)
	signed, err := types.SignTx(tx, *types.LatestSignerForChainID(&tx.ChainID), f.key)
	require.NoError(t, err)
	return signed
}

func (f *sourceFixture) nonce(t *testing.T) uint64 {
	t.Helper()
	n, err := f.src.ibs.GetNonce(accounts.InternAddress(f.sender))
	require.NoError(t, err)
	return n
}

// echo answers every call with 0xbeef, keeping the calls.
type echo struct{ calls []*Call }

func (e *echo) dispatch(c *Call) ([]byte, error) {
	e.calls = append(e.calls, c)
	return []byte{0xbe, 0xef}, nil
}

func TestSourceDispatchesProxyCalls(t *testing.T) {
	f := newSourceFixture(t)
	e := &echo{}
	calls, err := f.src.Run(f.tx(t, 0, forwarder, 5, []byte{0x12, 0x34}), e.dispatch)
	require.NoError(t, err)
	want := &Call{Target: target, RollupID: 1, Source: forwarder, SourceRollup: 0, Data: []byte{0x12, 0x34}, Return: []byte{0xbe, 0xef}}
	want.Value.SetUint64(5)
	require.Equal(t, []*Call{want}, calls)
	require.Equal(t, e.calls, calls)
	balance, err := f.src.ibs.GetBalance(accounts.InternAddress(proxy))
	require.NoError(t, err)
	require.True(t, balance.IsZero(), "the value stays with the caller")
	balance, err = f.src.ibs.GetBalance(accounts.InternAddress(forwarder))
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(5), &balance)
	require.Equal(t, uint64(1), f.nonce(t), "kept")
}

func TestSourceCalls(t *testing.T) {
	for _, c := range []struct {
		name  string
		to    common.Address
		calls []common.Address
		err   error
	}{
		{"two calls", twice, []common.Address{target, target}, nil},
		{"a proxy registered in the transaction", registrar, []common.Address{lateTarget}, nil},
		{"a reverted caller", reverter, nil, ErrRevertedCall},
		{"a caught revert", catcher, nil, ErrRevertedCall},
		{"a static call", static, nil, ErrNoCalls},
		{"no proxy", unproxied, nil, ErrNoCalls},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newSourceFixture(t)
			calls, err := f.src.Run(f.tx(t, 0, c.to, 0, nil), (&echo{}).dispatch)
			if c.err != nil {
				require.ErrorIs(t, err, c.err)
				require.True(t, IsPoison(err))
				require.Zero(t, f.nonce(t), "discarded")
				return
			}
			require.NoError(t, err)
			var targets []common.Address
			for _, call := range calls {
				targets = append(targets, call.Target)
			}
			require.Equal(t, c.calls, targets)
		})
	}
}

func TestSourceRefuses(t *testing.T) {
	t.Run("an invalid transaction is poison", func(t *testing.T) {
		f := newSourceFixture(t)
		_, err := f.src.Run(f.tx(t, 5, forwarder, 0, nil), (&echo{}).dispatch)
		require.True(t, IsPoison(err), err)
	})
	t.Run("a dispatch failure is the dispatcher's", func(t *testing.T) {
		f := newSourceFixture(t)
		down := errors.New("rollup unavailable")
		_, err := f.src.Run(f.tx(t, 0, forwarder, 0, nil), func(*Call) ([]byte, error) { return nil, down })
		require.ErrorIs(t, err, down)
		require.False(t, IsPoison(err))
		require.Zero(t, f.nonce(t), "discarded")
	})
	t.Run("a refused transaction leaves no trace", func(t *testing.T) {
		f := newSourceFixture(t)
		_, err := f.src.Run(f.tx(t, 0, forwarder, 0, nil), (&echo{}).dispatch)
		require.NoError(t, err)
		_, err = f.src.Run(f.tx(t, 1, reverter, 0, nil), (&echo{}).dispatch)
		require.Error(t, err)
		_, err = f.src.Run(f.tx(t, 1, forwarder, 0, nil), (&echo{}).dispatch)
		require.NoError(t, err)
		require.Equal(t, uint64(2), f.nonce(t))
	})
}

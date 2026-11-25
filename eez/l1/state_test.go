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
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// view is what a state reader reports of a few accounts at one block.
type view struct {
	header   *types.Header
	accounts map[common.Address]*accounts.Account
	code     map[common.Address][]byte
	size     map[common.Address]int
	slot0    uint256.Int
	stored   bool
	slot1    uint256.Int
	stored1  bool
	has      map[common.Address]bool
}

func read(t *testing.T, s l1.States, hash common.Hash, addrs []common.Address) view {
	t.Helper()
	v := view{accounts: map[common.Address]*accounts.Account{}, code: map[common.Address][]byte{}, size: map[common.Address]int{}, has: map[common.Address]bool{}}
	require.NoError(t, s.ViewState(t.Context(), hash, func(h *types.Header, st state.StateReader) error {
		v.header = h
		for _, a := range addrs {
			acc, err := st.ReadAccountData(accounts.InternAddress(a))
			require.NoError(t, err)
			if acc != nil {
				// The roots of storage tries are no state a simulation reads.
				acc.Root = common.Hash{}
			}
			v.accounts[a] = acc
			code, err := st.ReadAccountCode(accounts.InternAddress(a))
			require.NoError(t, err)
			v.code[a] = code
			size, err := st.ReadAccountCodeSize(accounts.InternAddress(a))
			require.NoError(t, err)
			v.size[a] = size
			has, err := st.HasAccount(a)
			require.NoError(t, err)
			v.has[a] = has
		}
		var err error
		v.slot0, v.stored, err = st.ReadAccountStorage(accounts.InternAddress(l1test.Registry), accounts.InternKey(common.Hash{}))
		if err != nil {
			return err
		}
		v.slot1, v.stored1, err = st.ReadAccountStorage(accounts.InternAddress(l1test.Registry), accounts.InternKey(common.Hash{31: 1}))
		return err
	}))
	return v
}

// TestStatesAgree: a node's JSON-RPC serves the state its database holds.
func TestStatesAgree(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	ctx := t.Context()
	head, err := model.Head(ctx)
	require.NoError(t, err)
	key, err := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	addrs := []common.Address{sender, l1test.Registry, l1test.EmitterA, common.HexToAddress("0xdead")}
	native := l1.NewNative(m.DB, m.BlockReader, m.ReceiptsReader, m.Engine, m.ChainConfig)
	rpc := erigonRPC(t, m)
	for _, number := range []uint64{head.Number, 2} {
		h, err := model.HeaderByNumber(ctx, number)
		require.NoError(t, err)
		want := read(t, native, h.Hash, addrs)
		got := read(t, rpc, h.Hash, addrs)
		require.Equal(t, h.Hash, want.header.Hash())
		require.Equal(t, want.header.Hash(), got.header.Hash())
		for _, a := range addrs {
			require.Equal(t, describe(want.accounts[a]), describe(got.accounts[a]), "block %d account %x", number, a)
			require.Equal(t, want.code[a], got.code[a], "block %d code of %x", number, a)
			require.Equal(t, want.size[a], got.size[a], "block %d code size of %x", number, a)
			require.Equal(t, want.has[a], got.has[a], "block %d existence of %x", number, a)
		}
		require.Equal(t, want.slot0, got.slot0, "block %d", number)
		require.Equal(t, want.stored, got.stored, "block %d", number)
		require.Equal(t, want.slot1, got.slot1, "block %d", number)
		require.Equal(t, want.stored1, got.stored1, "block %d", number)
		require.False(t, got.stored1, "slot 1 is unset")
		require.NotNil(t, got.accounts[sender])
		require.NotZero(t, got.accounts[sender].Nonce)
		require.NotEmpty(t, got.code[l1test.Registry])
		require.True(t, got.stored, "the registry's root is stored")
		require.Nil(t, got.accounts[common.HexToAddress("0xdead")])
	}
	err = rpc.ViewState(ctx, common.Hash{0xbe, 0xef}, func(*types.Header, state.StateReader) error { return nil })
	require.ErrorIs(t, err, l1.ErrNotFound)
}

func describe(a *accounts.Account) string {
	if a == nil {
		return "none"
	}
	return fmt.Sprintf("nonce %d balance %s code %x incarnation %d", a.Nonce, &a.Balance, a.CodeHash.Value(), a.Incarnation)
}

// forger serves a header that is not the one of the block asked for.
type forger struct{}

func (forger) GetBlockByHash(_ context.Context, hash common.Hash, full bool) (*types.Header, error) {
	return &types.Header{Number: *uint256.NewInt(7), Extra: []byte{}}, nil
}

func TestRPCStateRefusesAForgedHeader(t *testing.T) {
	err := serveRPC(t, forger{}).ViewState(t.Context(), common.Hash{1}, func(*types.Header, state.StateReader) error {
		t.Fatal("read a forged header")
		return nil
	})
	require.ErrorIs(t, err, l1.ErrInconsistent)
}

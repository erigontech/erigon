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

package exec

import (
	"errors"
	"slices"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func legacyTo(to *common.Address) types.Transaction {
	return &types.LegacyTx{CommonTx: types.CommonTx{To: to, GasLimit: 21_000}}
}

func accessListTo(to *common.Address, list types.AccessList) types.Transaction {
	return &types.AccessListTx{LegacyTx: types.LegacyTx{CommonTx: types.CommonTx{To: to, GasLimit: 21_000}}, AccessList: list}
}

func sortedAddrs(addrs ...common.Address) []common.Address {
	slices.SortFunc(addrs, common.Address.Cmp)
	return addrs
}

func TestStaticKeysCollectsSortedUniqueAddressesAndSlots(t *testing.T) {
	coinbase := common.Address{19: 0x10}
	s1, s2 := common.Address{19: 0x03}, common.Address{19: 0x30}
	dest, listed, withdrawn := common.Address{19: 0x20}, common.Address{19: 0x05}, common.Address{19: 0x40}
	slotA, slotB := common.Hash{31: 0x02}, common.Hash{31: 0x01}
	txns := types.Transactions{
		legacyTo(&dest),
		accessListTo(&dest, types.AccessList{
			{Address: listed, StorageKeys: []common.Hash{slotA, slotB, slotA}},
			{Address: dest, StorageKeys: []common.Hash{slotB}},
		}),
		legacyTo(nil),
	}
	header := &types.Header{Coinbase: coinbase}
	body := &types.Body{Transactions: txns, Withdrawals: []*types.Withdrawal{{Address: withdrawn}, {Address: coinbase}}}
	senders := []accounts.Address{accounts.InternAddress(s1), accounts.InternAddress(s2), accounts.InternAddress(s1)}

	addrs, slots := staticKeys(header, body, senders)
	require.Equal(t, sortedAddrs(coinbase, s1, s2, dest, listed, withdrawn), addrs)
	require.Equal(t, []staticSlot{{listed, slotB}, {listed, slotA}, {dest, slotB}}, slots)
}

func TestStaticKeysLeavesSendersOutOnLengthMismatch(t *testing.T) {
	coinbase, sender, dest := common.Address{1}, common.Address{2}, common.Address{3}
	body := &types.Body{Transactions: types.Transactions{legacyTo(&dest), legacyTo(&dest)}}
	addrs, slots := staticKeys(&types.Header{Coinbase: coinbase}, body, []accounts.Address{accounts.InternAddress(sender)})
	require.Equal(t, sortedAddrs(coinbase, dest), addrs)
	require.Empty(t, slots)
}

func TestStaticTouchesAreSortedKeccakPaths(t *testing.T) {
	a, b := common.Address{1}, common.Address{2}
	slot := common.Hash{31: 9}
	touches := staticTouches([]common.Address{a, b}, []staticSlot{{addr: b, slot: slot}})
	require.True(t, slices.IsSortedFunc(touches, ComparePathTouch))
	ha, hb, hs := keccak.Sum256(a[:]), keccak.Sum256(b[:]), keccak.Sum256(slot[:])
	require.Len(t, touches, 3)
	require.Contains(t, touches, PathTouch{Account: ha})
	require.Contains(t, touches, PathTouch{Account: hb})
	require.Contains(t, touches, PathTouch{Account: hb, Slot: hs, Storage: true})
}

func TestWarmStaticWarmsAccountsAndQueuesPaths(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(true))
	f := newPreExecFixture(t, []byte{0x00})
	ctx := t.Context()
	p := NewBranchPrefetcher(ctx, f.db)
	defer p.Close()
	sender := crypto.PubkeyToAddress(f.key.PublicKey)
	body := &types.Body{Transactions: types.Transactions{f.txn(t, 100_000, 1)}}

	require.NoError(t, f.readAheader.warmStatic(ctx, p, f.header(), body, recoveredSenders(nil, sender), 4))
	for _, addr := range []common.Address{sender, f.contract} {
		_, ok := f.stateCache.View(nil).Get(kv.AccountsDomain, addr[:])
		require.True(t, ok, "account %x was not warmed", addr)
	}
	require.Equal(t, 3, p.Stats().Queued)
}

func TestWarmStaticQueuesNothingWhenSenderRecoveryFailed(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(true))
	f := newPreExecFixture(t, []byte{0x00})
	p := NewBranchPrefetcher(t.Context(), f.db)
	defer p.Close()
	body := &types.Body{Transactions: types.Transactions{f.txn(t, 100_000, 1)}}
	_ = f.readAheader.warmStatic(t.Context(), p, f.header(), body, recoveredSenders(errors.New("bad"), common.Address{}), 4)
	require.Zero(t, p.Stats().Queued)
}

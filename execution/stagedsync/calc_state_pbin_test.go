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

package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type calcPBinReader struct {
	values map[string][]byte
}

func (r *calcPBinReader) WithHistory() bool { return false }

func (r *calcPBinReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *calcPBinReader) Read(domain kv.Domain, key []byte, _ uint64) ([]byte, kv.Step, error) {
	return append([]byte(nil), r.values[fmt.Sprintf("%d:%x", domain, key)]...), 0, nil
}

func (r *calcPBinReader) Clone(kv.TemporalTx) commitmentdb.StateReader { return r }

func (r *calcPBinReader) CloneForWorker(context.Context, kv.TemporalTx) commitmentdb.StateReader {
	return r
}

func calcPBinReaderKey(domain kv.Domain, key []byte) string { return fmt.Sprintf("%d:%x", domain, key) }

func calcFeedState(account commitment.PBinFeedAccount) eip8297.State {
	slots := make(map[string][]byte, len(account.Slots))
	for _, slot := range account.Slots {
		slots[string(slot.Key)] = slot.Value
	}
	return eip8297.State{Address: account.Address, Nonce: account.Nonce, Balance: account.Balance, Code: account.Code, Slots: slots, Deleted: !account.Exists || account.Wiped}
}

type calcPBinTrieContext struct {
	records map[string][]byte
}

func (c *calcPBinTrieContext) Branch(key []byte) ([]byte, kv.Step, error) {
	return bytes.Clone(c.records[string(key)]), 0, nil
}

func (c *calcPBinTrieContext) PutBranch(key, data, prev []byte) error {
	if !bytes.Equal(c.records[string(key)], prev) {
		return fmt.Errorf("previous record mismatch for %x", key)
	}
	if len(data) == 0 {
		delete(c.records, string(key))
	} else {
		c.records[string(key)] = bytes.Clone(data)
	}
	return nil
}

func (*calcPBinTrieContext) Account([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected account read")
}

func (*calcPBinTrieContext) Storage([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected storage read")
}

func TestCalcStatePBinFeedFlagsResetWithBlock(t *testing.T) {
	addr := accounts.InternAddress([20]byte{1})
	code := accounts.NewCode([]byte{0x60, 0x00})
	cs := newTestCalcState()
	cs.ApplyWrites(newWS().selfDestruct(addr, state.Version{}, true).code(addr, state.Version{}, code).build(), false)
	require.Contains(t, cs.codeKeys, addr)
	require.Contains(t, cs.wiped, addr)
	cs.ResetBlockFlags()
	require.Empty(t, cs.codeKeys)
	require.Empty(t, cs.wiped)
}

func TestCalcStatePBinFeedUsesFinalAccountAndSlots(t *testing.T) {
	addr := accounts.InternAddress([20]byte{2})
	code := accounts.NewCode([]byte{0x60, 0x00})
	slot := accounts.InternKey(common.Hash{3})
	account := accounts.Account{Nonce: 4, Balance: *uint256.NewInt(8), CodeHash: code.Hash}
	address := addr.Value()
	slotValue := []byte{9}
	key := slot.Value()
	reader := &calcPBinReader{values: map[string][]byte{
		calcPBinReaderKey(kv.AccountsDomain, address[:]):                                           accounts.SerialiseV3(&account),
		calcPBinReaderKey(kv.CodeDomain, address[:]):                                               code.Bytes,
		calcPBinReaderKey(kv.StorageDomain, append(append([]byte(nil), address[:]...), key[:]...)): slotValue,
	}}
	cs := newTestCalcState()
	cs.reader = reader
	cs.ApplyWrites(newWS().bal(addr, state.Version{}, account.Balance).nonce(addr, state.Version{}, account.Nonce).code(addr, state.Version{}, code).stor(addr, slot, state.Version{}, *uint256.NewInt(9)).build(), false)
	feed, err := cs.BinFeed()
	require.NoError(t, err)
	require.Len(t, feed.Accounts, 1)
	got := feed.Accounts[0]
	require.Equal(t, address[:], got.Address)
	require.Equal(t, account.Nonce, got.Nonce)
	require.True(t, got.Balance.Eq(&account.Balance))
	require.Equal(t, common.Hash(account.CodeHash.Value()), got.CodeHash)
	require.Equal(t, code.Bytes, got.Code)
	require.Equal(t, []commitment.PBinFeedSlot{{Key: key[:], Value: slotValue}}, got.Slots)
	wantState := eip8297.State{Address: address[:], Nonce: account.Nonce, Balance: account.Balance, Code: code.Bytes, Slots: map[string][]byte{string(key[:]): slotValue}}
	require.Equal(t,
		eip8297.StateRoot(eip8297.EmbedState([][]eip8297.State{{wantState}})),
		eip8297.StateRoot(eip8297.EmbedState([][]eip8297.State{{calcFeedState(got)}})),
	)
	trieContext := &calcPBinTrieContext{records: make(map[string][]byte)}
	gotRoot, err := pbt.NewTrie(trieContext).ProcessFeed(feed)
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot(eip8297.EmbedState([][]eip8297.State{{wantState}})), gotRoot)
}

func TestCalcStatePBinFeedWipedSurvivesRecreate(t *testing.T) {
	addr := accounts.InternAddress([20]byte{4})
	cs := newTestCalcState()
	cs.ApplyWrites(newWS().selfDestruct(addr, state.Version{}, true).bal(addr, state.Version{}, *uint256.NewInt(1)).build(), false)
	feed := cs.wiped
	require.Contains(t, feed, addr)
	require.False(t, cs.accounts[addr].Deleted)
}

func TestCalcStatePBinFeedBALTracksCodeAndEmptyRemoval(t *testing.T) {
	addr := common.Address{5}
	emptyAddr := common.Address{6}
	code := []byte{0x60, 0x00}
	cs := newTestCalcState()
	cs.LoadFromBAL(types.BlockAccessList{
		{Address: addr, CodeChanges: []*types.CodeChange{{Index: 0, Bytecode: code}}},
		{Address: emptyAddr, BalanceChanges: []*types.BalanceChange{{Index: 0, Value: uint256.Int{}}}},
	}, true, false, false)
	address := accounts.InternAddress(addr)
	emptyAddress := accounts.InternAddress(emptyAddr)
	require.Contains(t, cs.codeKeys, address)
	require.Contains(t, cs.wiped, emptyAddress)
	require.NotEqual(t, empty.CodeHash, cs.accounts[address].CodeHash)
	require.Equal(t, crypto.Keccak256Hash(code), common.Hash(cs.accounts[address].CodeHash))
	codeAccount := accounts.Account{CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code))}
	cs.reader = &calcPBinReader{values: map[string][]byte{
		calcPBinReaderKey(kv.AccountsDomain, addr[:]): accounts.SerialiseV3(&codeAccount),
		calcPBinReaderKey(kv.CodeDomain, addr[:]):     code,
	}}
	feed, err := cs.BinFeed()
	require.NoError(t, err)
	require.Len(t, feed.Accounts, 2)
	want := []eip8297.State{{Address: addr[:], Code: code}, {Address: emptyAddr[:], Deleted: true}}
	got := make([]eip8297.State, len(feed.Accounts))
	for i, account := range feed.Accounts {
		got[i] = calcFeedState(account)
	}
	require.Equal(t, eip8297.StateRoot(eip8297.EmbedState([][]eip8297.State{want})), eip8297.StateRoot(eip8297.EmbedState([][]eip8297.State{got})))
}

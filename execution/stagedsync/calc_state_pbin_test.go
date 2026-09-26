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
	require.Equal(
		t,
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

func TestCalcStatePBinFeedCreateOverStorageWipesStorage(t *testing.T) {
	addr := common.Address{0xc0, 0xff, 0xee}
	address := accounts.InternAddress(addr)
	initial := commitment.PBinFeedAccount{Address: addr[:], Exists: true, Balance: *uint256.NewInt(7)}
	for i := range 192 {
		key := common.Hash{31: byte(i)}
		initial.Slots = append(initial.Slots, commitment.PBinFeedSlot{Key: key[:], Value: []byte{1}})
	}
	ctx := &calcPBinTrieContext{records: make(map[string][]byte)}
	trie := pbt.NewTrie(ctx)
	_, err := trie.ProcessFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{initial}})
	require.NoError(t, err)

	reader := &calcPBinReader{values: map[string][]byte{
		calcPBinReaderKey(kv.AccountsDomain, addr[:]): accounts.SerialiseV3(&accounts.Account{Nonce: 1, Balance: *uint256.NewInt(7)}),
	}}
	cs := newTestCalcState()
	cs.reader = reader
	writes := newWS().createContract(address, state.Version{}, true).nonce(address, state.Version{}, 1).bal(address, state.Version{}, *uint256.NewInt(7)).build()
	cs.ApplyWrites(writes, false)
	require.Contains(t, cs.wiped, address)
	feed, err := cs.BinFeed()
	require.NoError(t, err)
	got, err := pbt.NewTrie(ctx).ProcessFeed(feed)
	require.NoError(t, err)
	want := eip8297.StateRoot(eip8297.EmbedState([][]eip8297.State{
		{{Address: addr[:], Balance: *uint256.NewInt(7), Slots: func() map[string][]byte {
			slots := make(map[string][]byte, 192)
			for i := range 192 {
				key := common.Hash{31: byte(i)}
				slots[string(key[:])] = []byte{1}
			}
			return slots
		}()}},
		{{Address: addr[:], Deleted: true}},
		{{Address: addr[:], Nonce: 1, Balance: *uint256.NewInt(7)}},
	}))
	require.Equal(t, want, got)
}

func TestLoadFromBALPBinFeedGenesisAccountFirstTransactionKeepsStorage(t *testing.T) {
	addr := accounts.InternAddress(common.Address{0xc0, 0xff, 0xee})
	io := state.NewVersionedIO(1)
	io.RecordWrites(state.Version{TxIndex: 0}, newWS().nonce(addr, state.Version{}, 1).bal(addr, state.Version{}, *uint256.NewInt(7)).build())
	bal := io.AsBlockAccessList()
	require.Len(t, bal, 1)
	require.Len(t, bal[0].NonceChanges, 1)
	cs := newTestCalcState()
	cs.domainReader = &preBlockReader{addr: addr, acc: &accounts.Account{Nonce: 0, Balance: *uint256.NewInt(7), Incarnation: 1}}
	cs.LoadFromBAL(bal, true, false, false)
	require.NotContains(t, cs.wiped, addr)
}

func TestLoadFromBALPBinFeedLegacyContractFirstCreateKeepsStorage(t *testing.T) {
	addr := accounts.InternAddress(common.Address{0xc0, 0xff, 0xef})
	io := state.NewVersionedIO(1)
	io.RecordWrites(state.Version{TxIndex: 0}, newWS().nonce(addr, state.Version{}, 1).build())
	bal := io.AsBlockAccessList()
	cs := newTestCalcState()
	cs.domainReader = &preBlockReader{addr: addr, acc: &accounts.Account{Nonce: 0, Balance: *uint256.NewInt(7), Incarnation: 1}}
	cs.LoadFromBAL(bal, true, false, false)
	require.NotContains(t, cs.wiped, addr)
}

func TestPBinFeedSourcesMatchGeneratedCreateOverStorage(t *testing.T) {
	addr := common.Address{0xc0, 0xff, 0xee}
	address := accounts.InternAddress(addr)
	initial := &commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{{Address: addr[:], Exists: true, Balance: *uint256.NewInt(7)}}}
	for i := range 192 {
		key := common.Hash{31: byte(i)}
		initial.Accounts[0].Slots = append(initial.Accounts[0].Slots, commitment.PBinFeedSlot{Key: key[:], Value: []byte{1}})
	}
	finalReader := &calcPBinReader{values: map[string][]byte{
		calcPBinReaderKey(kv.AccountsDomain, addr[:]): accounts.SerialiseV3(&accounts.Account{Nonce: 1, Balance: *uint256.NewInt(7)}),
	}}
	writes := newWS().createContract(address, state.Version{}, true).nonce(address, state.Version{}, 1).bal(address, state.Version{}, *uint256.NewInt(7)).build()
	calcState := newTestCalcState()
	calcState.reader = finalReader
	calcState.ApplyWrites(writes, false)
	calcFeed, err := calcState.BinFeed()
	require.NoError(t, err)

	io := state.NewVersionedIO(1)
	io.RecordWrites(state.Version{TxIndex: 0}, writes)

	sharedFeed, err := commitmentdb.BinFeedFromState(
		map[string]struct{}{string(addr[:]): {}},
		map[string]struct{}{},
		map[string]struct{}{string(addr[:]): {}},
		finalReader,
	)
	require.NoError(t, err)

	process := func(feed *commitment.PBinFeed) (common.Hash, map[string][]byte) {
		ctx := &calcPBinTrieContext{records: make(map[string][]byte)}
		trie := pbt.NewTrie(ctx)
		_, err := trie.ProcessFeed(initial)
		require.NoError(t, err)
		root, err := trie.ProcessFeed(feed)
		require.NoError(t, err)
		require.NoError(t, pbt.NewTrie(ctx).Verify())
		return root, ctx.records
	}

	calcRoot, calcRecords := process(calcFeed)
	sharedRoot, sharedRecords := process(sharedFeed)
	want := eip8297.StateRoot(eip8297.EmbedState([][]eip8297.State{
		{{Address: addr[:], Balance: *uint256.NewInt(7), Slots: func() map[string][]byte {
			slots := make(map[string][]byte, 192)
			for i := range 192 {
				key := common.Hash{31: byte(i)}
				slots[string(key[:])] = []byte{1}
			}
			return slots
		}()}},
		{{Address: addr[:], Deleted: true}},
		{{Address: addr[:], Nonce: 1, Balance: *uint256.NewInt(7)}},
	}))
	require.Equal(t, want, calcRoot)
	require.Equal(t, calcRoot, sharedRoot)
	require.Equal(t, calcRecords, sharedRecords)
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
	require.Empty(t, cs.wiped)
	require.True(t, cs.accounts[emptyAddress].Deleted)
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

func TestPBinFeedSourcesMatchReferenceAcrossBlocks(t *testing.T) {
	address := common.Address{7}
	codeA := []byte{0x60, 0x01}
	codeB := []byte{0x60, 0x02}
	values := []struct {
		code    []byte
		nonce   uint64
		balance uint64
		deleted bool
	}{
		{code: codeA, nonce: 1, balance: 9},
		{deleted: true},
		{code: codeB, nonce: 3, balance: 11},
	}

	makeReader := func(value struct {
		code    []byte
		nonce   uint64
		balance uint64
		deleted bool
	},
	) *calcPBinReader {
		reader := &calcPBinReader{values: make(map[string][]byte)}
		if value.deleted {
			return reader
		}
		code := accounts.NewCode(value.code)
		account := accounts.Account{Nonce: value.nonce, Balance: *uint256.NewInt(value.balance), CodeHash: code.Hash}
		reader.values[calcPBinReaderKey(kv.AccountsDomain, address[:])] = accounts.SerialiseV3(&account)
		reader.values[calcPBinReaderKey(kv.CodeDomain, address[:])] = code.Bytes
		return reader
	}

	makeCalcFeed := func(value struct {
		code    []byte
		nonce   uint64
		balance uint64
		deleted bool
	},
	) *commitment.PBinFeed {
		reader := makeReader(value)
		cs := newTestCalcState()
		cs.reader = reader
		if value.deleted {
			cs.ApplyWrites(newWS().selfDestruct(accounts.InternAddress(address), state.Version{}, true).build(), false)
		} else {
			cs.ApplyWrites(newWS().bal(accounts.InternAddress(address), state.Version{}, *uint256.NewInt(value.balance)).nonce(accounts.InternAddress(address), state.Version{}, value.nonce).code(accounts.InternAddress(address), state.Version{}, accounts.NewCode(value.code)).build(), false)
		}
		feed, err := cs.BinFeed()
		require.NoError(t, err)
		return feed
	}

	makeBALFeed := func(value struct {
		code    []byte
		nonce   uint64
		balance uint64
		deleted bool
	},
	) *commitment.PBinFeed {
		reader := makeReader(value)
		cs := newTestCalcState()
		cs.reader = reader
		changes := types.AccountChanges{Address: address}
		if value.deleted {
			changes.BalanceChanges = []*types.BalanceChange{{Value: uint256.Int{}}}
			changes.NonceChanges = []*types.NonceChange{{Value: 0}}
		} else {
			changes.BalanceChanges = []*types.BalanceChange{{Value: *uint256.NewInt(value.balance)}}
			changes.NonceChanges = []*types.NonceChange{{Value: value.nonce}}
			changes.CodeChanges = []*types.CodeChange{{Bytecode: value.code}}
		}
		cs.LoadFromBAL(types.BlockAccessList{changes}, true, false, false)
		feed, err := cs.BinFeed()
		require.NoError(t, err)
		return feed
	}

	makeSharedFeed := func(value struct {
		code    []byte
		nonce   uint64
		balance uint64
		deleted bool
	},
	) *commitment.PBinFeed {
		reader := makeReader(value)
		keys := map[string]struct{}{string(address[:]): {}}
		codeKeys := make(map[string]struct{})
		if !value.deleted {
			codeKeys[string(address[:])] = struct{}{}
		}
		feed, err := commitmentdb.BinFeedFromState(keys, codeKeys, nil, reader)
		require.NoError(t, err)
		return feed
	}

	process := func(feeds []*commitment.PBinFeed) (common.Hash, map[string][]byte) {
		ctx := &calcPBinTrieContext{records: make(map[string][]byte)}
		trie := pbt.NewTrie(ctx)
		var root common.Hash
		for _, feed := range feeds {
			var err error
			root, err = trie.ProcessFeed(feed)
			require.NoError(t, err)
		}
		require.NoError(t, pbt.NewTrie(ctx).Verify())
		return root, ctx.records
	}

	calcFeeds := make([]*commitment.PBinFeed, 0, len(values))
	balFeeds := make([]*commitment.PBinFeed, 0, len(values))
	sharedFeeds := make([]*commitment.PBinFeed, 0, len(values))
	for _, value := range values {
		calcFeeds = append(calcFeeds, makeCalcFeed(value))
		balFeeds = append(balFeeds, makeBALFeed(value))
		sharedFeeds = append(sharedFeeds, makeSharedFeed(value))
	}
	calcRoot, calcRecords := process(calcFeeds)
	balRoot, balRecords := process(balFeeds)
	sharedRoot, sharedRecords := process(sharedFeeds)
	states := [][]eip8297.State{{{Address: address[:], Nonce: 1, Balance: *uint256.NewInt(9), Code: codeA}}, {{Address: address[:], Deleted: true}}, {{Address: address[:], Nonce: 3, Balance: *uint256.NewInt(11), Code: codeB}}}
	wantRoot := eip8297.StateRoot(eip8297.EmbedState(states))
	require.Equal(t, wantRoot, calcRoot)
	require.Equal(t, wantRoot, balRoot)
	require.Equal(t, wantRoot, sharedRoot)
	require.Equal(t, calcRecords, balRecords)
	require.Equal(t, calcRecords, sharedRecords)
}

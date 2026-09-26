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

package pbt

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestTranslateFeedBuildsBasicData(t *testing.T) {
	address := common.Hex2Bytes("0000000000000000000000000000000000000001")
	feed := &commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{{
		Address: address,
		Exists:  true,
		Nonce:   7,
		Balance: *uint256FromUint64(9),
	}}}

	ops, err := TranslateFeed(feed)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	require.Equal(t, eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), ops[0].Key)
	require.Equal(t, eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey), ops[1].Key)
}

func TestTranslateFeedRejectsNil(t *testing.T) {
	_, err := TranslateFeed(nil)
	require.ErrorContains(t, err, "nil feed")
}

func TestTranslateFeedRejectsShortAddress(t *testing.T) {
	account := feedAccount([]byte{1})
	_, err := TranslateFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{account}})
	require.ErrorContains(t, err, "address has length")
}

func TestTranslateFeedRejectsOversizedSlot(t *testing.T) {
	account := feedAccount(bytes.Repeat([]byte{1}, 20))
	account.Slots = []commitment.PBinFeedSlot{{Key: make([]byte, 33)}}
	var err error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("panic: %v", recovered)
			}
		}()
		_, err = TranslateFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{account}})
	}()
	require.ErrorContains(t, err, "slot key or value is too long")
}

func TestTranslateFeedRejectsDuplicateAccount(t *testing.T) {
	account := feedAccount(bytes.Repeat([]byte{1}, 20))
	_, err := TranslateFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{account, account}})
	require.ErrorContains(t, err, "duplicate operation key")
}

func uint256FromUint64(value uint64) *uint256.Int {
	return new(uint256.Int).SetUint64(value)
}

func feedAccount(address []byte) commitment.PBinFeedAccount {
	return commitment.PBinFeedAccount{Address: bytes.Clone(address), Exists: true}
}

func feedCode(code []byte) common.Hash {
	return common.Hash(keccak.Sum256(code))
}

func feedState(account commitment.PBinFeedAccount) eip8297.State {
	state := eip8297.State{
		Address: bytes.Clone(account.Address),
		Nonce:   account.Nonce,
		Balance: account.Balance,
		Code:    bytes.Clone(account.Code),
		Slots:   make(map[string][]byte),
		Deleted: !account.Exists,
	}
	for _, slot := range account.Slots {
		state.Slots[string(slot.Key)] = bytes.Clone(slot.Value)
	}
	return state
}

func entriesOps(entries []eip8297.Entry) []Op {
	entries = append([]eip8297.Entry(nil), entries...)
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	ops := make([]Op, len(entries))
	for i, entry := range entries {
		var value [eip8297.ValueLength]byte
		copy(value[:], entry.Value)
		ops[i] = Op{Key: bytes.Clone(entry.Key), Value: value}
	}
	return ops
}

func assertFeedState(t *testing.T, feeds []commitment.PBinFeed, states [][]eip8297.State) {
	t.Helper()
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	for _, feed := range feeds {
		_, err := trie.ProcessFeed(&feed)
		require.NoError(t, err)
	}
	entries := eip8297.EmbedState(states)
	wantRoot := eip8297.StateRoot(entries)
	gotRoot, err := NewTrie(ctx).Process(nil)
	require.NoError(t, err)
	require.Equal(t, wantRoot, gotRoot)
	fresh := newTrieTestContext()
	freshRoot, err := NewTrie(fresh).Process(entriesOps(entries))
	require.NoError(t, err)
	require.Equal(t, wantRoot, freshRoot)
	require.Equal(t, fresh.records, ctx.records)
	require.NoError(t, NewTrie(ctx).Verify())
}

func TestProcessFeedCodelessAccountMatchesReference(t *testing.T) {
	address := common.Hex2Bytes("0000000000000000000000000000000000000001")
	account := feedAccount(address)
	account.Nonce = 7
	account.Balance = *uint256.NewInt(9)
	assertFeedState(t, []commitment.PBinFeed{{Accounts: []commitment.PBinFeedAccount{account}}}, [][]eip8297.State{{feedState(account)}})
}

func TestProcessFeedKeepsCodeSizeFromExistingBasicData(t *testing.T) {
	address := common.Hex2Bytes("0000000000000000000000000000000000000002")
	code := []byte{0x60, 0x01, 0x60, 0x00}
	first := feedAccount(address)
	first.CodeWritten = true
	first.Code = code
	first.CodeHash = feedCode(code)
	second := feedAccount(address)
	second.Nonce = 2
	second.CodeHash = first.CodeHash
	secondState := feedState(second)
	secondState.Code = bytes.Clone(code)
	assertFeedState(t, []commitment.PBinFeed{{Accounts: []commitment.PBinFeedAccount{first}}, {Accounts: []commitment.PBinFeedAccount{second}}}, [][]eip8297.State{{feedState(first)}, {secondState}})
}

func TestProcessFeedRejectsMissingCodeSize(t *testing.T) {
	address := common.Hex2Bytes("0000000000000000000000000000000000000003")
	account := feedAccount(address)
	account.CodeHash = common.Hash{1}
	_, err := NewTrie(newTrieTestContext()).ProcessFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{account}})
	require.ErrorContains(t, err, "code size unavailable")
}

func TestProcessFeedDroppedBasicDataDoesNotProvideCodeSize(t *testing.T) {
	address := common.Hex2Bytes("0000000000000000000000000000000000000004")
	code := []byte{0x60, 0x01, 0x60, 0x00}
	first := feedAccount(address)
	first.CodeWritten = true
	first.Code = code
	first.CodeHash = feedCode(code)
	require.NoError(t, func() error {
		trie := NewTrie(newTrieTestContext())
		_, err := trie.ProcessFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{first}})
		return err
	}())
	second := feedAccount(address)
	second.Wiped = true
	second.CodeHash = first.CodeHash
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	_, err := trie.ProcessFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{first}})
	require.NoError(t, err)
	_, err = trie.ProcessFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{second}})
	require.ErrorContains(t, err, "code size unavailable")
}

func TestTranslateFeedDeduplicatesSharedCodeChunks(t *testing.T) {
	code := bytes.Repeat([]byte{0x60, 0x01}, 20)
	hash := feedCode(code)
	a := feedAccount(bytes.Repeat([]byte{1}, 20))
	a.CodeWritten, a.Code, a.CodeHash = true, code, hash
	b := feedAccount(bytes.Repeat([]byte{2}, 20))
	b.CodeWritten, b.Code, b.CodeHash = true, code, hash
	ops, err := TranslateFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{b, a}})
	require.NoError(t, err)
	chunkKeys := make(map[string]struct{})
	for _, op := range ops {
		if len(op.Key) == eip8297.CodeKeyLength && op.Key[0] == eip8297.CodeZone {
			chunkKeys[string(op.Key)] = struct{}{}
		}
	}
	require.Len(t, chunkKeys, len(eip8297.ChunkifyCode(code)))
	assertFeedState(t, []commitment.PBinFeed{{Accounts: []commitment.PBinFeedAccount{a, b}}}, [][]eip8297.State{{feedState(a), feedState(b)}})
}

func TestProcessFeedDelegationAndClear(t *testing.T) {
	address := common.Hex2Bytes("0000000000000000000000000000000000000005")
	delegation := append([]byte{0xef, 0x01, 0x00}, bytes.Repeat([]byte{0x09}, 20)...)
	set := feedAccount(address)
	set.CodeWritten, set.Code = true, delegation
	clear := feedAccount(address)
	clear.CodeWritten = true
	clear.Code = []byte{}
	clear.CodeHash = empty.CodeHash
	assertFeedState(t, []commitment.PBinFeed{{Accounts: []commitment.PBinFeedAccount{set}}, {Accounts: []commitment.PBinFeedAccount{clear}}}, [][]eip8297.State{{feedState(set)}, {feedState(clear)}})
}

func TestTranslateFeedSortsDropsBeforeWrites(t *testing.T) {
	address := common.Hex2Bytes("0000000000000000000000000000000000000006")
	account := feedAccount(address)
	account.Wiped = true
	account.Nonce = 1
	account.Slots = []commitment.PBinFeedSlot{{Key: []byte{64}, Value: []byte{3}}}
	ops, err := TranslateFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{account}})
	require.NoError(t, err)
	for i := 1; i < len(ops); i++ {
		previous := ops[i-1].Key
		current := ops[i].Key
		if len(ops[i-1].Drop) != 0 {
			previous = ops[i-1].Drop
		}
		if len(ops[i].Drop) != 0 {
			current = ops[i].Drop
		}
		require.Less(t, bytes.Compare(previous, current), 0)
	}
	require.Equal(t, new(eip8297.DigestCache).AccountHeaderStem(address), ops[0].Drop)
}

func TestProcessFeedRoutesHeaderAndOverflowSlots(t *testing.T) {
	address := common.Hex2Bytes("0000000000000000000000000000000000000007")
	account := feedAccount(address)
	account.Nonce = 1
	account.Slots = []commitment.PBinFeedSlot{
		{Key: []byte{63}, Value: []byte{1}},
		{Key: []byte{64}, Value: []byte{2}},
		{Key: []byte{65}, Value: make([]byte, 32)},
	}
	ops, err := TranslateFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{account}})
	require.NoError(t, err)
	keys := make(map[string]Op)
	for _, op := range ops {
		if len(op.Key) != 0 {
			keys[string(op.Key)] = op
		}
	}
	require.Contains(t, keys, string(eip8297.TreeKeyAccount(address, eip8297.HeaderStorageOffset+63)))
	require.Contains(t, keys, string(eip8297.TreeKeyStorage(address, []byte{64})))
	require.Contains(t, keys, string(eip8297.TreeKeyStorage(address, []byte{65})))
	require.Equal(t, [eip8297.ValueLength]byte{}, keys[string(eip8297.TreeKeyStorage(address, []byte{65}))].Value)
}

func TestProcessFeedWipeAndRewriteMatchesReference(t *testing.T) {
	address := common.Hex2Bytes("0000000000000000000000000000000000000008")
	code := []byte{0x60, 0x01, 0x60, 0x00}
	first := feedAccount(address)
	first.Nonce = 1
	first.CodeWritten, first.Code, first.CodeHash = true, code, feedCode(code)
	first.Slots = []commitment.PBinFeedSlot{{Key: []byte{64}, Value: []byte{9}}}
	second := feedAccount(address)
	second.Wiped = true
	second.Nonce = 2
	second.CodeWritten = true
	second.Code = []byte{0x60, 0x02}
	second.CodeHash = feedCode(second.Code)
	second.Slots = []commitment.PBinFeedSlot{{Key: []byte{64}, Value: []byte{8}}}
	firstState := feedState(first)
	secondState := feedState(second)
	assertFeedState(t, []commitment.PBinFeed{{Accounts: []commitment.PBinFeedAccount{first}}, {Accounts: []commitment.PBinFeedAccount{second}}}, [][]eip8297.State{{firstState}, {secondState}})
}

func TestProcessFeedAbsentDropsOnly(t *testing.T) {
	address := common.Hex2Bytes("0000000000000000000000000000000000000009")
	first := feedAccount(address)
	first.Nonce = 1
	first.Slots = []commitment.PBinFeedSlot{{Key: []byte{64}, Value: []byte{4}}}
	gone := commitment.PBinFeedAccount{Address: address, Wiped: true}
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	_, err := trie.ProcessFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{first}})
	require.NoError(t, err)
	_, err = trie.ProcessFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{gone}})
	require.NoError(t, err)
	require.Empty(t, ctx.records)
	assertFeedState(t, []commitment.PBinFeed{{Accounts: []commitment.PBinFeedAccount{first}}, {Accounts: []commitment.PBinFeedAccount{gone}}}, [][]eip8297.State{{feedState(first)}, {feedState(gone)}})
}

func TestProcessFeedEmptyCodeWriteLeavesCodeHash(t *testing.T) {
	address := common.Hex2Bytes("000000000000000000000000000000000000000a")
	account := feedAccount(address)
	account.CodeWritten = true
	account.Code = []byte{}
	account.CodeHash = empty.CodeHash
	ops, err := TranslateFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{account}})
	require.NoError(t, err)
	codeHashKey := string(eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey))
	var found bool
	for _, op := range ops {
		if string(op.Key) == codeHashKey {
			found = true
			require.Equal(t, [eip8297.ValueLength]byte(empty.CodeHash), op.Value)
		}
	}
	require.True(t, found)
	assertFeedState(t, []commitment.PBinFeed{{Accounts: []commitment.PBinFeedAccount{account}}}, [][]eip8297.State{{feedState(account)}})
}

func TestTranslateFeedRejectsCodeHashMismatch(t *testing.T) {
	address := common.Hex2Bytes("000000000000000000000000000000000000000b")
	account := feedAccount(address)
	account.CodeWritten = true
	account.Code = []byte{1}
	account.CodeHash = common.Hash{2}
	_, err := TranslateFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{account}})
	require.ErrorContains(t, err, "code hash does not match code")
}

func TestTranslateFeedRejectsDelegationCodeHash(t *testing.T) {
	account := feedAccount(bytes.Repeat([]byte{2}, 20))
	account.CodeWritten = true
	account.Code = append([]byte{0xef, 0x01, 0x00}, bytes.Repeat([]byte{0x08}, 20)...)
	account.CodeHash = common.Hash{1}
	_, err := TranslateFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{account}})
	require.ErrorContains(t, err, "delegation has a non-empty code hash")
}

func TestProcessFeedZeroCodeChunkIsDeleted(t *testing.T) {
	address := common.Hex2Bytes("000000000000000000000000000000000000000c")
	account := feedAccount(address)
	account.CodeWritten = true
	account.Code = make([]byte, eip8297.ChunkDataLen)
	account.CodeHash = feedCode(account.Code)
	assertFeedState(t, []commitment.PBinFeed{{Accounts: []commitment.PBinFeedAccount{account}}}, [][]eip8297.State{{feedState(account)}})
	ops, err := TranslateFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{account}})
	require.NoError(t, err)
	for _, op := range ops {
		if len(op.Key) == eip8297.CodeKeyLength && op.Key[0] == eip8297.CodeZone {
			require.Equal(t, [eip8297.ValueLength]byte{}, op.Value)
		}
	}
}

func TestTranslateFeedSharesDelegationWithoutChunks(t *testing.T) {
	delegation := append([]byte{0xef, 0x01, 0x00}, bytes.Repeat([]byte{0x07}, 20)...)
	first := feedAccount(bytes.Repeat([]byte{3}, 20))
	first.CodeWritten, first.Code = true, delegation
	second := feedAccount(bytes.Repeat([]byte{4}, 20))
	second.CodeWritten, second.Code = true, delegation
	ops, err := TranslateFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{first, second}})
	require.NoError(t, err)
	for _, op := range ops {
		if len(op.Key) != 0 {
			require.NotEqual(t, eip8297.CodeZone, op.Key[0])
		}
	}
	assertFeedState(t, []commitment.PBinFeed{{Accounts: []commitment.PBinFeedAccount{first, second}}}, [][]eip8297.State{{feedState(first), feedState(second)}})
}

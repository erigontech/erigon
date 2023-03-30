// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie_test

import (
	"context"
	"fmt"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/integrity"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

func TestIHCursor(t *testing.T) {
	db, tx := memdb.NewTestTx(t)
	require := require.New(t)
	hash := libcommon.HexToHash(fmt.Sprintf("%064d", 0))

	newV := make([]byte, 0, 1024)
	put := func(ks string, hasState, hasTree, hasHash uint16, hashes []libcommon.Hash) {
		k := common.FromHex(ks)
		integrity.AssertSubset(k, hasTree, hasState)
		integrity.AssertSubset(k, hasHash, hasState)
		_ = tx.Put(kv.TrieOfAccounts, k, common.CopyBytes(trie.MarshalTrieNodeTyped(hasState, hasTree, hasHash, hashes, newV)))
	}

	put("00", 0b0000000000000010, 0b0000000000000000, 0b0000000000000010, []libcommon.Hash{hash})
	put("01", 0b0000000000000111, 0b0000000000000010, 0b0000000000000111, []libcommon.Hash{hash, hash, hash})
	put("0101", 0b0000000000000111, 0b0000000000000000, 0b0000000000000111, []libcommon.Hash{hash, hash, hash})
	put("02", 0b1000000000000000, 0b0000000000000000, 0b1000000000000000, []libcommon.Hash{hash})
	put("03", 0b0000000000000001, 0b0000000000000001, 0b0000000000000000, []libcommon.Hash{})
	put("030000", 0b0000000000000001, 0b0000000000000000, 0b0000000000000001, []libcommon.Hash{hash})
	put("03000e", 0b0000000000000001, 0b0000000000000001, 0b0000000000000001, []libcommon.Hash{hash})
	put("03000e000000", 0b0000000000000100, 0b0000000000000000, 0b0000000000000100, []libcommon.Hash{hash})
	put("03000e00000e", 0b0000000000000100, 0b0000000000000000, 0b0000000000000100, []libcommon.Hash{hash})
	put("05", 0b0000000000000001, 0b0000000000000001, 0b0000000000000001, []libcommon.Hash{hash})
	put("050001", 0b0000000000000001, 0b0000000000000000, 0b0000000000000001, []libcommon.Hash{hash})
	put("05000f", 0b0000000000000001, 0b0000000000000000, 0b0000000000000001, []libcommon.Hash{hash})
	put("06", 0b0000000000000001, 0b0000000000000000, 0b0000000000000001, []libcommon.Hash{hash})

	integrity.Trie(db, tx, false, context.Background())

	cursor, err := tx.Cursor(kv.TrieOfAccounts)
	require.NoError(err)
	rl := trie.NewRetainList(0)
	rl.AddHex(common.FromHex("01"))
	rl.AddHex(common.FromHex("0101"))
	rl.AddHex(common.FromHex("030000"))
	rl.AddHex(common.FromHex("03000e"))
	rl.AddHex(common.FromHex("03000e00"))
	rl.AddHex(common.FromHex("0500"))
	var canUse = func(prefix []byte) (bool, []byte) {
		retain, nextCreated := rl.RetainWithMarker(prefix)
		return !retain, nextCreated
	}
	ih := trie.AccTrie(canUse, func(keyHex []byte, _, _, _ uint16, hashes, rootHash []byte) error {
		return nil
	}, cursor, nil)
	k, _, _, _ := ih.AtPrefix([]byte{})
	require.Equal(common.FromHex("0001"), k)
	require.False(ih.SkipState)
	require.Equal([]byte{}, ih.FirstNotCoveredPrefix())
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("0100"), k)
	require.True(ih.SkipState)
	require.Equal(common.FromHex("02"), ih.FirstNotCoveredPrefix())
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("010100"), k)
	require.True(ih.SkipState)
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("010101"), k)
	require.True(ih.SkipState)
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("010102"), k)
	require.True(ih.SkipState)
	require.Equal(common.FromHex("1120"), ih.FirstNotCoveredPrefix())
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("0102"), k)
	require.True(ih.SkipState)
	require.Equal(common.FromHex("1130"), ih.FirstNotCoveredPrefix())
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("020f"), k)
	require.True(ih.SkipState)
	require.Equal(common.FromHex("13"), ih.FirstNotCoveredPrefix())
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("03000000"), k)
	require.True(ih.SkipState)
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("03000e00000002"), k)
	require.True(ih.SkipState)
	require.Equal(common.FromHex("3001"), ih.FirstNotCoveredPrefix())
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("03000e00000e02"), k)
	require.True(ih.SkipState)
	require.Equal(common.FromHex("30e00030"), ih.FirstNotCoveredPrefix())
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("05000100"), k)
	require.True(ih.SkipState)
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("05000f00"), k)
	require.True(ih.SkipState)
	k, _, _, _ = ih.Next()
	require.Equal(common.FromHex("0600"), k)
	require.True(ih.SkipState)
	k, _, _, _ = ih.Next()
	assert.Nil(t, k)

	//cursorS := tx.Cursor(dbutils.TrieOfStorage)
	//ihStorage := AccTrie(canUse, cursorS)
	//
	//k, _, _ = ihStorage.SeekToAccount(common.FromHex(acc))
	//require.Equal(common.FromHex(acc+"00"), k)
	//require.True(isDenseSequence(ihStorage.prev, k))
	//k, _, _ = ihStorage.Next()
	//require.Equal(common.FromHex(acc+"02"), k)
	//require.False(isDenseSequence(ihStorage.prev, k))
	//k, _, _ = ihStorage.Next()
	//assert.Nil(t, k)
	//require.False(isDenseSequence(ihStorage.prev, k))

}

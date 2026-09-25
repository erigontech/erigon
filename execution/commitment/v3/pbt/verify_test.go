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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestTrieVerifyRejectsStaleBranchHash(t *testing.T) {
	ctx := newTrieTestContext()
	a := trieCodeKey(0, 0, 1)
	b := trieCodeKey(0, 2, 2)
	storage := eip8297.TreeKeyStorage(bytes.Repeat([]byte{1}, 20), storageSlotKey())
	trie := NewTrie(ctx)
	_, err := trie.Process([]Op{{Key: a, Value: testTrieValue(1)}, {Key: b, Value: testTrieValue(2)}})
	require.NoError(t, err)
	trie = NewTrie(ctx)
	_, err = trie.Process([]Op{{Key: storage, Value: eip8297.EncodeStorageValue([]byte{9})}})
	require.NoError(t, err)

	record, err := DecodeRecord(GlobalRootKey(), ctx.records[string(GlobalRootKey())])
	require.NoError(t, err)
	record.Cells[0].Left[0]++
	data, err := EncodeRecord(GlobalRootKey(), &record)
	require.NoError(t, err)
	ctx.records[string(GlobalRootKey())] = data

	require.Error(t, NewTrie(ctx).Verify())
}

func TestTrieVerifyRejectsWrongRootSelfExtensionBits(t *testing.T) {
	ctx := newTrieTestContext()
	a := trieCodeKey(0, 0, 1)
	b := trieCodeKey(0, 1, 2)
	_, err := NewTrie(ctx).Process([]Op{{Key: a, Value: testTrieValue(1)}, {Key: b, Value: testTrieValue(2)}})
	require.NoError(t, err)

	record, err := DecodeRecord(GlobalRootKey(), ctx.records[string(GlobalRootKey())])
	require.NoError(t, err)
	require.Equal(t, ExtRoot, record.Form)
	record.SelfExt.SetBitAt(20, record.SelfExt.Bit(20)^1)
	data, err := EncodeRecord(GlobalRootKey(), &record)
	require.NoError(t, err)
	ctx.records[string(GlobalRootKey())] = data

	require.Error(t, NewTrie(ctx).Verify())
}

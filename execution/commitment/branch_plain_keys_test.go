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

package commitment

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// branchOfCells builds a BranchData whose touched/after maps cover len(cells) positions. Each cell
// is a fieldBits byte followed by uvarint(len)+key for every key it carries.
func branchOfCells(t *testing.T, cells ...[]byte) BranchData {
	t.Helper()
	require.LessOrEqual(t, len(cells), 16)
	bitmap := uint16(1)<<len(cells) - 1
	var header [4]byte
	binary.BigEndian.PutUint16(header[0:], bitmap)
	binary.BigEndian.PutUint16(header[2:], bitmap)
	b := header[:]
	for _, c := range cells {
		b = append(b, c...)
	}
	return b
}

func plainAccountStorageCell() []byte {
	return cellOf(0x06, make([]byte, length.Addr), make([]byte, length.Addr+length.Hash))
}

func shortenedAccountCell() []byte {
	return cellOf(0x02, []byte{0x81, 0x02})
}

func cellOf(fieldBits byte, keys ...[]byte) []byte {
	b := []byte{fieldBits}
	var n [binary.MaxVarintLen64]byte
	for _, k := range keys {
		c := binary.PutUvarint(n[:], uint64(len(k)))
		b = append(b, n[:c]...)
		b = append(b, k...)
	}
	return b
}

// TestCountPlainKeysWalksPastAShortenedKey pins the whole-branch walk. Aborting at the first
// shortened key undercounts every kind at once: the plain keys behind it go unseen and the
// shortened ones are never tallied, so neither number can be read as a total.
func TestCountPlainKeysWalksPastAShortenedKey(t *testing.T) {
	t.Parallel()

	t.Run("plain keys on both sides of a shortened one", func(t *testing.T) {
		branch := branchOfCells(t, plainAccountStorageCell(), shortenedAccountCell(), plainAccountStorageCell())

		plainAccounts, plainStorages, shortened, err := branch.CountPlainKeys()
		require.NoError(t, err)
		require.Equal(t, uint64(2), plainAccounts)
		require.Equal(t, uint64(2), plainStorages)
		require.Equal(t, uint64(1), shortened)
		require.True(t, branch.HasShortenedKeys())
	})

	t.Run("a fully plain branch reports no shortened keys", func(t *testing.T) {
		branch := branchOfCells(t, plainAccountStorageCell(), plainAccountStorageCell())

		plainAccounts, plainStorages, shortened, err := branch.CountPlainKeys()
		require.NoError(t, err)
		require.Equal(t, uint64(2), plainAccounts)
		require.Equal(t, uint64(2), plainStorages)
		require.Zero(t, shortened)
		require.False(t, branch.HasShortenedKeys())
	})

	// A cell header that runs off the end was the one unchecked index in
	// ReplacePlainKeys. It only became reachable once the count stopped returning
	// at the first shortened key: before that, a referencing branch aborted on
	// cell 0 and never advanced into the overrun.
	t.Run("a branch ending on a cell boundary errors instead of panicking", func(t *testing.T) {
		var branch BranchData = []byte{0, 3, 0, 3, 0x00} // two cells advertised, one fieldBits byte

		require.NotPanics(t, func() {
			_, _, _, err := branch.CountPlainKeys()
			require.ErrorContains(t, err, "buffer too small for cell fields")
		})
		require.True(t, branch.HasShortenedKeys(), "an unparseable branch must never read as plain")
	})

	t.Run("a truncated branch errors and still reads as referencing", func(t *testing.T) {
		branch := branchOfCells(t, plainAccountStorageCell())
		truncated := branch[:len(branch)-8]

		_, _, _, err := truncated.CountPlainKeys()
		require.Error(t, err)
		require.True(t, truncated.HasShortenedKeys(), "an unparseable branch must never read as plain")
	})
}

func TestBranchData_AccountLeafStorageKey(t *testing.T) {
	account := make([]byte, length.Addr)
	slot := bytes.Repeat([]byte{0xbb}, length.Addr+length.Hash)
	shortened := []byte{0x81, 0x02}
	branch := branchOfCells(t,
		cellOf(0x08, make([]byte, length.Hash)),
		cellOf(0x02, account),
		cellOf(0x1e, account, slot, make([]byte, length.Hash), make([]byte, length.Hash)),
		cellOf(0x06, account, shortened),
	)

	tests := []struct {
		nibble    int
		isAccount bool
		storage   []byte
	}{
		{nibble: 0, isAccount: false},
		{nibble: 1, isAccount: true},
		{nibble: 2, isAccount: true, storage: slot},
		{nibble: 3, isAccount: true, storage: shortened},
		{nibble: 4, isAccount: false},
	}
	for _, tt := range tests {
		isAccount, storage, err := branch.AccountLeafStorageKey(tt.nibble)
		require.NoError(t, err)
		require.Equal(t, tt.isAccount, isAccount, "nibble %d", tt.nibble)
		require.Equal(t, tt.storage, storage, "nibble %d", tt.nibble)
	}

	isAccount, storage, err := BranchData(nil).AccountLeafStorageKey(0)
	require.NoError(t, err)
	require.False(t, isAccount)
	require.Nil(t, storage)

	_, _, err = branch[:5].AccountLeafStorageKey(1)
	require.Error(t, err, "a truncated cell is reported")
}

type storageReadRecorder struct {
	*MockState
	reads [][]byte
}

func (r *storageReadRecorder) Storage(plainKey []byte) (*Update, error) {
	r.reads = append(r.reads, bytes.Clone(plainKey))
	return r.MockState.Storage(plainKey)
}

func TestFoldReadsTheSlotKeptInAnAccountLeafWhenOnlyTheAccountChanges(t *testing.T) {
	ms := &storageReadRecorder{MockState: NewMockState(t)}
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	address := fmt.Sprintf("%040x", 4)
	builder := NewUpdateBuilder().Storage(address, fmt.Sprintf("%064x", 1), "0401")
	for i := range 32 {
		builder = builder.Balance(fmt.Sprintf("%040x", i), uint64(i)+1)
	}
	plainKeys, updates := builder.Build()
	var slot []byte
	for _, key := range plainKeys {
		if len(key) == length.Addr+length.Hash {
			slot = key
		}
	}
	require.NotNil(t, slot)
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()
	_, err := hph.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	hashed := KeyToHexNibbleHash(common.FromHex(address))
	var leafSlot []byte
	for depth := 0; depth < len(hashed); depth++ {
		isAccount, key, err := BranchData(ms.cm[string(nibbles.HexToCompactInto(nil, hashed[:depth]))]).AccountLeafStorageKey(int(hashed[depth]))
		require.NoError(t, err)
		if isAccount {
			leafSlot = key
			break
		}
	}
	require.Equal(t, slot, leafSlot, "the account leaf keeps the key of its only slot")

	hph.Reset()
	ms.reads = nil
	plainKeys, updates = NewUpdateBuilder().Balance(address, 99).Build()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	WrapKeyUpdatesInto(t, upds, plainKeys, updates)
	_, err = hph.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, [][]byte{slot}, ms.reads, "the fold reads the slot of a changed account")
}

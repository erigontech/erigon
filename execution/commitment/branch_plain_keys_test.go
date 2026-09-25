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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
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

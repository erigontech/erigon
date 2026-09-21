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

package v4

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	require.NoError(t, err)
	return decoded
}

func TestLeafRefVectors(t *testing.T) {
	key := nibbles.HexToCompact([]byte{1, 2})
	payload := bytes.Repeat([]byte{0x11}, 32)
	got := leafRef(planeStorage, key, payload, nil)
	require.Equal(t, mustDecodeHex(t, "e2d0b037b975d0e18dfaad291e1c147eeab6f0547d7056bdf3ea0b4fbeeca973"), got)

	inline := leafRef(planeStorage, key, []byte{1}, nil)
	require.Equal(t, []byte{0xc4, 0x82, 0x00, 0x12, 0x01}, inline)
	accountPayload := []byte{0xc0}
	accountInline := leafRef(planeAccount, key, accountPayload, nil)
	require.Equal(t, []byte{0xc4, 0x82, 0x00, 0x12, 0xc0}, accountInline)
}

func TestLeafRefHashesAtBoundary(t *testing.T) {
	key := nibbles.HexToCompact([]byte{1, 2})
	below := leafRef(planeStorage, key, bytes.Repeat([]byte{0x22}, 26), nil)
	at := leafRef(planeStorage, key, bytes.Repeat([]byte{0x22}, 27), nil)
	require.Less(t, len(below), 32)
	require.Len(t, at, 32)
}

func TestExtensionRefVector(t *testing.T) {
	child := mustDecodeHex(t, "c7c2cc5c8eba62b7ff0b998ee46c6cc41075ee8d2dbefa0fa4b26bdb2530f272")
	got := extensionRef([]byte{1, 2}, child)
	require.Equal(t, mustDecodeHex(t, "c5ab940b6ad7c39b31afef79e182f2d568c79617ad4bca225ceb3e9d1af4cd1d"), got[:])
}

func TestBranchRefVector(t *testing.T) {
	var refs [16][]byte
	refs[1] = bytes.Repeat([]byte{0x11}, 32)
	refs[14] = bytes.Repeat([]byte{0x22}, 32)
	got := branchRef(&refs, 3)
	require.Equal(t, mustDecodeHex(t, "c7c2cc5c8eba62b7ff0b998ee46c6cc41075ee8d2dbefa0fa4b26bdb2530f272"), got[:])
}

func TestBranchRefRejectsInlinableBranch(t *testing.T) {
	var refs [16][]byte
	refs[0] = []byte{0xc1, 0x01}
	refs[1] = []byte{0xc1, 0x02}
	require.PanicsWithValue(t, "commitment v4: inlinable branch child at depth 56", func() {
		branchRef(&refs, 56)
	})
}

func TestBranchRefAllowsOrdinaryBranches(t *testing.T) {
	for childCount := 2; childCount <= 16; childCount++ {
		t.Run(string(rune('a'+childCount)), func(t *testing.T) {
			var refs [16][]byte
			for i := range childCount {
				refs[i] = bytes.Repeat([]byte{byte(i + 1)}, 32)
			}
			require.NotPanics(t, func() { branchRef(&refs, 3) })
		})
	}
}

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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordHasherExpectsEveryExtensionChild(t *testing.T) {
	n := fork(nil)
	n.plane = planeAccount
	n.setStoredChild(1, bytes.Repeat([]byte{0x11}, 32), []byte{2, 3})
	n.setStoredChild(4, bytes.Repeat([]byte{0x44}, 32), []byte{5, 6, 7})
	record := encodeRecord(n, 0, nil)
	want, err := fold(n, 0)
	require.NoError(t, err)

	var keys [][]byte
	got, err := NewRecordHasher().Hash(AccountRootKey(), record, func(key, _ []byte) error {
		keys = append(keys, bytes.Clone(key))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, [][]byte{AccountNodeKey([]byte{1, 2, 3}, nil), AccountNodeKey([]byte{4, 5, 6, 7}, nil)}, keys)
}
